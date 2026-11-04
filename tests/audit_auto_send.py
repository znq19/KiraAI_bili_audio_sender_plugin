"""音频条 v1.2.0 离线回归（不依赖 KiraAI 本体，不联网）。

覆盖：
  A. 链接钩子非阻塞（0 await）+ 检测即登记 pending
  B. 文字链接 / b23 短链 / QQ 小程序卡片 都能触发
  C. 标注状态机：pending 当轮必中、成功静默收尾、失败下一轮补原因
  D. 空白归一化匹配、message_id 匹配
  E. TTL、自身消息防御、terminate 取消后台任务
  F. QQ 官方无 bridge 明确报错（不发占位文本）
  G. info 缓存：重复发送不再请求 get_info
  H. 绝对 cache_dir 不再因 relative_to 抛异常

跑法：cd <插件目录> && python3 tests/audit_auto_send.py
"""
import asyncio
import json
import os
import sys
import tempfile
import time
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name}" + (f"  [{extra}]" if extra and not cond else ""))


# ── 桩掉 core 包 ──
def install_stubs(data_dir):
    core = types.ModuleType("core")
    plugin = types.ModuleType("core.plugin")

    class _P:
        LOW, MEDIUM, HIGH = -50, 0, 50

    class _Reg:
        def __getattr__(self, name):
            def deco(*a, **k):
                if len(a) == 1 and callable(a[0]) and not k:
                    return a[0]
                return lambda f: f
            return deco

    plugin.BasePlugin = object
    plugin.Priority = _P
    plugin.on = _Reg()

    logmod = types.ModuleType("core.logging_manager")
    logmod.get_logger = lambda *a, **k: types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None)

    tool_utils = types.ModuleType("core.utils.tool_utils")

    class BaseTool:
        def __init__(self, *a, ctx=None, **k):
            self.ctx = ctx

    tool_utils.BaseTool = BaseTool

    provider = types.ModuleType("core.provider")
    provider.LLMRequest = type("LLMRequest", (), {})

    chat = types.ModuleType("core.chat")
    chat.MessageChain = lambda items=None: list(items or [])
    msg_utils = types.ModuleType("core.chat.message_utils")
    msg_utils.KiraMessageEvent = type("KiraMessageEvent", (), {})
    msg_utils.KiraMessageBatchEvent = type("KiraMessageBatchEvent", (), {})
    chat.message_utils = msg_utils

    elements = types.ModuleType("core.chat.message_elements")

    class Text:
        def __init__(self, text=""):
            self.text = text

    class Record:
        def __init__(self, record=None, mime=None, name=None, transcript=None):
            self.record = record
            self.name = name

    elements.Text = Text
    elements.Record = Record
    chat.message_elements = elements

    path_utils = types.ModuleType("core.utils.path_utils")
    path_utils.get_data_path = lambda: __import__("pathlib").Path(data_dir)

    prompt_manager = types.ModuleType("core.prompt_manager")

    class Prompt:
        def __init__(self, content="", name=None, source=None, end="\n", persist=True, **kw):
            self.content, self.name, self.source = content, name, source
            self.end, self.persist = end, persist

        def to_string(self):
            return (self.content or "") + (self.end or "")

    prompt_manager.Prompt = Prompt

    sys.modules.update({
        "core": core, "core.plugin": plugin, "core.logging_manager": logmod,
        "core.utils": types.ModuleType("core.utils"),
        "core.utils.tool_utils": tool_utils, "core.provider": provider,
        "core.chat": chat, "core.chat.message_utils": msg_utils,
        "core.chat.message_elements": elements,
        "core.utils.path_utils": path_utils, "core.prompt_manager": prompt_manager,
    })
    return Text, Record


def build_plugin(data_dir, Text, Record):
    for m in ("main", "bili_dl"):
        sys.modules.pop(m, None)
    import importlib
    main_mod = importlib.import_module("main")
    p = main_mod.BiliAudioSenderPlugin.__new__(main_mod.BiliAudioSenderPlugin)
    p._auto_sent = {}
    p._info_cache = {}
    p._bg_tasks = set()
    p.enabled = True
    p.enable_tool = True
    p.enable_command = False
    p.auto_send_link = True
    p.timeout = 5
    p.proxy_mode = "auto"
    p.max_seconds = 600
    p.cookie = ""
    p.include_desc = False
    p.desc_max_chars = 100
    p.search_count = 5
    p.auto_pick_top = False
    p.files_dir = __import__("pathlib").Path(data_dir) / "cache"
    p.files_dir.mkdir(parents=True, exist_ok=True)

    sent = []

    class Ctx:
        adapter_mgr = None

        async def send_message_chain(self, sid, chain):
            sent.append((sid, chain))
            return types.SimpleNamespace(ok=True, err="")

    p.ctx = Ctx()
    p._sent = sent
    p._main = main_mod
    return p, main_mod


def make_event(Text, text, mid="m1", sid="qq:gm:1", chain=None, self_msg=False):
    msg = types.SimpleNamespace(
        message_id=mid, self_id="10000",
        sender=types.SimpleNamespace(user_id="10000" if self_msg else "42"),
        group=None, raw_message=None,
        chain=chain if chain is not None else [Text(text)])
    ev = types.SimpleNamespace(
        session=types.SimpleNamespace(sid=sid),
        adapter=types.SimpleNamespace(name="QQ", platform="QQ"),
        message=msg, raw_message=None, supported_elements=["text", "record"])
    ev.discard = lambda force=False: None
    ev.stop = lambda: None
    return ev


def make_batch(event, Text, text):
    return types.SimpleNamespace(
        session=event.session,
        messages=[types.SimpleNamespace(message_id="m1", chain=[Text(text)])])


def make_req(Prompt, text):
    return types.SimpleNamespace(
        user_prompt=[Prompt(text, name="message", source="system")])


async def drain(p):
    for t in list(p._bg_tasks):
        if not t.done():
            await t


async def t_hook(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    calls = []

    async def slow(sid, bvid):
        calls.append(bvid)
        await asyncio.sleep(0.4)
        return "已直接发送语音条：《T》\n发送的文件：data/files/bili_cache/x.m4a"

    async def fake_extract(url, *a, **k):
        return "BV1xx411c7mD"

    mod.bili_dl.extract_bvid = fake_extract
    p._download_and_send = slow
    ev = make_event(Text, "https://b23.tv/V5Xhy88")
    t0 = time.time()
    await p.handle_link(ev)
    cost = (time.time() - t0) * 1000
    rec = p._auto_sent.get("qq:gm:1") or {}
    check("A1 钩子不阻塞（<50ms）", cost < 50, f"{cost:.1f}ms")
    check("A2 立即登记 pending", rec.get("state") == "pending", str(rec))
    check("A3 匹配键含短链 id", "V5Xhy88" in (rec.get("match_keys") or []))
    await drain(p)
    rec = p._auto_sent.get("qq:gm:1") or {}
    check("A4 完成后 state=sent", rec.get("state") == "sent", str(rec))
    check("A5 实际发送一次", calls == ["BV1xx411c7mD"], str(calls))

    # 标注：当轮命中 + 成功静默
    ev2 = make_event(Text, "https://b23.tv/V5Xhy88")
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "sent", "bvid": "BV1xx411c7mD", "title": "T",
        "file_rel": "data/x.m4a", "match_keys": ["V5Xhy88"], "message_id": "m1",
        "text_key": "https://b23.tv/V5Xhy88", "ts": time.time()}
    req = make_req(sys.modules["core.prompt_manager"].Prompt, "https://b23.tv/V5Xhy88")
    await p.inject_auto_sent_note(make_batch(ev2, Text, "https://b23.tv/V5Xhy88"), req)
    check("C1 标注命中原始消息", "已自动转为语音条发送" in req.user_prompt[0].content,
          req.user_prompt[0].content)
    check("C2 记录已消费", "qq:gm:1" not in p._auto_sent)
    check("C3 尾部没有多余提示",
          not any(getattr(x, "name", "") == "bili_auto_sent" for x in req.user_prompt))
    return tmp


async def t_card(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)

    async def ok(sid, bvid):
        return "已直接发送语音条：《T》\n发送的文件：data/x.m4a"

    p._download_and_send = ok

    class Card:
        def __init__(self, url):
            self.data = {"app": "com.tencent.miniapp_01",
                         "meta": {"detail_1": {"qqdocurl": url, "title": "卡片"}}}

    ev = make_event(Text, "", mid="m2", chain=[Card("https://b23.tv/V5Xhy88")])
    await p.handle_link(ev)
    rec = p._auto_sent.get("qq:gm:1") or {}
    check("B1 卡片能识别并登记", rec.get("state") == "pending", str(rec))
    await drain(p)
    # 卡片标注
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "sent", "bvid": "BV1xx411c7mD", "title": "T",
        "file_rel": "data/x.m4a", "match_keys": ["V5Xhy88"], "message_id": "m2",
        "text_key": "", "ts": time.time()}
    req = make_req(sys.modules["core.prompt_manager"].Prompt, "<card>")
    batch = types.SimpleNamespace(
        session=ev.session,
        messages=[types.SimpleNamespace(message_id="m2", chain=[Card("https://b23.tv/V5Xhy88")])])
    await p.inject_auto_sent_note(batch, req)
    check("B2 卡片标注命中", "已自动转为语音条发送" in req.user_prompt[0].content)
    return tmp


async def t_whitespace_ttl_fail(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)

    # 空白归一化
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "sent", "bvid": "BVx", "title": "T", "file_rel": "",
        "match_keys": ["BVx"], "message_id": "", "text_key": "来首 BVx 语音条", "ts": time.time()}
    ev = make_event(Text, "  来首 BVx 语音条  ")
    req = make_req(sys.modules["core.prompt_manager"].Prompt, "  来首 BVx 语音条  ")
    await p.inject_auto_sent_note(make_batch(ev, Text, "  来首 BVx 语音条  "), req)
    check("D1 空白差异不再导致丢失", "已自动转为语音条发送" in req.user_prompt[0].content)

    # TTL
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "sent", "bvid": "BVx", "match_keys": ["BVx"],
        "message_id": "x", "text_key": "x", "ts": time.time() - 301}
    req2 = make_req(sys.modules["core.prompt_manager"].Prompt, "x")
    await p.inject_auto_sent_note(make_batch(ev, Text, "x"), req2)
    check("E1 超过 5 分钟记录被清理", "qq:gm:1" not in p._auto_sent)

    # 注入即消费（不再等下一轮）
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "pending", "bvid": "BVx", "match_keys": ["BVx"],
        "message_id": "", "text_key": "x", "ts": time.time()}
    req3 = make_req(sys.modules["core.prompt_manager"].Prompt, "x")
    await p.inject_auto_sent_note(make_batch(ev, Text, "x"), req3)
    check("C4 注入即消费（不等下一轮）", "qq:gm:1" not in p._auto_sent)
    check("C5 不再追加任何尾部提示",
          not any(getattr(x, "name", "") == "bili_auto_sent" for x in req3.user_prompt))

    # 原消息不在 batch：什么都不加（绝不塞进无关的后续对话）
    p._auto_sent["qq:gm:1"] = {
        "rid": "r", "state": "sent", "bvid": "BVx", "title": "T", "file_rel": "",
        "match_keys": ["BVx"], "message_id": "zzz", "text_key": "zzz", "ts": time.time()}
    req5 = make_req(sys.modules["core.prompt_manager"].Prompt, "别的消息")
    ev5 = make_event(Text, "别的消息", mid="m10")
    await p.inject_auto_sent_note(make_batch(ev5, Text, "别的消息"), req5)
    check("C6 原消息不在 batch → 不追加任何提示",
          not any(getattr(x, "name", "") == "bili_auto_sent" for x in req5.user_prompt))
    check("C7 记录已丢弃", "qq:gm:1" not in p._auto_sent)
    return tmp


async def t_official_bridge(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    cache = p.files_dir / "BV1xx411c7mD_1.m4a"
    cache.write_bytes(b"\0" * 64)
    p._info_cache["BV1xx411c7mD"] = (time.time(), {"title": "T", "duration": 10, "desc": ""})

    class FakeAdapter:
        info = types.SimpleNamespace(platform="QQ Official", name="qqofficial")
        im = types.SimpleNamespace()

    p.ctx.adapter_mgr = types.SimpleNamespace(get_adapter=lambda n: FakeAdapter())
    out = await p._download_and_send("qqofficial:gm:g1", "BV1xx411c7mD")
    check("F1 官方无 bridge 明确报错", "qqbot-fullmsg-bridge" in out, out)
    check("F2 官方无 bridge 不发占位文本", not p._sent, str(p._sent))
    return tmp


async def t_info_cache_abs_path(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    cache = p.files_dir / "BV1xx411c7mD_1.m4a"
    cache.write_bytes(b"\0" * 64)
    calls = []

    async def fake_info(bvid, *a, **k):
        calls.append(bvid)
        return {"title": "T", "duration": 10, "desc": ""}

    mod.bili_dl.get_info = fake_info
    out1 = await p._download_and_send("qq:gm:1", "BV1xx411c7mD")
    out2 = await p._download_and_send("qq:gm:1", "BV1xx411c7mD")
    check("G1 第一次请求 get_info", len(calls) == 1, str(calls))
    check("G2 第二次命中 info 缓存（不再请求）", len(calls) == 1, str(calls))
    check("G3 两次都发送成功", out1.startswith("已直接发送") and out2.startswith("已直接发送"))

    # 绝对 cache_dir：files_dir 在 data 之外 → 不能崩
    abs_dir = tempfile.mkdtemp(prefix="bili_abs_")
    p.files_dir = __import__("pathlib").Path(abs_dir)
    (p.files_dir / "BV1xx411c7mD_1.m4a").write_bytes(b"\0" * 64)
    p._info_cache.pop("BV1xx411c7mD", None)
    out3 = await p._download_and_send("qq:gm:1", "BV1xx411c7mD")
    check("H1 绝对缓存目录不崩", out3.startswith("已直接发送"), out3)
    return tmp


async def t_command(Text, Record):
    """命令入口：立即返回（不阻塞）+ 后台发送语音条与结果文案。"""
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    p.enable_command = True
    p.command_words = ["/点歌"]
    p.allowed_users = []
    p.allowed_groups = []
    calls = []

    async def fake(sid, bvid, *a, **k):
        calls.append(("send", sid, bvid))
        return "已直接发送语音条：《T》\n发送的文件：data/x.m4a"

    p._download_and_send = fake
    ev = make_event(Text, "/点歌 BV1xx411c7mD", mid="m4")
    t0 = time.time()
    await p.handle_command(ev)
    cost = (time.time() - t0) * 1000
    check("J1 命令入口立即返回（<50ms）", cost < 50, f"{cost:.1f}ms")
    check("J2 命令已转后台", len(p._bg_tasks) == 1)
    await drain(p)
    check("J3 后台完成发送", calls and calls[0][0] == "send")
    check("J4 结果文案已回复", any("已直接发送" in "".join(
        getattr(x, "text", "") for x in chain) for _, chain in p._sent))
    return tmp


async def t_failure_notice(Text, Record):
    """失败要立即回到会话（不需要下一轮 LLM 激活）。"""
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)

    async def fake_extract(url, *a, **k):
        return "BV1xx411c7mD"

    async def fail(sid, bvid):
        return "发送失败：没装 bridge"

    mod.bili_dl.extract_bvid = fake_extract
    p._download_and_send = fail
    ev = make_event(Text, "https://b23.tv/V5Xhy88", mid="m5")
    await p.handle_link(ev)
    await drain(p)
    check("K1 失败不发会话消息（只记日志）", not p._sent, str(p._sent))
    rec = p._auto_sent.get("qq:gm:1") or {}
    check("K2 失败状态保留，供首次注入说明", rec.get("state") == "failed", str(rec))
    req = make_req(sys.modules["core.prompt_manager"].Prompt, "https://b23.tv/V5Xhy88")
    await p.inject_auto_sent_note(make_batch(ev, Text, "https://b23.tv/V5Xhy88"), req)
    check("K3 首次注入直接说明失败", "自动转语音失败" in req.user_prompt[0].content,
          req.user_prompt[0].content)
    check("K4 注入后记录清理", "qq:gm:1" not in p._auto_sent)
    return tmp


async def t_title_prefetch(Text, Record):
    """后台预取 get_info → 1.5s 的标注尽量带《标题》。"""
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)

    async def fake_extract(url, *a, **k):
        return "BV1xx411c7mD"

    async def fake_info(bvid, *a, **k):
        return {"title": "预取标题", "duration": 60, "desc": ""}

    async def slow_send(sid, bvid):
        await asyncio.sleep(0.3)
        return "已直接发送语音条：《预取标题》\n发送的文件：data/x.m4a"

    mod.bili_dl.extract_bvid = fake_extract
    mod.bili_dl.get_info = fake_info
    p._download_and_send = slow_send
    ev = make_event(Text, "https://b23.tv/V5Xhy88", mid="m6")
    await p.handle_link(ev)
    for _ in range(20):
        if (p._auto_sent.get("qq:gm:1") or {}).get("title"):
            break
        await asyncio.sleep(0.02)
    rec = p._auto_sent.get("qq:gm:1") or {}
    check("L1 后台预取到标题", rec.get("title") == "预取标题", str(rec.get("title")))
    req = make_req(sys.modules["core.prompt_manager"].Prompt, "https://b23.tv/V5Xhy88")
    await p.inject_auto_sent_note(make_batch(ev, Text, "https://b23.tv/V5Xhy88"), req)
    check("L2 当轮标注带《标题》", "《预取标题》" in req.user_prompt[0].content,
          req.user_prompt[0].content)
    await drain(p)
    return tmp


async def t_permission(Text, Record):
    """命令权限拒绝：钩子不等待发送，提示放后台。"""
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    p.enable_command = True
    p.command_words = ["/点歌"]
    p.allowed_users = ["999"]      # 发送者 user_id=42 → 拒绝
    p.allowed_groups = []
    p.permission_denied_message = "❌ 权限不足"
    ev = make_event(Text, "/点歌 某歌", mid="m7")
    t0 = time.time()
    await p.handle_command(ev)
    cost = (time.time() - t0) * 1000
    check("M1 拒绝提示不阻塞钩子（<20ms）", cost < 20, f"{cost:.1f}ms")
    check("M2 拒绝提示已转后台", len(p._bg_tasks) == 1)
    await drain(p)
    texts = ["".join(getattr(x, "text", "") for x in chain) for _, chain in p._sent]
    check("M3 后台发出拒绝提示", any("权限不足" in t for t in texts), str(texts))
    return tmp


async def t_platforms(Text, Record):
    """平台通用性：record 能力判断 + 非 record 平台明确拒绝。"""
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)

    ev_tg = make_event(Text, "https://b23.tv/V5Xhy88", sid="Telegram:gm:1")
    ev_tg.adapter = types.SimpleNamespace(name="tg", platform="Telegram")
    check("I1 Telegram 声明 record → 可发语音", p._main._can_voice(ev_tg) is True)

    ev_bili = make_event(Text, "https://b23.tv/V5Xhy88", sid="bili:dm:1")
    ev_bili.adapter = types.SimpleNamespace(name="bili", platform="Bilibili")
    ev_bili.supported_elements = ["text", "img"]
    check("I2 纯文本平台不支持 record", p._main._can_voice(ev_bili) is False)

    async def ok(sid, bvid):
        return "已直接发送语音条：《T》\n发送的文件：data/x.m4a"

    p._download_and_send = ok
    await p.handle_link(ev_bili)
    check("I3 非 record 平台不触发自动发送", not p._auto_sent)

    async def fake_extract(url, *a, **k):
        return "BV1xx411c7mD"

    mod.bili_dl.extract_bvid = fake_extract
    await p.handle_link(ev_tg)
    check("I4 Telegram 能触发自动发送", p._auto_sent.get("Telegram:gm:1", {}).get("state") == "pending")
    await drain(p)

    # 工具入口：非 record 平台明确拒绝（adapter_mgr 不可用也不崩）
    tool = mod.BiliAudioTool(p.ctx, p)
    out = await tool.execute(ev_bili, target="BV1xx411c7mD")
    check("I5 工具在非 record 平台明确拒绝", "不支持" in out, out)
    return tmp


async def t_self_and_terminate(Text, Record):
    tmp = tempfile.mkdtemp(prefix="bili_t_")
    p, mod = build_plugin(tmp, Text, Record)
    ev = make_event(Text, "https://b23.tv/V5Xhy88", self_msg=True)
    await p.handle_link(ev)
    check("E2 自身消息不触发", not p._auto_sent)

    async def slow(sid, bvid):
        await asyncio.sleep(5)
        return "已直接发送语音条：《T》"

    async def fake_extract2(url, *a, **k):
        return "BV1xx411c7mD"

    mod.bili_dl.extract_bvid = fake_extract2
    p._download_and_send = slow
    ev2 = make_event(Text, "https://b23.tv/V5Xhy88", mid="m3")
    await p.handle_link(ev2)
    check("E3 后台任务已创建", len(p._bg_tasks) == 1)
    await p.terminate()
    check("E4 terminate 后任务表清空", not p._bg_tasks)
    return tmp


async def main():
    data = tempfile.mkdtemp(prefix="bili_data_")
    Text, Record = install_stubs(data)
    for fn in (t_hook, t_card, t_whitespace_ttl_fail, t_official_bridge,
               t_info_cache_abs_path, t_failure_notice, t_title_prefetch,
               t_permission, t_platforms, t_command, t_self_and_terminate):
        await fn(Text, Record)
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("失败项：" + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
