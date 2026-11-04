import asyncio
import json
import re
import time
import uuid
from pathlib import Path

try:
    from . import bili_dl
except ImportError:  # 独立运行 / 非包方式加载时回退
    import bili_dl  # type: ignore

from core.plugin import BasePlugin, on, Priority
from core.logging_manager import get_logger
from core.utils.tool_utils import BaseTool
from core.provider import LLMRequest
from core.chat import MessageChain
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat.message_elements import Text, Record
from core.utils.path_utils import get_data_path
from core.prompt_manager import Prompt

logger = get_logger("bili_audio_sender", "cyan")

#: B站视频链接 / b23 短链（和视频插件保持同一套识别规则）
_BV_LINK_RE = re.compile(r'https?://(?:www\.|m\.)?bilibili\.com/video/(BV[0-9A-Za-z]{10})', re.I)
_B23_RE = re.compile(r'https?://b23\.tv/([0-9A-Za-z]+)')


def _collect_chain_text(chain) -> str:
    """收集消息链里所有可能带链接的文本。

    ⚠️ 不能只看 Text 元素：QQ 小程序卡片（com.tencent.miniapp_01）等不是 Text，
    但字段里带着 qqdocurl（B站短链）。这里展开每个元素的字符串/dict 字段，
    只用于「找链接 + 匹配原消息」，不影响其它逻辑。
    """
    parts = []
    try:
        items = list(chain or [])
    except Exception:
        return ""
    for ele in items:
        try:
            if isinstance(ele, Text):
                t = getattr(ele, "text", "") or ""
                if t:
                    parts.append(t)
                continue
            attrs = {}
            try:
                attrs = dict(vars(ele))
            except Exception:
                attrs = {}
            for key in ("text", "data", "json", "raw", "url", "content", "summary",
                        "qqdocurl", "prompt", "desc", "title"):
                v = getattr(ele, key, None)
                if v is not None and key not in attrs:
                    attrs[key] = v
            for v in attrs.values():
                if isinstance(v, str):
                    if v:
                        parts.append(v)
                elif isinstance(v, (dict, list, tuple)):
                    try:
                        parts.append(json.dumps(v, ensure_ascii=False))
                    except Exception:
                        pass
        except Exception:
            continue
    return "\n".join(parts)


def _normalize_text(s: str) -> str:
    """空白归一化：避免「存的时候没 strip、比的时候 strip」导致永远匹配不上。"""
    return re.sub(r"\s+", " ", str(s or "")).strip()

#: 本插件对**所有能发语音条（record）的平台**开放：
#:   * `"QQ"`          —— OneBot 系 QQ 适配器（NapCat / LLOneBot 等）
#:   * `"QQ Official"` —— QQ 官方机器人（需 `qqbot-fullmsg-bridge` 补 file_type=3 / silk）
#:   * `"Telegram"` / `"Discord"` / `"WebChat"` —— 框架适配器原生支持 record
#: 判断依据是事件声明的 `supported_elements`（2.x 回落 `message_types`），
#: 不再写死平台名；不支持 record 的平台会明确提示，不会静默失败。


def _element_names(event) -> list:
    els = getattr(event, "supported_elements", None)
    if els is None:  # 2.x 兼容
        els = getattr(event, "message_types", None) or []
    try:
        return [str(e).strip().lower() for e in (els or [])]
    except Exception:
        return []


def _can_voice(event) -> bool:
    """当前平台是否声明支持发送语音条（record）。"""
    return "record" in _element_names(event)


class BiliAudioTool(BaseTool):
    name = "bili_audio_send"
    description = (
        "下载B站视频音频并发送语音条到当前聊天平台（需适配器支持 record）。"
        "用户要求点歌/播放/下载B站音频/发语音条时调用。"
        "target传B站链接、BV号或歌曲关键词；关键词会返回候选列表，可自行挑选后传bvid直接发送。"
    )
    parameters = {
        "type": "object",
        "properties": {
            "target": {"type": "string", "description": "B站链接、BV号或歌曲搜索关键词（与bvid至少填一个）"},
            "bvid": {"type": "string", "description": "已知BV号（可选，优先于target解析）"},
        },
    }

    def __init__(self, ctx, plugin):
        self.ctx = ctx
        self.plugin = plugin

    async def execute(self, event, target: str = "", bvid: str = "", *args, **kwargs) -> str:
        if not _can_voice(event):
            ok = False
            try:
                ad = self.ctx.adapter_mgr.get_adapter(event.adapter.name)
                ok = await self.plugin._adapter_supports_voice(ad)
            except Exception:
                ok = False
            if not ok:
                return "当前平台不支持发送语音条"
        target = target or ""
        try:
            return await self.plugin._handle_request(event, target, bvid)
        except bili_dl.BiliError as e:
            return str(e)          # 可读错误（-404/-412 等已翻译），LLM 直接决策
        except Exception as e:
            logger.exception("[bili_audio_sender] tool failed")
            return f"处理失败：{e}"


class BiliAudioSenderPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        # 默认值与 schema.json 保持一致
        self.enabled = True
        self.enable_tool = True
        self.enable_command = False
        self.command_words = ["/点歌", "/下音频"]
        self.auto_send_link = False
        self.allowed_users: list[str] = []
        self.allowed_groups: list[str] = []
        self.permission_denied_message = "❌ 权限不足：你不在本功能白名单内"
        self.search_count = 5
        self.auto_pick_top = False
        self.include_desc = False
        self.desc_max_chars = 100
        self.max_seconds = 600
        self.timeout = 60
        self.cookie = ""
        self.proxy_mode = "auto"
        self.cache_dir_str = "files/bili_cache"
        self.max_cache_files = 50
        self.cleanup_count = 20
        self.files_dir: Path | None = None

    async def initialize(self):
        # sid → {rid, state, bvid, title, file_rel, match_keys, message_id, text_key, ts}
        self._auto_sent: dict[str, dict] = {}
        self._info_cache: dict[str, tuple[float, dict]] = {}   # bvid → (ts, info)
        self._bg_tasks: set[asyncio.Task] = set()

        sec = self.plugin_cfg.get("section_basic", {})
        self.enabled = sec.get("enabled", True)
        self.enable_tool = sec.get("enable_tool", True)
        self.enable_command = sec.get("enable_command", False)
        self.command_words = [str(w).strip() for w in sec.get("command_words", ["/点歌", "/下音频"])
                              if str(w).strip()]
        self.auto_send_link = sec.get("auto_send_link", False)

        perm = self.plugin_cfg.get("section_permission", {})
        self.allowed_users = [str(u).strip() for u in perm.get("allowed_users", []) if str(u).strip()]
        self.allowed_groups = [str(g).strip() for g in perm.get("allowed_groups", []) if str(g).strip()]
        self.permission_denied_message = perm.get(
            "permission_denied_message", "❌ 权限不足：你不在本功能白名单内")

        s = self.plugin_cfg.get("section_search", {})
        self.search_count = max(1, int(s.get("search_count", 5) or 5))
        self.auto_pick_top = s.get("auto_pick_top", False)
        self.include_desc = s.get("include_desc", False)
        self.desc_max_chars = max(1, int(s.get("desc_max_chars", 100) or 100))

        d = self.plugin_cfg.get("section_download", {})
        self.max_seconds = max(0, int(d.get("max_seconds", 600) or 600))
        self.timeout = max(5, int(d.get("timeout", 60) or 60))
        self.cookie = d.get("cookie", "")
        pm = d.get("proxy_mode", "auto")
        self.proxy_mode = pm if pm in bili_dl.PROXY_MODES else "auto"
        self.cache_dir_str = d.get("cache_dir", "files/bili_cache") or "files/bili_cache"
        self.max_cache_files = max(1, int(d.get("max_cache_files", 50) or 50))
        self.cleanup_count = max(1, int(d.get("cleanup_count", 20) or 20))

        self.files_dir = Path(get_data_path()) / self.cache_dir_str
        self.files_dir.mkdir(parents=True, exist_ok=True)
        self._cleanup_cache()
        logger.info(
            f"[bili_audio_sender] ready | tool={'on' if self.enable_tool else 'off'} "
            f"cmd={'on' if self.enable_command else 'off'} link={'on' if self.auto_send_link else 'off'} "
            f"pick_top={'on' if self.auto_pick_top else 'off'} max_sec={self.max_seconds} "
            f"proxy={self.proxy_mode} cache={self.cache_dir_str}")

    async def terminate(self):
        """取消所有后台发送任务，避免热重载后旧任务继续往会话里发东西。"""
        tasks = [t for t in getattr(self, "_bg_tasks", set()) if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bg_tasks = set()

    # ---------- 权限（仅命令式入口生效；LLM 工具与链接钩子不受白名单限制） ----------
    def _is_allowed_event(self, event) -> bool:
        if not self.allowed_users and not self.allowed_groups:
            return True
        user_id = group_id = ""
        if hasattr(event, "message"):  # KiraMessageEvent
            msg = event.message
            if msg.sender and getattr(msg.sender, "user_id", None):
                user_id = str(msg.sender.user_id)
            if msg.group:
                group_id = str(getattr(msg.group, "group_id", ""))
        elif getattr(event, "messages", None):  # KiraMessageBatchEvent
            last = event.messages[-1]
            if last.sender and getattr(last.sender, "user_id", None):
                user_id = str(last.sender.user_id)
            if last.group:
                group_id = str(getattr(last.group, "group_id", ""))
        if self.allowed_users and (not user_id or user_id not in self.allowed_users):
            return False
        if self.allowed_groups and (not group_id or group_id not in self.allowed_groups):
            return False
        return True

    @staticmethod
    def _get_sid(event) -> str:
        sid = getattr(event, "sid", None)
        if sid:
            return sid
        if getattr(event, "session", None) and getattr(event.session, "sid", None):
            return event.session.sid
        last = event.messages[-1]
        adapter = event.adapter.name if event.adapter else "qq"
        if getattr(last, "group", None) and last.group:
            return f"{adapter}:gm:{last.group.group_id}"
        return f"{adapter}:dm:{last.sender.user_id}"

    def _find_cache(self, bvid: str) -> Path | None:
        if self.files_dir is None:
            return None
        for f in self.files_dir.glob(f"{bvid}_*.m4a"):
            if f.stat().st_size > 0:
                return f
        return None

    async def _adapter_supports_voice(self, ad) -> bool:
        """适配器是否声明支持发送 record（3.x 能力元数据，2.x 回落 message_types）。"""
        if ad is None:
            return False
        try:
            from core.adapter.capabilities import IMCapability
            cap = ad.get_capability(IMCapability)
        except Exception:
            cap = getattr(ad, "im", None) or ad
        try:
            meta = await cap.get_message_metadata()
            els = getattr(meta, "supported_elements", None) or []
            return "record" in {str(e).strip().lower() for e in els}
        except Exception:
            pass
        try:
            els = getattr(ad, "message_types", None) or []
            return "record" in {str(e).strip().lower() for e in els}
        except Exception:
            return False

    def _cleanup_cache(self) -> None:
        """缓存文件超过 max_cache_files 时，按修改时间删除最旧的 cleanup_count 个（对齐 pixiv 插件）。"""
        try:
            if self.files_dir is None or not self.files_dir.exists():
                return
            files = [f for f in self.files_dir.iterdir() if f.is_file()]
            if len(files) <= self.max_cache_files:
                return
            files.sort(key=lambda f: f.stat().st_mtime)
            to_delete = files[:self.cleanup_count]
            deleted = 0
            for f in to_delete:
                try:
                    f.unlink()
                    deleted += 1
                except Exception:
                    logger.warning(f"[bili_audio_sender] 清理旧缓存失败 {f}")
            logger.info(f"[bili_audio_sender] 缓存清理：删除 {deleted} 个旧文件，剩余 {len(files) - deleted} 个")
        except Exception:
            logger.warning("[bili_audio_sender] 缓存清理异常")

    # ---------- 入口 A：LLM 工具注入 ----------
    @on.llm_request(priority=Priority.HIGH)
    async def inject_tool(self, event, req: LLMRequest, *_):
        if not self.enabled or not self.enable_tool:
            return
        try:
            req.tool_set.add(BiliAudioTool(ctx=self.ctx, plugin=self))
        except Exception:
            logger.exception("[bili_audio_sender] tool inject failed")

    # ---------- 入口 B：命令词钩子 ----------
    @on.im_message(priority=Priority.HIGH)
    async def handle_command(self, event: KiraMessageEvent, *_):
        if not self.enabled or not self.enable_command or not _can_voice(event):
            return
        text = "".join(e.text for e in event.message.chain if isinstance(e, Text)).strip()
        if not text:
            return
        if not any(text == c or text.startswith(c + " ") for c in self.command_words):
            return
        if not self._is_allowed_event(event):
            event.discard(force=True)
            event.stop()
            # 拒绝提示也放后台：钩子里不等待任何网络发送
            self._spawn(self._send_text(event.session.sid, self.permission_denied_message),
                        name="bili_deny")
            return
        args = text.split(maxsplit=1)
        target = args[1].strip() if len(args) > 1 else ""
        sid = event.session.sid
        # 命令不需要进 LLM：先让事件出队，再后台下载/发送，避免阻塞消息总线
        event.discard(force=True)
        event.stop()
        self._spawn(self._command_do(event, sid, target), name="bili_cmd")

    async def _send_text(self, sid: str, text: str):
        try:
            await self.ctx.send_message_chain(sid, MessageChain([Text(str(text))]))
        except Exception:
            logger.warning("[bili_audio_sender] 发送文本失败")

    async def _command_do(self, event, sid: str, target: str):
        """命令式入口的后台执行体（输出顺序与旧版一致：先语音条，后结果文案）。"""
        try:
            reply = await self._handle_request(event, target, "")
        except bili_dl.BiliError as e:
            reply = f"点歌失败：{e}"
        except Exception as e:
            logger.exception("[bili_audio_sender] command failed")
            reply = f"点歌失败：{e}"
        await self._send_text(sid, reply)

    # ---------- 入口 C：链接钩子（0 token，非阻塞） ----------
    @staticmethod
    def _msg_id(event) -> str:
        """取消息 ID（用于在上下文里精确定位原消息）。"""
        try:
            for holder in (getattr(event, "message", None), event):
                if holder is None:
                    continue
                v = getattr(holder, "message_id", None)
                if v:
                    return str(v)
        except Exception:
            pass
        return ""

    @staticmethod
    def _is_self_message(event) -> bool:
        """防御：bot 自己发的消息（部分实现会上报）不能再触发自动发送。"""
        try:
            msg = getattr(event, "message", None)
            if msg is None:
                return False
            self_id = str(getattr(msg, "self_id", "") or "")
            sender = getattr(msg, "sender", None)
            sender_id = str(getattr(sender, "user_id", "") or "")
            if self_id and sender_id and self_id == sender_id:
                return True
            raw = getattr(msg, "raw_message", None)
            if isinstance(raw, dict) and str(raw.get("post_type") or "") == "message_sent":
                return True
        except Exception:
            pass
        return False

    def _spawn(self, coro, name: str = ""):
        t = asyncio.create_task(coro, name=name or "bili_bg")
        self._bg_tasks.add(t)
        t.add_done_callback(self._bg_tasks.discard)
        return t

    def _prune_state(self):
        """清理过期记录：标注 5 分钟、info 缓存 1 小时 / 最多 200 条。"""
        now = time.time()
        for sid, rec in list(self._auto_sent.items()):
            if now - float(rec.get("ts") or 0) > 300:
                self._auto_sent.pop(sid, None)
        for bvid, (ts, _info) in list(self._info_cache.items()):
            if now - float(ts) > 3600:
                self._info_cache.pop(bvid, None)
        if len(self._info_cache) > 200:
            for bvid, _ in sorted(self._info_cache.items(),
                                  key=lambda kv: kv[1][0])[:len(self._info_cache) - 200]:
                self._info_cache.pop(bvid, None)

    @on.im_message(priority=Priority.HIGH)
    async def handle_link(self, event: KiraMessageEvent, *_):
        """检测到 B站链接 → **立即登记** pending 记录 + 后台发送（0 await / 0 阻塞）。

        ⚠️ 记录必须在消息进入 batch 之前写好：框架默认 debounce 1.5s 后就发 LLM
        请求，而下载通常要几秒。等发送完成再写记录会永远赶不上（视频插件踩过）。
        """
        if not self.enabled or not self.auto_send_link or not _can_voice(event):
            return
        if self._is_self_message(event):
            return
        sid = getattr(event.session, "sid", "") or ""
        if not sid:
            return
        self._prune_state()

        chain = getattr(event.message, "chain", None)
        text = _collect_chain_text(chain)
        bvid = ""
        short_url = ""
        m = _BV_LINK_RE.search(text)
        if m:
            bvid = m.group(1)
            if bvid[:2].lower() == "bv":
                bvid = "BV" + bvid[2:]
        if not bvid:
            m2 = _B23_RE.search(text)
            if m2:
                short_url = m2.group(0)
        # 兜底：raw_message（只在链里有非 Text 元素时才扫，避免每条消息白做 json.dumps）
        if not bvid and not short_url:
            try:
                items = list(chain or [])
                if any(not isinstance(ele, Text) for ele in items):
                    raw = getattr(event, "raw_message", None)
                    if raw is None and hasattr(event, "message"):
                        raw = getattr(event.message, "raw_message", None)
                    if raw is None and hasattr(event, "message"):
                        raw = getattr(event.message, "source_message", None)
                    if isinstance(raw, dict):
                        raw = json.dumps(raw, ensure_ascii=False)
                    if isinstance(raw, str) and raw:
                        m3 = _B23_RE.search(raw)
                        if m3:
                            short_url = m3.group(0)
                        else:
                            m4 = _BV_LINK_RE.search(raw)
                            if m4:
                                bvid = m4.group(1)
            except Exception:
                pass
        if not bvid and not short_url:
            return

        match_keys = []
        if bvid:
            match_keys.append(bvid)
        m2 = re.search(r'b23\.tv/([0-9A-Za-z]+)', text)
        if m2:
            match_keys.append(m2.group(1))
        if short_url:
            match_keys.append(short_url)
        match_keys = list(dict.fromkeys(match_keys))

        rid = uuid.uuid4().hex
        self._auto_sent[sid] = {
            "rid": rid, "state": "pending", "bvid": bvid, "title": "", "file_rel": "",
            "match_keys": match_keys, "message_id": self._msg_id(event),
            "text_key": _normalize_text(text), "ts": time.time(),
        }
        logger.info("[bili_audio_sender] 检测到B站链接，后台发送: %s（sid=%s）",
                    bvid or short_url, sid)
        self._spawn(self._auto_send_do(rid, bvid, short_url, sid),
                    name=f"bili_auto_{rid[:8]}")

    async def _auto_send_do(self, rid: str, bvid: str, short_url: str, sid: str):
        """后台发送：先解析短链，再下载/发送，最后把状态推进到 sent / failed。"""
        def _rec() -> dict:
            r = self._auto_sent.get(sid) or {}
            return r if r.get("rid") == rid else {}

        try:
            if not bvid and short_url:
                try:
                    bvid = await bili_dl.extract_bvid(short_url, self.timeout, self.proxy_mode)
                except Exception as e:
                    logger.info("[bili_audio_sender] b23 解析失败: %s (%s)", short_url, e)
                    bvid = ""
            if not bvid:
                rec = _rec()
                if rec:
                    rec.update(state="failed", reason="短链解析失败", ts=time.time())
                return
            rec = _rec()
            if rec:
                rec["bvid"] = bvid
                keys = list(rec.get("match_keys") or [])
                if bvid not in keys:
                    keys.append(bvid)
                rec["match_keys"] = keys

            # 预取标题：让 1.5s 时的标注尽量带上《标题》；失败不影响发送。
            # _download_and_send 会命中同一份 info 缓存，不会重复请求。
            try:
                cached_info = self._info_cache.get(bvid)
                if cached_info and time.time() - float(cached_info[0]) < 3600:
                    info = cached_info[1]
                else:
                    # 限时 8 秒：只是给标注补标题，慢网不能拖住后台发送
                    info = await bili_dl.get_info(bvid, self.cookie,
                                                  min(self.timeout, 8), self.proxy_mode)
                    self._info_cache[bvid] = (time.time(), info)
                if rec and info.get("title"):
                    rec["title"] = str(info["title"])
            except Exception:
                pass

            reply = await self._download_and_send(sid, bvid)
            rec = _rec()
            if reply.startswith("已直接发送"):
                title = reply.split("《")[1].split("》")[0] if "《" in reply else bvid
                file_rel = reply.split("发送的文件：")[-1].strip() if "发送的文件：" in reply else ""
                if rec:
                    rec.update(state="sent", bvid=bvid, title=title,
                               file_rel=file_rel, ts=time.time())
            else:
                if rec:
                    rec.update(state="failed", reason=str(reply)[:120], ts=time.time())
                # 自动发送是后台行为：失败**只记日志**，不往会话里发消息
                logger.warning("[bili_audio_sender] auto_send 未成功: %s", str(reply)[:200])
        except bili_dl.BiliError as e:
            rec = _rec()
            if rec:
                rec.update(state="failed", reason=str(e)[:120], ts=time.time())
            logger.warning("[bili_audio_sender] auto_send 下载失败: %s", e)
        except Exception:
            logger.exception("[bili_audio_sender] link hook failed")
            rec = _rec()
            if rec:
                rec.update(state="failed", reason="内部异常", ts=time.time())

    # ---------- 入口 D：已直发的 LLM 上下文标注 ----------
    @staticmethod
    def _auto_sent_note(sent: dict) -> str:
        if (sent.get("state") or "") == "failed":
            return f"\n[系统提示：该链接自动转语音失败（{sent.get('reason') or '未知原因'}）]"
        title = str(sent.get("title") or "")
        file_rel = str(sent.get("file_rel") or "")
        if title or file_rel:
            label = f"《{title}》" if title else ""
            tail = f"（{file_rel}）" if file_rel else ""
            return f"\n[系统提示：该链接{label}已自动转为语音条发送{tail}]"
        return "\n[系统提示：该链接已由插件自动转为语音条发送]"

    @on.llm_request(priority=Priority.LOW)
    async def inject_auto_sent_note(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if not self.enabled:
            return
        sid = getattr(event.session, "sid", None)
        if not sid:
            return
        sent = self._auto_sent.get(sid)
        if not sent:
            return
        if time.time() - float(sent.get("ts") or 0) > 300:
            self._auto_sent.pop(sid, None)
            return
        note = self._auto_sent_note(sent)
        keys = [k for k in (sent.get("match_keys") or []) if k]
        want_mid = str(sent.get("message_id") or "")
        text_key = sent.get("text_key") or ""

        prompt_idx = 0
        for msg in event.messages:
            while prompt_idx < len(req.user_prompt) and not (
                    req.user_prompt[prompt_idx].name == "message"
                    and req.user_prompt[prompt_idx].source == "system"):
                prompt_idx += 1
            if prompt_idx >= len(req.user_prompt):
                break
            p = req.user_prompt[prompt_idx]
            prompt_idx += 1

            hit = bool(want_mid) and want_mid == self._msg_id(msg)
            msg_text = ""
            if not hit:
                msg_text = _collect_chain_text(getattr(msg, "chain", None))
                hit = bool(text_key) and _normalize_text(msg_text) == text_key
            if not hit:
                if not msg_text:
                    msg_text = _collect_chain_text(getattr(msg, "chain", None))
                hit = any(k in msg_text for k in keys)
            if hit and isinstance(p.content, str):
                p.content += note
                self._auto_sent.pop(sid, None)   # 注入即消费（不依赖下一轮）
                logger.info("[bili_audio_sender] 已在上下文里标注「已自动转为语音条」: %s",
                            sent.get("bvid") or (keys[0] if keys else "?"))
                return

        # 原始消息不在本轮 batch（例如它被 discard 了）：**什么都不加**。
        # 绝不把「某条链接发没发」塞进无关的后续对话；记录直接丢弃。
        self._auto_sent.pop(sid, None)

    # ---------- 核心处理 ----------
    async def _handle_request(self, event, target: str, bvid: str) -> str:
        sid = self._get_sid(event)
        if bvid:
            return await self._download_and_send(sid, bvid)
        link_bvid = await bili_dl.extract_bvid(target or "", self.timeout, self.proxy_mode)
        if link_bvid:
            return await self._download_and_send(sid, link_bvid)
        keyword = (target or "").strip()
        if not keyword:
            return "请提供B站链接、BV号或歌曲关键词"
        if self.auto_pick_top:
            items = await bili_dl.search(keyword, 5, self.cookie, self.proxy_mode)
            if not items:
                return f"B站未找到「{keyword}」相关视频"
            top = max(items, key=lambda x: x.get("play", 0))
            return await self._download_and_send(sid, top["bvid"])
        return await self._search_and_pick(event, keyword)

    async def _search_and_pick(self, event, keyword: str) -> str:
        items = await bili_dl.search(keyword, self.search_count, self.cookie, self.proxy_mode)
        if not items:
            return f"B站未找到「{keyword}」相关视频"
        lines = []
        for i, it in enumerate(items, 1):
            lines.append(f"{i}. {it['title']}｜{it['author']}｜{it['bvid']}")
            if self.include_desc and it.get("desc"):
                lines.append(f"   简介：{it['desc'][:self.desc_max_chars]}")
        lines.append("可直接传 bvid 再次调用直接发送，或展示候选让用户选择。")
        return "B站搜索「" + keyword + "」结果：\n" + "\n".join(lines)

    def _official_bridge_ready(self, ad) -> bool:
        """QQ 官方适配器是否已装上 qqbot-fullmsg-bridge 的媒体补丁。

        核心官方适配器不认 `Record`（`_text_content` 会填
        `[Unsupported message element]`，且 media 白名单只有 File/Image）。
        没装桥接时宁可明确报错，也不能把占位文本当语音条发出去。
        """
        if ad is None:
            return False
        try:
            from core.adapter.capabilities import IMCapability
            cap = ad.get_capability(IMCapability)
        except Exception:
            cap = getattr(ad, "im", None) or ad
        return (bool(getattr(getattr(cap, "_upload_file", None), "_kira_bridge_ftype", False))
                and "_send_message" in getattr(cap, "__dict__", {}))

    async def _download_and_send(self, sid: str, bvid: str) -> str:
        # QQ 官方：没有桥接时明确拒绝（放在最前面，避免白下载一首发不出去的音频）
        ad = None
        try:
            ad = self.ctx.adapter_mgr.get_adapter(str(sid).split(":", 1)[0])
        except Exception:
            ad = None
        platform = str(getattr(getattr(ad, "info", None), "platform", "") or "")
        if platform == "QQ Official" and not self._official_bridge_ready(ad):
            return ("发送失败：QQ 官方机器人需要安装 qqbot-fullmsg-bridge 才能发送语音条"
                    "（核心适配器不支持 Record 元素）")

        self._cleanup_cache()
        path = self._find_cache(bvid)

        # info 缓存：同一 BV 1 小时内重复点歌 / 重新发送不再走网络
        info = None
        cached = self._info_cache.get(bvid)
        if cached and time.time() - float(cached[0]) < 3600:
            info = cached[1]
        if info is None:
            try:
                info = await bili_dl.get_info(bvid, self.cookie, self.timeout, self.proxy_mode)
                self._info_cache[bvid] = (time.time(), info)
            except Exception:
                if path is None:
                    raise
                # 已有缓存音频：网络暂时不可用也照发，标题退化为 BV 号
                info = {"title": bvid, "duration": 0, "desc": ""}
                logger.warning("[bili_audio_sender] get_info 失败，使用缓存文件继续发送: %s", bvid)

        if path is None:
            if self.max_seconds and info["duration"] > self.max_seconds:
                return f"视频时长 {info['duration']}s 超过上限 {self.max_seconds}s，未下载"
            path, _ = await bili_dl.download(bvid, self.files_dir, info=info,
                                             cookie=self.cookie, timeout=self.timeout,
                                             proxy_mode=self.proxy_mode)

        title, duration, desc = info["title"], info["duration"], info["desc"]
        chain = MessageChain([Record(record=str(path), name=Path(path).name)])
        result = await self.ctx.send_message_chain(sid, chain)
        err = str(result.err or "") if not result.ok else ""
        # NapCat 长语音发送可能报"超时"但消息实际已送达：超时不当失败报给 LLM，仅记日志
        if not result.ok and "超时" not in err:
            return f"发送失败：{err}"
        if not result.ok:
            logger.warning(f"[bili_audio_sender] send reported timeout but may have delivered: {err}")
        extra = f"\n简介：{desc[:self.desc_max_chars]}" if (self.include_desc and desc) else ""
        try:
            rel = f"data/{Path(path).relative_to(Path(get_data_path())).as_posix()}"
        except ValueError:
            # cache_dir 配成绝对路径（不在 data/ 下）时不能崩：发送已经成功了
            rel = str(path)
        return (
            f"已直接发送语音条：《{title}》{extra}\n"
            "已发送完毕，你无需再次发送，也无需使用 <file> 标签，只需简单回应用户。\n"
            f"发送的文件：{rel}"
        )
