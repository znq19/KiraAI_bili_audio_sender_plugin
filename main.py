import re
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

INTENT_WORDS = ("下载", "语音条", "语音", "音频", "拉下来", "转语音", "提取", "弄成")


class BiliAudioTool(BaseTool):
    name = "bili_audio_send"
    description = (
        "下载B站视频音频并发QQ语音条。用户要求点歌/播放/下载B站音频/发语音条时调用。"
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
        if event.adapter.platform != "QQ":
            return "当前会话不是QQ，无法发送语音条"
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
        self.strict_intent = True
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
        self._auto_sent: dict[str, dict] = {}  # sid → {"bvid": ..., "title": ..., "file_rel": ...}

        sec = self.plugin_cfg.get("section_basic", {})
        self.enabled = sec.get("enabled", True)
        self.enable_tool = sec.get("enable_tool", True)
        self.enable_command = sec.get("enable_command", False)
        self.command_words = [str(w).strip() for w in sec.get("command_words", ["/点歌", "/下音频"])
                              if str(w).strip()]
        self.auto_send_link = sec.get("auto_send_link", False)
        self.strict_intent = sec.get("strict_intent", True)

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
        pass

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
        if not self.enabled or not self.enable_command or event.adapter.platform != "QQ":
            return
        text = "".join(e.text for e in event.message.chain if isinstance(e, Text)).strip()
        if not text:
            return
        if not any(text == c or text.startswith(c + " ") for c in self.command_words):
            return
        if not self._is_allowed_event(event):
            await self.ctx.message_processor.send_message_chain(
                event.session.sid, MessageChain([Text(self.permission_denied_message)]))
            event.discard(force=True)
            event.stop()
            return
        args = text.split(maxsplit=1)
        target = args[1].strip() if len(args) > 1 else ""
        try:
            reply = await self._handle_request(event, target, "")
        except bili_dl.BiliError as e:
            reply = f"点歌失败：{e}"
        except Exception as e:
            logger.exception("[bili_audio_sender] command failed")
            reply = f"点歌失败：{e}"
        # 命令式路径直接把结果（候选/错误/确认）发给用户
        await self.ctx.message_processor.send_message_chain(
            event.session.sid, MessageChain([Text(reply)]))
        event.discard(force=True)
        event.stop()

    # ---------- 入口 C：链接钩子（0 token） ----------
    @on.im_message(priority=Priority.HIGH)
    async def handle_link(self, event: KiraMessageEvent, *_):
        if not self.enabled or not self.auto_send_link or event.adapter.platform != "QQ":
            return
        text = "".join(e.text for e in event.message.chain if isinstance(e, Text))
        if not text:
            return
        bvid = await bili_dl.extract_bvid(text, self.timeout, self.proxy_mode)
        if not bvid:
            return
        if self.strict_intent and not any(w in text for w in INTENT_WORDS):
            return
        try:
            reply = await self._download_and_send(event.session.sid, bvid)
            if reply.startswith("已直接发送"):
                # 成功 → 记录 auto_sent 但不 discard，消息继续自然流转
                title = reply.split("《")[1].split("》")[0] if "《" in reply else bvid
                self._auto_sent[event.session.sid] = {
                    "bvid": bvid,
                    "title": title,
                    "file_rel": reply.split("发送的文件：")[-1].strip() if "发送的文件：" in reply else "",
                    "text": text,  # 原始文本用于后续匹配定位
                }
            else:
                # 失败（时长超限等）→ 补发文字提示，不记录 auto_sent
                await self.ctx.message_processor.send_message_chain(
                    event.session.sid, MessageChain([Text(reply)]))
        except bili_dl.BiliError as e:
            await self.ctx.message_processor.send_message_chain(
                event.session.sid, MessageChain([Text(f"下载失败：{e}")]))
        except Exception:
            logger.exception("[bili_audio_sender] link hook failed")

    # ---------- 入口 D：已直发的 LLM 上下文标注 ----------
    @on.llm_request(priority=Priority.LOW)
    async def inject_auto_sent_note(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        sid = event.session.sid
        sent = self._auto_sent.pop(sid, None)
        if not sent:
            return
        note = (
            f"\n[系统提示：该链接（《{sent['title']}》）"
            f"已自动转为语音条发送（{sent['file_rel']}）]"
        )
        # 按顺序遍历 messages ↔ user_prompt，定位原始链接消息追加 note
        prompt_idx = 0
        for msg in event.messages:
            # 跳过 user_prompt 中非 message 条目
            while prompt_idx < len(req.user_prompt) and not (
                    req.user_prompt[prompt_idx].name == "message"
                    and req.user_prompt[prompt_idx].source == "system"
            ):
                prompt_idx += 1
            if prompt_idx >= len(req.user_prompt):
                break
            p = req.user_prompt[prompt_idx]
            prompt_idx += 1
            msg_text = "".join(e.text for e in msg.chain if isinstance(e, Text)).strip()
            if msg_text == sent["text"]:
                p.content += note
                break

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

    async def _download_and_send(self, sid: str, bvid: str) -> str:
        self._cleanup_cache()
        path = self._find_cache(bvid)
        info = await bili_dl.get_info(bvid, self.cookie, self.timeout, self.proxy_mode)
        if path is None:
            if self.max_seconds and info["duration"] > self.max_seconds:
                return f"视频时长 {info['duration']}s 超过上限 {self.max_seconds}s，未下载"
            path, _ = await bili_dl.download(bvid, self.files_dir, info=info,
                                             cookie=self.cookie, timeout=self.timeout,
                                             proxy_mode=self.proxy_mode)
        title, duration, desc = info["title"], info["duration"], info["desc"]
        chain = MessageChain([Record(record=str(path), name=Path(path).name)])
        result = await self.ctx.message_processor.send_message_chain(sid, chain)
        err = str(result.err or "") if not result.ok else ""
        # NapCat 长语音发送可能报"超时"但消息实际已送达：超时不当失败报给 LLM，仅记日志
        if not result.ok and "超时" not in err:
            return f"发送失败：{err}"
        if not result.ok:
            logger.warning(f"[bili_audio_sender] send reported timeout but may have delivered: {err}")
        extra = f"\n简介：{desc[:self.desc_max_chars]}" if (self.include_desc and desc) else ""
        rel = f"data/{Path(path).relative_to(Path(get_data_path())).as_posix()}"
        return (
            f"已直接发送语音条：《{title}》{extra}\n"
            "已发送完毕，你无需再次发送，也无需使用 <file> 标签，只需简单回应用户。\n"
            f"发送的文件：{rel}"
        )
