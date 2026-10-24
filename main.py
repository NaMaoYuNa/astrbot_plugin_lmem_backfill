import asyncio
import importlib
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register

LM_PLUGIN_NAME = "astrbot_plugin_livingmemory"
ASTRBOT_DB = "/AstrBot/data/data_v4.db"
DEFAULT_COUNT = 500
DEFAULT_SINCE = "2026-07-01"
CHUNK_SIZE = 20
NAPCAT_COUNT = 200
CST = timezone(timedelta(hours=8))

_GLOBAL_TASK: asyncio.Task | None = None

_SEGMENT_TEXT = {
    "text": lambda d: str(d.get("text") or ""),
    "at": lambda d: f"@{d.get('name') or d.get('qq') or ''}",
    "image": lambda d: str(d.get("summary") or "[图片]"),
    "mface": lambda d: str(d.get("summary") or "[表情]"),
    "face": lambda d: "[表情]",
    "record": lambda d: "[语音]",
    "video": lambda d: "[视频]",
    "file": lambda d: "[文件]",
    "reply": lambda d: "",
}


def _segment_text(message: Any) -> str:
    if isinstance(message, str):
        return message
    if not isinstance(message, list):
        return ""
    parts: list[str] = []
    for seg in message:
        if not isinstance(seg, dict):
            continue
        fn = _SEGMENT_TEXT.get(str(seg.get("type") or ""))
        if fn is None:
            continue
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
        try:
            text = fn(data)
        except Exception:  # noqa: BLE001
            text = ""
        if text:
            parts.append(text)
    return " ".join(parts).strip()


def _parse_since(raw: str) -> float:
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).replace(tzinfo=CST).timestamp()
        except ValueError:
            continue
    raise ValueError(f"时间格式无法识别：{raw}，请用 YYYY-MM-DD")


def _parse_db_time(raw: Any) -> float:
    text = str(raw or "").strip()
    if not text:
        return 0.0
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CST)
    return dt.timestamp()


def _pick(msg: dict, *keys: str) -> Any:
    for key in keys:
        value = msg.get(key)
        if value is not None:
            return value
    return None


def _lm_helpers():
    base = None
    for name in list(sys.modules.keys()):
        if name.endswith(LM_PLUGIN_NAME + ".main"):
            base = name[: -len(".main")]
            break
    if not base:
        raise RuntimeError("未在进程内找到 LivingMemory 插件模块")
    models = importlib.import_module(base + ".core.models.conversation_models")
    source = importlib.import_module(base + ".core.memory_source")
    getter = None
    try:
        capture = importlib.import_module(base + ".core.passive_group_capture")
        getter = getattr(capture, "get_active_plugin", None)
    except Exception:  # noqa: BLE001
        pass
    return models.Message, source.serialize_source_messages, getter


@register("lmem_backfill", "local", "群聊历史回填 LivingMemory", "1.1.0")
class LmemBackfill(Star):
    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        self.config = config or {}
        self._task: asyncio.Task | None = None
        self._progress = "空闲"
        self._state_path = f"{str(StarTools.get_data_dir('lmem_backfill'))}/state.json"
        asyncio.create_task(self._bootstrap())

    # ---------- 启动自检 / 自动回填 ----------

    def _load_state(self) -> dict:
        try:
            with open(self._state_path, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:  # noqa: BLE001
            return {}

    def _save_state(self, state: dict) -> None:
        try:
            with open(self._state_path, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=1)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] 状态保存失败：{exc}")

    def _aiocqhttp_adapter(self):
        try:
            for inst in self.context.platform_manager.get_insts():
                bot = getattr(inst, "bot", None)
                if bot is not None and hasattr(bot, "call_action"):
                    return inst, bot
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] 获取平台实例失败：{exc}")
        return None, None

    def _platform_id(self, adapter) -> str:
        meta = getattr(adapter, "metadata", None)
        return str(getattr(meta, "id", None) or "723")

    def _umo(self, adapter, group_id: str) -> str:
        return f"{self._platform_id(adapter)}:GroupMessage:{group_id}"

    async def _bootstrap(self) -> None:
        await asyncio.sleep(5)
        adapter, bot = self._aiocqhttp_adapter()
        if bot is None:
            logger.warning("[lmem_backfill] 自检：未找到 NapCat 适配器")
            return
        try:
            groups = await bot.call_action("get_group_list")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"[lmem_backfill] 自检：get_group_list 失败：{exc}")
            groups = []
        table = [
            f"{g.get('group_id')}({g.get('group_name')}, {g.get('member_count')}人)"
            for g in (groups or [])
            if isinstance(g, dict)
        ]
        logger.info(f"[lmem_backfill] 自检：群列表 {' | '.join(table)}")
        self._log_db_coverage()

        if not bool(self.config.get("auto_start", False)):
            return
        keyword = str(self.config.get("group_keyword") or "多多后宫")
        target = next(
            (
                g
                for g in (groups or [])
                if isinstance(g, dict) and keyword in str(g.get("group_name") or "")
            ),
            None,
        )
        if not target:
            logger.warning(f"[lmem_backfill] 自动回填：未找到群名包含「{keyword}」的群")
            return
        gid = str(target["group_id"])
        state = self._load_state()
        if not bool(self.config.get("force_rerun", False)) and gid in (state.get("done") or {}):
            logger.info(f"[lmem_backfill] 自动回填：群 {gid} 已回填过，跳过")
            return
        if _GLOBAL_TASK and not _GLOBAL_TASK.done():
            logger.info("[lmem_backfill] 自动回填：已有任务在跑，跳过")
            return
        count = int(self.config.get("count") or DEFAULT_COUNT)
        since = str(self.config.get("since") or DEFAULT_SINCE)
        since_ts = _parse_since(since)
        logger.info(
            f"[lmem_backfill] 自动回填启动：群 {gid}「{target.get('group_name')}」，"
            f"{count} 条，since {since}"
        )
        self._task = self._start_job(bot, gid, count, since_ts, since, source="auto")

    def _log_db_coverage(self) -> None:
        try:
            conn = sqlite3.connect(f"file:{ASTRBOT_DB}?mode=ro", uri=True)
            rows = conn.execute(
                "select platform_id, user_id, count(*), min(created_at), max(created_at) "
                "from platform_message_history group by platform_id, user_id"
            ).fetchall()
            conn.close()
            for pid, uid, cnt, first, last in rows:
                logger.info(
                    f"[lmem_backfill] 本地消息库 {pid}:{uid} 条数={cnt} 起={first} 止={last}"
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] 读取本地消息库失败：{exc}")

    # ---------- 命令 ----------

    def _start_job(
        self,
        bot,
        group_id: str,
        count: int,
        since_ts: float,
        since_text: str,
        source: str,
        self_id: str = "",
    ):
        global _GLOBAL_TASK
        task = asyncio.create_task(
            self._run_job(bot, group_id, count, since_ts, since_text, source, self_id)
        )
        _GLOBAL_TASK = task

        def _clear(_t):
            global _GLOBAL_TASK
            _GLOBAL_TASK = None

        task.add_done_callback(_clear)
        return task

    @filter.command("记忆回填")
    async def backfill(
        self,
        event: AstrMessageEvent,
        count: int = DEFAULT_COUNT,
        since: str = DEFAULT_SINCE,
    ):
        if not event.is_admin():
            yield event.plain_result("仅管理员可用。")
            return
        group_id = event.get_group_id()
        if not group_id:
            yield event.plain_result("请在目标群里发送该命令。")
            return
        if _GLOBAL_TASK and not _GLOBAL_TASK.done():
            yield event.plain_result(f"已有回填任务在运行：{self._progress}")
            return
        try:
            since_ts = _parse_since(since)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        if count <= 0 or count > 5000:
            yield event.plain_result("数量需在 1~5000 之间。")
            return
        bot = getattr(event, "bot", None)
        if bot is None:
            yield event.plain_result("未找到 NapCat 连接。")
            return
        self._progress = "准备中"
        self._task = self._start_job(
            bot,
            str(group_id),
            int(count),
            since_ts,
            since,
            source="command",
            self_id=str(event.get_self_id() or ""),
        )
        yield event.plain_result(
            f"开始回填：群 {group_id}，最多 {count} 条，起始 {since}。完成后会在此报告。"
        )

    @filter.command("回填状态")
    async def status(self, event: AstrMessageEvent):
        if not event.is_admin():
            yield event.plain_result("仅管理员可用。")
            return
        running = bool(_GLOBAL_TASK and not _GLOBAL_TASK.done())
        yield event.plain_result(f"{'运行中' if running else '空闲'}：{self._progress}")

    # ---------- 核心流程 ----------

    async def _notify(self, umo: str, text: str) -> None:
        logger.info(f"[lmem_backfill] {text}")
        if not umo:
            return
        try:
            from astrbot.api.message_components import Plain
            from astrbot.core.message.message_event_result import MessageChain

            await self.context.send_message(umo, MessageChain([Plain(text)]))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] 群消息发送失败：{exc}")

    def _fetch_from_db(
        self, platform_id: str, group_id: str, count: int, since_ts: float
    ) -> list[dict]:
        conn = sqlite3.connect(f"file:{ASTRBOT_DB}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "select id, sender_id, sender_name, content, created_at "
                "from platform_message_history "
                "where platform_id = ? and user_id = ? "
                "order by created_at desc limit ?",
                (platform_id, group_id, count),
            ).fetchall()
        finally:
            conn.close()
        items = []
        for rid, sender_id, sender_name, content, created_at in reversed(rows):
            ts = _parse_db_time(created_at)
            if ts < since_ts:
                continue
            try:
                segments = json.loads(content) if content else []
            except Exception:  # noqa: BLE001
                segments = [{"type": "text", "data": {"text": str(content or "")}}]
            items.append(
                {
                    "message_id": rid,
                    "sender": {"user_id": sender_id, "nickname": sender_name},
                    "message": segments,
                    "time": ts,
                }
            )
        return items

    async def _fetch_from_napcat(
        self, bot, group_id: str, since_ts: float
    ) -> list[dict]:
        try:
            res = await bot.call_action(
                "get_group_msg_history", group_id=int(group_id), count=NAPCAT_COUNT
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] NapCat 历史补充失败：{exc}")
            return []
        msgs = [m for m in ((res or {}).get("messages") or []) if isinstance(m, dict)]
        return [m for m in msgs if float(m.get("time") or 0) >= since_ts]

    def _merge(self, *sources: list[dict]) -> list[dict]:
        merged: dict[tuple, dict] = {}
        for source in sources:
            for msg in source:
                sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
                key = (
                    int(float(msg.get("time") or 0)),
                    str(sender.get("user_id") or ""),
                    _segment_text(msg.get("message")),
                )
                if key[2]:
                    merged.setdefault(key, msg)
        items = list(merged.values())
        items.sort(key=lambda m: float(m.get("time") or 0))
        return items

    def _to_lm_messages(
        self, raw_messages: list[dict], group_id: str, umo: str, self_id: str, Message
    ) -> list[Any]:
        converted = []
        for msg in raw_messages:
            content = _segment_text(msg.get("message"))
            if not content:
                continue
            sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
            sender_id = str(sender.get("user_id") or "")
            is_bot = bool(self_id) and sender_id == self_id
            converted.append(
                Message(
                    id=int(_pick(msg, "message_id", "real_id") or 0),
                    session_id=umo,
                    role="assistant" if is_bot else "user",
                    content=content,
                    sender_id=sender_id or "unknown",
                    sender_name=sender.get("card") or sender.get("nickname"),
                    group_id=group_id,
                    platform="aiocqhttp",
                    timestamp=float(msg.get("time") or time.time()),
                    metadata={"is_bot_message": is_bot},
                )
            )
        return converted

    def _dump_raw(self, group_id: str, raw_messages: list[dict]) -> None:
        try:
            path = f"{str(StarTools.get_data_dir('lmem_backfill'))}/raw_{group_id}.json"
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(raw_messages, fh, ensure_ascii=False)
            logger.info(f"[lmem_backfill] 原始消息已保存：{path}")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[lmem_backfill] 保存原始消息失败：{exc}")

    def _resolve_lm(self):
        try:
            Message, serialize_source, getter = _lm_helpers()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"无法加载 LivingMemory 模块：{exc}") from exc
        plugin = None
        if callable(getter):
            try:
                plugin = getter()
            except Exception:  # noqa: BLE001
                plugin = None
        if plugin is None:
            meta = self.context.get_registered_star(LM_PLUGIN_NAME)
            plugin = getattr(meta, "star_cls", None) if meta else None
            if plugin is not None and not hasattr(plugin, "initializer"):
                plugin = None
        initializer = getattr(plugin, "initializer", None)
        if not initializer:
            raise RuntimeError("未取到 LivingMemory 插件实例")
        return Message, serialize_source, initializer

    async def _run_job(
        self,
        bot,
        group_id: str,
        count: int,
        since_ts: float,
        since_text: str,
        source: str,
        self_id: str = "",
    ) -> None:
        adapter = self._aiocqhttp_adapter()[0]
        umo = self._umo(adapter, group_id)
        try:
            Message, serialize_source, initializer = self._resolve_lm()
        except Exception as exc:  # noqa: BLE001
            self._progress = "失败：LivingMemory 未就绪"
            await self._notify(umo, f"回填失败：{exc}")
            return

        for _ in range(40):
            if getattr(initializer, "memory_engine", None) and getattr(
                initializer, "memory_processor", None
            ):
                break
            await asyncio.sleep(1)
        engine = getattr(initializer, "memory_engine", None)
        processor = getattr(initializer, "memory_processor", None)
        if not engine or not processor:
            self._progress = "失败：引擎未初始化"
            await self._notify(umo, "回填失败：LivingMemory 引擎尚未初始化。")
            return

        try:
            db_items = self._fetch_from_db(
                self._platform_id(adapter), group_id, count, since_ts
            )
            self._progress = f"本地库取出 {len(db_items)} 条"
            napcat_items = await self._fetch_from_napcat(bot, group_id, since_ts)
        except Exception as exc:  # noqa: BLE001
            self._progress = "失败：抓取历史"
            await self._notify(umo, f"回填失败：{exc}")
            return

        raw = self._merge(db_items, napcat_items)
        if len(raw) > count:
            raw = raw[-count:]
        if not raw:
            self._progress = "空闲"
            await self._notify(umo, f"没有取到 {since_text} 之后的消息。")
            return
        logger.info(
            f"[lmem_backfill] 数据源：本地库 {len(db_items)} 条 + NapCat {len(napcat_items)} 条，"
            f"去重后 {len(raw)} 条"
        )
        self._dump_raw(group_id, raw)

        if not self_id:
            self_id = str(getattr(bot, "self_id", "") or "")
        messages = self._to_lm_messages(raw, group_id, umo, self_id, Message)
        if not messages:
            self._progress = "空闲"
            await self._notify(umo, "取到的消息都不含文本内容。")
            return

        chunks = [messages[i : i + CHUNK_SIZE] for i in range(0, len(messages), CHUNK_SIZE)]
        total = len(chunks)
        ok = failed = 0
        await self._notify(
            umo,
            f"共 {len(raw)} 条消息（{datetime.fromtimestamp(messages[0].timestamp, CST):%Y-%m-%d %H:%M}"
            f" ~ {datetime.fromtimestamp(messages[-1].timestamp, CST):%Y-%m-%d %H:%M}），"
            f"分 {total} 块开始写入。",
        )

        for idx, chunk in enumerate(chunks, start=1):
            try:
                content, metadata, importance = await processor.process_conversation(
                    messages=chunk, is_group_chat=True, persona_id=None
                )
                metadata = dict(metadata or {})
                metadata.update(
                    {
                        "memory_origin": "history_backfill",
                        "backfill_since": since_text,
                        "backfill_group_id": group_id,
                        "backfill_source": source,
                    }
                )
                metadata["importance"] = importance
                atoms = None
                classify = getattr(processor, "classify_atoms_from_metadata", None)
                if callable(classify):
                    atoms = classify(
                        metadata=metadata,
                        parent_importance=importance,
                        session_id=umo,
                        persona_id=None,
                    )
                await engine.add_memory(
                    content=content,
                    session_id=umo,
                    persona_id=None,
                    importance=importance,
                    metadata=metadata,
                    atoms=atoms,
                    preserve_create_time=False,
                    source_messages=serialize_source(chunk),
                )
                ok += 1
                self._progress = f"写入 {ok}/{total} 块（失败 {failed}）"
                logger.info(f"[lmem_backfill] 第 {idx}/{total} 块写入成功")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.error(f"[lmem_backfill] 第 {idx}/{total} 块写入失败：{exc}")

            if idx % 10 == 0 and idx < total:
                await self._notify(umo, f"回填进度：{ok}/{total} 块成功，失败 {failed}。")
            await asyncio.sleep(1)

        self._progress = f"完成：成功 {ok} 块，失败 {failed} 块"
        state = self._load_state()
        state.setdefault("done", {})[group_id] = {
            "at": time.time(),
            "messages": len(messages),
            "ok": ok,
            "failed": failed,
            "since": since_text,
        }
        self._save_state(state)
        await self._notify(
            umo,
            f"回填完成：消息 {len(messages)} 条 / {total} 块，成功 {ok} 块，失败 {failed} 块。",
        )
