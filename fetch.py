import argparse
import asyncio
import html
import json
import os
import time
from pathlib import Path
from typing import Any

import aiohttp


BASE_DIR = Path(__file__).resolve().parent
USERS_FILE = BASE_DIR / "users.json"
BLACKLIST_FILE = BASE_DIR / "blacklist.json"
STATE_FILE = BASE_DIR / "forwarded.json"
MAX_STATE_SIZE = 2000
TG_MAX_RETRIES = 5


class TelegramAPIError(RuntimeError):
    def __init__(self, method: str, status: int, raw: str, description: str = "") -> None:
        super().__init__(f"Telegram {method} failed ({status}): {raw}")
        self.method = method
        self.status = status
        self.raw = raw
        self.description = description

POLICY_ALL = "retweets_and_media"
POLICY_MEDIA_ONLY = "media_only"
POLICY_RETWEET_MEDIA_ONLY = "retweet_media_only"

POLICY_ALIASES = {
    "retweets_and_media": POLICY_ALL,
    "all": POLICY_ALL,
    "media_only": POLICY_MEDIA_ONLY,
    "retweet_media_only": POLICY_RETWEET_MEDIA_ONLY,
}


def norm_handle(value: str) -> str:
    return value.strip().lstrip("@").strip().lower()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip()
        if key and key not in os.environ:
            os.environ[key] = val


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_users(raw: Any) -> list[dict[str, str]]:
    users: list[dict[str, str]] = []
    if not isinstance(raw, list):
        return users

    for item in raw:
        username = ""
        strategy = POLICY_ALL
        if isinstance(item, str):
            username = item
        elif isinstance(item, dict):
            username = str(item.get("username", "")).strip()
            strategy = str(item.get("strategy", POLICY_ALL)).strip() or POLICY_ALL
        if not username:
            continue
        handle = norm_handle(username)
        if not handle:
            continue
        strategy = POLICY_ALIASES.get(strategy.lower(), POLICY_ALL)
        users.append({"username": handle, "strategy": strategy})
    return users


def parse_blacklist(raw: Any) -> set[str]:
    result: set[str] = set()
    if not isinstance(raw, list):
        return result
    for name in raw:
        if not isinstance(name, str):
            continue
        handle = norm_handle(name)
        if handle:
            result.add(handle)
    return result


async def fetch_user_posts(screen_name: str, max_count: int | None) -> list[dict[str, Any]]:
    cmd = ["pdm", "run", "twitter", "user-posts", screen_name, "--json"]
    if max_count is not None:
        cmd.extend(["--max", str(max_count)])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(f"twitter user-posts failed for @{screen_name}: {err}")

    try:
        payload = json.loads(stdout.decode("utf-8", errors="ignore"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from twitter CLI for @{screen_name}") from exc

    if not payload.get("ok"):
        raise RuntimeError(f"twitter CLI returned ok=false for @{screen_name}")

    data = payload.get("data", [])
    if not isinstance(data, list):
        return []
    return data


def has_media(tweet: dict[str, Any]) -> bool:
    media = tweet.get("media")
    return isinstance(media, list) and len(media) > 0


def should_forward(tweet: dict[str, Any], strategy: str) -> bool:
    is_retweet = bool(tweet.get("isRetweet"))
    media = has_media(tweet)

    if strategy == POLICY_MEDIA_ONLY:
        return media and (not is_retweet)
    if strategy == POLICY_RETWEET_MEDIA_ONLY:
        return is_retweet and media
    return is_retweet or media


def in_blacklist(tweet: dict[str, Any], blacklist: set[str]) -> bool:
    author = tweet.get("author") or {}
    author_name = norm_handle(str(author.get("screenName", "")))
    retweeted_by = norm_handle(str(tweet.get("retweetedBy", "")))
    return author_name in blacklist or retweeted_by in blacklist


def build_caption(tweet: dict[str, Any]) -> str:
    text = str(tweet.get("text", "")).strip()
    author = tweet.get("author") or {}
    author_name = norm_handle(str(author.get("screenName", "")))
    reposter = norm_handle(str(tweet.get("retweetedBy", "")))

    escaped_text = html.escape(text)
    if author_name:
        author_link = f"<a href=\"https://x.com/{author_name}\">@{author_name}</a>"
        lines = [f"{escaped_text} by {author_link}"]
    else:
        lines = [escaped_text]
    if bool(tweet.get("isRetweet")) and reposter:
        reposter_link = f"<a href=\"https://x.com/{reposter}\">@{reposter}</a>"
        lines.append(f"(由 {reposter_link} 转发)")
    return "\n".join(line for line in lines if line)


async def tg_call(session: aiohttp.ClientSession, token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    for attempt in range(1, TG_MAX_RETRIES + 1):
        async with session.post(url, json=payload) as resp:
            raw = await resp.text()
            result: dict[str, Any]
            try:
                result = json.loads(raw)
            except json.JSONDecodeError:
                result = {"ok": False, "description": raw}

            if resp.status < 400 and result.get("ok"):
                return result

            retry_after = None
            if isinstance(result, dict):
                params = result.get("parameters")
                if isinstance(params, dict):
                    retry_after = params.get("retry_after")

            if resp.status == 429 and retry_after is not None and attempt < TG_MAX_RETRIES:
                wait_s = int(retry_after)
                print(f"[限流] Telegram {method} 命中 429，{wait_s}s 后重试 (attempt {attempt}/{TG_MAX_RETRIES})")
                await asyncio.sleep(wait_s)
                continue

            description = ""
            if isinstance(result, dict):
                description = str(result.get("description", ""))
            if (
                resp.status == 400
                and "failed to get HTTP URL content" in description
                and attempt < TG_MAX_RETRIES
            ):
                wait_s = min(2 * attempt, 10)
                print(f"[重试] Telegram {method} 拉取媒体失败，{wait_s}s 后重试 (attempt {attempt}/{TG_MAX_RETRIES})")
                await asyncio.sleep(wait_s)
                continue

            if resp.status >= 500 and attempt < TG_MAX_RETRIES:
                wait_s = min(2 ** attempt, 30)
                print(f"[重试] Telegram {method} 服务异常({resp.status})，{wait_s}s 后重试")
                await asyncio.sleep(wait_s)
                continue

            raise TelegramAPIError(method=method, status=resp.status, raw=raw, description=description)

    raise TelegramAPIError(method=method, status=599, raw="failed after retries", description="failed after retries")


async def send_tweet_to_telegram(
    session: aiohttp.ClientSession,
    token: str,
    chat_id: str,
    tweet: dict[str, Any],
) -> None:
    caption = build_caption(tweet)
    media = tweet.get("media") or []
    valid_media = [m for m in media if isinstance(m, dict) and str(m.get("url", "")).strip()]

    if valid_media:
        if len(valid_media) == 1:
            m = valid_media[0]
            m_type = str(m.get("type", "")).lower()
            media_url = str(m.get("url", "")).strip()
            if m_type == "video":
                try:
                    await tg_call(
                        session,
                        token,
                        "sendVideo",
                        {
                            "chat_id": chat_id,
                            "video": media_url,
                            "caption": caption,
                            "parse_mode": "HTML",
                        },
                    )
                except TelegramAPIError as exc:
                    if exc.status == 400 and "wrong type of the web page content" in exc.description:
                        fallback_text = f"{caption}\n\n[video url] {html.escape(media_url)}"
                        await tg_call(
                            session,
                            token,
                            "sendMessage",
                            {
                                "chat_id": chat_id,
                                "text": fallback_text,
                                "disable_web_page_preview": False,
                                "parse_mode": "HTML",
                            },
                        )
                    else:
                        raise
            else:
                await tg_call(
                    session,
                    token,
                    "sendPhoto",
                    {
                        "chat_id": chat_id,
                        "photo": media_url,
                        "caption": caption,
                        "parse_mode": "HTML",
                    },
                )
            return

        media_group: list[dict[str, Any]] = []
        for i, m in enumerate(valid_media):
            m_type = str(m.get("type", "")).lower()
            tg_type = "video" if m_type == "video" else "photo"
            entry: dict[str, Any] = {
                "type": tg_type,
                "media": str(m.get("url", "")).strip(),
            }
            if i == 0:
                entry["caption"] = caption
                entry["parse_mode"] = "HTML"
            media_group.append(entry)

        await tg_call(
            session,
            token,
            "sendMediaGroup",
            {
                "chat_id": chat_id,
                "media": media_group,
            },
        )
        return

    await tg_call(
        session,
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": caption or "(empty tweet)",
            "disable_web_page_preview": True,
            "parse_mode": "HTML",
        },
    )


def clip_state(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(records) <= MAX_STATE_SIZE:
        return records
    return records[-MAX_STATE_SIZE:]


def prune_records_by_blacklist(records: list[dict[str, Any]], blacklist: set[str]) -> tuple[list[dict[str, Any]], int]:
    if not blacklist:
        return records, 0

    kept: list[dict[str, Any]] = []
    removed = 0
    for row in records:
        if not isinstance(row, dict):
            continue
        author_handle = norm_handle(str(row.get("author_handle", "")))
        reposter_handle = norm_handle(str(row.get("reposter_handle", "")))
        if author_handle in blacklist or reposter_handle in blacklist:
            removed += 1
            continue
        kept.append(row)
    return kept, removed


def persist_state(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], set[str]]:
    clipped = clip_state(records)
    save_json_file(STATE_FILE, {"records": clipped})
    ids = {
        str(row.get("tweet_id", "")).strip()
        for row in clipped
        if isinstance(row, dict) and str(row.get("tweet_id", "")).strip()
    }
    return clipped, ids


async def main() -> None:
    total_start = time.perf_counter()
    parser = argparse.ArgumentParser(
        description="读取 Twitter 用户推文，按策略筛选后转发到 Telegram 频道，并维护去重状态。"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=50,
        help="抓取条数：传给 twitter user-posts 的 --max 参数（默认 50）。",
    )
    parser.add_argument(
        "--user",
        type=str,
        default="",
        help="仅处理指定用户（支持带或不带 @）。",
    )
    args = parser.parse_args()

    load_env_file(BASE_DIR / ".env")

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_channel = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
    if not tg_token or not tg_channel:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL_ID in .env")

    print("[启动] 开始执行抓取与转发流程")
    print(f"[配置] 目标频道: {tg_channel}")

    users = parse_users(load_json_file(USERS_FILE, []))
    if not users:
        raise RuntimeError("users.json is empty or invalid")

    if args.user.strip():
        only_user = norm_handle(args.user)
        users = [u for u in users if u.get("username") == only_user]
        if not users:
            raise RuntimeError(f"users.json 中未找到指定用户: @{only_user}")
        print(f"[过滤] 仅处理指定用户: @{only_user}")
    print(f"[配置] 用户数量: {len(users)}")

    blacklist = parse_blacklist(load_json_file(BLACKLIST_FILE, []))
    print(f"[配置] 黑名单数量: {len(blacklist)}")

    state_raw = load_json_file(STATE_FILE, {"records": []})
    state_records = state_raw.get("records", []) if isinstance(state_raw, dict) else []
    if not isinstance(state_records, list):
        state_records = []

    state_records, removed_count = prune_records_by_blacklist(state_records, blacklist)
    if removed_count > 0:
        print(f"[状态] 因黑名单更新移除了 {removed_count} 条去重记录")
        state_records, _ = persist_state(state_records)

    seen_ids = {
        str(row.get("tweet_id", "")).strip()
        for row in state_records
        if isinstance(row, dict) and str(row.get("tweet_id", "")).strip()
    }

    fetched_count = 0
    selected_count = 0
    sent_count = 0
    failed_count = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for user in users:
            user_start = time.perf_counter()
            username = user["username"]
            strategy = user["strategy"]
            print(f"[用户] @{username} strategy={strategy}")

            limit = args.max

            fetch_start = time.perf_counter()
            tweets = await fetch_user_posts(username, limit)
            fetch_elapsed = time.perf_counter() - fetch_start
            fetched_count += len(tweets)
            print(f"[抓取] @{username} 获取到 {len(tweets)} 条, 用时 {fetch_elapsed:.2f}s")

            for tweet in tweets:
                tweet_id = str(tweet.get("id", "")).strip()
                if not tweet_id:
                    continue
                if tweet_id in seen_ids:
                    continue
                if not should_forward(tweet, strategy):
                    continue
                if in_blacklist(tweet, blacklist):
                    continue

                selected_count += 1
                try:
                    await send_tweet_to_telegram(session, tg_token, tg_channel, tweet)
                    print(f"[发送] tweet_id={tweet_id} 已发送")
                except Exception as exc:
                    failed_count += 1
                    print(f"[错误] tweet_id={tweet_id} 发送失败，已跳过: {exc}")
                    continue

                seen_ids.add(tweet_id)
                state_records.append(
                    {
                        "tweet_id": tweet_id,
                        "source_user": username,
                        "author_handle": norm_handle(str((tweet.get("author") or {}).get("screenName", ""))),
                        "reposter_handle": norm_handle(str(tweet.get("retweetedBy", ""))),
                        "is_retweet": bool(tweet.get("isRetweet")),
                        "has_media": has_media(tweet),
                        "created_at": str(tweet.get("createdAtISO", "")),
                    }
                )
                state_records, seen_ids = persist_state(state_records)
                sent_count += 1

            user_elapsed = time.perf_counter() - user_start
            print(f"[用户完成] @{username} 处理用时 {user_elapsed:.2f}s")

    state_records, _ = persist_state(state_records)
    print(f"[状态] 去重记录已保存到 {STATE_FILE.name}, 当前 {len(state_records)} 条")
    total_elapsed = time.perf_counter() - total_start
    print(f"[完成] 总用时 {total_elapsed:.2f}s")

    print(
        json.dumps(
            {
                "ok": True,
                "users": len(users),
                "fetched": fetched_count,
                "selected": selected_count,
                "sent": sent_count,
                "failed": failed_count,
                "state_size": len(state_records),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
