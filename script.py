import os
import json
import hashlib
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

JSON_URL = os.environ["CRYPTOCRAFT_JSON_URL"]
WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "UTC").strip()  # bv. Europe/Amsterdam

STATE_FILE = "state.json"

REMINDER_MINUTES = 30
RUN_WINDOW_MINUTES = 5  # workflow draait elke 5 min -> window om reminders te pakken
DAILY_POST_HOUR = 0
DAILY_POST_MINUTE = 1   # "00:01"


def tzinfo():
    if TIMEZONE_NAME.upper() == "UTC":
        return timezone.utc
    if ZoneInfo is None:
        # fallback als zoneinfo niet beschikbaar is
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE_NAME)
    except Exception:
        return timezone.utc


TZ = tzinfo()


def fetch_json(url: str):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def post_discord(content: str, max_retries: int = 6):
    payload = json.dumps({"content": content}).encode("utf-8")

    for _ in range(max_retries):
        req = urllib.request.Request(
            WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                r.read()
            return
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 429:
                try:
                    retry_after = float(json.loads(body).get("retry_after", 1.0))
                except Exception:
                    retry_after = 1.0
                time.sleep(retry_after + 0.2)
                continue
            raise RuntimeError(f"Discord HTTP {e.code}: {body}") from e

    raise RuntimeError("Discord: te vaak rate limited, probeer later opnieuw.")


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"reminded": [], "daily_sent": []}
    except Exception:
        return {"reminded": [], "daily_sent": []}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_events(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for key in ("events", "data", "items", "calendar"):
            v = obj.get(key)
            if isinstance(v, list):
                return v
    return []


def normalize_impact(ev: dict) -> str:
    impact = ev.get("impact") or ev.get("importance") or ev.get("level") or ""
    return str(impact).strip().upper()


def parse_dt(ev: dict):
    dt_str = (ev.get("datetime") or ev.get("date") or "").strip()
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ)
    except Exception:
        return None


def event_uid(ev: dict) -> str:
    raw = json.dumps(ev, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def fmt_time(dt: datetime) -> str:
    # Toon tijd in jouw TIMEZONE (bv. Europe/Amsterdam)
    return dt.strftime("%H:%M")


def daily_key(today: datetime) -> str:
    return today.strftime("%Y-%m-%d")  # date in TIMEZONE


def make_daily_message(today: datetime, todays_events: list) -> str:
    # jouw gewenste format: alleen van vandaag
    header = f"📅 **Crypto Craft – HIGH impact (vandaag {today.strftime('%d-%m-%Y')})**"
    if not todays_events:
        return header + "\n\nGeen HIGH impact events vandaag."

    blocks = []
    for ev, dt in todays_events:
        title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
        blocks.append(f"🔥 Impact: HIGH\n⏰ {fmt_time(dt)} {TIMEZONE_NAME}\n📌 {title}")
    return header + "\n\n" + "\n\n".join(blocks)


def make_reminder_message(ev: dict, dt: datetime) -> str:
    title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
    return (
        "⏰ **REMINDER (30 min)**\n\n"
        f"🔥 Impact: HIGH\n"
        f"⏰ {fmt_time(dt)} {TIMEZONE_NAME}\n"
        f"📌 {title}"
    )


def main():
    now = datetime.now(TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    state = load_state()
    reminded = set(state.get("reminded", []))
    daily_sent = set(state.get("daily_sent", []))

    obj = fetch_json(JSON_URL)
    events = get_events(obj)

    # Filter: alleen HIGH + alleen vandaag
    todays_high = []
    for ev in events:
        if normalize_impact(ev) != "HIGH":
            continue
        dt = parse_dt(ev)
        if dt is None:
            continue
        if not (today_start <= dt < today_end):
            continue
        todays_high.append((ev, dt))

    # Sorteer op tijd
    todays_high.sort(key=lambda x: x[1])

    # 1) Daily post rond 00:01 (in jouw TIMEZONE)
    key = daily_key(now)
    in_daily_window = (now.hour == DAILY_POST_HOUR and DAILY_POST_MINUTE <= now.minute < DAILY_POST_MINUTE + RUN_WINDOW_MINUTES)
    if in_daily_window and key not in daily_sent:
        post_discord(make_daily_message(now, todays_high))
        daily_sent.add(key)

    # 2) Reminders: 30 min vooraf, binnen de 5-minuten window
    window_start = now
    window_end = now + timedelta(minutes=RUN_WINDOW_MINUTES)

    for ev, dt in todays_high:
        uid = event_uid(ev)
        remind_at = dt - timedelta(minutes=REMINDER_MINUTES)

        if uid in reminded:
            continue

        if window_start <= remind_at < window_end:
            post_discord(make_reminder_message(ev, dt))
            reminded.add(uid)

    state["reminded"] = sorted(reminded)
    state["daily_sent"] = sorted(daily_sent)
    save_state(state)


if __name__ == "__main__":
    main()
