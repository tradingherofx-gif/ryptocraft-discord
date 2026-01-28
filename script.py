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

# ---- Secrets / Config ----
JSON_URL = os.environ["CRYPTOCRAFT_JSON_URL"]
WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]

TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "Europe/Amsterdam").strip()
STATE_FILE = os.environ.get("STATE_FILE", "state.json").strip()

REMINDER_MINUTES = 30
RUN_WINDOW_MINUTES = 120

DAILY_AFTER_MINUTES = 1

RESULT_DELAY_MINUTES = 5
RESULT_WINDOW_MINUTES = 120
WEEKLY_AFTER_MINUTES = 23 * 60 + 59
WEEKLY_WINDOW_MINUTES = 120

DISCORD_MAX_LEN = 2000

# Calendar link (ALLEEN in daily bericht)
CALENDAR_URL = "https://www.cryptocraft.com/calendar"
CALENDAR_LINK = f"[Crypto Craft]({CALENDAR_URL})"
CALENDAR_LABEL = "Calendar"

DEBUG = (os.environ.get("DEBUG", "1").strip() != "0")


def tzinfo():
    if TIMEZONE_NAME.upper() == "UTC":
        return timezone.utc
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE_NAME)
    except Exception:
        return timezone.utc


TZ = tzinfo()

print("CONFIG_TIMEZONE_NAME:", TIMEZONE_NAME)
print("CONFIG_TZ:", TZ)
print("CONFIG_STATE_FILE:", STATE_FILE)
print("CONFIG_DEBUG:", DEBUG)


def fetch_json(url: str):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def post_discord(content: str, max_retries: int = 8) -> bool:
    payload = json.dumps({"content": content}).encode("utf-8")
    try:
        for _ in range(max_retries):
            try:
                req = urllib.request.Request(
                    WEBHOOK,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30):
                    return True
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(1.5)
                    continue
                print(f"DISCORD_POST_FAILED: HTTP {e.code}")
                return False
            except Exception as e:
                print(f"DISCORD_POST_FAILED: {e}")
                return False
    except Exception as e:
        print(f"DISCORD_POST_FATAL: {e}")
        return False
    return False


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            state.setdefault("reminded", [])
            state.setdefault("daily_sent", [])
            state.setdefault("results_sent", [])
            state.setdefault("weekly_sent", [])
            return state
    except Exception:
        return {"reminded": [], "daily_sent": [], "results_sent": [], "weekly_sent": []}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_events(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("events", "data", "items", "calendar"):
            if isinstance(obj.get(k), list):
                return obj[k]
    return []


def normalize_impact(ev):
    return str(ev.get("impact", "")).upper()


def parse_dt_local(ev):
    dt_str = ev.get("datetime") or ev.get("date")
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def event_uid(ev):
    raw = json.dumps({
        "title": ev.get("title"),
        "datetime": ev.get("datetime") or ev.get("date"),
    }, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def fmt_time_local(dt):
    return f"{dt.strftime('%H:%M')} {dt.tzname()}"


def get_actual(ev):
    for k in ("actual", "result", "value", "outcome"):
        if ev.get(k):
            return str(ev[k])
    return None


def chunk_messages(blocks, header):
    footer = f"\n\n🔗 {CALENDAR_LABEL}: {CALENDAR_LINK}"
    msgs, current = [], header
    for b in blocks:
        test = current + "\n\n" + b + footer
        if len(test) > DISCORD_MAX_LEN:
            msgs.append(current + footer)
            current = header + "\n\n" + b
        else:
            current += "\n\n" + b
    msgs.append(current + footer)
    return msgs


def main():
    now = datetime.now(TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    state = load_state()
    reminded = set(state["reminded"])
    daily_sent = set(state["daily_sent"])
    results_sent = set(state["results_sent"])
    weekly_sent = set(state["weekly_sent"])

    events = get_events(fetch_json(JSON_URL))

    todays_high = []
    for ev in events:
        if normalize_impact(ev) != "HIGH":
            continue
        dt = parse_dt_local(ev)
        if dt and today_start <= dt < today_end:
            todays_high.append((ev, dt))

    todays_high.sort(key=lambda x: x[1])

    key = today_start.strftime("%Y-%m-%d")
    display_date = now.strftime("%d-%m-%Y")
    if key not in daily_sent and now >= today_start + timedelta(minutes=DAILY_AFTER_MINUTES):
        if todays_high:
            blocks = [
                f"🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                for ev, dt in todays_high
            ]
        else:
            blocks = ["Geen HIGH impact events vandaag."]
        if not blocks:
            blocks = ["Geen HIGH impact events vandaag."]
        posted_all = True
        for msg in chunk_messages(blocks, f"📅 **Crypto Craft – HIGH impact ({display_date})**"):
            if not post_discord(msg):
                posted_all = False
                break
        if posted_all:
            daily_sent.add(key)

    for ev, dt in todays_high:
        uid = event_uid(ev)
        if uid not in reminded:
            remind_at = dt - timedelta(minutes=REMINDER_MINUTES)
            if remind_at <= now < remind_at + timedelta(minutes=RUN_WINDOW_MINUTES):
                if post_discord(
                    f"⏰ **REMINDER (30 min)**\n\n🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                ):
                    reminded.add(uid)

    for ev, dt in todays_high:
        uid = event_uid(ev)
        actual = get_actual(ev)
        if uid not in results_sent and actual:
            result_at = dt + timedelta(minutes=RESULT_DELAY_MINUTES)
            if result_at <= now < result_at + timedelta(minutes=RESULT_WINDOW_MINUTES):
                if post_discord(
                    f"📊 **RESULT – HIGH impact**\n\n📌 {ev.get('title')}\n⏰ {fmt_time_local(dt)}\n\n📈 Actual: {actual}"
                ):
                    results_sent.add(uid)

    weekly_key = today_start.strftime("%G-%V")
    if (
        weekly_key not in weekly_sent
        and now.weekday() == 6
        and now >= today_start + timedelta(minutes=WEEKLY_AFTER_MINUTES)
        and now < today_start + timedelta(minutes=WEEKLY_AFTER_MINUTES + WEEKLY_WINDOW_MINUTES)
    ):
        week_start = today_start + timedelta(days=1)
        week_end = week_start + timedelta(days=7)
        week_start_label = week_start.strftime("%d-%m-%Y")
        week_end_label = (week_end - timedelta(days=1)).strftime("%d-%m-%Y")
        upcoming_high = []
        for ev in events:
            if normalize_impact(ev) != "HIGH":
                continue
            dt = parse_dt_local(ev)
            if dt and week_start <= dt < week_end:
                upcoming_high.append((ev, dt))
        upcoming_high.sort(key=lambda x: x[1])
        if upcoming_high:
            blocks = [
                f"🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                for ev, dt in upcoming_high
            ]
        else:
            blocks = ["Geen HIGH impact events aankomende week."]
        header = (
            f"📅 **Crypto Craft – HIGH impact weekoverzicht "
            f"({week_start_label} – {week_end_label})**"
        )
        posted_all = True
        for msg in chunk_messages(blocks, header):
            if not post_discord(msg):
                posted_all = False
                break
        if posted_all:
            weekly_sent.add(weekly_key)

    state["reminded"] = list(reminded)
    state["daily_sent"] = list(daily_sent)
    state["results_sent"] = list(results_sent)
    state["weekly_sent"] = list(weekly_sent)
    save_state(state)


if __name__ == "__main__":
    main()
