import os
import json
import hashlib
import urllib.request
import urllib.error
import time
import random
import ssl
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ---- Secrets / Config ----
JSON_URL = os.environ["CRYPTOCRAFT_JSON_URL"]

# Strip is belangrijk: GitHub Secrets kunnen onbedoeld \n/spaties bevatten
WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"].strip()

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
print("CONFIG_WEBHOOK_SET:", bool(WEBHOOK))
print("CONFIG_WEBHOOK_HOST:", (WEBHOOK.split("/")[2] if WEBHOOK else None))


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


def post_discord(content: str, max_retries: int = 8):
    """
    Post a message to Discord via webhook.
    - Logs useful info on HTTP errors (especially 403).
    - Retries on 429 with backoff.
    """
    if not WEBHOOK:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set")

    # Zorg dat we nooit over Discord limiet gaan
    content = (content or "")[:DISCORD_MAX_LEN]

    payload = json.dumps({"content": content}).encode("utf-8")

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(
                WEBHOOK,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "github-actions-discord-webhook/1.0",
                },
                method="POST",
            )

            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                if DEBUG:
                    print(f"✅ Discord webhook OK (status={resp.status})")
                return

        except urllib.error.HTTPError as e:
            last_exc = e

            # Probeer body te lezen; Discord vertelt hier meestal precies de reden
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""

            if DEBUG:
                print(f"❌ Discord HTTPError (attempt {attempt}/{max_retries}): {e.code}")
                print("❌ Discord headers:", dict(e.headers))
                if body:
                    print("❌ Discord body:", body)

            # Rate limit -> retry met backoff
            if e.code == 429:
                # Discord kan Retry-After header sturen (seconden)
                retry_after = None
                try:
                    retry_after = e.headers.get("Retry-After")
                except Exception:
                    retry_after = None

                try:
                    wait_s = float(retry_after) if retry_after else (1.5 + attempt * 0.75 + random.random())
                except Exception:
                    wait_s = 2.0 + attempt

                time.sleep(min(wait_s, 30))
                continue

            # Voor 403/400/401 etc: niet blijven retrypen, maar falen met duidelijke info
            raise RuntimeError(f"Discord webhook failed ({e.code}). Body: {body}") from e

        except Exception as e:
            last_exc = e
            if DEBUG:
                print(f"❌ Discord onverwachte fout (attempt {attempt}/{max_retries}): {repr(e)}")
            # Kleine backoff voor tijdelijke netwerk issues
            time.sleep(min(1.0 + attempt * 0.5, 10))

    raise RuntimeError(f"Discord webhook failed after {max_retries} attempts: {repr(last_exc)}") from last_exc


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
    raw = json.dumps(
        {
            "title": ev.get("title"),
            "datetime": ev.get("datetime") or ev.get("date"),
        },
        sort_keys=True,
    )
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
        for msg in chunk_messages(blocks, f"📅 **Crypto Craft – HIGH impact ({display_date})**"):
            post_discord(msg)
        daily_sent.add(key)

    for ev, dt in todays_high:
        uid = event_uid(ev)
        if uid not in reminded:
            remind_at = dt - timedelta(minutes=REMINDER_MINUTES)
            if remind_at <= now < remind_at + timedelta(minutes=RUN_WINDOW_MINUTES):
                post_discord(
                    f"⏰ **REMINDER (30 min)**\n\n🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                )
                reminded.add(uid)

    for ev, dt in todays_high:
        uid = event_uid(ev)
        actual = get_actual(ev)
        if uid not in results_sent and actual:
            result_at = dt + timedelta(minutes=RESULT_DELAY_MINUTES)
            if result_at <= now < result_at + timedelta(minutes=RESULT_WINDOW_MINUTES):
                post_discord(
                    f"📊 **RESULT – HIGH impact**\n\n📌 {ev.get('title')}\n⏰ {fmt_time_local(dt)}\n\n📈 Actual: {actual}"
                )
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
        for msg in chunk_messages(blocks, header):
            post_discord(msg)
        weekly_sent.add(weekly_key)

    state["reminded"] = list(reminded)
    state["daily_sent"] = list(daily_sent)
    state["results_sent"] = list(results_sent)
    state["weekly_sent"] = list(weekly_sent)
    save_state(state)


if __name__ == "__main__":
    main()
