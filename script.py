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

# Dagoverzicht
DAILY_AFTER_MINUTES = 1

# Reminder: 1x tussen 10:00-11:00
DAILY_REMINDER_HOUR = 10
DAILY_REMINDER_MINUTE = 0
DAILY_REMINDER_WINDOW_MINUTES = 60  # 10:00 t/m 11:00

# Daguitslag: 1x rond 23:00
DAILY_RESULTS_HOUR = 23
DAILY_RESULTS_MINUTE = 0
DAILY_RESULTS_WINDOW_MINUTES = 120  # 23:00 t/m 01:00

# Weekly overzicht: hele zondag (zodat je cron niet perfect hoeft te zijn)
WEEKLY_SEND_HOUR = 0
WEEKLY_SEND_MINUTE = 0
WEEKLY_SEND_WINDOW_MINUTES = 24 * 60  # hele zondag

DISCORD_MAX_LEN = 2000

# Calendar link
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
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""

            if DEBUG:
                print(f"❌ Discord HTTPError (attempt {attempt}/{max_retries}): {e.code}")
                print("❌ Discord headers:", dict(e.headers))
                if body:
                    print("❌ Discord body:", body)

            if e.code == 429:
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

            raise RuntimeError(f"Discord webhook failed ({e.code}). Body: {body}") from e

        except Exception as e:
            last_exc = e
            if DEBUG:
                print(f"❌ Discord onverwachte fout (attempt {attempt}/{max_retries}): {repr(e)}")
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


def fmt_time_local(dt):
    return f"{dt.strftime('%H:%M')} {dt.tzname()}"


def get_actual(ev):
    """
    Robuuste actual extractor (direct + nested + varianten).
    """
    direct_keys = (
        "actual", "Actual",
        "result", "Result",
        "value", "Value",
        "outcome", "Outcome",
        "reading", "Reading",
        "actualValue", "actual_value",
        "act", "Act",
    )
    for k in direct_keys:
        v = ev.get(k)
        if v is not None and v != "":
            return str(v)

    nested_parents = ("data", "meta", "stats", "values", "event")
    nested_keys = direct_keys
    for p in nested_parents:
        obj = ev.get(p)
        if isinstance(obj, dict):
            for k in nested_keys:
                v = obj.get(k)
                if v is not None and v != "":
                    return str(v)

    return None


def get_forecast(ev):
    """
    Robuuste forecast extractor (direct + nested + varianten).
    """
    direct_keys = (
        "forecast", "Forecast",
        "consensus", "Consensus",
        "estimate", "Estimate",
        "expected", "Expected",
    )
    for k in direct_keys:
        v = ev.get(k)
        if v is not None and v != "":
            return str(v)

    for p in ("data", "meta", "stats", "values", "event"):
        obj = ev.get(p)
        if isinstance(obj, dict):
            for k in direct_keys:
                v = obj.get(k)
                if v is not None and v != "":
                    return str(v)

    return None


def weekday_nl(dt: datetime):
    names = ["Maandag", "Dinsdag", "Woensdag", "Donderdag", "Vrijdag", "Zaterdag", "Zondag"]
    return names[dt.weekday()]


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

    # HIGH impact events van vandaag
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

    if DEBUG:
        print("DEBUG_NOW:", now.isoformat())
        print("DEBUG_WEEKDAY:", now.weekday(), "(zondag=6)")
        print("DEBUG_TODAYS_HIGH_COUNT:", len(todays_high))

    # --- Dagoverzicht ---
    if key not in daily_sent and now >= today_start + timedelta(minutes=DAILY_AFTER_MINUTES):
        if todays_high:
            blocks = [
                f"🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                for ev, dt in todays_high
            ]
        else:
            blocks = ["Geen HIGH impact events vandaag."]

        for msg in chunk_messages(blocks, f"📅 **Crypto Craft – HIGH impact ({display_date})**"):
            post_discord(msg)
        daily_sent.add(key)

    # --- 1x Dag reminder (10:00-11:00) ---
    reminder_key = f"reminder-{key}"
    reminder_time = today_start.replace(hour=DAILY_REMINDER_HOUR, minute=DAILY_REMINDER_MINUTE)
    if DEBUG:
        print("DEBUG_REMINDER_KEY:", reminder_key, "already_sent=", reminder_key in reminded)
        print("DEBUG_REMINDER_WINDOW:", reminder_time.isoformat(), "to",
              (reminder_time + timedelta(minutes=DAILY_REMINDER_WINDOW_MINUTES)).isoformat())

    if reminder_key not in reminded:
        if reminder_time <= now < reminder_time + timedelta(minutes=DAILY_REMINDER_WINDOW_MINUTES):
            if todays_high:
                blocks = [f"⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}" for ev, dt in todays_high]
                header = f"⏰ **Dag reminder – HIGH impact vandaag ({display_date})**"
                for msg in chunk_messages(blocks, header):
                    post_discord(msg)
            else:
                post_discord(f"⏰ **Dag reminder ({display_date})**\n\nGeen HIGH impact events vandaag.")
            reminded.add(reminder_key)

    # --- 1x Daguitslag (23:00) met Forecast + Actual ---
    results_key = f"results-{key}"
    results_time = today_start.replace(hour=DAILY_RESULTS_HOUR, minute=DAILY_RESULTS_MINUTE)
    if DEBUG:
        print("DEBUG_RESULTS_KEY:", results_key, "already_sent=", results_key in results_sent)
        print("DEBUG_RESULTS_WINDOW:", results_time.isoformat(), "to",
              (results_time + timedelta(minutes=DAILY_RESULTS_WINDOW_MINUTES)).isoformat())

    if results_key not in results_sent:
        if results_time <= now < results_time + timedelta(minutes=DAILY_RESULTS_WINDOW_MINUTES):
            if todays_high:
                blocks = []
                for ev, dt in todays_high:
                    forecast = get_forecast(ev)
                    actual = get_actual(ev)

                    forecast_text = forecast if forecast is not None else "Geen forecast"
                    actual_text = actual if actual is not None else "Nog geen uitslag"

                    blocks.append(
                        f"📌 **{ev.get('title')}**\n"
                        f"⏰ {fmt_time_local(dt)}\n"
                        f"📊 Forecast: {forecast_text}\n"
                        f"📈 Actual: {actual_text}"
                    )

                header = f"📊 **Daguitslag – HIGH impact ({display_date})**"
                for msg in chunk_messages(blocks, header):
                    post_discord(msg)
            else:
                post_discord(f"📊 **Daguitslag ({display_date})**\n\nGeen HIGH impact events vandaag.")
            results_sent.add(results_key)

    # --- Weekly overzicht met daglabels (hele zondag window) ---
    weekly_key = today_start.strftime("%G-%V")
    weekly_time = today_start.replace(hour=WEEKLY_SEND_HOUR, minute=WEEKLY_SEND_MINUTE)
    weekly_end = weekly_time + timedelta(minutes=WEEKLY_SEND_WINDOW_MINUTES)

    if DEBUG:
        print("DEBUG_WEEKLY_KEY:", weekly_key, "already_sent=", weekly_key in weekly_sent)
        print("DEBUG_WEEKLY_WINDOW:", weekly_time.isoformat(), "to", weekly_end.isoformat())

    if (
        weekly_key not in weekly_sent
        and now.weekday() == 6
        and weekly_time <= now < weekly_end
    ):
        week_start = today_start + timedelta(days=1)   # maandag
        week_end = week_start + timedelta(days=7)      # volgende maandag
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
            # groepeer per dag
            grouped = {}
            for ev, dt in upcoming_high:
                day_key = dt.date().isoformat()
                grouped.setdefault(day_key, {"dt": dt, "items": []})
                grouped[day_key]["items"].append((ev, dt))

            blocks = []
            for day_key in sorted(grouped.keys()):
                first_dt = grouped[day_key]["dt"]
                day_label = f"**— {weekday_nl(first_dt)} {first_dt.strftime('%d-%m-%Y')} —**"
                lines = [day_label]
                for ev, dt in grouped[day_key]["items"]:
                    lines.append(f"⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}")
                blocks.append("\n\n".join(lines))
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
