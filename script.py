import os
import json
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
JSON_URL = os.environ["CRYPTOCRAFT_JSON_URL"].strip()

# Default kanaal (fallback voor alles)
WEBHOOK_DEFAULT = os.environ["DISCORD_WEBHOOK_URL"].strip()

# Optioneel: aparte kanalen per type bericht
WEBHOOK_WEEKLY = os.environ.get("DISCORD_WEBHOOK_WEEKLY", "").strip()
WEBHOOK_REMINDER = os.environ.get("DISCORD_WEBHOOK_REMINDER", "").strip()
WEBHOOK_RESULTS = os.environ.get("DISCORD_WEBHOOK_RESULTS", "").strip()  # daguitslag kanaal

TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "Europe/Amsterdam").strip()
STATE_FILE = os.environ.get("STATE_FILE", "state.json").strip()
DEBUG = (os.environ.get("DEBUG", "1").strip() != "0")

DISCORD_MAX_LEN = 2000

# Links
CALENDAR_URL = "https://www.cryptocraft.com/calendar"
CALENDAR_LINK = f"[Crypto Craft]({CALENDAR_URL})"
CALENDAR_LABEL = "Calendar"

# Dagoverzicht (blijft)
DAILY_AFTER_MINUTES = 1

# Reminder: vanaf 10:00, mag op elk moment later op de dag nog (1x)
REMINDER_START_HOUR = 10
REMINDER_START_MINUTE = 0
REMINDER_WINDOW_MINUTES = 14 * 60  # 10:00 -> 00:00 (hele rest van de dag)

# Daguitslag: vanaf 23:00, mag ook nog in de nacht/ochtend erna (1x)
RESULTS_START_HOUR = 23
RESULTS_START_MINUTE = 0
RESULTS_WINDOW_MINUTES = 10 * 60  # 23:00 -> 09:00 (volgende ochtend)

# Weekoverzicht: zondagavond vanaf 18:00 tot middernacht (1x)
WEEKLY_SUNDAY_START_HOUR = 18
WEEKLY_SUNDAY_START_MINUTE = 0
WEEKLY_SUNDAY_WINDOW_MINUTES = 6 * 60  # 18:00 -> 00:00


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


def _host(url: str):
    try:
        return url.split("/")[2] if url else None
    except Exception:
        return None


print("CONFIG_WEBHOOK_DEFAULT_SET:", bool(WEBHOOK_DEFAULT))
print("CONFIG_WEBHOOK_DEFAULT_HOST:", _host(WEBHOOK_DEFAULT))
print("CONFIG_WEBHOOK_WEEKLY_SET:", bool(WEBHOOK_WEEKLY))
print("CONFIG_WEBHOOK_WEEKLY_HOST:", _host(WEBHOOK_WEEKLY))
print("CONFIG_WEBHOOK_REMINDER_SET:", bool(WEBHOOK_REMINDER))
print("CONFIG_WEBHOOK_REMINDER_HOST:", _host(WEBHOOK_REMINDER))
print("CONFIG_WEBHOOK_RESULTS_SET:", bool(WEBHOOK_RESULTS))
print("CONFIG_WEBHOOK_RESULTS_HOST:", _host(WEBHOOK_RESULTS))


def pick_webhook(kind: str) -> str:
    """
    kind: "daily" | "reminder" | "results" | "weekly"
    fallback: WEBHOOK_DEFAULT
    """
    mapping = {
        "daily": WEBHOOK_DEFAULT,
        "reminder": WEBHOOK_REMINDER or WEBHOOK_DEFAULT,
        "results": WEBHOOK_RESULTS or WEBHOOK_DEFAULT,
        "weekly": WEBHOOK_WEEKLY or WEBHOOK_DEFAULT,
    }
    return mapping.get(kind, WEBHOOK_DEFAULT)


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


def post_discord(content: str, webhook_url: str, max_retries: int = 8):
    if not webhook_url:
        raise RuntimeError("Webhook URL is not set for this message type")

    content = (content or "")[:DISCORD_MAX_LEN]
    payload = json.dumps({"content": content}).encode("utf-8")

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(
                webhook_url,
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


# -------- Results helpers (kleuren) --------

def _to_number(val):
    """
    Probeert strings als '0.4%', '1,2', '250K', '1.5M', '2B' om te zetten naar float.
    Returnt None als het niet kan.
    """
    if val is None:
        return None

    s = str(val).strip()
    if not s or s in {"-", "–", "n/a", "N/A", "NA", "None"}:
        return None

    s = s.replace(" ", "")

    mult = 1.0
    if s[-1:] in {"K", "k"}:
        mult = 1_000.0
        s = s[:-1]
    elif s[-1:] in {"M", "m"}:
        mult = 1_000_000.0
        s = s[:-1]
    elif s[-1:] in {"B", "b"}:
        mult = 1_000_000_000.0
        s = s[:-1]

    s = s.replace("%", "")

    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        s = s.replace(",", ".")

    if s.startswith("+"):
        s = s[1:]

    try:
        return float(s) * mult
    except Exception:
        return None


def compare_actual_forecast(actual, forecast):
    """
    🟢 = actual > forecast
    🔴 = actual < forecast
    🟡 = gelijk
    ⚪ = niet te vergelijken
    """
    a = _to_number(actual)
    f = _to_number(forecast)

    if a is None or f is None:
        return "⚪", "n.v.t."

    if abs(a - f) < 1e-12:
        return "🟡", "gelijk"
    if a > f:
        return "🟢", "boven forecast"
    return "🔴", "onder forecast"


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

    # --- Dagoverzicht (blijft) ---
    if key not in daily_sent and now >= today_start + timedelta(minutes=DAILY_AFTER_MINUTES):
        if todays_high:
            blocks = [
                f"🔥 Impact: HIGH\n⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}"
                for ev, dt in todays_high
            ]
        else:
            blocks = ["Geen HIGH impact events vandaag."]

        for msg in chunk_messages(blocks, f"📅 **Crypto Craft – HIGH impact ({display_date})**"):
            post_discord(msg, pick_webhook("daily"))
        daily_sent.add(key)

    # --- Reminder 1x per dag (vanaf 10:00, brede window) ---
    reminder_key = f"reminder-{key}"
    reminder_start = today_start.replace(hour=REMINDER_START_HOUR, minute=REMINDER_START_MINUTE)
    reminder_end = reminder_start + timedelta(minutes=REMINDER_WINDOW_MINUTES)

    if DEBUG:
        print("DEBUG_REMINDER_KEY:", reminder_key, "already_sent=", reminder_key in reminded)
        print("DEBUG_REMINDER_WINDOW:", reminder_start.isoformat(), "to", reminder_end.isoformat())

    if reminder_key not in reminded and reminder_start <= now < reminder_end:
        if todays_high:
            blocks = [f"⏰ {fmt_time_local(dt)}\n📌 {ev.get('title')}" for ev, dt in todays_high]
            header = f"⏰ **Dag reminder – HIGH impact vandaag ({display_date})**"
            for msg in chunk_messages(blocks, header):
                post_discord(msg, pick_webhook("reminder"))
        else:
            post_discord(
                f"⏰ **Dag reminder ({display_date})**\n\nGeen HIGH impact events vandaag.",
                pick_webhook("reminder"),
            )
        reminded.add(reminder_key)

    # --- Daguitslag met echte resultaten + kleuren (vanaf 23:00, brede window) ---
    yesterday_start = today_start - timedelta(days=1)
    yesterday_key = yesterday_start.strftime("%Y-%m-%d")
    yesterday_display = yesterday_start.strftime("%d-%m-%Y")

    # Als het tussen 00:00 en 09:00 is, sturen we de "daguitslag" voor gisteren.
    if now.hour < 9:
        results_effective_key = f"results-{yesterday_key}"
        results_effective_date = yesterday_display
        results_day_start = yesterday_start
        results_day_end = yesterday_start + timedelta(days=1)
        results_start = yesterday_start.replace(hour=RESULTS_START_HOUR, minute=RESULTS_START_MINUTE)
        results_end = results_start + timedelta(minutes=RESULTS_WINDOW_MINUTES)
    else:
        results_effective_key = f"results-{key}"
        results_effective_date = display_date
        results_day_start = today_start
        results_day_end = today_end
        results_start = today_start.replace(hour=RESULTS_START_HOUR, minute=RESULTS_START_MINUTE)
        results_end = results_start + timedelta(minutes=RESULTS_WINDOW_MINUTES)

    if DEBUG:
        print("DEBUG_RESULTS_KEY:", results_effective_key, "already_sent=", results_effective_key in results_sent)
        print("DEBUG_RESULTS_WINDOW:", results_start.isoformat(), "to", results_end.isoformat())

    if results_effective_key not in results_sent and results_start <= now < results_end:
        results_blocks = []

        for ev in events:
            if normalize_impact(ev) != "HIGH":
                continue

            dt = parse_dt_local(ev)
            if not dt or not (results_day_start <= dt < results_day_end):
                continue

            title = ev.get("title", "Onbekend event")
            actual = ev.get("actual", "–")
            forecast = ev.get("forecast", "–")
            previous = ev.get("previous", "–")

            emoji, verdict = compare_actual_forecast(actual, forecast)

            results_blocks.append(
                f"{emoji} **{title}**\n"
                f"⏰ {fmt_time_local(dt)}\n"
                f"Actual: {actual}\n"
                f"Forecast: {forecast}\n"
                f"Previous: {previous}\n"
                f"Resultaat: {verdict}"
            )

        header = f"📊 **Daguitslag – HIGH impact ({results_effective_date})**"

        if results_blocks:
            for msg in chunk_messages(results_blocks, header):
                post_discord(msg, pick_webhook("results"))
        else:
            post_discord(
                f"{header}\n\nNog geen resultaten beschikbaar.\n\n🔗 {CALENDAR_LABEL}: {CALENDAR_LINK}",
                pick_webhook("results"),
            )

        results_sent.add(results_effective_key)

    # --- Weekoverzicht: zondagavond (18:00 -> 00:00), met daglabels ---
    weekly_key = today_start.strftime("%G-%V")  # ISO-week van vandaag (zondag)
    weekly_start = today_start.replace(hour=WEEKLY_SUNDAY_START_HOUR, minute=WEEKLY_SUNDAY_START_MINUTE)
    weekly_end = weekly_start + timedelta(minutes=WEEKLY_SUNDAY_WINDOW_MINUTES)

    if DEBUG:
        print("DEBUG_WEEKLY_KEY:", weekly_key, "already_sent=", weekly_key in weekly_sent)
        print("DEBUG_WEEKLY_WINDOW:", weekly_start.isoformat(), "to", weekly_end.isoformat())

    if weekly_key not in weekly_sent and now.weekday() == 6 and weekly_start <= now < weekly_end:
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
            post_discord(msg, pick_webhook("weekly"))

        weekly_sent.add(weekly_key)

    # Save state
    state["reminded"] = list(reminded)
    state["daily_sent"] = list(daily_sent)
    state["results_sent"] = list(results_sent)
    state["weekly_sent"] = list(weekly_sent)
    save_state(state)


if __name__ == "__main__":
    main()
