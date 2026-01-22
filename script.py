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

# Default naar Europe/Amsterdam
TIMEZONE_NAME = (os.environ.get("TIMEZONE") or "Europe/Amsterdam").strip()

# State file (handig als je dit later wil verplaatsen)
STATE_FILE = os.environ.get("STATE_FILE", "state.json").strip()

# Reminder instellingen
REMINDER_MINUTES = 30

# Hoe lang na "remind_at" we nog mogen versturen als Actions te laat is
# Zet dit ruim, GitHub Actions schedule kan vaak 20-40 min later starten
RUN_WINDOW_MINUTES = 120

# Daily post: 1× per dag NA 00:01 lokale tijd
DAILY_AFTER_MINUTES = 1

# Result post (alleen als 'actual/result/value/outcome' bestaat in de feed)
RESULT_DELAY_MINUTES = 5    # post resultaat 5 min na event-tijd
RESULT_WINDOW_MINUTES = 120 # tot 120 min erna nog toegestaan

# Discord limiet
DISCORD_MAX_LEN = 2000

# Link: alleen in daily/standaard bericht (niet in reminders / results)
CALENDAR_URL = "https://www.cryptocraft.com/calendar"
CALENDAR_LINK = f"[Crypto Craft Calendar]({CALENDAR_URL})"
CALENDAR_LABEL = "Calendar"  # Engels label

# Debug: zet op "0" om uit te zetten
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
            "Accept-Language": "en-US,en;q=0.9",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Fetch JSON failed (HTTP {e.code}): {body}") from e
    except Exception as e:
        raise RuntimeError(f"Fetch JSON failed: {e}") from e


def post_discord(content: str, max_retries: int = 8):
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
                # Discord rate limit
                try:
                    retry_after = float(json.loads(body).get("retry_after", 1.0))
                except Exception:
                    retry_after = 1.0
                time.sleep(retry_after + 0.25)
                continue
            raise RuntimeError(f"Discord HTTP {e.code}: {body}") from e

    raise RuntimeError("Discord: te vaak rate limited, probeer later opnieuw.")


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            state.setdefault("reminded", [])
            state.setdefault("daily_sent", [])
            state.setdefault("results_sent", [])
            return state
    except FileNotFoundError:
        return {"reminded": [], "daily_sent": [], "results_sent": []}
    except Exception:
        return {"reminded": [], "daily_sent": [], "results_sent": []}


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


def parse_dt_local(ev: dict):
    """
    Parse ISO8601 datetime from the feed and return it in local TZ (Europe/Amsterdam).
    Assumptie: als tz ontbreekt, interpreteren we het als UTC.
    """
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
    """
    Stable UID: hash alleen velden die (meestal) niet wijzigen tussen fetches.
    """
    stable = {
        "id": ev.get("id") or ev.get("uid") or ev.get("event_id"),
        "title": ev.get("title") or ev.get("event") or ev.get("name") or "Event",
        "datetime": (ev.get("datetime") or ev.get("date") or "").strip(),
    }
    raw = json.dumps(stable, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def fmt_time_local(dt: datetime) -> str:
    """
    Returns 'HH:MM CET/CEST' (of fallback op offset mapping).
    """
    local_dt = dt.astimezone(TZ)
    tzname = local_dt.tzname() or ""
    if tzname in ("CET", "CEST"):
        label = tzname
    else:
        offset = local_dt.strftime("%z")
        label = "CEST" if offset == "+0200" else "CET"
    return f"{local_dt.strftime('%H:%M')} {label}"


def daily_key(today_start: datetime) -> str:
    return today_start.strftime("%Y-%m-%d")


def get_actual(ev: dict):
    for k in ("actual", "result", "value", "outcome"):
        v = ev.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def chunk_messages(blocks: list[str], header: str, include_calendar_link: bool) -> list[str]:
    footer = f"\n\n🔗 {CALENDAR_LABEL}: {CALENDAR_LINK}" if include_calendar_link else ""
    msgs = []
    current = header

    for block in blocks:
        candidate = current + "\n\n" + block + footer
        if len(candidate) > DISCORD_MAX_LEN:
            msgs.append(current + footer)
            current = header + "\n\n" + block
        else:
            current = current + "\n\n" + block

    msgs.append(current + footer)
    return msgs


def make_daily_messages(today_start: datetime, todays_events: list) -> list[str]:
    header = f"📅 **Crypto Craft – HIGH impact (vandaag {today_start.strftime('%d-%m-%Y')})**"

    if not todays_events:
        return [header + f"\n\nGeen HIGH impact events vandaag.\n\n🔗 {CALENDAR_LABEL}: {CALENDAR_LINK}"]

    blocks = []
    for ev, dt in todays_events:
        title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
        blocks.append(
            "🔥 Impact: HIGH\n"
            f"⏰ {fmt_time_local(dt)}\n"
            f"📌 {title}"
        )

    return chunk_messages(blocks, header, include_calendar_link=True)


def make_reminder_message(ev: dict, dt: datetime) -> str:
    title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
    return (
        "⏰ **REMINDER (30 min)**\n\n"
        "🔥 Impact: HIGH\n"
        f"⏰ {fmt_time_local(dt)}\n"
        f"📌 {title}"
    )


def make_result_message(ev: dict, dt: datetime) -> str:
    title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
    actual = get_actual(ev)
    forecast = (ev.get("forecast") or "").strip()
    previous = (ev.get("previous") or "").strip()

    lines = [
        "📊 **RESULT – HIGH impact**",
        "",
        f"📌 {title}",
        f"⏰ {fmt_time_local(dt)}",
        "",
        f"📈 Actual: {actual}",
    ]
    if forecast:
        lines.append(f"📊 Forecast: {forecast}")
    if previous:
        lines.append(f"📉 Previous: {previous}")
    return "\n".join(lines)


def main():
    now = datetime.now(TZ)

    # Today range in local TZ
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Load state
    state = load_state()
    reminded = set(state.get("reminded", []))
    daily_sent = set(state.get("daily_sent", []))
    results_sent = set(state.get("results_sent", []))

    # Fetch events
    obj = fetch_json(JSON_URL)
    events = get_events(obj)

    # Filter: HIGH impact + today only
    todays_high = []
    for ev in events:
        if normalize_impact(ev) != "HIGH":
            continue

        dt = parse_dt_local(ev)
        if dt is None:
            continue

        if not (today_start <= dt < today_end):
            continue

        todays_high.append((ev, dt))

    # Sort by time
    todays_high.sort(key=lambda x: x[1])

    # DEBUG: laat zien wat het script "ziet"
    if DEBUG:
        print("NOW_LOCAL:", now.isoformat())
        print("TODAY_START:", today_start.isoformat(), "TODAY_END:", today_end.isoformat())
        print("TODAYS_HIGH_COUNT:", len(todays_high))
        for ev, dt in todays_high[:10]:
            title = ev.get("title") or ev.get("event") or ev.get("name")
            remind_at = dt - timedelta(minutes=REMINDER_MINUTES)
            print(
                "EVENT:", title,
                "| EVENT_LOCAL:", dt.isoformat(),
                "| REMIND_AT:", remind_at.isoformat(),
            )

    # 1) Daily post: 1× per day after 00:01 local time
    key = daily_key(today_start)
    if key not in daily_sent and now >= (today_start + timedelta(minutes=DAILY_AFTER_MINUTES)):
        for msg in make_daily_messages(today_start, todays_high):
            post_discord(msg)
        daily_sent.add(key)

    # 2) Reminders: catch-up proof window
    for ev, dt in todays_high:
        uid = event_uid(ev)
        if uid in reminded:
            continue

        remind_at = dt - timedelta(minutes=REMINDER_MINUTES)
        if remind_at <= now < (remind_at + timedelta(minutes=RUN_WINDOW_MINUTES)):
            if DEBUG:
                print("SENDING_REMINDER_FOR_UID:", uid, "AT_NOW:", now.isoformat())
            post_discord(make_reminder_message(ev, dt))
            reminded.add(uid)

    # 3) Result posts (alleen als 'actual' bestaat)
    for ev, dt in todays_high:
        uid = event_uid(ev)
        if uid in results_sent:
            continue

        actual = get_actual(ev)
        if not actual:
            continue  # nog geen resultaat in de feed

        result_at = dt + timedelta(minutes=RESULT_DELAY_MINUTES)
        if result_at <= now < (result_at + timedelta(minutes=RESULT_WINDOW_MINUTES)):
            if DEBUG:
                print("SENDING_RESULT_FOR_UID:", uid, "AT_NOW:", now.isoformat())
            post_discord(make_result_message(ev, dt))
            results_sent.add(uid)

    # Save state
    state["reminded"] = sorted(reminded)
    state["daily_sent"] = sorted(daily_sent)
    state["results_sent"] = sorted(results_sent)
    save_state(state)


if __name__ == "__main__":
    main()
