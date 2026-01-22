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

# Waar we state opslaan (handig als je later cache-pad wilt aanpassen)
STATE_FILE = os.environ.get("STATE_FILE", "state.json").strip()

REMINDER_MINUTES = 30

# Hoe lang na "remind_at" we nog mogen versturen als Actions te laat is
RUN_WINDOW_MINUTES = 20

# Daily post: 1× per dag NA 00:01 lokale tijd
DAILY_AFTER_MINUTES = 1

# Discord limiet
DISCORD_MAX_LEN = 2000

# Link die altijd mee moet
CALENDAR_URL = "https://www.cryptocraft.com/calendar"


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


def parse_dt_local(ev: dict):
    """
    Parse ISO8601 datetime from the feed and return it in local TZ (Europe/Amsterdam).
    Assumptie: als tz ontbreekt, interpreteren we het als UTC (zoals je oude code).
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
    Returns 'HH:MM CET/CEST' (of fallback op tzname als dat anders is).
    """
    local_dt = dt.astimezone(TZ)
    tzname = local_dt.tzname() or ""
    # Veelal: 'CET' of 'CEST' op Europe/Amsterdam
    if tzname in ("CET", "CEST"):
        label = tzname
    else:
        # fallback naar offset-based CET/CEST voor Amsterdam-achtige offsets
        offset = local_dt.strftime("%z")
        label = "CEST" if offset == "+0200" else "CET"
    return f"{local_dt.strftime('%H:%M')} {label}"


def daily_key(today_start: datetime) -> str:
    return today_start.strftime("%Y-%m-%d")


def chunk_messages(lines: list[str], header: str) -> list[str]:
    """
    Split een daily post in meerdere Discord-berichten als het te lang wordt.
    We nemen een vaste footer met link mee in elk bericht.
    """
    footer = f"\n\n🔗 Calendar: {CALENDAR_URL}"
    msgs = []
    current = header

    for block in lines:
        candidate = current + "\n\n" + block + footer
        if len(candidate) > DISCORD_MAX_LEN:
            # finalize current
            msgs.append(current + footer)
            current = header + "\n\n" + block
        else:
            current = current + "\n\n" + block

    msgs.append(current + footer)
    return msgs


def make_daily_messages(today_start: datetime, todays_events: list) -> list[str]:
    header = f"📅 **Crypto Craft – HIGH impact (vandaag {today_start.strftime('%d-%m-%Y')})**"

    if not todays_events:
        return [header + f"\n\nGeen HIGH impact events vandaag.\n\n🔗 Kalender: {CALENDAR_URL}"]

    blocks = []
    for ev, dt in todays_events:
        title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
        blocks.append(
            "🔥 Impact: HIGH\n"
            f"⏰ {fmt_time_local(dt)}\n"
            f"📌 {title}"
        )

    return chunk_messages(blocks, header)


def make_reminder_message(ev: dict, dt: datetime) -> str:
    title = ev.get("title") or ev.get("event") or ev.get("name") or "Event"
    return (
        "⏰ **REMINDER (30 min)**\n\n"
        "🔥 Impact: HIGH\n"
        f"⏰ {fmt_time_local(dt)}\n"
        f"📌 {title}\n\n"
    )


def main():
    now = datetime.now(TZ)

    # Today range in local TZ
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Load state
    state = load_state()
    reminded = set(state.get("reminded", []))
    daily_sent = set(state.get("daily_sent", []))

    # Fetch events (met nette error logging)
    try:
        obj = fetch_json(JSON_URL)
    except Exception as e:
        print("ERROR_FETCH_JSON:", str(e))
        # Fail hard zodat Actions het ziet (maar geen Discord spam)
        raise

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

    # 1) Daily post: 1× per day after 00:01 local time
    key = daily_key(today_start)
    if key not in daily_sent and now >= (today_start + timedelta(minutes=DAILY_AFTER_MINUTES)):
        daily_msgs = make_daily_messages(today_start, todays_high)
        for msg in daily_msgs:
            post_discord(msg)
        daily_sent.add(key)

    # 2) Reminders: catch-up proof window
    for ev, dt in todays_high:
        uid = event_uid(ev)
        if uid in reminded:
            continue

        remind_at = dt - timedelta(minutes=REMINDER_MINUTES)
        if remind_at <= now < (remind_at + timedelta(minutes=RUN_WINDOW_MINUTES)):
            post_discord(make_reminder_message(ev, dt))
            reminded.add(uid)

    # Save state
    state["reminded"] = sorted(reminded)
    state["daily_sent"] = sorted(daily_sent)
    save_state(state)


if __name__ == "__main__":
    main()
