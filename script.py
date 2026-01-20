import os
import json
import hashlib
import urllib.request

JSON_URL = os.environ["CRYPTOCRAFT_JSON_URL"]
WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]
STATE_FILE = "state.txt"


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


def post_discord(msg: str):
    data = json.dumps({"content": msg}).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        r.read()


def event_id(ev: dict) -> str:
    raw = json.dumps(ev, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def save_state(ids):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(ids)))


def get_events(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for key in ("events", "data", "items", "calendar"):
            if key in obj and isinstance(obj[key], list):
                return obj[key]
    raise RuntimeError("Onverwachte JSON-structuur: geen events gevonden")


def main():
    obj = fetch_json(JSON_URL)
    events = get_events(obj)

    seen = load_state()
    new_seen = set(seen)

    posted = 0
    for ev in events:
        eid = event_id(ev)
        new_seen.add(eid)
        if eid in seen:
            continue

        title = ev.get("title") or ev.get("event") or ev.get("name") or "Crypto Craft event"
        impact = ev.get("impact") or ev.get("importance") or ev.get("level") or ""
        when = ev.get("datetime") or ev.get("date") or ev.get("time") or ""
        cur = ev.get("currency") or ev.get("symbol") or ev.get("coin") or ""

        msg = f"📅 **{title}**"
        if cur:
            msg += f" | 🪙 {cur}"
        if impact:
            msg += f" | 🔥 {impact}"
        if when:
            msg += f"\n⏰ {when}"

        post_discord(msg)
        posted += 1
        if posted >= 10:  # anti-spam
            break

    save_state(new_seen)


if __name__ == "__main__":
    main()
