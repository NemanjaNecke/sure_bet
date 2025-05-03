import os, time, logging, re, pickle
from datetime import datetime
import requests, pandas as pd

# ─── CONFIG ─────────────────────────────────────────────────────────
BASE        = "https://wwin.com:8443/livecontent/api"
SITE_NAME   = "wwin"

LOCALE_KEY  = "bs"                               # from the HAR query‑string
SOCCER_ID   = 2                                  # sportId for football
WINDOW_H    = 7 * 24                             # scrape one week ahead
CHUNK       = 40                                 # 40 IDs ≈ 1 400‑char URL

# ─── LOGGING ────────────────────────────────────────────────────────
os.makedirs("log", exist_ok=True)
logging.basicConfig(
    filename=f"log/{SITE_NAME}.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

# ─── HEADERS (clone from HAR) ───────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://wwin.com/",
    "Origin":  "https://wwin.com",
    "X-Key":   "D1557B63-9958-4959-9E8D-F356391ABACF",   # ← exact value in HAR
}

# ─── SMALL HELPERS ─────────────────────────────────────────────────
SAFE = re.compile(r"[A-Za-z0-9+:./ ]{1,30}$")
def fetch_json(url, timeout=20):
    logging.info("GET %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return r.text          # WWIN sends plain text on rate‑limit
    except Exception as exc:
        logging.error("Failed %s – %s", url, exc)
        return None

# ─── 1) discover football bet‑types & captions ─────────────────────
static = fetch_json(f"{BASE}/PrematchEvents/staticContent?key={LOCALE_KEY}") or {}
foot   = [m for m in static.get("markets", []) if m.get("ids") == SOCCER_ID]

BET_IDS = sorted({m["id"] for m in foot})        # football‑only IDs
CAPTION = {m["id"]: (m.get("npl") or m.get("np") or str(m["id"])) for m in foot}

def header(id_bt, tip):
    name = CAPTION.get(id_bt, f"Market {id_bt}")
    return f"{name} – {tip if SAFE.fullmatch(str(tip)) else id_bt}"

# ─── 2) main scrape ────────────────────────────────────────────────
def run():
    now   = int(time.time())
    tfrom = now - 3600
    tto   = now + WINDOW_H * 3600

    rows = []
    id_chunks = (BET_IDS[i:i+CHUNK] for i in range(0, len(BET_IDS), CHUNK))

    for chunk in id_chunks:
        bets = ",".join(map(str, chunk))
        base = f"{BASE}/PrematchEvents/events?key={LOCALE_KEY}&bets={bets}&spid={SOCCER_ID}"
        urls = [f"{base}&tfrom={tfrom}&tto={tto}", base]   # fallback w/o window

        events = None
        for url in urls:
            data = fetch_json(url)

            if isinstance(data, dict):
                events = data.get("events")
            elif isinstance(data, list):
                events = data
            else:                    # empty string or "Too many requests"
                logging.warning("Payload skipped (%s): %s",
                                type(data).__name__, data)
                time.sleep(1.5)
                continue
            if events:
                break

        if not events:
            logging.info("No events for chunk %s", bets)
            time.sleep(0.5)
            continue

        for ev in events:
            row = {
                "Sport":  "S",
                "League": ev.get("tname") or ev.get("cnt"),
                "Home":   ev.get("hname"),
                "Away":   ev.get("aname"),
                "KickOff": datetime.fromtimestamp(ev.get("startTime")),
            }
            for bet in ev.get("bets", []):
                bt = bet.get("idBt")
                for odd in bet.get("odds", []):
                    row[header(bt, odd.get("t"))] = odd.get("va")
            rows.append(row)

        time.sleep(0.3)              # stay under WWIN’s throttle

    # ─── 3) export ────────────────────────────────────────────────
    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    out_xlsx = f"data/{SITE_NAME}_soccer_all_odds.xlsx"
    df.to_excel(out_xlsx, index=False)

    os.makedirs("pickle_data", exist_ok=True)
    with open(f"pickle_data/{SITE_NAME}_soccer_all_odds.pkl", "wb") as f:
        pickle.dump(df, f)

    logging.info("Saved %d matches to %s", len(df), out_xlsx)
    print(f"✅ Done – {len(df)} matches, {len(df.columns)-5} markets ➜ {out_xlsx}")

# ─── entry‑point ───────────────────────────────────────────────────
if __name__ == "__main__":
    run()