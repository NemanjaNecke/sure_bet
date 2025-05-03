
import os, time, logging, re
from datetime import datetime
import requests
import pandas as pd
import pickle
# ─────────────────────────────────────────────
# 1)  Logging & constants                     ▼
# ─────────────────────────────────────────────
BASE        = "https://www.soccerbet.ba/restapi"
DESKTOP_V   = "1.5.1.3"
SITE_NAME   = "soccerbet"               # used only in file names / logs
# ------------------------------------------- ▲

os.makedirs("log", exist_ok=True)
logging.basicConfig(
    filename=f"log/{SITE_NAME}.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": f"https://www.{SITE_NAME}.ba/",
    "Origin":  f"https://www.{SITE_NAME}.ba",
}

TARGET_SPORTS = {"S"}        # ← football / soccer only

def fetch_json(url: str, timeout: int = 20) -> dict:
    logging.info("GET %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logging.error("Failed to fetch %s – %s", url, exc)
        return {}

# ─────────────────────────────────────────────
# 2)  Translation tables
# ─────────────────────────────────────────────
ttg = fetch_json(
    f"{BASE}/offer/ba/ttg_lang?annex=0&desktopVersion={DESKTOP_V}&locale=ba"
)

bet_line_caption = {int(k): v["caption"] for k, v in ttg.get("betMap", {}).items()}
pick_by_tip      = {rec["tipTypeCode"]: rec for rec in ttg.get("betPickMap", {}).values()}
bpg_raw          = ttg.get("betPickGroupMap", {})

group_caption = {
    int(gid): (g.get("name") or g.get("description") or f"Market {gid}")
    for gid, g in bpg_raw.items()
}
group_picks = {int(gid): (g.get("picks") or []) for gid, g in bpg_raw.items()}

tip_to_gid = {}
for gid, plist in group_picks.items():
    for p in plist or []:
        tip_to_gid[p["betPickCode"]] = gid

_caption_safe = re.compile(r"[A-Za-z0-9:±+./ ]{1,6}$")
def translate_pick(tip):
    rec = pick_by_tip.get(tip);       cap = (rec or {}).get("caption", "").strip()
    if cap and _caption_safe.fullmatch(cap): return cap
    return (rec or {}).get("specValue") or (rec or {}).get("label") \
           or (rec or {}).get("betMedCaption") or str(tip)

def market_name(gid, rec):
    if gid in group_caption: return group_caption[gid]
    if rec and (lc := rec.get("lineCode")) in bet_line_caption: return bet_line_caption[lc]
    return f"Market {gid if gid else '?'}"

def header(gid, tip): return f"{market_name(gid, pick_by_tip.get(tip))} – {translate_pick(tip)}"


def run():
    # ─────────────────────────────────────────────
    # 3)  Soccer-only scrape
    # ─────────────────────────────────────────────
    sports = fetch_json(
        f"{BASE}/translate/ba/sports?desktopVersion={DESKTOP_V}&locale=ba"
    ) or []

    rows: list[dict] = []

    for s in sports:
        sc = s.get("sportTypeCode")
        if sc not in TARGET_SPORTS:              # ← skip everything except “S”
            continue

        leagues = fetch_json(
            f"{BASE}/offer/ba/categories/sport/{sc}/l"
            f"?annex=0&desktopVersion={DESKTOP_V}&locale=ba"
        ).get("categories", [])

        for lg in leagues:
            matches = fetch_json(
                f"{BASE}/offer/ba/sport/{sc}/league/{lg['id']}/mob"
                f"?annex=0&desktopVersion={DESKTOP_V}&locale=ba"
            ).get("esMatches", [])

            for m in matches:
                row = {
                    "Sport":  sc,
                    "League": lg.get("name", lg["id"]),
                    "Home":   m.get("home"),
                    "Away":   m.get("away"),
                    "KickOff": datetime.fromtimestamp(m.get("kickOffTime", 0)/1000),
                }
                for tip_str, price in m.get("odds", {}).items():
                    tip = int(tip_str)
                    rec = pick_by_tip.get(tip)
                    gid = (rec.get("betPickGroupId") if rec else None) or tip_to_gid.get(tip)
                    row[header(gid, tip)] = price
                rows.append(row)
            time.sleep(0.002)

    # ─────────────────────────────────────────────
    # 4)  Export
    # ─────────────────────────────────────────────
    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    outfile = f"data/{SITE_NAME}_soccer_odds.xlsx"
    df.to_excel(outfile, index=False)
    # Save to Pickle
    os.makedirs("pickle_data", exist_ok=True)
    pickle_path = f"pickle_data/{SITE_NAME}_soccer_odds.pkl"
    with open(pickle_path, "wb") as f:
        pickle.dump(df, f)
    logging.info(f"Data pickled to {pickle_path}")
    logging.info("Saved %d matches to %s", len(df), outfile)
    print(f"✅  Done – {len(df)} matches written to {outfile}")

if __name__ == "__main__":
    run()