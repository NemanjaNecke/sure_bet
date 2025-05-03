"""
MozzartBet – scrape *all* markets, save one wide Excel sheet  (ALL_MARKETS)

    python api/mozzart.py          # all fixtures  (default: all_days)
    python api/mozzart.py today    # only today’s fixtures
"""
from __future__ import annotations
import datetime as dt, json, logging, os, pickle, random, sys, time
from typing import Dict, List, Set

import pandas as pd, requests

# ─────────────────────────── const / paths ────────────────────────────────
BASE_URL   = "https://www.mozzartbet.ba"
API_MATCH  = f"{BASE_URL}/betting/matches"
API_MAP    = f"{BASE_URL}/gamesConfig?id=1"

OUT_PKL  = "./pickle_data/mozzart.pkl"          # still: 1×2 only (for sure‑bet)
OUT_XLSX = "./data/mozzart.xlsx"                # one sheet (wide)
LOG_FILE = "./log/mozart_scraper.log"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "sr-RS,sr;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type": "application/json",
    "medium": "WEB",
    "origin": BASE_URL,
    "referer": f"{BASE_URL}/bs/kladjenje/sport/1?date=all_days",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/135.0.0.0 Safari/537.36"),
}
PAGE_SIZE, WAIT, TIMEOUT = 100, 0.25, 20

# ─────────────────────────── helpers ──────────────────────────────────────
def _ensure_lang_cookie(s: requests.Session) -> None:
    if "i18next" not in s.cookies:
        s.cookies.set("i18next", "bs", domain="www.mozzartbet.ba", path="/")

def _flt(v) -> float | None:            # safe float
    try: return float(v)
    except (TypeError, ValueError): return None

def _download_bet_map(s: requests.Session
                      ) -> Dict[int, Dict[str, Set[int]]]:
    r = s.get(API_MAP, headers={"accept": "application/json"}, timeout=TIMEOUT)
    r.raise_for_status()
    mapping: Dict[int, Dict[str, Set[int]]] = {}
    for sport_id, groups in r.json().items():
        bucket = {"1": set(), "X": set(), "2": set()}
        for grp in groups:
            if str(grp.get("groupName", "")).lower().startswith("konačan"):
                for o in grp.get("odds", []):
                    name = o.get("subgame", {}).get("name")
                    if name in bucket: bucket[name].add(int(o["id"]))
        mapping[int(sport_id)] = bucket
    return mapping

def _post(s: requests.Session, payload: dict) -> dict:
    r = s.post(API_MATCH, json=payload, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# ───────── rows builders ──────────────────────────────────────────────────
def _row_1x2(m: dict, id_map: Dict[str, Set[int]]) -> dict | None:
    want = {"1": None, "X": None, "2": None}

    def probe(o: dict):
        v = _flt(o.get("value"))
        if v is None: return
        oid, label = int(o["id"]), o.get("subgame", {}).get("name")
        for k, allowed in id_map.items():
            if oid in allowed: want[k] = v
        if o.get("game", {}).get("shortName") == "ki" and label in want:
            want[label] = v

    for o in m.get("odds", []): probe(o)
    for grp in m.get("oddsGroup", []):
        for o in grp.get("odds", []): probe(o)

    if None in want.values(): return None
    return {"match_id": m["id"],
            "time": dt.datetime.fromtimestamp(m["startTime"]/1000),
            "home": m["home"]["name"],
            "away": m.get("visitor", {}).get("name", ""),
            "1": want["1"], "x": want["X"], "2": want["2"]}

def _rows_long(m: dict) -> List[dict]:
    rows=[]
    def push(o):
        v=_flt(o.get("value"));
        if v is None: return
        rows.append({"match_id":m["id"],
                     "time":dt.datetime.fromtimestamp(m["startTime"]/1000),
                     "home":m["home"]["name"],
                     "away":m.get("visitor",{}).get("name",""),
                     "col":f"{o['game']['shortName']}_{o['subgame']['name']}",
                     "val":v})
    for o in m.get("odds", []): push(o)
    for grp in m.get("oddsGroup", []):
        for o in grp.get("odds", []): push(o)
    return rows

# ─────────────────────────── main ─────────────────────────────────────────
def run(date_token: str="all_days", sport_id:int=1) -> None:
    for p in (OUT_PKL, OUT_XLSX, LOG_FILE):
        os.makedirs(os.path.dirname(p), exist_ok=True)
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        filemode="w")

    s = requests.Session();  _ensure_lang_cookie(s)
    try:
        id_map = _download_bet_map(s).get(sport_id, {"1":set(),"X":set(),"2":set()})
    except Exception as e:
        logging.warning("bet‑map download failed, labels only: %s", e)
        id_map = {"1":set(),"X":set(),"2":set()}

    rows1, rows_long, page = [], [], 0
    while True:
        pl = {"date":date_token,"sort":"bycompetition",
              "currentPage":page,"pageSize":PAGE_SIZE,
              "sportId":sport_id,"competitionIds":[],
              "search":"","matchTypeId":0,"offerType":"PRE_MATCH",
              "random":random.random()}
        try: blob=_post(s,pl)
        except Exception as e:
            logging.error("p%s failed %s",page,e); break
        items=blob.get("items",[])
        if not items: logging.info("p%s empty – end",page); break

        for m in items:
            r=_row_1x2(m,id_map);  rows1+=([] if r is None else [r])
            rows_long.extend(_rows_long(m))
        logging.info("p%s rec=%d  1x2=%d  odds=%d",
                     page,len(items),len(rows1),len(rows_long))
        page+=1; time.sleep(WAIT)

    if not rows1:
        logging.warning("nothing scraped"); return

    # wide table: pivot long rows into columns
    df_long = pd.DataFrame(rows_long)
    wide = (df_long
            .pivot_table(index=["match_id","time","home","away"],
                         columns="col", values="val", aggfunc="first")
            .reset_index())
    wide.sort_values(["time","home"], inplace=True)

    # pickle still for 1×2 pipeline
    pd.DataFrame(rows1).to_pickle(OUT_PKL)
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xls:
        wide.to_excel(xls, index=False, sheet_name="ALL_MARKETS")
    logging.info("DONE fixtures=%d  cols=%d",
                 len(wide), len(wide.columns)-4)

# ─────────────────────────── cli ──────────────────────────────────────────
if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv)>1 else "all_days")