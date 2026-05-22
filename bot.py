import asyncio
import json
import logging
import math
import os
import random
import sqlite3
import string
from collections import Counter, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN  = "8828842195:AAGdzF60aoUbBv6PJf8_LnQ0AunYF3UN8C8"
ADMIN_IDS  = [8001225219]
DB_PATH    = "bot_ultra.db"
MEM_WINDOW = 500

SICBO_INTERVAL   = 1.9
LC_INTERVAL      = 0.5
MAX_RETRIES      = 3
MIN_CONF_PREDICT = 0.58

HOT_STREAK_THRESHOLDS = {3, 5, 7, 10, 15}

WELCOME_GIF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "welcome.gif")

SICBO_API = (
    "https://api.wsktnus8.net/v2/history/getLastResult"
    "?gameId=ktrng_3979&size=100&tableId=39791215743193&curPage=1"
)
LC_MD5_API     = "https://wtxmd52.tele68.com/v1/txmd5/sessions?cp=R&cl=R&pf=web&at=07d01d98fd85e91efaa91fe492970412"
LC_HU_API      = "https://wtx.tele68.com/v1/tx/sessions?cp=R&cl=R&pf=web&at=07d01d98fd85e91efaa91fe492970412"
BETVIP_MD5_API = "https://wtxmd52.macminim6.online/v1/txmd5/sessions?cp=R&cl=R&pf=web&at=4256ce1eed33ffa0e0990d398f1f907f"
BETVIP_HU_API  = "https://wtx.macminim6.online/v1/tx/sessions?cp=R&cl=R&pf=web&at=4256ce1eed33ffa0e0990d398f1f907f"

SICBO_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Referer": "https://sunwin.gs/",
    "Origin":  "https://sunwin.gs",
    "Cache-Control": "no-cache",
    "Pragma":  "no-cache",
}
LC_HEADERS     = {"accept": "*/*", "accept-language": "vi-VN,vi;q=0.9", "Referer": "https://lc79b.bet/"}
BETVIP_HEADERS = {"accept": "*/*", "accept-language": "vi-VN,vi;q=0.9", "Referer": "https://betvip.net/"}

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/112.0.0.0 Mobile Safari/537.36",
]

SICBO   = "sicbo"
LC_MD5  = "lc_md5"
LC_HU   = "lc_hu"
BET_MD5 = "bet_md5"
BET_HU  = "bet_hu"

ALL_GAMES = (SICBO, LC_MD5, LC_HU, BET_MD5, BET_HU)

GAME_LABELS = {
    SICBO:   "🎲 SICBO SUNWIN",
    LC_MD5:  "🦀 LẨU CUA MD5",
    LC_HU:   "🏺 LẨU CUA HŨ",
    BET_MD5: "🎰 BETVIP MD5",
    BET_HU:  "🎯 BETVIP HŨ",
}

TX_ONLY_GAMES = {LC_MD5, LC_HU, BET_MD5, BET_HU}

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_states: Dict[str, dict] = {
    gm: {
        "history":     deque(maxlen=MEM_WINDOW),
        "latest":      {},
        "pred":        {},
        "prev_pred":   {},
        "auto_msg":    {},
        "api_ok":      False,
        "consec_loss": 0,
        "consec_win":  0,
    }
    for gm in ALL_GAMES
}

_maintenance = {
    "active":   False,
    "end_time": None,
    "reason":   "",
    "task":     None,
}

_locked_cmds: set = set()


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with _db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id   INTEGER PRIMARY KEY,
                username  TEXT,
                added_at  TEXT,
                added_by  INTEGER
            );
            CREATE TABLE IF NOT EXISTS activation_keys (
                key        TEXT PRIMARY KEY,
                created_by INTEGER,
                created_at TEXT,
                expires_at TEXT,
                used_by    INTEGER,
                used_at    TEXT,
                is_trial   INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS user_expiry (
                user_id    INTEGER PRIMARY KEY,
                expires_at TEXT
            );
            CREATE TABLE IF NOT EXISTS trial_used (
                user_id    INTEGER PRIMARY KEY,
                used_at    TEXT
            );
            CREATE TABLE IF NOT EXISTS predictions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                game_mode   TEXT DEFAULT 'sicbo',
                game_num    TEXT,
                pred_type   TEXT,
                pred_vi1    INTEGER,
                pred_vi2    INTEGER,
                pred_vi3    INTEGER,
                confidence  INTEGER,
                cau_type    TEXT,
                actual_vi   INTEGER,
                actual_type TEXT,
                dice        TEXT,
                outcome     TEXT,
                vi_hit      INTEGER DEFAULT 0,
                created_at  TEXT,
                UNIQUE(game_mode, game_num)
            );
            CREATE TABLE IF NOT EXISTS algo_weights (
                game_mode TEXT,
                algo_name TEXT,
                weight    REAL DEFAULT 1.0,
                hits      INTEGER DEFAULT 0,
                misses    INTEGER DEFAULT 0,
                updated   TEXT,
                PRIMARY KEY(game_mode, algo_name)
            );
            CREATE TABLE IF NOT EXISTS locked_cmds (
                cmd       TEXT PRIMARY KEY,
                locked_by INTEGER,
                locked_at TEXT,
                reason    TEXT
            );
        """)

    with _db() as db:
        rows = db.execute("SELECT cmd FROM locked_cmds").fetchall()
        for r in rows:
            _locked_cmds.add(r["cmd"])

    log.info("Database initialised. Locked cmds: %s", _locked_cmds)


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def is_allowed(uid: int) -> bool:
    if is_admin(uid):
        return True
    try:
        with _db() as db:
            row = db.execute(
                "SELECT user_id FROM allowed_users WHERE user_id=?", (uid,)
            ).fetchone()
            if not row:
                return False
            exp = db.execute(
                "SELECT expires_at FROM user_expiry WHERE user_id=?", (uid,)
            ).fetchone()
            if exp and exp["expires_at"]:
                if datetime.now() > datetime.fromisoformat(exp["expires_at"]):
                    return False
    except Exception as e:
        log.error("is_allowed error: %s", e)
        return False
    return True


def get_expiry_str(uid: int) -> str:
    try:
        with _db() as db:
            e = db.execute("SELECT expires_at FROM user_expiry WHERE user_id=?", (uid,)).fetchone()
        if e and e["expires_at"]:
            dt   = datetime.fromisoformat(e["expires_at"])
            left = dt - datetime.now()
            hrs  = max(0, int(left.total_seconds() // 3600))
            mins = max(0, int((left.total_seconds() % 3600) // 60))
            return f"{dt.strftime('%H:%M %d/%m/%Y')} (còn {hrs}h{mins}m)"
    except Exception:
        pass
    return "Không xác định"


def lock_cmd(cmd: str, uid: int, reason: str = ""):
    _locked_cmds.add(cmd)
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO locked_cmds (cmd, locked_by, locked_at, reason) VALUES (?,?,?,?)",
                (cmd, uid, datetime.now().isoformat(), reason)
            )
        log.info("LOCK: cmd='%s' by uid=%d reason='%s'", cmd, uid, reason)
    except Exception as e:
        log.error("lock_cmd DB error: %s", e)


def unlock_cmd(cmd: str):
    _locked_cmds.discard(cmd)
    try:
        with _db() as db:
            db.execute("DELETE FROM locked_cmds WHERE cmd=?", (cmd,))
        log.info("UNLOCK: cmd='%s'", cmd)
    except Exception as e:
        log.error("unlock_cmd DB error: %s", e)


def is_locked(cmd: str) -> bool:
    if cmd in _locked_cmds:
        return True
    try:
        with _db() as db:
            row = db.execute("SELECT 1 FROM locked_cmds WHERE cmd=?", (cmd,)).fetchone()
            if row:
                _locked_cmds.add(cmd)
                return True
    except Exception:
        pass
    return False


def get_lock_reason(cmd: str) -> str:
    try:
        with _db() as db:
            row = db.execute("SELECT reason FROM locked_cmds WHERE cmd=?", (cmd,)).fetchone()
            return row["reason"] if row and row["reason"] else "Bảo trì"
    except Exception:
        return "Bảo trì"


def classify_game(score: int, faces: list, game_mode: str) -> str:
    sf = sorted(faces)
    if game_mode == SICBO:
        if sf == [4, 4, 4]:
            return "BÃO"
        return "TÀI" if score > 10 else "XỈU"
    if game_mode in (LC_HU, BET_HU):
        if sf == [1, 1, 1]:
            return "NỔ HŨ XỈU"
        if sf == [6, 6, 6]:
            return "NỔ HŨ TÀI"
    return "TÀI" if score > 10 else "XỈU"


def is_tai(score: int, faces: list, game_mode: str) -> Optional[bool]:
    c = classify_game(score, faces, game_mode)
    if c == "BÃO":
        return None
    if "TÀI" in c:
        return True
    if "XỈU" in c:
        return False
    return None


class CauDetector:

    @staticmethod
    def streak(seq: List[bool]) -> Tuple[int, bool]:
        if not seq:
            return 0, True
        last = seq[-1]
        n = 1
        for i in range(len(seq) - 2, -1, -1):
            if seq[i] == last:
                n += 1
            else:
                break
        return n, last

    @staticmethod
    def detect(seq: List[bool]) -> dict:
        empty = {"type": "CHƯA RÕ", "len": 0, "pred": None, "conf": 50,
                 "desc": "Chưa đủ dữ liệu", "break_risk": 0}
        if len(seq) < 4:
            return empty

        sk, last_val = CauDetector.streak(seq)

        if sk >= 14:
            return {"type": "BỆT SIÊU DÀI", "len": sk, "pred": not last_val,
                    "conf": min(95 + (sk - 14), 99),
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván ⚠️ Gãy cực cao",
                    "break_risk": 97}

        if sk >= 10:
            return {"type": "BỆT DÀI", "len": sk, "pred": not last_val,
                    "conf": min(88 + (sk - 10) * 2, 94),
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Rất dễ gãy",
                    "break_risk": 88}

        if sk >= 7:
            return {"type": "BỆT DÀI VỪA", "len": sk, "pred": not last_val,
                    "conf": min(78 + (sk - 7) * 3, 87),
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Dễ gãy",
                    "break_risk": 75}

        if sk >= 5:
            return {"type": "BỆT VỪA", "len": sk, "pred": not last_val,
                    "conf": 68 + (sk - 5) * 4,
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Dễ gãy",
                    "break_risk": 60}

        if sk >= 3:
            return {"type": "BỆT NGẮN", "len": sk, "pred": last_val,
                    "conf": 58 + sk * 4,
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Theo cầu",
                    "break_risk": 22}

        if len(seq) >= 8 and all(seq[-(i+1)] != seq[-(i+2)] for i in range(6)):
            return {"type": "CẦU 1-1", "len": 8, "pred": not seq[-1], "conf": 80,
                    "desc": "Cầu ping-pong 1-1 dài → Tiếp tục xen kẽ", "break_risk": 18}

        if len(seq) >= 6 and all(seq[-(i+1)] != seq[-(i+2)] for i in range(4)):
            return {"type": "CẦU 1-1", "len": 6, "pred": not seq[-1], "conf": 76,
                    "desc": "Cầu ping-pong 1-1 → Tiếp tục xen kẽ", "break_risk": 22}

        if len(seq) >= 4 and all(seq[-(i+1)] != seq[-(i+2)] for i in range(2)):
            return {"type": "CẦU 1-1 NGẮN", "len": 4, "pred": not seq[-1], "conf": 67,
                    "desc": "Cầu ping-pong 4 ván → Theo xen kẽ", "break_risk": 32}

        if len(seq) >= 8:
            r8 = seq[-8:]
            if (r8[0]==r8[1] and r8[1]!=r8[2] and r8[2]==r8[3] and
                    r8[3]!=r8[4] and r8[4]==r8[5] and r8[5]!=r8[6] and r8[6]==r8[7]):
                return {"type": "CẦU 2-2", "len": 8, "pred": r8[-1], "conf": 76,
                        "desc": "Cầu 2-2 → Tiếp tục theo cặp", "break_risk": 18}

        if len(seq) >= 6:
            r6 = seq[-6:]
            if r6[0]==r6[1] and r6[1]!=r6[2] and r6[2]==r6[3] and r6[3]!=r6[4] and r6[4]==r6[5]:
                return {"type": "CẦU 2-2 NGẮN", "len": 6, "pred": r6[-1], "conf": 70,
                        "desc": "Cầu 2-2 ngắn → Theo cặp", "break_risk": 28}

        if len(seq) >= 12:
            r12 = seq[-12:]
            ok = all(r12[i*3]==r12[i*3+1]==r12[i*3+2] and
                     (i==0 or r12[i*3] != r12[(i-1)*3]) for i in range(4))
            if ok:
                return {"type": "CẦU 3-3", "len": 12, "pred": r12[-1], "conf": 78,
                        "desc": "Cầu 3-3 → Theo bộ 3", "break_risk": 14}

        if len(seq) >= 9:
            r9 = seq[-9:]
            a = r9[0]
            if r9 == [a, a, not a, a, a, not a, a, a, not a]:
                return {"type": "CẦU 2-1", "len": 9, "pred": not a, "conf": 82,
                        "desc": "Cầu 2-1 → Đổi chiều", "break_risk": 12}

        if len(seq) >= 9:
            r9 = seq[-9:]
            a = r9[0]
            if r9 == [a, not a, not a, a, not a, not a, a, not a, not a]:
                return {"type": "CẦU 1-2", "len": 9, "pred": a, "conf": 80,
                        "desc": "Cầu 1-2 → Theo a", "break_risk": 14}

        if len(seq) >= 12:
            r12 = seq[-12:]
            a = r12[0]
            if r12 == [a, a, a, not a, a, a, a, not a, a, a, a, not a]:
                return {"type": "CẦU 3-1", "len": 12, "pred": not a, "conf": 79,
                        "desc": "Cầu 3-1 → Đổi chiều", "break_risk": 16}

        if len(seq) >= 12:
            r12 = seq[-12:]
            a = r12[0]
            if r12 == [a, not a, not a, not a, a, not a, not a, not a, a, not a, not a, not a]:
                return {"type": "CẦU 1-3", "len": 12, "pred": a, "conf": 78,
                        "desc": "Cầu 1-3 → Theo a", "break_risk": 16}

        if len(seq) >= 7:
            r7 = seq[-7:]
            changes = sum(1 for i in range(6) if r7[i] != r7[i+1])
            if changes >= 5:
                return {"type": "ZIGZAG", "len": 7, "pred": not seq[-1], "conf": 68,
                        "desc": "Zigzag lệch → Đổi chiều", "break_risk": 38}

        if len(seq) >= 10:
            r10 = seq[-10:]
            groups = []
            cur = 1
            for i in range(1, 10):
                if r10[i] == r10[i-1]:
                    cur += 1
                else:
                    groups.append(cur)
                    cur = 1
            groups.append(cur)
            if len(groups) >= 4:
                avg_g = sum(groups) / len(groups)
                last_g = groups[-1]
                if last_g >= avg_g * 1.8:
                    return {"type": "BỆT BẤT THƯỜNG", "len": last_g, "pred": not seq[-1],
                            "conf": 72, "desc": "Bệt vượt ngưỡng trung bình → Gãy sớm",
                            "break_risk": 68}

        return {"type": "HỖN HỢP", "len": len(seq), "pred": None, "conf": 50,
                "desc": "Xu hướng hỗn hợp — chờ cầu rõ", "break_risk": 50}


class AdvancedPredictor:

    _ALGO_DEFAULTS: Dict[str, float] = {
        "cau_detect":      9.0,
        "markov5":         7.5,
        "markov4":         7.0,
        "markov3":         6.5,
        "markov2":         5.5,
        "markov1":         3.5,
        "pattern8":        7.0,
        "pattern6":        6.5,
        "pattern5":        6.0,
        "pattern4":        5.5,
        "pattern3":        5.0,
        "streak_break":    6.5,
        "zigzag":          4.5,
        "gap_analysis":    5.0,
        "window10":        4.5,
        "window20":        4.0,
        "window50":        3.0,
        "entropy":         4.0,
        "run_length":      4.5,
        "oscillation":     4.5,
        "chi_balance":     3.5,
        "score_trend":     4.5,
        "adaptive_ma":     4.0,
        "linear_reg":      4.0,
        "perceptron":      5.5,
        "cycle_detect":    4.0,
        "hot_cold":        3.5,
        "prob_weight":     3.0,
        "momentum":        5.5,
        "reversal":        5.0,
        "support_resist":  4.0,
        "bayesian":        6.0,
        "anti_streak":     5.5,
        "trend_follow":    4.5,
        "mean_revert":     4.0,
        "double_markov":   5.5,
        "transition_prob": 5.0,
        "block_analysis":  4.5,
        "volatility":      3.5,
        "consensus_vote":  6.5,
        "score_dist":      3.5,
        "entropy_rate":    4.0,
    }

    def __init__(self, game_mode: str):
        self.gm = game_mode
        self.weights: Dict[str, float] = {}
        self._load_weights()

    def _load_weights(self):
        try:
            with _db() as db:
                for r in db.execute(
                    "SELECT algo_name, weight FROM algo_weights WHERE game_mode=?", (self.gm,)
                ).fetchall():
                    self.weights[r["algo_name"]] = r["weight"]
        except Exception:
            pass
        for k, v in self._ALGO_DEFAULTS.items():
            if k not in self.weights:
                self.weights[k] = v

    def update_weights_bulk(self, algo_names: List[str], correct: bool):
        updates = []
        for algo in algo_names:
            w = self.weights.get(algo, 1.0)
            self.weights[algo] = min(20.0, w * 1.10) if correct else max(0.20, w * 0.90)
            updates.append(algo)
        try:
            with _db() as db:
                for algo in updates:
                    r = db.execute(
                        "SELECT hits,misses FROM algo_weights WHERE game_mode=? AND algo_name=?",
                        (self.gm, algo)
                    ).fetchone()
                    h = (r["hits"] if r else 0) + (1 if correct else 0)
                    m = (r["misses"] if r else 0) + (0 if correct else 1)
                    db.execute(
                        "INSERT OR REPLACE INTO algo_weights "
                        "(game_mode, algo_name, weight, hits, misses, updated) VALUES (?,?,?,?,?,?)",
                        (self.gm, algo, self.weights[algo], h, m, datetime.now().isoformat())
                    )
        except Exception as e:
            log.debug("update_weights_bulk: %s", e)

    def w(self, name: str) -> float:
        return self.weights.get(name, 1.0)

    def _markov(self, seq: List[bool], order: int) -> Optional[Tuple[bool, float]]:
        if len(seq) < order + 5:
            return None
        pat = tuple(seq[-order:])
        cnt: Counter = Counter()
        for i in range(len(seq) - order):
            if tuple(seq[i:i+order]) == pat and i + order < len(seq):
                cnt[seq[i+order]] += 1
        total = sum(cnt.values())
        if total < 3:
            return None
        best, n = cnt.most_common(1)[0]
        conf = n / total
        if conf < 0.54:
            return None
        return best, min(conf, 0.93)

    def _double_markov(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 14:
            return None
        results = []
        for order in [3, 4, 5]:
            r = self._markov(seq, order)
            if r:
                results.append(r)
        if len(results) < 2:
            return None
        tai_w = sum(c for p, c in results if p)
        xiu_w = sum(c for p, c in results if not p)
        total = tai_w + xiu_w
        if total == 0:
            return None
        pred = tai_w >= xiu_w
        conf = max(tai_w, xiu_w) / total
        if conf < 0.58:
            return None
        return pred, min(conf, 0.90)

    def _transition_prob(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        tt = tx = xt = xx = 0
        for i in range(len(seq) - 1):
            a, b = seq[i], seq[i+1]
            if a and b:       tt += 1
            elif a and not b: tx += 1
            elif not a and b: xt += 1
            else:             xx += 1
        last = seq[-1]
        if last:
            total = tt + tx
            if total < 4:
                return None
            prob_tai = tt / total
            pred = prob_tai >= 0.5
            conf = max(prob_tai, 1 - prob_tai)
        else:
            total = xt + xx
            if total < 4:
                return None
            prob_tai = xt / total
            pred = prob_tai >= 0.5
            conf = max(prob_tai, 1 - prob_tai)
        if conf < 0.54:
            return None
        return pred, min(conf, 0.88)

    def _bayesian(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 15:
            return None
        prior_tai = 0.5
        alpha = 2.0
        recent = seq[-30:] if len(seq) >= 30 else seq
        n = len(recent)
        k = sum(recent)
        posterior_tai = (k + alpha * prior_tai) / (n + alpha)
        posterior_xiu = 1 - posterior_tai
        if abs(posterior_tai - 0.5) < 0.08:
            return None
        pred = posterior_tai > 0.5
        conf = min(0.52 + abs(posterior_tai - 0.5) * 1.2, 0.85)
        sk, last = CauDetector.streak(seq)
        if sk >= 4 and last == pred:
            pass
        elif sk >= 4 and last != pred:
            conf = min(conf * 1.1, 0.88)
        return pred, conf

    def _anti_streak(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 5:
            return None
        sk, last = CauDetector.streak(seq)
        if sk < 5:
            return None
        base_conf = min(0.60 + (sk - 5) * 0.05, 0.90)
        if sk >= 10:
            base_conf = min(0.85 + (sk - 10) * 0.02, 0.95)
        return not last, base_conf

    def _block_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        block_size = 5
        n_blocks = min(len(seq) // block_size, 8)
        block_ratios = []
        for i in range(n_blocks):
            chunk = seq[-(i+1)*block_size : -i*block_size if i > 0 else None]
            if chunk:
                block_ratios.append(sum(chunk) / len(chunk))
        if len(block_ratios) < 3:
            return None
        trend = block_ratios[0] - block_ratios[-1]
        if abs(trend) < 0.15:
            return None
        pred = trend > 0
        conf = min(0.54 + abs(trend) * 0.5, 0.80)
        return pred, conf

    def _volatility(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 15:
            return None
        r10 = seq[-10:]
        changes = sum(1 for i in range(9) if r10[i] != r10[i+1])
        r5 = sum(r10[-5:]) / 5
        if changes >= 8:
            return not seq[-1], 0.70
        if changes <= 2:
            sk, last = CauDetector.streak(seq)
            if sk >= 5:
                return not last, min(0.62 + sk * 0.03, 0.84)
            return last, 0.62
        return None

    def _trend_follow(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        r5  = sum(seq[-5:]) / 5
        r10 = sum(seq[-10:]) / 10
        r20 = sum(seq[-20:]) / 20
        if r5 > r10 > r20 and r5 > 0.65:
            momentum = r5 - r20
            return True, min(0.56 + momentum * 0.8, 0.82)
        if r5 < r10 < r20 and r5 < 0.35:
            momentum = r20 - r5
            return False, min(0.56 + momentum * 0.8, 0.82)
        return None

    def _mean_revert(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        r5  = sum(seq[-5:]) / 5
        r20 = sum(seq[-20:]) / 20
        dev = r5 - r20
        if abs(dev) < 0.25:
            return None
        pred = dev < 0
        conf = min(0.54 + abs(dev) * 0.7, 0.80)
        return pred, conf

    def _score_dist(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 10:
            return None
        cnt = Counter()
        for d1 in range(1, 7):
            for d2 in range(1, 7):
                for d3 in range(1, 7):
                    cnt[d1+d2+d3] += 1
        dp = {s: c/216 for s, c in cnt.items()}
        recent = scores[-15:]
        tai_expected = sum(dp.get(s, 0) for s in range(11, 19)) * 216
        xiu_expected = sum(dp.get(s, 0) for s in range(3, 11)) * 216
        recent_tai = sum(1 for s in recent if s > 10)
        recent_xiu = len(recent) - recent_tai
        tai_dev = recent_tai / max(len(recent) * tai_expected / 216, 1)
        xiu_dev = recent_xiu / max(len(recent) * xiu_expected / 216, 1)
        if tai_dev > 1.4:
            return False, min(0.54 + (tai_dev - 1.4) * 0.2, 0.76)
        if xiu_dev > 1.4:
            return True, min(0.54 + (xiu_dev - 1.4) * 0.2, 0.76)
        return None

    def _entropy_rate(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        bigrams: Counter = Counter()
        for i in range(len(seq) - 1):
            bigrams[(seq[i], seq[i+1])] += 1
        total_bg = sum(bigrams.values())
        if total_bg == 0:
            return None
        H = 0.0
        for v in bigrams.values():
            p = v / total_bg
            H -= p * math.log2(p)
        last = seq[-1]
        if H < 1.4:
            if last:
                p_tt = bigrams.get((True, True), 0) / max(bigrams.get((True, True), 0) + bigrams.get((True, False), 0), 1)
                return (True, 0.62 + (1.4 - H) * 0.15) if p_tt > 0.55 else (False, 0.62 + (1.4 - H) * 0.15)
            else:
                p_xx = bigrams.get((False, False), 0) / max(bigrams.get((False, True), 0) + bigrams.get((False, False), 0), 1)
                return (False, 0.62 + (1.4 - H) * 0.15) if p_xx > 0.55 else (True, 0.62 + (1.4 - H) * 0.15)
        return None

    def _consensus_vote(self, algos_results: list) -> Optional[Tuple[bool, float]]:
        if len(algos_results) < 8:
            return None
        tai_votes = sum(1 for p, c, w, _ in algos_results if p)
        xiu_votes = len(algos_results) - tai_votes
        total = len(algos_results)
        ratio = max(tai_votes, xiu_votes) / total
        if ratio < 0.68:
            return None
        pred = tai_votes >= xiu_votes
        conf = min(0.56 + (ratio - 0.68) * 1.2, 0.88)
        return pred, conf

    def _pattern(self, seq: List[bool], depth: int) -> Optional[Tuple[bool, float]]:
        if len(seq) < depth + 5:
            return None
        pat = tuple(seq[-depth:])
        cnt: Counter = Counter()
        for i in range(len(seq) - depth):
            if tuple(seq[i:i+depth]) == pat and i + depth < len(seq):
                cnt[seq[i+depth]] += 1
        total = sum(cnt.values())
        if total < 2:
            return None
        best, n = cnt.most_common(1)[0]
        conf = n / total
        if conf < 0.54:
            return None
        return best, min(conf, 0.91)

    def _streak_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 3:
            return None
        sk, last = CauDetector.streak(seq)
        if sk >= 10:
            return not last, min(0.85 + (sk - 10) * 0.02, 0.95)
        if sk >= 7:
            return not last, 0.75 + (sk - 7) * 0.03
        if sk >= 5:
            return not last, 0.68 + (sk - 5) * 0.035
        if sk == 4:
            return last, 0.65
        if sk == 3:
            return last, 0.60
        return None

    def _momentum(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 10:
            return None
        w3  = sum(seq[-3:]) / 3
        w5  = sum(seq[-5:]) / 5
        w10 = sum(seq[-10:]) / 10
        mom = w3 * 0.5 + w5 * 0.3 + w10 * 0.2
        if mom > 0.72:
            return True, min(0.58 + (mom - 0.72) * 1.6, 0.82)
        if mom < 0.28:
            return False, min(0.58 + (0.28 - mom) * 1.6, 0.82)
        return None

    def _reversal(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 15:
            return None
        r5  = sum(seq[-5:]) / 5
        r15 = sum(seq[-15:]) / 15
        diff = r5 - r15
        if diff > 0.38:
            return False, min(0.60 + diff * 0.65, 0.84)
        if diff < -0.38:
            return True, min(0.60 + abs(diff) * 0.65, 0.84)
        return None

    def _support_resist(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 30:
            return None
        r30 = sum(seq[-30:]) / 30
        r10 = sum(seq[-10:]) / 10
        if r30 > 0.68 and r10 > 0.70:
            return False, min(0.58 + (r30 - 0.68) * 0.9, 0.80)
        if r30 < 0.32 and r10 < 0.30:
            return True, min(0.58 + (0.32 - r30) * 0.9, 0.80)
        return None

    def _zigzag_detect(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 6:
            return None
        if all(seq[-(i+1)] != seq[-(i+2)] for i in range(5)):
            return not seq[-1], 0.80
        if all(seq[-(i+1)] != seq[-(i+2)] for i in range(4)):
            return not seq[-1], 0.76
        if len(seq) >= 4 and all(seq[-(i+1)] != seq[-(i+2)] for i in range(2)):
            return not seq[-1], 0.66
        return None

    def _gap_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 15:
            return None
        tp = [i for i, v in enumerate(seq) if v]
        fp = [i for i, v in enumerate(seq) if not v]
        if len(tp) < 3 or len(fp) < 3:
            return None
        avg_tg = sum(tp[i+1]-tp[i] for i in range(len(tp)-1)) / (len(tp)-1)
        avg_fg = sum(fp[i+1]-fp[i] for i in range(len(fp)-1)) / (len(fp)-1)
        cur = len(seq) - 1
        dt = cur - tp[-1]
        df = cur - fp[-1]
        if dt >= avg_tg * 1.6:
            c = min(0.60 + (dt - avg_tg) / max(avg_tg, 1) * 0.08, 0.84)
            return True, c
        if df >= avg_fg * 1.6:
            c = min(0.60 + (df - avg_fg) / max(avg_fg, 1) * 0.08, 0.84)
            return False, c
        return None

    def _window_freq(self, seq: List[bool], window: int) -> Optional[Tuple[bool, float]]:
        chunk = seq[-window:] if len(seq) >= window else seq
        if len(chunk) < max(window // 2, 5):
            return None
        r = sum(chunk) / len(chunk)
        if abs(r - 0.5) < 0.15:
            return None
        pred = r < 0.5
        conf = min(0.52 + abs(r - 0.5) * 0.80, 0.82)
        return pred, conf

    def _entropy_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 12:
            return None
        w12 = seq[-12:]
        tc = sum(w12)
        if tc >= 11:
            return False, 0.86
        if tc <= 1:
            return True, 0.86
        if tc >= 10:
            return False, 0.80
        if tc <= 2:
            return True, 0.80
        if tc >= 9:
            return False, 0.74
        if tc <= 3:
            return True, 0.74
        return None

    def _run_length(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        runs = []
        cur_run = 1
        for i in range(1, len(seq)):
            if seq[i] == seq[i-1]:
                cur_run += 1
            else:
                runs.append(cur_run)
                cur_run = 1
        runs.append(cur_run)
        if len(runs) < 3:
            return None
        avg_run = sum(runs) / len(runs)
        sk, last = CauDetector.streak(seq)
        if sk >= avg_run * 1.8:
            return not last, min(0.64 + (sk / max(avg_run, 1) - 1.8) * 0.06, 0.86)
        if sk <= avg_run * 0.35 and sk >= 2:
            return last, 0.63
        return None

    def _oscillation(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 12:
            return None
        changes = sum(1 for i in range(len(seq)-12, len(seq)-1) if seq[i] != seq[i+1])
        if changes >= 10:
            return not seq[-1], 0.74
        if changes >= 9:
            return not seq[-1], 0.70
        if changes <= 2:
            sk, last = CauDetector.streak(seq)
            if sk >= 5:
                return not last, min(0.68 + sk * 0.02, 0.84)
        return None

    def _chi_balance(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        w = seq[-40:] if len(seq) >= 40 else seq
        if not w:
            return None
        r = sum(w) / len(w)
        if abs(r - 0.5) < 0.12:
            return None
        pred = r < 0.5
        conf = min(0.52 + abs(r - 0.5) * 0.65, 0.78)
        return pred, conf

    def _score_trend(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 10:
            return None
        r5 = sum(scores[-5:]) / 5
        o5 = sum(scores[-10:-5]) / 5
        diff = r5 - o5
        if abs(diff) < 0.8:
            return None
        return diff > 0, min(0.54 + abs(diff) * 0.045, 0.80)

    def _adaptive_ma(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 20:
            return None
        ma5  = sum(scores[-5:]) / 5
        ma20 = sum(scores[-20:]) / 20
        diff = ma5 - ma20
        if abs(diff) < 0.5:
            return None
        return diff > 0, min(0.54 + abs(diff) * 0.04, 0.80)

    def _linear_reg(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 12:
            return None
        n = min(len(scores), 25)
        y = scores[-n:]
        x = list(range(n))
        sx, sy = sum(x), sum(y)
        sxy = sum(x[i]*y[i] for i in range(n))
        sx2 = sum(i*i for i in x)
        d = n*sx2 - sx**2
        if d == 0:
            return None
        slope = (n*sxy - sx*sy) / d
        base = sum(y[-3:]) / 3
        pred_score = base + slope * 2
        if pred_score > 14.5:
            return False, 0.68
        if pred_score < 6.5:
            return True, 0.68
        if pred_score > 13.0:
            return False, 0.61
        if pred_score < 8.0:
            return True, 0.61
        return None

    def _perceptron(self, seq: List[bool], scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 15:
            return None
        l5  = seq[-5:]
        l3  = seq[-3:]
        tr12 = sum(seq[-12:]) / 12
        tr20 = sum(seq[-20:]) / min(len(seq), 20)
        avg7 = sum(scores[-7:]) / 7 if len(scores) >= 7 else 10.5
        sk, last = CauDetector.streak(seq)
        feats = [
            l5[0]*2-1, l5[1]*2-1, l5[2]*2-1, l5[3]*2-1, l5[4]*2-1,
            l3[0]*2-1, l3[1]*2-1, l3[2]*2-1,
            (tr12 - 0.5) * 2,
            (tr20 - 0.5) * 2,
            (avg7 - 10.5) / 5,
            (sk - 3) / 5,
            (1 if last else -1),
        ]
        ws = [0.35, 0.30, 0.26, 0.22, 0.18, 0.40, 0.35, 0.30,
              0.65, 0.45, 0.50, 0.35, 0.28]
        bias = 0.05
        dot = sum(f * w for f, w in zip(feats, ws)) + bias
        prob = 1 / (1 + math.exp(-dot * 1.3))
        if prob > 0.62:
            return True, min(prob, 0.90)
        if prob < 0.38:
            return False, min(1-prob, 0.90)
        return None

    def _cycle_detect(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 24:
            return None
        best_lag, best_corr = None, 0.0
        for lag in range(2, min(18, len(seq)//2)):
            n = len(seq) - lag
            corr = sum(1 for i in range(n) if seq[i] == seq[i+lag]) / n
            if corr > best_corr:
                best_corr, best_lag = corr, lag
        if best_corr > 0.68 and best_lag and len(seq) > best_lag:
            return seq[-best_lag], min(0.52 + best_corr * 0.40, 0.84)
        return None

    def _hot_cold(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 20:
            return None
        recent = scores[-20:]
        ht = sum(1 for s in recent if s > 13)
        hx = sum(1 for s in recent if s < 7)
        avg5 = sum(scores[-5:]) / 5
        if avg5 > 15.0 and ht > 9:
            return False, 0.74
        if avg5 < 5.0 and hx > 9:
            return True, 0.74
        if avg5 > 14.0 and ht > 7:
            return False, 0.67
        if avg5 < 6.0 and hx > 7:
            return True, 0.67
        return None

    def _prob_weight(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 10:
            return None
        cnt: Counter = Counter()
        for d1 in range(1, 7):
            for d2 in range(1, 7):
                for d3 in range(1, 7):
                    cnt[d1+d2+d3] += 1
        dp = {s: c/216 for s, c in cnt.items()}
        recent = scores[-12:]
        lo = sum(dp.get(s, 0) for s in recent if s <= 10)
        hi = sum(dp.get(s, 0) for s in recent if s > 10)
        if hi > lo * 1.6:
            return False, 0.63
        if lo > hi * 1.6:
            return True, 0.63
        return None

    def predict(self, state: dict) -> dict:
        history = state["history"]
        if len(history) < 8:
            return self._empty("Chờ đủ dữ liệu (cần ≥8 phiên)")

        seq: List[bool] = []
        scores: List[int] = []
        for g in reversed(list(history)):
            tx = is_tai(g["score"], g["faces"], self.gm)
            if tx is not None:
                seq.append(tx)
                scores.append(g["score"])

        if len(seq) < 6:
            return self._empty("Chưa đủ dữ liệu hợp lệ")

        cau = CauDetector.detect(seq)

        algos_results = []

        def vote(name: str, fn):
            try:
                r = fn()
                if r is not None:
                    pred, conf = r
                    algos_results.append((pred, conf, self.w(name), name))
            except Exception as e:
                log.debug("algo %s: %s", name, e)

        if cau["pred"] is not None:
            algos_results.append((cau["pred"], cau["conf"]/100, self.w("cau_detect"), "cau_detect"))

        for order, name in [(5,"markov5"),(4,"markov4"),(3,"markov3"),(2,"markov2"),(1,"markov1")]:
            vote(name, lambda o=order: self._markov(seq, o))

        vote("double_markov",   lambda: self._double_markov(seq))
        vote("transition_prob", lambda: self._transition_prob(seq))

        for depth, name in [(8,"pattern8"),(6,"pattern6"),(5,"pattern5"),(4,"pattern4"),(3,"pattern3")]:
            vote(name, lambda d=depth: self._pattern(seq, d))

        vote("streak_break",  lambda: self._streak_analysis(seq))
        vote("anti_streak",   lambda: self._anti_streak(seq))
        vote("zigzag",        lambda: self._zigzag_detect(seq))
        vote("gap_analysis",  lambda: self._gap_analysis(seq))
        vote("oscillation",   lambda: self._oscillation(seq))
        vote("momentum",      lambda: self._momentum(seq))
        vote("reversal",      lambda: self._reversal(seq))
        vote("support_resist",lambda: self._support_resist(seq))
        vote("trend_follow",  lambda: self._trend_follow(seq))
        vote("mean_revert",   lambda: self._mean_revert(seq))
        vote("block_analysis",lambda: self._block_analysis(seq))
        vote("volatility",    lambda: self._volatility(seq))
        vote("bayesian",      lambda: self._bayesian(seq))

        vote("window10",   lambda: self._window_freq(seq, 10))
        vote("window20",   lambda: self._window_freq(seq, 20))
        vote("window50",   lambda: self._window_freq(seq, 50))
        vote("entropy",    lambda: self._entropy_analysis(seq))
        vote("run_length", lambda: self._run_length(seq))
        vote("chi_balance",lambda: self._chi_balance(seq))
        vote("entropy_rate",lambda: self._entropy_rate(seq))

        vote("score_trend",  lambda: self._score_trend(scores))
        vote("adaptive_ma",  lambda: self._adaptive_ma(scores))
        vote("linear_reg",   lambda: self._linear_reg(scores))
        vote("hot_cold",     lambda: self._hot_cold(scores))
        vote("prob_weight",  lambda: self._prob_weight(scores))
        vote("score_dist",   lambda: self._score_dist(scores))

        vote("perceptron",  lambda: self._perceptron(seq, scores))
        vote("cycle_detect",lambda: self._cycle_detect(seq))

        if not algos_results:
            return {**self._empty("Không đủ tín hiệu"), "cau_type": cau["type"], "cau_desc": cau["desc"]}

        consensus_r = self._consensus_vote(algos_results)
        if consensus_r is not None:
            algos_results.append((consensus_r[0], consensus_r[1], self.w("consensus_vote"), "consensus_vote"))

        tai_score = sum(c * w for p, c, w, _ in algos_results if p)
        xiu_score = sum(c * w for p, c, w, _ in algos_results if not p)
        total = tai_score + xiu_score

        if total == 0:
            return {**self._empty("Tín hiệu trung hoà"), "algo_count": len(algos_results),
                    "cau_type": cau["type"], "cau_desc": cau["desc"]}

        pred_bool = tai_score >= xiu_score
        consensus = max(tai_score, xiu_score) / total

        cl = state.get("consec_loss", 0)
        if cl >= 6 and consensus < 0.74:
            return {**self._empty(f"🔴 Tạm dừng — Sai {cl} lần liên tiếp, chờ tín hiệu ≥74%"),
                    "algo_count": len(algos_results), "cau_type": cau["type"], "cau_desc": cau["desc"]}

        if cl >= 4 and consensus < 0.68:
            return {**self._empty(f"⚠️ Thận trọng — Sai {cl} lần, chờ tín hiệu mạnh hơn"),
                    "algo_count": len(algos_results), "cau_type": cau["type"], "cau_desc": cau["desc"]}

        if consensus < MIN_CONF_PREDICT:
            return {**self._empty(f"⚠️ Tín hiệu yếu ({consensus:.0%}) — Chờ cầu rõ hơn"),
                    "confidence": int(consensus * 100), "algo_count": len(algos_results),
                    "cau_type": cau["type"], "cau_desc": cau["desc"]}

        confidence = max(55, min(97, int(consensus * 100)))

        if len(seq) >= 20:
            recent20 = seq[-20:]
            r20 = sum(recent20) / len(recent20)
            if (pred_bool and r20 > 0.80) or (not pred_bool and r20 < 0.20):
                confidence = max(52, confidence - 10)

        if cau["pred"] == pred_bool and cau["conf"] >= 68:
            confidence = min(97, confidence + 5)
        elif cau["pred"] is not None and cau["pred"] != pred_bool and cau["conf"] >= 75:
            confidence = max(52, confidence - 6)

        tai_vote_count = sum(1 for p, c, w, _ in algos_results if p)
        xiu_vote_count = len(algos_results) - tai_vote_count
        maj_ratio = max(tai_vote_count, xiu_vote_count) / max(len(algos_results), 1)
        if maj_ratio >= 0.80:
            confidence = min(97, confidence + 3)
        elif maj_ratio < 0.60:
            confidence = max(52, confidence - 5)

        vi1 = vi2 = vi3 = 0
        if self.gm not in TX_ONLY_GAMES:
            rel_scores = [s for s in scores[-100:] if (s > 10) == pred_bool]
            if len(rel_scores) < 4:
                rel_scores = list(range(11, 18)) if pred_bool else list(range(3, 11))
            cnt2 = Counter(rel_scores)
            top = [v for v, _ in cnt2.most_common(15)]
            prev = state.get("prev_pred", {})
            pvs = {prev.get("vi1"), prev.get("vi2"), prev.get("vi3")}
            fresh = [v for v in top if v not in pvs] or top
            random.shuffle(fresh)
            sel = fresh[:3]
            attempts = 0
            while len(sel) < 3 and attempts < 20:
                e = random.randint(11, 17) if pred_bool else random.randint(3, 10)
                if e not in sel:
                    sel.append(e)
                attempts += 1
            sel.sort()
            if len(sel) >= 3:
                vi1, vi2, vi3 = sel[0], sel[1], sel[2]

        hw_parts = []
        for ws in [20, 50, 100]:
            chunk = seq[-ws:] if len(seq) >= ws else seq
            if len(chunk) >= 10:
                t = sum(chunk)
                x = len(chunk) - t
                hw_parts.append(f"{ws}v:{t}T/{x}X")

        winner_algos = [name for p, c, w, name in algos_results if p == pred_bool]

        return {
            "pred":           "TÀI" if pred_bool else "XỈU",
            "vi1": vi1, "vi2": vi2, "vi3": vi3,
            "confidence":     confidence,
            "algo_count":     len(algos_results),
            "cau_type":       cau["type"],
            "cau_desc":       cau["desc"],
            "cau_break_risk": cau.get("break_risk", 0),
            "history_windows": "  ".join(hw_parts),
            "consensus":      consensus,
            "winner_algos":   winner_algos,
            "tai_votes":      tai_vote_count,
            "xiu_votes":      xiu_vote_count,
        }

    def _empty(self, note: str = "") -> dict:
        return {
            "pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
            "confidence": 0, "algo_count": 0,
            "cau_type": "—", "cau_desc": "",
            "cau_break_risk": 0, "history_windows": "",
            "consensus": 0.0, "winner_algos": [],
            "tai_votes": 0, "xiu_votes": 0,
            "note": note,
        }


_engines: Dict[str, AdvancedPredictor] = {}


def get_engine(gm: str) -> AdvancedPredictor:
    if gm not in _engines:
        _engines[gm] = AdvancedPredictor(gm)
    return _engines[gm]


async def _fetch_sicbo(session: aiohttp.ClientSession) -> Optional[list]:
    for attempt in range(MAX_RETRIES):
        hdrs = {**SICBO_HEADERS, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                SICBO_API, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=8), ssl=False
            ) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.read()
                if not raw or raw.strip().startswith(b"<"):
                    continue
                data = json.loads(raw.decode("utf-8", errors="replace"))
                items = None
                dc = data.get("data")
                if isinstance(dc, dict):
                    items = dc.get("resultList") or dc.get("list") or dc.get("rows")
                elif isinstance(dc, list):
                    items = dc
                if not items:
                    items = data.get("resultList") or data.get("list") or data.get("rows")
                if isinstance(items, list) and items:
                    _states[SICBO]["api_ok"] = True
                    return items
        except Exception as e:
            log.debug("sicbo fetch #%d: %s", attempt+1, e)
        await asyncio.sleep(0.5 * (attempt+1))
    _states[SICBO]["api_ok"] = False
    return None


async def _fetch_lc(session: aiohttp.ClientSession, gm: str) -> Optional[list]:
    urls = {
        LC_MD5:  LC_MD5_API,
        LC_HU:   LC_HU_API,
        BET_MD5: BETVIP_MD5_API,
        BET_HU:  BETVIP_HU_API,
    }
    hdrs_map = {
        LC_MD5: LC_HEADERS, LC_HU: LC_HEADERS,
        BET_MD5: BETVIP_HEADERS, BET_HU: BETVIP_HEADERS,
    }
    url      = urls[gm]
    base_hdr = hdrs_map[gm]
    for attempt in range(MAX_RETRIES):
        hdrs = {**base_hdr, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                url, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=10), ssl=False
            ) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.read()
                if not raw:
                    continue
                data = json.loads(raw.decode("utf-8", errors="replace"))
                items = data.get("list") or data.get("data")
                if isinstance(items, list) and items:
                    _states[gm]["api_ok"] = True
                    return items
        except Exception as e:
            log.debug("lc %s fetch #%d: %s", gm, attempt+1, e)
        await asyncio.sleep(0.5 * (attempt+1))
    _states[gm]["api_ok"] = False
    return None


def _parse_sicbo(raw: dict) -> dict:
    faces = raw.get("facesList") or []
    score = raw.get("score") or sum(faces)
    f     = [int(x) for x in faces]
    s     = int(score)
    return {
        "game_num": str(raw.get("gameNum", "")),
        "score":    s,
        "faces":    f,
        "type":     classify_game(s, f, SICBO),
        "time":     datetime.now().strftime("%H:%M:%S"),
        "ts":       datetime.now().isoformat(),
    }


def _parse_lc(raw: dict, gm: str) -> dict:
    dices = raw.get("dices") or []
    point = raw.get("point") or sum(dices)
    d     = [int(x) for x in dices]
    p     = int(point)
    return {
        "game_num":   str(raw.get("id", "")),
        "score":      p,
        "faces":      d,
        "type":       classify_game(p, d, gm),
        "raw_result": raw.get("resultTruyenThong", ""),
        "time":       datetime.now().strftime("%H:%M:%S"),
        "ts":         datetime.now().isoformat(),
    }


async def _load_initial(session: aiohttp.ClientSession, gm: str):
    state    = _states[gm]
    raw_list = await (_fetch_sicbo(session) if gm == SICBO else _fetch_lc(session, gm))
    if not raw_list:
        log.warning("Cannot load history for %s", gm)
        return
    games = []
    for item in raw_list:
        try:
            g = _parse_sicbo(item) if gm == SICBO else _parse_lc(item, gm)
            if g["game_num"]:
                games.append(g)
        except Exception:
            pass
    if not games:
        return
    try:
        games.sort(key=lambda g: int(g["game_num"]))
    except Exception:
        pass
    state["history"].clear()
    for g in reversed(games):
        state["history"].appendleft(g)
    state["latest"] = games[-1]
    state["pred"]   = get_engine(gm).predict(state)
    log.info("✅ %s — loaded %d sessions", GAME_LABELS[gm], len(games))


_TYPE_EMO = {
    "TÀI": "🔴", "XỈU": "🔵", "BÃO": "🌪️",
    "NỔ HŨ TÀI": "🏺🔴", "NỔ HŨ XỈU": "🏺🔵", "CHỜ": "🟡",
}


def _te(t: str) -> str:
    return _TYPE_EMO.get(t, "🎲")


def _conf_bar(c: int) -> str:
    filled = round(c / 10)
    bar    = "█" * filled + "░" * (10 - filled)
    badge  = " 🔥🔥" if c >= 92 else (" 🔥" if c >= 85 else (" ⭐⭐" if c >= 78 else (" ⭐" if c >= 70 else "")))
    return f"{bar} <b>{c}%</b>{badge}"


def _build_msg(gm: str, pred: dict, prev_pred: dict, curr_game: dict) -> str:
    now     = datetime.now().strftime("%H:%M:%S %d/%m")
    label   = GAME_LABELS[gm]
    state   = _states[gm]
    tx_only = gm in TX_ONLY_GAMES

    result_block = outcome_block = special_block = ""

    if curr_game.get("game_num"):
        dice_str = " | ".join(f"[{d}]" for d in curr_game["faces"])
        c_type   = curr_game["type"]
        c_score  = curr_game["score"]
        result_block = (
            "\n\n📜 <b>PHIÊN VỪA KẾT THÚC</b>\n"
            "<blockquote>"
            f"🔢 Phiên    : <b>#{curr_game['game_num']}</b>\n"
            f"🎲 Xúc xắc : <b>{dice_str}</b>\n"
            f"💯 Tổng     : <b>{c_score}</b>\n"
            f"🏷 Kết quả  : <b>{_te(c_type)} {c_type}</b>\n"
            f"⏰ Lúc      : <b>{curr_game.get('time','—')}</b>"
            "</blockquote>"
        )
        sf = sorted(curr_game["faces"])
        if gm == SICBO and sf == [4, 4, 4]:
            special_block = "\n\n🌪️ <b>⚠️ BÃO 4-4-4 — MỌI CƯỢC THUA (TRỪ ĐẶT BÃO)!</b>"
        elif gm in (LC_HU, BET_HU):
            if sf == [1, 1, 1]:
                special_block = "\n\n🏺💥 <b>NỔ HŨ XỈU! 1-1-1 — JACKPOT!</b>"
            elif sf == [6, 6, 6]:
                special_block = "\n\n🏺💥 <b>NỔ HŨ TÀI! 6-6-6 — JACKPOT!</b>"

        if prev_pred and prev_pred.get("pred") in ("TÀI", "XỈU") and c_type in ("TÀI","XỈU","NỔ HŨ TÀI","NỔ HŨ XỈU"):
            pred_was_tai = prev_pred["pred"] == "TÀI"
            actual_tai   = "TÀI" in c_type
            if pred_was_tai == actual_tai:
                vi_hit = not tx_only and any(prev_pred.get(vk) == c_score for vk in ("vi1","vi2","vi3"))
                cw = state.get("consec_win", 0)
                win_str = f" (thắng {cw} liên tiếp 🔥)" if cw >= 3 else ""
                outcome_block = (
                    f"\n\n💎 <b>═════ CHUẨN VỊ! 🎯{win_str} ═════</b>"
                    if vi_hit else
                    f"\n\n🏆 <b>═════ ĐÚNG ✅{win_str} ═════</b>"
                )
            else:
                outcome_block = "\n\n💔 <b>═════ SAI ❌ ═════</b>"

    p_label   = pred.get("pred", "—")
    cau_type  = pred.get("cau_type", "")
    cau_desc  = pred.get("cau_desc", "")
    cau_risk  = pred.get("cau_break_risk", 0)
    hw_str    = pred.get("history_windows", "")
    consensus = pred.get("consensus", 0)
    api_st    = "🟢" if state["api_ok"] else "🔴"

    if p_label == "CHỜ":
        pred_block = (
            "🟡 <b>TẠM DỪNG — BOT AN TOÀN</b>\n"
            "<blockquote>"
            f"⚠️ {pred.get('note','Chờ tín hiệu rõ hơn')}\n"
            f"🤖 Thuật toán phân tích: <b>{pred.get('algo_count',0)}</b>\n"
            f"🃏 Loại cầu: <b>{cau_type}</b>"
            "</blockquote>"
        )
    else:
        conf   = pred.get("confidence", 50)
        algos  = pred.get("algo_count", 0)
        tv     = pred.get("tai_votes", 0)
        xv     = pred.get("xiu_votes", 0)
        filled = min(int(cau_risk / 20), 5)
        risk_bar = "🔴" * filled + "⚪" * (5 - filled) if cau_risk else "⚪⚪⚪⚪⚪"

        base_lines = (
            f"🎯 Dự đoán     : <b>{_te(p_label)} {p_label}</b>\n"
            f"📊 Độ tin cậy  : {_conf_bar(conf)}\n"
            f"🤝 Đồng thuận  : <b>{consensus:.0%}</b> ({tv}T/{xv}X)\n"
            "━━━━━━━━━━━━━━━━━━━\n"
        )
        if not tx_only:
            vi1, vi2, vi3 = pred.get("vi1","—"), pred.get("vi2","—"), pred.get("vi3","—")
            base_lines += (
                f"3️⃣ <b>VỊ TIN CẬY:</b>\n"
                f"   🥇 Vị 1 : <b>{vi1}</b>\n"
                f"   🥈 Vị 2 : <b>{vi2}</b>\n"
                f"   🥉 Vị 3 : <b>{vi3}</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
            )
        base_lines += (
            f"🃏 Loại cầu    : <b>{cau_type}</b>\n"
            f"📝 Phân tích   : <i>{cau_desc}</i>\n"
            f"⚡ Nguy cơ gãy : {risk_bar} <b>{cau_risk}%</b>\n"
        )
        if hw_str:
            base_lines += f"📈 Lịch sử     : <i>{hw_str}</i>\n"
        base_lines += f"🤖 Thuật toán  : <b>{algos} layers</b>"

        pred_block = (
            "🔮 <b>DỰ ĐOÁN PHIÊN TIẾP THEO</b>\n"
            f"<blockquote>{base_lines}</blockquote>"
        )

    return (
        f"🎲 <b>{label} — DỰ ĐOÁN TỰ ĐỘNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + pred_block
        + result_block
        + special_block
        + outcome_block
        + f"\n\n<i>🔄 {now} | {api_st} Live</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>       SicBo Bot </i>"
    )


def _record_pred(gm: str, pred: dict, actual: dict):
    if not pred or not actual.get("game_num"):
        return
    p_type  = pred.get("pred")
    a_type  = actual["type"]
    a_score = actual["score"]
    outcome = None
    vi_hit  = 0
    if p_type in ("TÀI", "XỈU") and a_type in ("TÀI","XỈU","NỔ HŨ TÀI","NỔ HŨ XỈU"):
        actual_tai = "TÀI" in a_type
        outcome    = "✅ ĐÚNG" if (p_type == "TÀI") == actual_tai else "❌ SAI"
    if gm not in TX_ONLY_GAMES:
        for vk in ("vi1","vi2","vi3"):
            if pred.get(vk) == a_score:
                vi_hit = 1
                break
    try:
        with _db() as db:
            db.execute(
                "INSERT OR IGNORE INTO predictions "
                "(game_mode,game_num,pred_type,pred_vi1,pred_vi2,pred_vi3,"
                "confidence,cau_type,actual_vi,actual_type,dice,outcome,vi_hit,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (gm, actual["game_num"], p_type,
                 pred.get("vi1"), pred.get("vi2"), pred.get("vi3"),
                 pred.get("confidence"), pred.get("cau_type"),
                 a_score, a_type,
                 "-".join(map(str, actual["faces"])),
                 outcome, vi_hit, actual["ts"])
            )
    except Exception as e:
        log.debug("record_pred: %s", e)


async def _broadcast_hot_streak(app: Application, gm: str, streak: int):
    label = GAME_LABELS[gm]
    text  = (
        f"🔥🔥🔥 <b>SÀN {label} ĐANG THÔNG {streak} PHIÊN LIÊN TIẾP!</b> 🔥🔥🔥\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Bot đã đúng <b>{streak}</b> phiên liên tiếp!\n"
        f"⚡ Đây là cơ hội vàng — Sàn đang cực kỳ ổn định!\n\n"
        f"🚀 Vào game ngay để không bỏ lỡ!\n"
        f"<i>🕐 {datetime.now().strftime('%H:%M:%S %d/%m/%Y')}</i>"
    )
    all_uids = set(_states[gm]["auto_msg"].keys())
    try:
        with _db() as db:
            rows = db.execute("SELECT user_id FROM allowed_users").fetchall()
            for r in rows:
                all_uids.add(r["user_id"])
    except Exception:
        pass

    for uid in all_uids:
        try:
            await app.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.06)
        except (Forbidden, TelegramError, Exception):
            pass
    log.info("Hot streak broadcast: %s streak=%d", gm, streak)


async def _push(app: Application, gm: str, prev_pred: dict, curr_game: dict):
    state    = _states[gm]
    pred     = state["pred"]
    auto_msg = state["auto_msg"]
    if not auto_msg or _maintenance["active"]:
        return
    text = _build_msg(gm, pred, prev_pred, curr_game)
    dead = []
    for chat_id in list(auto_msg.keys()):
        try:
            old_id = auto_msg.get(chat_id)
            if old_id:
                try:
                    await app.bot.delete_message(chat_id=chat_id, message_id=old_id)
                except Exception:
                    pass
            m = await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
            auto_msg[chat_id] = m.message_id
            await asyncio.sleep(0.05)
        except Forbidden:
            dead.append(chat_id)
        except Exception as e:
            log.debug("push %s %s: %s", gm, chat_id, e)
    for c in dead:
        auto_msg.pop(c, None)


async def _game_loop(app: Application, gm: str):
    state    = _states[gm]
    interval = SICBO_INTERVAL if gm == SICBO else LC_INTERVAL

    connector = aiohttp.TCPConnector(ssl=False, limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _load_initial(session, gm)

        while True:
            if _maintenance["active"]:
                await asyncio.sleep(1)
                continue
            try:
                await asyncio.sleep(interval)
                raw_list = await (_fetch_sicbo(session) if gm == SICBO else _fetch_lc(session, gm))
                if not raw_list:
                    continue

                latest_raw = raw_list[0]
                new_num    = str(latest_raw.get("gameNum" if gm == SICBO else "id", ""))
                if not new_num or new_num == state["latest"].get("game_num"):
                    continue

                prev_pred = state["pred"].copy()

                new_game = (_parse_sicbo(latest_raw) if gm == SICBO else _parse_lc(latest_raw, gm))
                state["history"].appendleft(new_game)
                state["latest"]    = new_game
                state["prev_pred"] = prev_pred

                if prev_pred.get("pred") in ("TÀI", "XỈU"):
                    pred_tai   = prev_pred["pred"] == "TÀI"
                    actual_tai = is_tai(new_game["score"], new_game["faces"], gm)
                    if actual_tai is not None:
                        correct = (pred_tai == actual_tai)
                        if correct:
                            state["consec_loss"] = 0
                            state["consec_win"]  = state.get("consec_win", 0) + 1
                            cw = state["consec_win"]
                            winner_algos = prev_pred.get("winner_algos", [])
                            if winner_algos:
                                get_engine(gm).update_weights_bulk(winner_algos, True)
                            if cw in HOT_STREAK_THRESHOLDS:
                                asyncio.create_task(_broadcast_hot_streak(app, gm, cw))
                        else:
                            state["consec_win"]  = 0
                            state["consec_loss"] = state.get("consec_loss", 0) + 1
                            loser_algos = prev_pred.get("winner_algos", [])
                            if loser_algos:
                                get_engine(gm).update_weights_bulk(loser_algos, False)

                state["pred"] = get_engine(gm).predict(state)
                _record_pred(gm, prev_pred, new_game)
                await _push(app, gm, prev_pred, new_game)

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("loop %s: %s", gm, e)
                await asyncio.sleep(3)


async def _notify_all(app: Application, text: str):
    notified = set()
    for gm in ALL_GAMES:
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            if chat_id not in notified:
                try:
                    await app.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                    notified.add(chat_id)
                except Exception:
                    pass


async def _start_maintenance(app: Application, minutes: int, reason: str):
    end = datetime.now() + timedelta(minutes=minutes)
    _maintenance.update({"active": True, "end_time": end, "reason": reason})
    await _notify_all(app,
        f"🔧 <b>BẢO TRÌ HỆ THỐNG</b>\n"
        f"⏳ <b>{minutes} phút</b>\n"
        f"📋 Lý do: {reason}\n"
        f"🕐 Xong: <b>{end.strftime('%H:%M %d/%m')}</b>"
    )

    async def _auto_end():
        await asyncio.sleep(minutes * 60)
        await _end_maintenance(app)

    if _maintenance["task"]:
        _maintenance["task"].cancel()
    _maintenance["task"] = asyncio.create_task(_auto_end())


async def _end_maintenance(app: Application):
    if not _maintenance["active"]:
        return
    _maintenance["active"] = False
    if _maintenance["task"]:
        _maintenance["task"].cancel()
        _maintenance["task"] = None
    await _notify_all(app, "✅ <b>BẢO TRÌ HOÀN TẤT!</b> Bot hoạt động trở lại.")


def _gen_key(prefix="SUNWIN") -> str:
    b = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    return f"{prefix}-{b[:4]}-{b[4:8]}-{b[8:12]}-{b[12:]}"


def _admin_only(fn):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_html("⛔ Chỉ admin mới dùng được lệnh này!")
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


async def _check_maint_lock(update: Update, cmd: str) -> bool:
    if _maintenance["active"]:
        end_str = (_maintenance["end_time"].strftime("%H:%M")
                   if _maintenance["end_time"] else "sắp tới")
        await update.message.reply_html(
            f"🔧 <b>Bot đang bảo trì!</b>\n"
            f"⏳ Xong lúc <b>{end_str}</b>\n"
            f"📋 Lý do: {_maintenance['reason']}"
        )
        return True
    if is_locked(cmd):
        reason = get_lock_reason(cmd)
        await update.message.reply_html(
            f"🔒 <b>Chức năng <code>{cmd}</code> đang tạm khóa!</b>\n"
            f"📋 Lý do: {reason}\n"
            "Liên hệ admin để biết thêm thông tin."
        )
        return True
    return False


async def _cmd_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE, gm: str, cmd_name: str):
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id
    if await _check_maint_lock(update, cmd_name):
        return
    if not is_allowed(uid):
        await update.message.reply_html(
            "🔒 <b>Chưa có quyền truy cập!</b>\n"
            "<blockquote>Dùng /trailkey để nhận key 2 giờ miễn phí\n"
            "Hoặc liên hệ admin mua key chính thức</blockquote>"
        )
        return
    state = _states[gm]
    if not state["latest"]:
        m = await update.message.reply_html(
            f"⏳ <b>Đang kết nối {GAME_LABELS[gm]}...</b>\n"
            "<i>Vui lòng thử lại sau vài giây.</i>"
        )
        state["auto_msg"][chat_id] = m.message_id
        return
    text   = _build_msg(gm, state["pred"], state.get("prev_pred", {}), state["latest"])
    old_id = state["auto_msg"].get(chat_id)
    if old_id:
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
        except Exception:
            pass
    m = await update.message.reply_html(text)
    state["auto_msg"][chat_id] = m.message_id
    try:
        await update.message.delete()
    except Exception:
        pass


async def cmd_autosicbo(u, c):    await _cmd_auto(u, c, SICBO,   "autosicbo")
async def cmd_auto_lc_md5(u, c):  await _cmd_auto(u, c, LC_MD5,  "auto_lc_md5")
async def cmd_auto_lc_hu(u, c):   await _cmd_auto(u, c, LC_HU,   "auto_lc_hu")
async def cmd_auto_bet_md5(u, c): await _cmd_auto(u, c, BET_MD5, "auto_bet_md5")
async def cmd_auto_bet_hu(u, c):  await _cmd_auto(u, c, BET_HU,  "auto_bet_hu")


async def _cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE, gm: str):
    chat_id = update.effective_chat.id
    state   = _states[gm]
    if chat_id in state["auto_msg"]:
        old_id = state["auto_msg"].pop(chat_id)
        if old_id:
            try:
                await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
            except Exception:
                pass
        await update.message.reply_html(f"⏹ <b>Đã dừng {GAME_LABELS[gm]} auto.</b>")
    else:
        await update.message.reply_html("ℹ️ Không có phiên auto nào đang chạy.")


async def cmd_stop_sicbo(u, c):    await _cmd_stop(u, c, SICBO)
async def cmd_stop_lc_md5(u, c):  await _cmd_stop(u, c, LC_MD5)
async def cmd_stop_lc_hu(u, c):   await _cmd_stop(u, c, LC_HU)
async def cmd_stop_bet_md5(u, c): await _cmd_stop(u, c, BET_MD5)
async def cmd_stop_bet_hu(u, c):  await _cmd_stop(u, c, BET_HU)


async def cmd_stop_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    stopped = []
    for gm in ALL_GAMES:
        if chat_id in _states[gm]["auto_msg"]:
            old_id = _states[gm]["auto_msg"].pop(chat_id)
            if old_id:
                try:
                    await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
                except Exception:
                    pass
            stopped.append(GAME_LABELS[gm])
    if stopped:
        await update.message.reply_html(
            "⏹ <b>Đã dừng tất cả auto:</b>\n" + "\n".join(f"• {g}" for g in stopped)
        )
    else:
        await update.message.reply_html("ℹ️ Không có phiên auto nào đang chạy.")


async def _cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE, gm: str, cmd_name: str):
    uid = update.effective_user.id
    if await _check_maint_lock(update, cmd_name):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    state = _states[gm]
    if not state["latest"]:
        await update.message.reply_html("⏳ Chưa có dữ liệu. Thử lại sau.")
        return
    text = _build_msg(gm, state["pred"], state.get("prev_pred", {}), state["latest"])
    await update.message.reply_html(text)


async def cmd_live(u, c):          await _cmd_live(u, c, SICBO,   "live")
async def cmd_live_md5(u, c):      await _cmd_live(u, c, LC_MD5,  "live_md5")
async def cmd_live_hu(u, c):       await _cmd_live(u, c, LC_HU,   "live_hu")
async def cmd_live_bet_md5(u, c):  await _cmd_live(u, c, BET_MD5, "live_bet_md5")
async def cmd_live_bet_hu(u, c):   await _cmd_live(u, c, BET_HU,  "live_bet_hu")


async def cmd_trailkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or ""
    name     = update.effective_user.full_name or str(uid)
    if await _check_maint_lock(update, "trailkey"):
        return

    with _db() as db:
        used = db.execute("SELECT 1 FROM trial_used WHERE user_id=?", (uid,)).fetchone()
    if used:
        await update.message.reply_html(
            "⚠️ <b>Bạn đã sử dụng key trải nghiệm rồi!</b>\n"
            "Mỗi tài khoản chỉ được dùng key Trial <b>1 lần duy nhất</b>.\n"
            "Liên hệ admin để nâng cấp tài khoản."
        )
        return

    k   = _gen_key("TRIAL")
    exp = (datetime.now() + timedelta(hours=2)).isoformat()
    with _db() as db:
        db.execute("INSERT INTO trial_used (user_id, used_at) VALUES (?,?)",
                   (uid, datetime.now().isoformat()))
        db.execute(
            "INSERT INTO activation_keys "
            "(key,created_by,created_at,expires_at,used_by,used_at,is_trial) "
            "VALUES (?,?,?,?,?,?,1)",
            (k, 0, datetime.now().isoformat(), exp, uid, datetime.now().isoformat())
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id,username,added_at,added_by) VALUES (?,?,?,?)",
            (uid, username, datetime.now().isoformat(), 0)
        )
        db.execute("INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)", (uid, exp))

    exp_fmt = datetime.fromisoformat(exp).strftime("%H:%M %d/%m/%Y")
    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(chat_id=adm, parse_mode=ParseMode.HTML,
                text=(f"🔔 <b>USER MỚI NHẬN TRIAL KEY!</b>\n"
                      f"<blockquote>👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                      f"🔑 <code>{k}</code>\n📅 Đến {exp_fmt}</blockquote>"))
        except Exception:
            pass

    await update.message.reply_html(
        "🎁 <b>KEY TRẢI NGHIỆM 2 GIỜ</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"🔑 <code>{k}</code>\n"
        f"⏳ Hết hạn: <b>{exp_fmt}</b>\n"
        "✅ Đã kích hoạt tự động!\n"
        "⚠️ Key Trial chỉ dùng được <b>1 lần/tài khoản</b>"
        "</blockquote>\n\n"
        "🚀 Dùng /autosicbo, /auto_lc_md5, /auto_lc_hu,\n"
        "/auto_bet_md5 hoặc /auto_bet_hu để bắt đầu!"
    )


async def cmd_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or ""
    name     = update.effective_user.full_name or str(uid)
    if await _check_maint_lock(update, "key"):
        return
    if not ctx.args:
        await update.message.reply_html("❌ Dùng: <code>/key YOUR_KEY_HERE</code>")
        return
    key = ctx.args[0].strip()
    with _db() as db:
        row = db.execute("SELECT * FROM activation_keys WHERE key=?", (key,)).fetchone()
        if not row:
            await update.message.reply_html("❌ Key không tồn tại!")
            return
        if row["used_by"] and int(row["used_by"]) != uid:
            await update.message.reply_html("❌ Key đã được người khác sử dụng!")
            return
        try:
            if datetime.now() > datetime.fromisoformat(row["expires_at"]):
                await update.message.reply_html("❌ Key đã hết hạn!")
                return
        except Exception:
            await update.message.reply_html("❌ Dữ liệu key lỗi.")
            return
        db.execute("UPDATE activation_keys SET used_by=?,used_at=? WHERE key=?",
                   (uid, datetime.now().isoformat(), key))
        db.execute("INSERT OR IGNORE INTO allowed_users (user_id,username,added_at,added_by) VALUES (?,?,?,?)",
                   (uid, username, datetime.now().isoformat(), 0))
        db.execute("INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)",
                   (uid, row["expires_at"]))
        exp_fmt = row["expires_at"][:16].replace("T", " ")

    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(chat_id=adm, parse_mode=ParseMode.HTML,
                text=(f"🔔 <b>USER KÍCH HOẠT KEY!</b>\n"
                      f"<blockquote>👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                      f"🔑 <code>{key}</code>\n📅 Đến {exp_fmt}</blockquote>"))
        except Exception:
            pass
    await update.message.reply_html(
        "🔑 <b>KÍCH HOẠT THÀNH CÔNG!</b>\n"
        f"<blockquote>📅 Hết hạn: <b>{exp_fmt}</b></blockquote>\n"
        "🚀 Dùng /autosicbo, /auto_lc_md5, /auto_lc_hu,\n"
        "/auto_bet_md5 hoặc /auto_bet_hu để bắt đầu!"
    )


LOCKABLE_CMDS = {
    "autosicbo", "auto_lc_md5", "auto_lc_hu", "auto_bet_md5", "auto_bet_hu",
    "live", "live_md5", "live_hu", "live_bet_md5", "live_bet_hu",
    "trailkey", "key", "listkq", "stop_auto",
}

_CMD_TO_GAME = {
    "autosicbo": SICBO, "live": SICBO,
    "auto_lc_md5": LC_MD5, "live_md5": LC_MD5,
    "auto_lc_hu":  LC_HU,  "live_hu":  LC_HU,
    "auto_bet_md5": BET_MD5, "live_bet_md5": BET_MD5,
    "auto_bet_hu":  BET_HU,  "live_bet_hu":  BET_HU,
}


@_admin_only
async def cmd_lock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        locked_now = ", ".join(f"<code>{c}</code>" for c in sorted(_locked_cmds)) or "Không có"
        await update.message.reply_html(
            "🔒 <b>KHÓA CHỨC NĂNG</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Dùng: <code>/lock {lệnh} [lý do]</code>\n\n"
            "<b>Lệnh có thể khóa:</b>\n"
            "<blockquote>"
            + "\n".join(f"• <code>{c}</code>" for c in sorted(LOCKABLE_CMDS)) +
            "</blockquote>\n"
            f"🔒 Đang khóa: {locked_now}"
        )
        return

    cmd    = ctx.args[0].lower().strip("/")
    reason = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else "Bảo trì"
    lock_cmd(cmd, update.effective_user.id, reason)

    gm = _CMD_TO_GAME.get(cmd)
    if gm:
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await ctx.bot.send_message(
                    chat_id=chat_id, parse_mode=ParseMode.HTML,
                    text=(f"🔒 <b>CHỨC NĂNG TẠM KHÓA</b>\n"
                          f"<blockquote><code>{cmd}</code> đang bảo trì\n"
                          f"Lý do: {reason}</blockquote>")
                )
            except Exception:
                pass

    await update.message.reply_html(
        f"🔒 <b>Đã khóa lệnh <code>{cmd}</code></b>\n"
        f"<blockquote>Lý do: {reason}\nĐã lưu vào DB ✅</blockquote>"
    )


@_admin_only
async def cmd_ulock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        locked_now = ", ".join(f"<code>{c}</code>" for c in sorted(_locked_cmds)) or "Không có"
        await update.message.reply_html(
            f"🔓 <b>MỞ KHÓA CHỨC NĂNG</b>\n"
            f"Dùng: <code>/ulock {{lệnh}}</code>\n"
            f"Đang khóa: {locked_now}"
        )
        return

    cmd = ctx.args[0].lower().strip("/")
    if not is_locked(cmd):
        await update.message.reply_html(f"ℹ️ Lệnh <code>{cmd}</code> không bị khóa.")
        return
    unlock_cmd(cmd)

    gm = _CMD_TO_GAME.get(cmd)
    if gm:
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await ctx.bot.send_message(
                    chat_id=chat_id, parse_mode=ParseMode.HTML,
                    text=f"🔓 <b>Chức năng <code>{cmd}</code> đã được mở khóa!</b>\n✅"
                )
            except Exception:
                pass

    await update.message.reply_html(
        f"🔓 <b>Đã mở khóa lệnh <code>{cmd}</code></b>\n"
        "<blockquote>Đã xóa khỏi DB ✅</blockquote>"
    )


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = update.effective_user.first_name or "bạn"
    role = ("👑 Admin" if is_admin(uid)
            else ("✅ Thành viên" if is_allowed(uid) else "🔒 Chưa kích hoạt"))

    welcome_text = (
        "🎲 <b>SICBO &amp; LẨU CUA &amp; BETVIP</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Chào <b>{name}</b>! [{role}]\n\n"
        "<blockquote>"
        "🤖 Dự đoán Tài/Xỉu tự động\n"
        "🎲 SicBo Sunwin | 🦀 LC MD5 | 🏺 LC Hũ\n"
        "🎰 Betvip MD5   | 🎯 Betvip Hũ\n"
        "━━━━━━━━━━━━━━\n"
        "</blockquote>\n\n"
        "📋 /help — Xem toàn bộ lệnh\n"
        "💡 <i>/trailkey — Nhận key 2 giờ miễn phí!</i>"
    )

    gif_sent = False
    if os.path.isfile(WELCOME_GIF_PATH):
        try:
            with open(WELCOME_GIF_PATH, "rb") as gif_file:
                await update.message.reply_animation(
                    animation=InputFile(gif_file, filename="welcome.gif"),
                    caption=welcome_text,
                    parse_mode=ParseMode.HTML
                )
            gif_sent = True
        except Exception as e:
            log.warning("Không gửi được GIF: %s", e)

    if not gif_sent:
        await update.message.reply_html(welcome_text)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    base = (
        "📖 <b>HƯỚNG DẪN SỬ DỤNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎲 <b>SicBo Sunwin:</b>\n"
        "<blockquote>/autosicbo — Auto dự đoán SicBo\n"
        "/stop_sicbo — Dừng SicBo\n"
        "/live — Xem trực tiếp SicBo</blockquote>\n"
        "🦀 <b>Lẩu Cua MD5:</b>\n"
        "<blockquote>/auto_lc_md5 — Auto dự đoán LC MD5\n"
        "/stop_lc_md5 — Dừng LC MD5\n"
        "/live_md5 — Xem trực tiếp LC MD5</blockquote>\n"
        "🏺 <b>Lẩu Cua Hũ:</b>\n"
        "<blockquote>/auto_lc_hu — Auto dự đoán LC Hũ\n"
        "/stop_lc_hu — Dừng LC Hũ\n"
        "/live_hu — Xem trực tiếp LC Hũ</blockquote>\n"
        "🎰 <b>Betvip MD5:</b>\n"
        "<blockquote>/auto_bet_md5 — Auto dự đoán Betvip MD5\n"
        "/stop_bet_md5 — Dừng Betvip MD5\n"
        "/live_bet_md5 — Xem trực tiếp Betvip MD5</blockquote>\n"
        "🎯 <b>Betvip Hũ:</b>\n"
        "<blockquote>/auto_bet_hu — Auto dự đoán Betvip Hũ\n"
        "/stop_bet_hu — Dừng Betvip Hũ\n"
        "/live_bet_hu — Xem trực tiếp Betvip Hũ</blockquote>\n"
        "📊 <b>Lịch sử:</b>\n"
        "<blockquote>/listkq sicbo | md5 | hu | bet_md5 | bet_hu</blockquote>\n"
        "👤 <b>Chung:</b>\n"
        "<blockquote>/stop_auto — Dừng TẤT CẢ\n"
        "/trailkey — Key trải nghiệm 2h (1 lần)\n"
        "/key {key} — Kích hoạt key\n"
        "/info — Thông tin tài khoản</blockquote>"
    )
    admin_extra = ""
    if is_admin(uid):
        locked_list = ", ".join(f"<code>{c}</code>" for c in sorted(_locked_cmds)) or "Không có"
        admin_extra = (
            "\n👑 <b>Admin:</b>\n"
            "<blockquote>"
            "/add {id} [hours] — Thêm user\n"
            "/bo {id} — Xoá user\n"
            "/luser — Danh sách user\n"
            "/tkey [hours] — Tạo key\n"
            "/delkey {key} — Xoá key\n"
            "/lkey — Danh sách key\n"
            "/noti {msg} — Broadcast\n"
            "/stat — Thống kê bot\n"
            "/baotri {phút} {lý do} — Bảo trì\n"
            "/huybaotri — Hủy bảo trì\n"
            "/lock {lệnh} [lý do] — Khóa chức năng\n"
            "/ulock {lệnh} — Mở khóa chức năng\n"
            f"🔒 Đang khóa: {locked_list}\n"
            "/reset_weights — Reset AI weights"
            "</blockquote>"
        )
    await update.message.reply_html(base + admin_extra)


async def cmd_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = update.effective_user.full_name or str(uid)
    if is_admin(uid):
        role, exp = "👑 Admin", "♾ Vĩnh viễn"
    elif is_allowed(uid):
        role, exp = "✅ Thành viên", get_expiry_str(uid)
    else:
        role, exp = "❌ Chưa kích hoạt", "—"

    with _db() as db:
        total   = db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        correct = db.execute("SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%'").fetchone()[0]
        vi_hits = db.execute("SELECT COUNT(*) FROM predictions WHERE vi_hit=1").fetchone()[0]
    acc    = f"{correct/total*100:.1f}%" if total else "—"
    vi_acc = f"{vi_hits/total*100:.1f}%" if total else "—"

    auto_status = [GAME_LABELS[gm] for gm in ALL_GAMES
                   if update.effective_chat.id in _states[gm]["auto_msg"]]

    streak_info = []
    for gm in ALL_GAMES:
        cw = _states[gm].get("consec_win", 0)
        if cw >= 2:
            streak_info.append(f"{GAME_LABELS[gm]}: 🔥{cw}")

    await update.message.reply_html(
        "👤 <b>THÔNG TIN TÀI KHOẢN</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"🪪 Tên     : <b>{name}</b>\n"
        f"🆔 ID      : <code>{uid}</code>\n"
        f"🏷 Vai trò : <b>{role}</b>\n"
        f"📅 Hết hạn : <b>{exp}</b>\n"
        f"📊 Độ chính xác: <b>{acc}</b>\n"
        f"🎯 Vị chính xác: <b>{vi_acc}</b>\n"
        f"🔴 Auto    : {', '.join(auto_status) or 'Không'}\n"
        f"{'🔥 Streak: ' + ' | '.join(streak_info) if streak_info else ''}"
        "</blockquote>"
    )


async def cmd_listkq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if await _check_maint_lock(update, "listkq"):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return

    arg = ctx.args[0].lower() if ctx.args else "sicbo"
    gm_map = {
        "sicbo": SICBO, "md5": LC_MD5, "hu": LC_HU,
        "lc_md5": LC_MD5, "lc_hu": LC_HU,
        "bet_md5": BET_MD5, "bet_hu": BET_HU,
    }
    gm      = gm_map.get(arg, SICBO)
    tx_only = gm in TX_ONLY_GAMES

    with _db() as db:
        rows = db.execute(
            "SELECT * FROM predictions WHERE game_mode=? ORDER BY id DESC LIMIT 15", (gm,)
        ).fetchall()
    if not rows:
        await update.message.reply_html(f"📭 Chưa có lịch sử cho {GAME_LABELS[gm]}.")
        return

    correct = sum(1 for r in rows if r["outcome"] and "ĐÚNG" in r["outcome"])
    vi_hit  = sum(1 for r in rows if r["vi_hit"])
    total   = len(rows)
    acc     = f"{correct/total*100:.0f}%"
    vi_stat = f" • Vị: {vi_hit}" if not tx_only else ""

    lines = [
        f"📜 <b>LỊCH SỬ {GAME_LABELS[gm]}</b>",
        f"<i>15 phiên • Đúng: {correct}/{total} ({acc}){vi_stat}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in rows:
        out = r["outcome"] or "⏳ CHỜ"
        vm  = " 🎯" if r["vi_hit"] else ""
        ct  = f" [{r['cau_type']}]" if r["cau_type"] else ""
        if tx_only:
            lines.append(
                "<blockquote>"
                f"📌 <b>#{r['game_num']}</b> {out}{ct}\n"
                f"🎯 Dự: {r['pred_type'] or '—'}\n"
                f"🎲 {r['dice'] or '—'} = <b>{r['actual_vi']}</b> {r['actual_type'] or ''}"
                "</blockquote>"
            )
        else:
            lines.append(
                "<blockquote>"
                f"📌 <b>#{r['game_num']}</b> {out}{vm}{ct}\n"
                f"🎯 {r['pred_type']} | Vị: {r['pred_vi1']}/{r['pred_vi2']}/{r['pred_vi3']}\n"
                f"🎲 {r['dice'] or '—'} = <b>{r['actual_vi']}</b> {r['actual_type'] or ''}"
                "</blockquote>"
            )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n<i>...</i>"
    await update.message.reply_html(text)


@_admin_only
async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/add {user_id} [hours]</code>")
        return
    try:
        tid   = int(ctx.args[0])
        hours = int(ctx.args[1]) if len(ctx.args) > 1 else 720
    except ValueError:
        await update.message.reply_html("❌ Tham số không hợp lệ!")
        return
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute("INSERT OR IGNORE INTO allowed_users (user_id,added_at,added_by) VALUES (?,?,?)",
                   (tid, datetime.now().isoformat(), update.effective_user.id))
        db.execute("INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)", (tid, exp))
    await update.message.reply_html(
        f"✅ Đã thêm <code>{tid}</code>\n"
        f"📅 Hạn: {exp[:16].replace('T',' ')} ({hours}h)"
    )


@_admin_only
async def cmd_bo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/bo {user_id}</code>")
        return
    try:
        tid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_html("❌ ID không hợp lệ!")
        return
    with _db() as db:
        db.execute("DELETE FROM allowed_users WHERE user_id=?", (tid,))
        db.execute("DELETE FROM user_expiry   WHERE user_id=?", (tid,))
    for gm in ALL_GAMES:
        _states[gm]["auto_msg"].pop(tid, None)
    await update.message.reply_html(f"✅ Đã xoá user <code>{tid}</code>!")


@_admin_only
async def cmd_luser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    with _db() as db:
        rows = db.execute(
            "SELECT u.user_id,u.username,u.added_at,e.expires_at "
            "FROM allowed_users u LEFT JOIN user_expiry e ON u.user_id=e.user_id "
            "ORDER BY u.added_at DESC"
        ).fetchall()
    if not rows:
        await update.message.reply_html("📭 Chưa có user nào.")
        return
    lines = [f"👥 <b>DANH SÁCH USER ({len(rows)})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    now = datetime.now()
    for r in rows:
        exp   = r["expires_at"][:16].replace("T"," ") if r["expires_at"] else "∞"
        uname = f"@{r['username']}" if r["username"] else "—"
        alive = ""
        if r["expires_at"]:
            try:
                alive = " ✅" if now < datetime.fromisoformat(r["expires_at"]) else " ⛔"
            except Exception:
                pass
        lines.append(f"• <code>{r['user_id']}</code> {uname} — {exp}{alive}")
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n<i>...</i>"
    await update.message.reply_html(text)


@_admin_only
async def cmd_tkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    hours = 720
    if ctx.args:
        try:
            hours = int(ctx.args[0])
        except ValueError:
            pass
    k   = _gen_key("SUNWIN")
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute(
            "INSERT INTO activation_keys (key,created_by,created_at,expires_at) VALUES (?,?,?,?)",
            (k, update.effective_user.id, datetime.now().isoformat(), exp)
        )
    await update.message.reply_html(
        "🔑 <b>KEY MỚI TẠO</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"Key  : <code>{k}</code>\n"
        f"Hạn  : {exp[:16].replace('T',' ')} ({hours}h)"
        "</blockquote>"
    )


@_admin_only
async def cmd_delkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/delkey {key}</code>")
        return
    key = ctx.args[0].strip()
    with _db() as db:
        db.execute("DELETE FROM activation_keys WHERE key=?", (key,))
    await update.message.reply_html(f"✅ Đã xoá key <code>{key}</code>")


@_admin_only
async def cmd_lkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    with _db() as db:
        rows = db.execute(
            "SELECT * FROM activation_keys ORDER BY created_at DESC LIMIT 25"
        ).fetchall()
    if not rows:
        await update.message.reply_html("📭 Chưa có key nào.")
        return
    lines = [f"🔑 <b>DANH SÁCH KEY ({len(rows)})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        used  = f"✅ {r['used_by']}" if r["used_by"] else "🟡 Chưa dùng"
        trial = " [TRIAL]" if r["is_trial"] else ""
        exp   = r["expires_at"][:16].replace("T"," ")
        lines.append(
            "<blockquote>"
            f"🔑 <code>{r['key']}</code>{trial}\n"
            f"📅 {exp} | {used}"
            "</blockquote>"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n<i>...</i>"
    await update.message.reply_html(text)


@_admin_only
async def cmd_noti(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/noti {thông báo}</code>")
        return
    msg = " ".join(ctx.args)
    with _db() as db:
        users = db.execute("SELECT user_id FROM allowed_users").fetchall()
    sent = fail = 0
    for row in users:
        try:
            await ctx.bot.send_message(
                chat_id=row["user_id"], parse_mode=ParseMode.HTML,
                text=(f"📢 <b>THÔNG BÁO TỪ ADMIN</b>\n"
                      "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                      f"<blockquote>{msg}</blockquote>\n"
                      f"<i>🕐 {datetime.now().strftime('%H:%M %d/%m/%Y')}</i>")
            )
            sent += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.08)
    await update.message.reply_html(
        f"📢 <b>Gửi xong</b>\n"
        f"<blockquote>✅ {sent} | ❌ {fail} | 📊 {sent+fail}</blockquote>"
    )


@_admin_only
async def cmd_stat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lines = ["📊 <b>THỐNG KÊ ĐỘ CHÍNH XÁC</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    with _db() as db:
        for gm in ALL_GAMES:
            total   = db.execute("SELECT COUNT(*) FROM predictions WHERE game_mode=?", (gm,)).fetchone()[0]
            correct = db.execute("SELECT COUNT(*) FROM predictions WHERE game_mode=? AND outcome LIKE '%ĐÚNG%'", (gm,)).fetchone()[0]
            vi      = db.execute("SELECT COUNT(*) FROM predictions WHERE game_mode=? AND vi_hit=1", (gm,)).fetchone()[0]
            acc     = f"{correct/total*100:.1f}%" if total else "—"
            vi_str  = (f" | Vị: <b>{vi}</b> ({vi/total*100:.1f}%)"
                       if (total and gm not in TX_ONLY_GAMES) else "")
            cl  = _states[gm].get("consec_loss", 0)
            cw  = _states[gm].get("consec_win", 0)
            api = "🟢" if _states[gm]["api_ok"] else "🔴"
            auto_count = len(_states[gm]["auto_msg"])
            lines.append(
                f"\n{GAME_LABELS[gm]} {api} | 👥 {auto_count}\n"
                "<blockquote>"
                f"Tổng: <b>{total}</b> | Đúng: <b>{correct}</b> ({acc}){vi_str}\n"
                f"🔥 Win: <b>{cw}</b> | ❌ Loss: <b>{cl}</b> | Hist: <b>{len(_states[gm]['history'])}</b>"
                "</blockquote>"
            )

    locked_str = ", ".join(f"<code>{c}</code>" for c in sorted(_locked_cmds)) or "Không có"
    lines.append(
        f"\n🔧 Bảo trì: {'🔴 Đang bảo trì' if _maintenance['active'] else '🟢 Bình thường'}\n"
        f"🔒 Khóa: {locked_str}"
    )
    await update.message.reply_html("\n".join(lines))


@_admin_only
async def cmd_baotri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html(
            "🔧 Dùng: <code>/baotri &lt;phút&gt; &lt;lý do&gt;</code>"
        )
        return
    try:
        minutes = int(ctx.args[0])
    except ValueError:
        await update.message.reply_html("❌ Số phút không hợp lệ!")
        return
    reason = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else "Bảo trì định kỳ"
    if _maintenance["active"]:
        await update.message.reply_html("⚠️ Bot đang bảo trì rồi!")
        return
    await _start_maintenance(ctx.application, minutes, reason)
    await update.message.reply_html(
        f"🔧 <b>ĐÃ BẮT ĐẦU BẢO TRÌ</b>\n"
        f"⏳ <b>{minutes} phút</b> | 📋 {reason}\n"
        f"🕐 Xong: <b>{_maintenance['end_time'].strftime('%H:%M %d/%m')}</b>"
    )


@_admin_only
async def cmd_huybaotri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _maintenance["active"]:
        await update.message.reply_html("ℹ️ Không có bảo trì nào đang diễn ra.")
        return
    await _end_maintenance(ctx.application)
    await update.message.reply_html("✅ <b>Đã hủy bảo trì!</b> Bot hoạt động trở lại.")


@_admin_only
async def cmd_reset_weights(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    for gm in ALL_GAMES:
        _engines.pop(gm, None)
        get_engine(gm)
    with _db() as db:
        db.execute("DELETE FROM algo_weights")
    await update.message.reply_html("🔄 <b>Đã reset toàn bộ trọng số AI về mặc định.</b>")


async def _cleanup_loop():
    while True:
        await asyncio.sleep(1800)
        try:
            with _db() as db:
                now = datetime.now().isoformat()
                expired = db.execute(
                    "SELECT user_id FROM user_expiry WHERE expires_at < ?", (now,)
                ).fetchall()
                for row in expired:
                    uid = row["user_id"]
                    db.execute("DELETE FROM allowed_users WHERE user_id=?", (uid,))
                    db.execute("DELETE FROM user_expiry   WHERE user_id=?", (uid,))
                    for gm in ALL_GAMES:
                        _states[gm]["auto_msg"].pop(uid, None)
                db.execute(
                    "DELETE FROM activation_keys WHERE used_by IS NOT NULL AND expires_at < ?",
                    (now,)
                )
        except Exception as e:
            log.debug("cleanup: %s", e)


async def post_init(app: Application):
    for gm in ALL_GAMES:
        asyncio.create_task(_game_loop(app, gm))
    asyncio.create_task(_cleanup_loop())
    log.info("✅ All %d game loops started.", len(ALL_GAMES))


def main():
    init_db()
    for gm in ALL_GAMES:
        get_engine(gm)

    if os.path.isfile(WELCOME_GIF_PATH):
        log.info("✅ welcome.gif found at: %s", WELCOME_GIF_PATH)
    else:
        log.warning("⚠️ welcome.gif NOT found at: %s", WELCOME_GIF_PATH)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    handlers = [
        CommandHandler("start",          cmd_start),
        CommandHandler("help",           cmd_help),
        CommandHandler("info",           cmd_info),
        CommandHandler("trailkey",       cmd_trailkey),
        CommandHandler("key",            cmd_key),
        CommandHandler("listkq",         cmd_listkq),
        CommandHandler("autosicbo",      cmd_autosicbo),
        CommandHandler("auto_lc_md5",    cmd_auto_lc_md5),
        CommandHandler("auto_lc_hu",     cmd_auto_lc_hu),
        CommandHandler("auto_bet_md5",   cmd_auto_bet_md5),
        CommandHandler("auto_bet_hu",    cmd_auto_bet_hu),
        CommandHandler("stop_sicbo",     cmd_stop_sicbo),
        CommandHandler("stop_lc_md5",    cmd_stop_lc_md5),
        CommandHandler("stop_lc_hu",     cmd_stop_lc_hu),
        CommandHandler("stop_bet_md5",   cmd_stop_bet_md5),
        CommandHandler("stop_bet_hu",    cmd_stop_bet_hu),
        CommandHandler("stop_auto",      cmd_stop_auto),
        CommandHandler("live",           cmd_live),
        CommandHandler("live_md5",       cmd_live_md5),
        CommandHandler("live_hu",        cmd_live_hu),
        CommandHandler("live_bet_md5",   cmd_live_bet_md5),
        CommandHandler("live_bet_hu",    cmd_live_bet_hu),
        CommandHandler("add",            cmd_add),
        CommandHandler("bo",             cmd_bo),
        CommandHandler("luser",          cmd_luser),
        CommandHandler("tkey",           cmd_tkey),
        CommandHandler("delkey",         cmd_delkey),
        CommandHandler("lkey",           cmd_lkey),
        CommandHandler("noti",           cmd_noti),
        CommandHandler("stat",           cmd_stat),
        CommandHandler("baotri",         cmd_baotri),
        CommandHandler("huybaotri",      cmd_huybaotri),
        CommandHandler("reset_weights",  cmd_reset_weights),
        CommandHandler("lock",           cmd_lock),
        CommandHandler("ulock",          cmd_ulock),
    ]
    for h in handlers:
        app.add_handler(h)

    log.info("🎲 SicBo + LC + Betvip Bot Ultra v9.0 starting...")
    app.run_polling(drop_pending_updates=True, poll_interval=1)


if __name__ == "__main__":
    main()
