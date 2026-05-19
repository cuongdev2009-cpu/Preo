#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SicBo + LC + Betvip Bot Ultra v7.0
Nâng cấp: betvip hũ/md5, lock/ulock, thuật toán dự đoán mạnh hơn,
          lịch sử dự đoán đầy đủ, multi-user auto, polling nhanh hơn.
"""

import asyncio
import json
import logging
import math
import random
import sqlite3
import string
from collections import Counter, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
BOT_TOKEN  = "8828842195:AAGdzF60aoUbBv6PJf8_LnQ0AunYF3UN8C8"
ADMIN_IDS  = [8001225219]
DB_PATH    = "bot_ultra.db"
MEM_WINDOW = 500          # số phiên giữ trong RAM

SICBO_INTERVAL   = 2.0    # giây polling SicBo
LC_INTERVAL      = 0.5    # giây polling tài/xỉu (LC + Betvip)
MAX_RETRIES      = 3
MIN_CONF_PREDICT = 0.60   # ngưỡng tối thiểu để ra dự đoán (60% đồng thuận)

# ── API endpoints ───────────────────────────────────────────────────────
SICBO_API = (
    "https://api.wsktnus8.net/v2/history/getLastResult"
    "?gameId=ktrng_3979&size=100&tableId=39791215743193&curPage=1"
)
LC_MD5_API = (
    "https://wtxmd52.tele68.com/v1/txmd5/lite-sessions"
    "?cp=R&cl=R&pf=web&at=07d01d98fd85e91efaa91fe492970412"
)
LC_HU_API = (
    "https://wtx.tele68.com/v1/tx/lite-sessions"
    "?cp=R&cl=R&pf=web&at=07d01d98fd85e91efaa91fe492970412"
)
BETVIP_MD5_API = (
    "https://wtxmd52.macminim6.online/v1/txmd5/lite-sessions"
    "?cp=R&cl=R&pf=web&at=4256ce1eed33ffa0e0990d398f1f907f"
)
BETVIP_HU_API = (
    "https://wtx.macminim6.online/v1/tx/sessions"
    "?cp=R&cl=R&pf=web&at=4256ce1eed33ffa0e0990d398f1f907f"
)

SICBO_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Referer": "https://sunwin.gs/",
    "Origin":  "https://sunwin.gs",
    "Cache-Control": "no-cache",
    "Pragma":  "no-cache",
}
LC_HEADERS = {
    "accept": "*/*",
    "accept-language": "vi-VN,vi;q=0.9",
    "Referer": "https://lc79b.bet/",
}
BETVIP_HEADERS = {
    "accept": "*/*",
    "accept-language": "vi-VN,vi;q=0.9",
    "Referer": "https://betvip.net/",
}

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/112.0.0.0 Mobile Safari/537.36",
]

# ═══════════════════════════════════════════════════════════════════════
# GAME MODE CONSTANTS
# ═══════════════════════════════════════════════════════════════════════
SICBO     = "sicbo"
LC_MD5    = "lc_md5"
LC_HU     = "lc_hu"
BET_MD5   = "bet_md5"
BET_HU    = "bet_hu"

ALL_GAMES = (SICBO, LC_MD5, LC_HU, BET_MD5, BET_HU)

GAME_LABELS = {
    SICBO:   "🎲 SICBO SUNWIN",
    LC_MD5:  "🦀 LẨU CUA MD5",
    LC_HU:   "🏺 LẨU CUA HŨ",
    BET_MD5: "🎰 BETVIP MD5",
    BET_HU:  "🎯 BETVIP HŨ",
}

# Games that only need TAI/XIU (no position/vi prediction)
TX_ONLY_GAMES = {LC_MD5, LC_HU, BET_MD5, BET_HU}

# ── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════════
_states: Dict[str, dict] = {
    gm: {
        "history":    deque(maxlen=MEM_WINDOW),
        "latest":     {},
        "pred":       {},
        "prev_pred":  {},
        "auto_msg":   {},   # chat_id -> message_id
        "api_ok":     False,
        "consec_loss": 0,
    }
    for gm in ALL_GAMES
}

_maintenance = {
    "active":   False,
    "end_time": None,
    "reason":   "",
    "task":     None,
}

# locked commands / features: set of command names (e.g. "autosicbo", "auto_lc_md5")
_locked_cmds: set = set()

# ═══════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════
def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
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
                cmd TEXT PRIMARY KEY,
                locked_by INTEGER,
                locked_at TEXT,
                reason TEXT
            );
        """)
    # Restore locked commands from DB
    with _db() as db:
        rows = db.execute("SELECT cmd FROM locked_cmds").fetchall()
        for r in rows:
            _locked_cmds.add(r["cmd"])
    log.info("Database initialised.")


# ═══════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def is_allowed(uid: int) -> bool:
    if is_admin(uid):
        return True
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
            try:
                if datetime.now() > datetime.fromisoformat(exp["expires_at"]):
                    return False
            except Exception:
                pass
    return True


# ═══════════════════════════════════════════════════════════════════════
# LOCK SYSTEM
# ═══════════════════════════════════════════════════════════════════════
def lock_cmd(cmd: str, uid: int, reason: str = ""):
    _locked_cmds.add(cmd)
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO locked_cmds (cmd, locked_by, locked_at, reason) VALUES (?,?,?,?)",
            (cmd, uid, datetime.now().isoformat(), reason)
        )


def unlock_cmd(cmd: str):
    _locked_cmds.discard(cmd)
    with _db() as db:
        db.execute("DELETE FROM locked_cmds WHERE cmd=?", (cmd,))


def is_locked(cmd: str) -> bool:
    return cmd in _locked_cmds


# ═══════════════════════════════════════════════════════════════════════
# GAME LOGIC
# ═══════════════════════════════════════════════════════════════════════
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
    if c in ("BÃO",):
        return None
    return "TÀI" in c if ("TÀI" in c or "XỈU" in c) else None


# ═══════════════════════════════════════════════════════════════════════
# UPGRADED PREDICTION ENGINE
# ═══════════════════════════════════════════════════════════════════════
class CauDetector:
    """Nhận diện loại cầu từ chuỗi lịch sử."""

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
        if len(seq) < 4:
            return {"type": "CHƯA RÕ", "len": 0, "pred": None, "conf": 50,
                    "desc": "Chưa đủ dữ liệu", "break_risk": 0}

        sk, last_val = CauDetector.streak(seq)

        # — Bệt siêu dài (≥10): rất dễ gãy
        if sk >= 10:
            return {"type": "BỆT SIÊU DÀI", "len": sk, "pred": not last_val,
                    "conf": min(88 + (sk - 10) * 1, 95),
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván ⚠️ Nguy cơ gãy rất cao",
                    "break_risk": 92}
        # — Bệt dài (6-9)
        if sk >= 6:
            return {"type": "BỆT DÀI", "len": sk, "pred": not last_val,
                    "conf": min(76 + (sk - 6) * 3, 88),
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Dễ gãy",
                    "break_risk": 72}
        # — Bệt ngắn (3-5): tiếp tục
        if sk >= 3:
            return {"type": "BỆT", "len": sk, "pred": last_val,
                    "conf": 56 + sk * 5,
                    "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {sk} ván — Theo cầu",
                    "break_risk": 28}

        # — Cầu 1-1 (ping-pong)
        if len(seq) >= 6 and all(seq[-(i + 1)] != seq[-(i + 2)] for i in range(4)):
            return {"type": "CẦU 1-1", "len": 5, "pred": not seq[-1], "conf": 74,
                    "desc": "Cầu ping-pong 1-1 → Tiếp tục xen kẽ", "break_risk": 24}

        # — Cầu 2-2
        if len(seq) >= 8:
            r8 = seq[-8:]
            if (r8[0]==r8[1] and r8[1]!=r8[2] and r8[2]==r8[3] and
                    r8[3]!=r8[4] and r8[4]==r8[5] and r8[5]!=r8[6] and r8[6]==r8[7]):
                return {"type": "CẦU 2-2", "len": 8, "pred": r8[-1], "conf": 72,
                        "desc": "Cầu 2-2 → Tiếp tục theo cặp", "break_risk": 20}

        # — Cầu 3-3
        if len(seq) >= 12:
            r12 = seq[-12:]
            ok = all(r12[i*3]==r12[i*3+1]==r12[i*3+2] and
                     (i==0 or r12[i*3]!=r12[(i-1)*3]) for i in range(4))
            if ok:
                return {"type": "CẦU 3-3", "len": 12, "pred": r12[-1], "conf": 75,
                        "desc": "Cầu 3-3 → Theo bộ 3", "break_risk": 16}

        # — Cầu 2-1 (xen kẽ không đều)
        if len(seq) >= 9:
            r9 = seq[-9:]
            a = r9[0]
            pat = [a,a,not a, a,a,not a, a,a,not a]
            if r9[:8] == pat[:8]:
                return {"type": "CẦU 2-1", "len": 9, "pred": not a, "conf": 78,
                        "desc": "Cầu 2-1 → Dự đoán đổi chiều", "break_risk": 15}

        # — Zigzag lệch
        if len(seq) >= 6:
            r6 = seq[-6:]
            if r6[0]==r6[1] and r6[1]!=r6[2] and r6[2]!=r6[3] and r6[3]==r6[4] and r6[4]!=r6[5]:
                return {"type": "ZIGZAG", "len": 6, "pred": not seq[-1], "conf": 66,
                        "desc": "Zigzag lệch nhịp → Đổi chiều", "break_risk": 36}

        return {"type": "HỖN HỢP", "len": len(seq), "pred": None, "conf": 50,
                "desc": "Xu hướng hỗn hợp — tín hiệu yếu", "break_risk": 50}


class AdvancedPredictor:
    """
    Engine dự đoán đa tầng với adaptive weighting.
    Chỉ xuất dự đoán khi tín hiệu đủ mạnh (≥ MIN_CONF_PREDICT đồng thuận).
    """

    def __init__(self, game_mode: str):
        self.gm = game_mode
        self.weights: Dict[str, float] = {}
        self._load_weights()

    # ── Weight management ───────────────────────────────────────────────
    def _load_weights(self):
        try:
            with _db() as db:
                for r in db.execute(
                    "SELECT algo_name, weight FROM algo_weights WHERE game_mode=?", (self.gm,)
                ).fetchall():
                    self.weights[r["algo_name"]] = r["weight"]
        except Exception:
            pass

        defaults = {
            "cau_detect":    7.0,
            "markov5":       6.5,
            "markov4":       6.0,
            "markov3":       5.5,
            "markov2":       4.5,
            "markov1":       3.5,
            "pattern8":      6.0,
            "pattern6":      5.5,
            "pattern5":      5.0,
            "pattern4":      4.5,
            "pattern3":      4.0,
            "streak_break":  5.5,
            "streak_cont":   4.5,
            "zigzag":        3.5,
            "gap_analysis":  4.0,
            "window10":      3.5,
            "window20":      3.0,
            "window50":      2.5,
            "entropy":       3.0,
            "run_length":    3.5,
            "oscillation":   4.0,
            "chi_balance":   2.5,
            "score_trend":   3.5,
            "adaptive_ma":   3.0,
            "linear_reg":    3.0,
            "perceptron":    3.5,
            "cycle_detect":  3.0,
            "hot_cold":      2.5,
            "prob_weight":   2.0,
        }
        for k, v in defaults.items():
            if k not in self.weights:
                self.weights[k] = v

    def update_weight(self, algo: str, correct: bool):
        w = self.weights.get(algo, 1.0)
        # Faster adaptation: +15% on correct, -12% on wrong
        self.weights[algo] = min(14.0, w * 1.15) if correct else max(0.3, w * 0.88)
        try:
            with _db() as db:
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
        except Exception:
            pass

    def w(self, name: str) -> float:
        return self.weights.get(name, 1.0)

    # ── Algorithm implementations ────────────────────────────────────────
    def _markov(self, seq: List[bool], order: int) -> Optional[Tuple[bool, float]]:
        if len(seq) < order + 3:
            return None
        pat = tuple(seq[-order:])
        cnt: Counter = Counter()
        for i in range(len(seq) - order):
            if tuple(seq[i:i+order]) == pat and i+order < len(seq):
                cnt[seq[i+order]] += 1
        total = sum(cnt.values())
        if total < 3:
            return None
        best, n = cnt.most_common(1)[0]
        conf = n / total
        if conf < 0.55:
            return None
        return best, conf

    def _pattern(self, seq: List[bool], depth: int) -> Optional[Tuple[bool, float]]:
        if len(seq) < depth + 3:
            return None
        pat = tuple(seq[-depth:])
        cnt: Counter = Counter()
        for i in range(len(seq) - depth):
            if tuple(seq[i:i+depth]) == pat and i+depth < len(seq):
                cnt[seq[i+depth]] += 1
        total = sum(cnt.values())
        if total < 2:
            return None
        best, n = cnt.most_common(1)[0]
        conf = n / total
        if conf < 0.55:
            return None
        return best, conf

    def _streak_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 3:
            return None
        sk, last = CauDetector.streak(seq)
        # Strong break signal for long streaks
        if sk >= 7:
            return not last, min(0.80 + (sk-7)*0.03, 0.93)
        if sk >= 5:
            return not last, 0.72 + (sk-5)*0.04
        # Short streak continuation
        if sk == 3:
            return last, 0.62
        if sk == 4:
            return last, 0.66
        return None

    def _zigzag_detect(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 6:
            return None
        # Perfect alternation for last 5
        if all(seq[-(i+1)] != seq[-(i+2)] for i in range(4)):
            return not seq[-1], 0.74
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
        cur = len(seq)-1
        dt = cur - tp[-1]
        df = cur - fp[-1]
        if dt >= avg_tg * 1.6:
            c = min(0.60 + (dt-avg_tg)/avg_tg * 0.08, 0.80)
            return True, c
        if df >= avg_fg * 1.6:
            c = min(0.60 + (df-avg_fg)/avg_fg * 0.08, 0.80)
            return False, c
        return None

    def _window_freq(self, seq: List[bool], window: int) -> Optional[Tuple[bool, float]]:
        chunk = seq[-window:] if len(seq) >= window else seq
        if len(chunk) < max(window // 2, 5):
            return None
        r = sum(chunk) / len(chunk)
        if abs(r - 0.5) < 0.14:
            return None
        # Mean reversion: over-represented side tends to revert
        pred = r < 0.5   # if too many XIU, predict TAI
        conf = min(0.52 + abs(r-0.5) * 0.8, 0.78)
        return pred, conf

    def _entropy_analysis(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 10:
            return None
        w = seq[-10:]
        tc = sum(w)
        if tc >= 9:
            return False, 0.76   # 9/10 TÀI → khả năng xỉu cao
        if tc <= 1:
            return True, 0.76    # 9/10 XỈU → khả năng tài cao
        return None

    def _run_length(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        """Phân tích độ dài run trung bình, dự đoán dựa trên expected run length."""
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
        # If current run much longer than average → break expected
        if sk >= avg_run * 1.8:
            return not last, min(0.62 + (sk / avg_run - 1.8) * 0.05, 0.82)
        # If current run much shorter than average → continuation expected
        if sk < avg_run * 0.5 and sk >= 2:
            return last, 0.60
        return None

    def _oscillation(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        """Detect oscillation patterns (like breathing)."""
        if len(seq) < 12:
            return None
        # Count changes in last 12
        changes = sum(1 for i in range(len(seq)-12, len(seq)-1) if seq[i] != seq[i+1])
        if changes >= 10:   # very high oscillation → continue alternating
            return not seq[-1], 0.70
        if changes <= 2:    # very low oscillation (bệt) → break expected
            sk, last = CauDetector.streak(seq)
            if sk >= 4:
                return not last, 0.72
        return None

    def _chi_balance(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        w = seq[-30:] if len(seq) >= 30 else seq
        if not w:
            return None
        r = sum(w) / len(w)
        if abs(r - 0.5) < 0.12:
            return None
        pred = r < 0.5
        conf = min(0.52 + abs(r-0.5) * 0.6, 0.72)
        return pred, conf

    def _score_trend(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 8:
            return None
        r = sum(scores[-4:]) / 4
        o = sum(scores[-8:-4]) / 4
        diff = r - o
        if abs(diff) < 1.0:
            return None
        return diff > 0, min(0.54 + abs(diff) * 0.04, 0.76)

    def _adaptive_ma(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 15:
            return None
        ma5  = sum(scores[-5:]) / 5
        ma15 = sum(scores[-15:]) / 15
        diff = ma5 - ma15
        if abs(diff) < 0.6:
            return None
        return diff > 0, min(0.54 + abs(diff) * 0.04, 0.76)

    def _linear_reg(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 10:
            return None
        n = min(len(scores), 20)
        y = scores[-n:]
        x = list(range(n))
        sx, sy = sum(x), sum(y)
        sxy = sum(x[i]*y[i] for i in range(n))
        sx2 = sum(i*i for i in x)
        d = n*sx2 - sx**2
        if d == 0:
            return None
        slope = (n*sxy - sx*sy) / d
        pred_score = sum(y[-3:])/3 + slope*2
        if pred_score > 13.0:
            return False, 0.62
        if pred_score < 8.0:
            return True, 0.62
        return None

    def _perceptron(self, seq: List[bool], scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 10:
            return None
        l3  = seq[-3:]
        tr  = sum(seq[-10:]) / 10
        avg = sum(scores[-5:]) / 5 if len(scores) >= 5 else 10.5
        feats = [l3[0]*2-1, l3[1]*2-1, l3[2]*2-1, (tr-0.5)*2, (avg-10.5)/5]
        ws = [0.45, 0.30, 0.20, 0.55, 0.40]
        dot = sum(f*w for f, w in zip(feats, ws)) + 0.05
        prob = 1 / (1 + math.exp(-dot))
        if prob > 0.58:
            return True, min(prob, 0.82)
        if prob < 0.42:
            return False, min(1-prob, 0.82)
        return None

    def _cycle_detect(self, seq: List[bool]) -> Optional[Tuple[bool, float]]:
        if len(seq) < 20:
            return None
        best_lag, best_corr = None, 0.0
        for lag in range(2, min(14, len(seq)//2)):
            corr = sum(1 for i in range(len(seq)-lag)
                      if seq[i] == seq[i+lag]) / (len(seq)-lag)
            if corr > best_corr:
                best_corr, best_lag = corr, lag
        if best_corr > 0.68 and best_lag and len(seq) > best_lag:
            return seq[-best_lag], 0.52 + best_corr * 0.30
        return None

    def _hot_cold(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 20:
            return None
        recent = scores[-20:]
        ht = sum(1 for s in recent if s > 13)
        hx = sum(1 for s in recent if s < 7)
        avg5 = sum(scores[-5:]) / 5
        if avg5 > 14.5 and ht > 8:
            return False, 0.67
        if avg5 < 5.5 and hx > 8:
            return True, 0.67
        return None

    def _prob_weight(self, scores: List[int]) -> Optional[Tuple[bool, float]]:
        if len(scores) < 8:
            return None
        # Dice probability distribution
        dp: Dict[int, float] = {}
        cnt: Counter = Counter()
        for d1 in range(1, 7):
            for d2 in range(1, 7):
                for d3 in range(1, 7):
                    cnt[d1+d2+d3] += 1
        for s, c in cnt.items():
            dp[s] = c / 216
        recent = scores[-8:]
        lo = sum(dp.get(s, 0) for s in recent if s <= 10)
        hi = sum(dp.get(s, 0) for s in recent if s > 10)
        if hi > lo * 1.4:
            return False, 0.58
        if lo > hi * 1.4:
            return True, 0.58
        return None

    # ── Main predict method ──────────────────────────────────────────────
    def predict(self, state: dict) -> dict:
        history = state["history"]
        if len(history) < 8:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0, "algo_count": 0,
                    "cau_type": "CHƯA ĐỦ DỮ LIỆU", "cau_desc": "",
                    "note": "Chờ đủ dữ liệu (cần ≥8 phiên)"}

        # Build sequences
        seq: List[bool] = []
        scores: List[int] = []
        for g in reversed(list(history)):
            tx = is_tai(g["score"], g["faces"], self.gm)
            if tx is not None:
                seq.append(tx)
                scores.append(g["score"])

        if len(seq) < 6:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0, "algo_count": 0,
                    "cau_type": "CHƯA ĐỦ", "cau_desc": ""}

        cau = CauDetector.detect(seq)

        # ── Collect weighted votes ──────────────────────────────────────
        algos_results = []  # (pred:bool, conf:float, weight:float, name:str)

        def add(name, fn):
            try:
                r = fn()
                if r is not None:
                    pred, conf = r
                    algos_results.append((pred, conf, self.w(name), name))
            except Exception as e:
                log.debug("algo %s: %s", name, e)

        # Cầu detection (highest weight)
        if cau["pred"] is not None:
            algos_results.append((cau["pred"], cau["conf"]/100, self.w("cau_detect"), "cau_detect"))

        # Markov chains
        for order, name in [(5,"markov5"),(4,"markov4"),(3,"markov3"),(2,"markov2"),(1,"markov1")]:
            add(name, lambda o=order: self._markov(seq, o))

        # Pattern matching
        for depth, name in [(8,"pattern8"),(6,"pattern6"),(5,"pattern5"),(4,"pattern4"),(3,"pattern3")]:
            add(name, lambda d=depth: self._pattern(seq, d))

        # Streak
        add("streak_break", lambda: self._streak_analysis(seq))
        add("zigzag",       lambda: self._zigzag_detect(seq))
        add("gap_analysis", lambda: self._gap_analysis(seq))
        add("window10",     lambda: self._window_freq(seq, 10))
        add("window20",     lambda: self._window_freq(seq, 20))
        add("window50",     lambda: self._window_freq(seq, 50))
        add("entropy",      lambda: self._entropy_analysis(seq))
        add("run_length",   lambda: self._run_length(seq))
        add("oscillation",  lambda: self._oscillation(seq))
        add("chi_balance",  lambda: self._chi_balance(seq))
        add("score_trend",  lambda: self._score_trend(scores))
        add("adaptive_ma",  lambda: self._adaptive_ma(scores))
        add("linear_reg",   lambda: self._linear_reg(scores))
        add("perceptron",   lambda: self._perceptron(seq, scores))
        add("cycle_detect", lambda: self._cycle_detect(seq))
        add("hot_cold",     lambda: self._hot_cold(scores))
        add("prob_weight",  lambda: self._prob_weight(scores))

        if not algos_results:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0, "algo_count": 0,
                    "cau_type": cau["type"], "cau_desc": cau["desc"],
                    "note": "Không đủ tín hiệu"}

        # ── Weighted voting ─────────────────────────────────────────────
        tai_score = sum(c * w for p, c, w, _ in algos_results if p)
        xiu_score = sum(c * w for p, c, w, _ in algos_results if not p)
        total = tai_score + xiu_score

        if total == 0:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0, "algo_count": len(algos_results),
                    "cau_type": cau["type"], "cau_desc": cau["desc"]}

        pred_bool = tai_score >= xiu_score
        consensus = max(tai_score, xiu_score) / total   # ratio 0..1

        # ── Pause when losing streak or weak signal ─────────────────────
        cl = state.get("consec_loss", 0)
        if cl >= 5 and consensus < 0.70:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0, "algo_count": len(algos_results),
                    "cau_type": cau["type"], "cau_desc": cau["desc"],
                    "note": f"🔴 Tạm dừng — Sai {cl} lần liên tiếp, đợi tín hiệu mạnh hơn (≥70%)"}

        if consensus < MIN_CONF_PREDICT:
            return {"pred": "CHỜ", "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": int(consensus*100), "algo_count": len(algos_results),
                    "cau_type": cau["type"], "cau_desc": cau["desc"],
                    "note": f"⚠️ Tín hiệu yếu ({consensus:.0%}) — Chờ cầu rõ hơn"}

        # ── Confidence calibration ──────────────────────────────────────
        confidence = max(54, min(96, int(consensus * 100)))

        # Penalise if over-dominant in recent window
        recent20 = seq[-20:] if len(seq) >= 20 else seq
        if recent20:
            r20 = sum(recent20) / len(recent20)
            if (pred_bool and r20 > 0.75) or (not pred_bool and r20 < 0.25):
                confidence = max(50, confidence - 10)

        # ── Position prediction (SicBo only) ────────────────────────────
        vi1 = vi2 = vi3 = 0
        if self.gm not in TX_ONLY_GAMES:
            rcent = [s for s in scores[-60:] if (s > 10) == pred_bool]
            if len(rcent) < 4:
                rcent = list(range(11, 18)) if pred_bool else list(range(3, 11))
            cnt2 = Counter(rcent)
            top = [v for v, _ in cnt2.most_common(10)]
            prev = state.get("prev_pred", {})
            pvs = {prev.get("vi1"), prev.get("vi2"), prev.get("vi3")}
            fresh = [v for v in top if v not in pvs] or top
            random.shuffle(fresh)
            sel = fresh[:3]
            while len(sel) < 3:
                e = random.randint(11, 17) if pred_bool else random.randint(3, 10)
                if e not in sel:
                    sel.append(e)
            sel.sort()
            vi1, vi2, vi3 = sel[0], sel[1], sel[2]

        # ── History window summary ──────────────────────────────────────
        hw_parts = []
        for ws in [20, 50, 100]:
            chunk = seq[-ws:] if len(seq) >= ws else seq
            if len(chunk) >= 10:
                t = sum(chunk)
                x = len(chunk) - t
                hw_parts.append(f"{ws}v:{t}T/{x}X")
        hw_str = "  ".join(hw_parts)

        return {
            "pred":       "TÀI" if pred_bool else "XỈU",
            "vi1": vi1, "vi2": vi2, "vi3": vi3,
            "confidence": confidence,
            "algo_count": len(algos_results),
            "cau_type":   cau["type"],
            "cau_desc":   cau["desc"],
            "cau_break_risk": cau.get("break_risk", 0),
            "history_windows": hw_str,
            "consensus":  consensus,
        }


_engines: Dict[str, AdvancedPredictor] = {}


def get_engine(gm: str) -> AdvancedPredictor:
    if gm not in _engines:
        _engines[gm] = AdvancedPredictor(gm)
    return _engines[gm]


# ═══════════════════════════════════════════════════════════════════════
# API FETCHERS
# ═══════════════════════════════════════════════════════════════════════
async def _fetch_sicbo(session: aiohttp.ClientSession) -> Optional[list]:
    for attempt in range(MAX_RETRIES):
        hdrs = {**SICBO_HEADERS, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                SICBO_API, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=6), ssl=False
            ) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.read()
                if not raw or raw.strip().startswith(b"<"):
                    continue
                data = json.loads(raw.decode("utf-8", errors="replace"))
                dc = data.get("data")
                items = None
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
        await asyncio.sleep(0.4 * (attempt+1))
    _states[SICBO]["api_ok"] = False
    return None


async def _fetch_lc(session: aiohttp.ClientSession, gm: str) -> Optional[list]:
    urls = {
        LC_MD5: LC_MD5_API,
        LC_HU:  LC_HU_API,
        BET_MD5: BETVIP_MD5_API,
        BET_HU:  BETVIP_HU_API,
    }
    hdrs_map = {
        LC_MD5: LC_HEADERS, LC_HU: LC_HEADERS,
        BET_MD5: BETVIP_HEADERS, BET_HU: BETVIP_HEADERS,
    }
    url = urls[gm]
    base_hdrs = hdrs_map[gm]
    for attempt in range(MAX_RETRIES):
        hdrs = {**base_hdrs, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                url, headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=8), ssl=False
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
        await asyncio.sleep(0.4 * (attempt+1))
    _states[gm]["api_ok"] = False
    return None


# ═══════════════════════════════════════════════════════════════════════
# PARSERS
# ═══════════════════════════════════════════════════════════════════════
def _parse_sicbo(raw: dict) -> dict:
    faces = raw.get("facesList") or []
    score = raw.get("score") or sum(faces)
    return {
        "game_num": str(raw.get("gameNum", "")),
        "score":    int(score),
        "faces":    [int(f) for f in faces],
        "type":     classify_game(int(score), [int(f) for f in faces], SICBO),
        "time":     datetime.now().strftime("%H:%M:%S"),
        "ts":       datetime.now().isoformat(),
    }


def _parse_lc(raw: dict, gm: str) -> dict:
    dices = raw.get("dices") or []
    point = raw.get("point") or sum(dices)
    return {
        "game_num":   str(raw.get("id", "")),
        "score":      int(point),
        "faces":      [int(d) for d in dices],
        "type":       classify_game(int(point), [int(d) for d in dices], gm),
        "raw_result": raw.get("resultTruyenThong", ""),
        "time":       datetime.now().strftime("%H:%M:%S"),
        "ts":         datetime.now().isoformat(),
    }


async def _load_initial(session: aiohttp.ClientSession, gm: str):
    state = _states[gm]
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


# ═══════════════════════════════════════════════════════════════════════
# MESSAGE BUILDER
# ═══════════════════════════════════════════════════════════════════════
_TYPE_EMO = {
    "TÀI": "🔴", "XỈU": "🔵", "BÃO": "🌪",
    "NỔ HŨ TÀI": "🏺🔴", "NỔ HŨ XỈU": "🏺🔵", "CHỜ": "🟡",
}

def _te(t: str) -> str:
    return _TYPE_EMO.get(t, "🎲")

def _conf_bar(c: int) -> str:
    filled = round(c / 10)
    bar = "█" * filled + "░" * (10 - filled)
    stars = " ⭐⭐" if c >= 88 else (" ⭐" if c >= 78 else (" 🔥" if c >= 70 else ""))
    return f"{bar} <b>{c}%</b>{stars}"


def _build_msg(gm: str, pred: dict, prev_pred: dict, curr_game: dict) -> str:
    now   = datetime.now().strftime("%H:%M:%S %d/%m")
    label = GAME_LABELS[gm]
    state = _states[gm]
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
        if gm == SICBO and sf == [4,4,4]:
            special_block = "\n\n🌪 <b>⚠️ BÃO 4-4-4 — MỌI CƯỢC THUA (TRỪ ĐẶT BÃO)!</b>"
        elif gm in (LC_HU, BET_HU):
            if sf == [1,1,1]:
                special_block = "\n\n🏺💥 <b>NỔ HŨ XỈU! 1-1-1 — JACKPOT!</b>"
            elif sf == [6,6,6]:
                special_block = "\n\n🏺💥 <b>NỔ HŨ TÀI! 6-6-6 — JACKPOT!</b>"

        if prev_pred and prev_pred.get("pred") in ("TÀI", "XỈU") and c_type in ("TÀI", "XỈU", "NỔ HŨ TÀI", "NỔ HŨ XỈU"):
            if prev_pred["pred"] == ("TÀI" if "TÀI" in c_type else "XỈU"):
                vi_hit = not tx_only and any(
                    prev_pred.get(vk) == c_score for vk in ("vi1","vi2","vi3"))
                outcome_block = (
                    "\n\n💎 <b>═══════ CHUẨN VỊ! 🎯 ═══════</b>"
                    if vi_hit else
                    "\n\n🏆 <b>═══════ ĐÚNG ✅ ═══════</b>"
                )
            else:
                outcome_block = "\n\n💔 <b>═══════ SAI ❌ ═══════</b>"

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
        conf = pred.get("confidence", 50)
        algos = pred.get("algo_count", 0)
        risk_bar = ("🔴" * min(int(cau_risk/20), 5) + "⚪" * (5 - min(int(cau_risk/20), 5))
                    if cau_risk else "⚪⚪⚪⚪⚪")

        if tx_only:
            # Tài/Xỉu only (no position)
            pred_block = (
                "🔮 <b>DỰ ĐOÁN PHIÊN TIẾP THEO</b>\n"
                "<blockquote>"
                f"🎯 Dự đoán     : <b>{_te(p_label)} {p_label}</b>\n"
                f"📊 Độ tin cậy  : {_conf_bar(conf)}\n"
                f"🤝 Đồng thuận  : <b>{consensus:.0%}</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                f"🃏 Loại cầu    : <b>{cau_type}</b>\n"
                f"📝 Phân tích   : <i>{cau_desc}</i>\n"
                f"⚡ Nguy cơ gãy : {risk_bar} <b>{cau_risk}%</b>\n"
            )
        else:
            vi1 = pred.get("vi1","—"); vi2 = pred.get("vi2","—"); vi3 = pred.get("vi3","—")
            pred_block = (
                "🔮 <b>DỰ ĐOÁN PHIÊN TIẾP THEO</b>\n"
                "<blockquote>"
                f"🎯 Dự đoán     : <b>{_te(p_label)} {p_label}</b>\n"
                f"📊 Độ tin cậy  : {_conf_bar(conf)}\n"
                f"🤝 Đồng thuận  : <b>{consensus:.0%}</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                f"3️⃣ <b>VỊ TIN CẬY:</b>\n"
                f"   🥇 Vị 1 : <b>{vi1}</b>\n"
                f"   🥈 Vị 2 : <b>{vi2}</b>\n"
                f"   🥉 Vị 3 : <b>{vi3}</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                f"🃏 Loại cầu    : <b>{cau_type}</b>\n"
                f"📝 Phân tích   : <i>{cau_desc}</i>\n"
                f"⚡ Nguy cơ gãy : {risk_bar} <b>{cau_risk}%</b>\n"
            )

        if hw_str:
            pred_block += f"📈 Lịch sử     : <i>{hw_str}</i>\n"
        pred_block += f"🤖 Thuật toán  : <b>{algos} layers</b></blockquote>"

    return (
        f"🎲 <b>{label} — DỰ ĐOÁN TỰ ĐỘNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + pred_block
        + result_block
        + special_block
        + outcome_block
        + f"\n\n<i>🔄 {now} | {api_st} Live</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>SicBo Bot </i>"
    )


# ═══════════════════════════════════════════════════════════════════════
# DB RECORD
# ═══════════════════════════════════════════════════════════════════════
def _record_pred(gm: str, pred: dict, actual: dict):
    if not pred or not actual.get("game_num"):
        return
    p_type = pred.get("pred")
    a_type = actual["type"]
    a_score = actual["score"]
    outcome = None
    vi_hit = 0
    if p_type in ("TÀI", "XỈU") and a_type in ("TÀI","XỈU","NỔ HŨ TÀI","NỔ HŨ XỈU"):
        actual_tai = "TÀI" in a_type
        outcome = "✅ ĐÚNG" if (p_type == "TÀI") == actual_tai else "❌ SAI"
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


# ═══════════════════════════════════════════════════════════════════════
# PUSH UPDATES
# ═══════════════════════════════════════════════════════════════════════
async def _push(app: Application, gm: str, prev_pred: dict, curr_game: dict):
    state = _states[gm]
    pred = state["pred"]
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


# ═══════════════════════════════════════════════════════════════════════
# GAME LOOPS
# ═══════════════════════════════════════════════════════════════════════
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
                new_num = str(latest_raw.get("gameNum" if gm == SICBO else "id", ""))
                if not new_num or new_num == state["latest"].get("game_num"):
                    continue

                prev_pred = state["pred"].copy()

                new_game = _parse_sicbo(latest_raw) if gm == SICBO else _parse_lc(latest_raw, gm)
                state["history"].appendleft(new_game)
                state["latest"]   = new_game
                state["prev_pred"] = prev_pred

                # Update consecutive loss counter
                if prev_pred.get("pred") in ("TÀI", "XỈU"):
                    pred_tai   = prev_pred["pred"] == "TÀI"
                    actual_tai = is_tai(new_game["score"], new_game["faces"], gm)
                    if actual_tai is not None:
                        if pred_tai == actual_tai:
                            state["consec_loss"] = 0
                        else:
                            state["consec_loss"] = state.get("consec_loss", 0) + 1

                state["pred"] = get_engine(gm).predict(state)
                _record_pred(gm, prev_pred, new_game)
                await _push(app, gm, prev_pred, new_game)

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("loop %s: %s", gm, e)
                await asyncio.sleep(3)


# ═══════════════════════════════════════════════════════════════════════
# MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════
async def _start_maintenance(app: Application, minutes: int, reason: str):
    end = datetime.now() + timedelta(minutes=minutes)
    _maintenance.update({"active": True, "end_time": end, "reason": reason})
    for gm in ALL_GAMES:
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await app.bot.send_message(
                    chat_id=chat_id, parse_mode=ParseMode.HTML,
                    text=(f"🔧 <b>BẢO TRÌ HỆ THỐNG</b>\n"
                          f"⏳ <b>{minutes} phút</b>\n"
                          f"📋 Lý do: {reason}\n"
                          f"🕐 Xong: <b>{end.strftime('%H:%M %d/%m')}</b>")
                )
            except Exception:
                pass

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
    for gm in ALL_GAMES:
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await app.bot.send_message(
                    chat_id=chat_id, parse_mode=ParseMode.HTML,
                    text="✅ <b>BẢO TRÌ HOÀN TẤT!</b> Bot hoạt động trở lại."
                )
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════
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


def _check_maint_lock(update: Update, cmd: str) -> bool:
    """Return True (and reply) if maintenance active or command locked."""
    if _maintenance["active"]:
        end_str = _maintenance["end_time"].strftime("%H:%M") if _maintenance["end_time"] else "sắp tới"
        asyncio.create_task(update.message.reply_html(
            f"🔧 <b>Bot đang bảo trì!</b>\n"
            f"⏳ Xong lúc <b>{end_str}</b>\n"
            f"📋 Lý do: {_maintenance['reason']}"
        ))
        return True
    if is_locked(cmd):
        asyncio.create_task(update.message.reply_html(
            f"🔒 <b>Chức năng <code>{cmd}</code> đang tạm khóa!</b>\n"
            "Liên hệ admin để biết thêm thông tin."
        ))
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS — AUTO & LIVE
# ═══════════════════════════════════════════════════════════════════════
async def _cmd_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE, gm: str, cmd_name: str):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    if _check_maint_lock(update, cmd_name):
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
    text = _build_msg(gm, state["pred"], state.get("prev_pred", {}), state["latest"])
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


async def cmd_autosicbo(u, c):   await _cmd_auto(u, c, SICBO,   "autosicbo")
async def cmd_auto_lc_md5(u, c): await _cmd_auto(u, c, LC_MD5,  "auto_lc_md5")
async def cmd_auto_lc_hu(u, c):  await _cmd_auto(u, c, LC_HU,   "auto_lc_hu")
async def cmd_auto_bet_md5(u, c):await _cmd_auto(u, c, BET_MD5, "auto_bet_md5")
async def cmd_auto_bet_hu(u, c): await _cmd_auto(u, c, BET_HU,  "auto_bet_hu")


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


async def cmd_stop_sicbo(u, c):   await _cmd_stop(u, c, SICBO)
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
    if _check_maint_lock(update, cmd_name):
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


async def cmd_live(u, c):        await _cmd_live(u, c, SICBO,   "live")
async def cmd_live_md5(u, c):    await _cmd_live(u, c, LC_MD5,  "live_md5")
async def cmd_live_hu(u, c):     await _cmd_live(u, c, LC_HU,   "live_hu")
async def cmd_live_bet_md5(u, c):await _cmd_live(u, c, BET_MD5, "live_bet_md5")
async def cmd_live_bet_hu(u, c): await _cmd_live(u, c, BET_HU,  "live_bet_hu")


# ═══════════════════════════════════════════════════════════════════════
# KEY SYSTEM
# ═══════════════════════════════════════════════════════════════════════
async def cmd_trailkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or ""
    name     = update.effective_user.full_name or str(uid)
    if _check_maint_lock(update, "trailkey"):
        return

    # Strict one-time trial per user
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
        db.execute(
            "INSERT INTO trial_used (user_id, used_at) VALUES (?,?)",
            (uid, datetime.now().isoformat())
        )
        db.execute(
            "INSERT INTO activation_keys (key,created_by,created_at,expires_at,used_by,used_at,is_trial) "
            "VALUES (?,?,?,?,?,?,1)",
            (k, 0, datetime.now().isoformat(), exp, uid, datetime.now().isoformat())
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id,username,added_at,added_by) VALUES (?,?,?,?)",
            (uid, username, datetime.now().isoformat(), 0)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)",
            (uid, exp)
        )
    exp_fmt = datetime.fromisoformat(exp).strftime("%H:%M %d/%m/%Y")

    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                chat_id=adm, parse_mode=ParseMode.HTML,
                text=(f"🔔 <b>USER MỚI NHẬN TRIAL KEY!</b>\n"
                      f"<blockquote>👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                      f"🔑 <code>{k}</code>\n📅 Đến {exp_fmt}</blockquote>")
            )
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
    if _check_maint_lock(update, "key"):
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
        db.execute(
            "UPDATE activation_keys SET used_by=?,used_at=? WHERE key=?",
            (uid, datetime.now().isoformat(), key)
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id,username,added_at,added_by) VALUES (?,?,?,?)",
            (uid, username, datetime.now().isoformat(), 0)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)",
            (uid, row["expires_at"])
        )
        exp_fmt = row["expires_at"][:16].replace("T", " ")

    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                chat_id=adm, parse_mode=ParseMode.HTML,
                text=(f"🔔 <b>USER KÍCH HOẠT KEY!</b>\n"
                      f"<blockquote>👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                      f"🔑 <code>{key}</code>\n📅 Đến {exp_fmt}</blockquote>")
            )
        except Exception:
            pass
    await update.message.reply_html(
        "🔑 <b>KÍCH HOẠT THÀNH CÔNG!</b>\n"
        f"<blockquote>📅 Hết hạn: <b>{exp_fmt}</b></blockquote>\n"
        "🚀 Dùng /autosicbo, /auto_lc_md5, /auto_lc_hu,\n"
        "/auto_bet_md5 hoặc /auto_bet_hu để bắt đầu!"
    )


# ═══════════════════════════════════════════════════════════════════════
# LOCK / UNLOCK COMMANDS
# ═══════════════════════════════════════════════════════════════════════
@_admin_only
async def cmd_lock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html(
            "🔒 Dùng: <code>/lock {lệnh} [lý do]</code>\n"
            "Ví dụ: <code>/lock autosicbo Đang bảo trì API</code>\n\n"
            "<b>Các lệnh có thể khóa:</b>\n"
            "<blockquote>"
            "autosicbo, auto_lc_md5, auto_lc_hu,\n"
            "auto_bet_md5, auto_bet_hu,\n"
            "live, live_md5, live_hu,\n"
            "live_bet_md5, live_bet_hu,\n"
            "trailkey, key"
            "</blockquote>"
        )
        return
    cmd = ctx.args[0].lower().strip("/")
    reason = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else "Bảo trì"
    lock_cmd(cmd, update.effective_user.id, reason)

    # Notify users in affected game
    gm_map = {
        "autosicbo": SICBO, "live": SICBO,
        "auto_lc_md5": LC_MD5, "live_md5": LC_MD5,
        "auto_lc_hu":  LC_HU,  "live_hu":  LC_HU,
        "auto_bet_md5": BET_MD5, "live_bet_md5": BET_MD5,
        "auto_bet_hu":  BET_HU,  "live_bet_hu":  BET_HU,
    }
    if cmd in gm_map:
        gm = gm_map[cmd]
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
        f"<blockquote>Lý do: {reason}</blockquote>"
    )


@_admin_only
async def cmd_ulock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        locked_list = ", ".join(f"<code>{c}</code>" for c in _locked_cmds) or "Không có"
        await update.message.reply_html(
            f"🔓 Dùng: <code>/ulock {{lệnh}}</code>\n"
            f"Lệnh đang khóa: {locked_list}"
        )
        return
    cmd = ctx.args[0].lower().strip("/")
    if cmd not in _locked_cmds:
        await update.message.reply_html(f"ℹ️ Lệnh <code>{cmd}</code> không bị khóa.")
        return
    unlock_cmd(cmd)
    await update.message.reply_html(
        f"🔓 <b>Đã mở khóa lệnh <code>{cmd}</code></b>"
    )


# ═══════════════════════════════════════════════════════════════════════
# INFO / HISTORY
# ═══════════════════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = update.effective_user.first_name or "bạn"
    role = "👑 Admin" if is_admin(uid) else ("✅ Thành viên" if is_allowed(uid) else "🔒 Chưa kích hoạt")
    await update.message.reply_html(
        "🎲 <b>SICBO &amp; LẨU CUA &amp; BET Dự đoán</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Chào <b>{name}</b>! [{role}]\n\n"
        "<blockquote>"
        "🤖 Dự đoán Tài/Xỉu tự động đa nền tảng\n"
        "🎲 SicBo Sunwin | 🦀 LC MD5 | 🏺 LC Hũ\n"
        "🎰 Betvip MD5   | 🎯 Betvip Hũ\n"
        "</blockquote>\n\n"
        "📋 /help để xem toàn bộ lệnh\n"
        "💡 <i>/trailkey để nhận key 2 giờ miễn phí!</i>"
    )


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
        "📊 <b>Lịch sử dự đoán:</b>\n"
        "<blockquote>"
        "/listkq sicbo — Lịch sử SicBo\n"
        "/listkq md5   — Lịch sử LC MD5\n"
        "/listkq hu    — Lịch sử LC Hũ\n"
        "/listkq bet_md5 — Lịch sử Betvip MD5\n"
        "/listkq bet_hu  — Lịch sử Betvip Hũ"
        "</blockquote>\n"
        "👤 <b>Chung:</b>\n"
        "<blockquote>/stop_auto — Dừng TẤT CẢ\n"
        "/trailkey — Key trải nghiệm 2h (1 lần)\n"
        "/key {key} — Kích hoạt key\n"
        "/info — Thông tin tài khoản</blockquote>"
    )
    admin_extra = ""
    if is_admin(uid):
        locked_list = ", ".join(f"<code>{c}</code>" for c in _locked_cmds) or "Không có"
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
            f"/lock {{lệnh}} [lý do] — Khóa chức năng\n"
            f"/ulock {{lệnh}} — Mở khóa chức năng\n"
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
        with _db() as db:
            e = db.execute("SELECT expires_at FROM user_expiry WHERE user_id=?", (uid,)).fetchone()
        if e and e["expires_at"]:
            dt   = datetime.fromisoformat(e["expires_at"])
            left = dt - datetime.now()
            hrs  = max(0, int(left.total_seconds() // 3600))
            mins = max(0, int((left.total_seconds() % 3600) // 60))
            exp  = f"{dt.strftime('%H:%M %d/%m/%Y')} (còn {hrs}h{mins}m)"
        else:
            exp = "Không xác định"
        role = "✅ Thành viên"
    else:
        role, exp = "❌ Chưa kích hoạt", "—"

    with _db() as db:
        total   = db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        correct = db.execute("SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%'").fetchone()[0]
        vi_hits = db.execute("SELECT COUNT(*) FROM predictions WHERE vi_hit=1").fetchone()[0]
    acc    = f"{correct/total*100:.1f}%" if total else "—"
    vi_acc = f"{vi_hits/total*100:.1f}%" if total else "—"

    auto_status = []
    for gm in ALL_GAMES:
        if update.effective_chat.id in _states[gm]["auto_msg"]:
            auto_status.append(GAME_LABELS[gm])

    await update.message.reply_html(
        "👤 <b>THÔNG TIN TÀI KHOẢN</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"🪪 Tên     : <b>{name}</b>\n"
        f"🆔 ID      : <code>{uid}</code>\n"
        f"🏷 Vai trò : <b>{role}</b>\n"
        f"📅 Hết hạn : <b>{exp}</b>\n"
        f"🔴 Auto    : {', '.join(auto_status) or 'Không'}"
        "</blockquote>\n"
        "📊 <b>Thống kê toàn bot:</b>\n"
        "<blockquote>"
        f"Tổng dự đoán : <b>{total}</b>\n"
        f"✅ Đúng loại : <b>{correct}</b> ({acc})\n"
        f"🎯 Trúng vị  : <b>{vi_hits}</b> ({vi_acc})\n"
        f"🔧 Bảo trì   : {'🔴 Đang bảo trì' if _maintenance['active'] else '🟢 Bình thường'}"
        "</blockquote>"
    )


async def cmd_listkq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _check_maint_lock(update, "listkq"):
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
    gm = gm_map.get(arg, SICBO)
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
                f"🎯 {r['pred_type'] or '—'}\n"
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


# ═══════════════════════════════════════════════════════════════════════
# ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════
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
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id,added_at,added_by) VALUES (?,?,?)",
            (tid, datetime.now().isoformat(), update.effective_user.id)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id,expires_at) VALUES (?,?)",
            (tid, exp)
        )
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
        db.execute("DELETE FROM user_expiry WHERE user_id=?",   (tid,))
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
    for r in rows:
        exp   = r["expires_at"][:16].replace("T"," ") if r["expires_at"] else "∞"
        uname = f"@{r['username']}" if r["username"] else "—"
        alive = ""
        if r["expires_at"]:
            try:
                alive = " ✅" if datetime.now() < datetime.fromisoformat(r["expires_at"]) else " ⛔"
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
        f"Hạn  : {exp[:16].replace('T',' ')} ({hours}h)\n"
        "Dùng : 1 lần"
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
            vi_str  = f" | Vị: <b>{vi}</b> ({vi/total*100:.1f}%)" if (total and gm not in TX_ONLY_GAMES) else ""
            cl      = _states[gm].get("consec_loss", 0)
            api     = "🟢" if _states[gm]["api_ok"] else "🔴"
            auto_count = len(_states[gm]["auto_msg"])
            lines.append(
                f"\n{GAME_LABELS[gm]} {api} | 👥 {auto_count} auto\n"
                "<blockquote>"
                f"Tổng: <b>{total}</b> | Đúng: <b>{correct}</b> ({acc}){vi_str}\n"
                f"Sai LT: <b>{cl}</b> | History: <b>{len(_states[gm]['history'])}</b>"
                "</blockquote>"
            )

    locked_str = ", ".join(f"<code>{c}</code>" for c in _locked_cmds) or "Không có"
    lines.append(
        f"\n🔧 Bảo trì: {'🔴 Đang bảo trì' if _maintenance['active'] else '🟢 Bình thường'}\n"
        f"🔒 Khóa: {locked_str}"
    )
    await update.message.reply_html("\n".join(lines))


@_admin_only
async def cmd_baotri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html(
            "🔧 Dùng: <code>/baotri &lt;phút&gt; &lt;lý do&gt;</code>\n"
            "Ví dụ: <code>/baotri 30 Nâng cấp server</code>"
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


# ═══════════════════════════════════════════════════════════════════════
# CLEANUP LOOP
# ═══════════════════════════════════════════════════════════════════════
async def _cleanup_loop():
    while True:
        await asyncio.sleep(1800)
        try:
            with _db() as db:
                now     = datetime.now().isoformat()
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


# ═══════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════
async def post_init(app: Application):
    for gm in ALL_GAMES:
        asyncio.create_task(_game_loop(app, gm))
    asyncio.create_task(_cleanup_loop())
    log.info("✅ All %d game loops started.", len(ALL_GAMES))


def main():
    init_db()
    for gm in ALL_GAMES:
        get_engine(gm)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    handlers = [
        # User commands
        CommandHandler("start",          cmd_start),
        CommandHandler("help",           cmd_help),
        CommandHandler("info",           cmd_info),
        CommandHandler("trailkey",       cmd_trailkey),
        CommandHandler("key",            cmd_key),
        CommandHandler("listkq",         cmd_listkq),
        # Auto commands
        CommandHandler("autosicbo",      cmd_autosicbo),
        CommandHandler("auto_lc_md5",    cmd_auto_lc_md5),
        CommandHandler("auto_lc_hu",     cmd_auto_lc_hu),
        CommandHandler("auto_bet_md5",   cmd_auto_bet_md5),
        CommandHandler("auto_bet_hu",    cmd_auto_bet_hu),
        # Stop commands
        CommandHandler("stop_sicbo",     cmd_stop_sicbo),
        CommandHandler("stop_lc_md5",    cmd_stop_lc_md5),
        CommandHandler("stop_lc_hu",     cmd_stop_lc_hu),
        CommandHandler("stop_bet_md5",   cmd_stop_bet_md5),
        CommandHandler("stop_bet_hu",    cmd_stop_bet_hu),
        CommandHandler("stop_auto",      cmd_stop_auto),
        # Live commands
        CommandHandler("live",           cmd_live),
        CommandHandler("live_md5",       cmd_live_md5),
        CommandHandler("live_hu",        cmd_live_hu),
        CommandHandler("live_bet_md5",   cmd_live_bet_md5),
        CommandHandler("live_bet_hu",    cmd_live_bet_hu),
        # Admin commands
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

    log.info("🎲 SicBo + LC + Betvip Bot Ultra v7.0 starting...")
    app.run_polling(drop_pending_updates=True, poll_interval=1)


if __name__ == "__main__":
    main()
