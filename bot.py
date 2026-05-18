#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import math
import random
import sqlite3
import string
from collections import Counter, deque, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = "8828842195:AAGdzF60aoUbBv6PJf8_LnQ0AunYF3UN8C8"
ADMIN_IDS = [8001225219]

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

SICBO_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://sunwin.gs/",
    "Origin": "https://sunwin.gs",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
}

LC_HEADERS = {
    "accept": "*/*",
    "accept-language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "priority": "u=1, i",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "Referer": "https://lc79b.bet/",
}

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/112.0.0.0 Mobile Safari/537.36",
]

DB_PATH = "bot_ultra.db"
MEM_WINDOW = 300
SICBO_INTERVAL = 2.5
LC_INTERVAL = 5.0
MAX_RETRIES = 3

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SICBO = "sicbo"
LC_MD5 = "lc_md5"
LC_HU = "lc_hu"

GAME_LABELS = {
    SICBO: "🎲 SICBO SUNWIN",
    LC_MD5: "🦀 LẨU CUA MD5",
    LC_HU: "🏺 LẨU CUA HŨ",
}

_states: Dict[str, dict] = {
    SICBO: {
        "history": deque(maxlen=MEM_WINDOW),
        "latest": {},
        "pred": {},
        "prev_pred": {},
        "auto_msg": {},
        "api_ok": False,
        "consecutive_losses": 0,
    },
    LC_MD5: {
        "history": deque(maxlen=MEM_WINDOW),
        "latest": {},
        "pred": {},
        "prev_pred": {},
        "auto_msg": {},
        "api_ok": False,
        "consecutive_losses": 0,
    },
    LC_HU: {
        "history": deque(maxlen=MEM_WINDOW),
        "latest": {},
        "pred": {},
        "prev_pred": {},
        "auto_msg": {},
        "api_ok": False,
        "consecutive_losses": 0,
    },
}

_maintenance = {
    "active": False,
    "end_time": None,
    "reason": "",
    "task": None,
}

_algo_weights: Dict[str, Dict[str, float]] = {
    SICBO: {},
    LC_MD5: {},
    LC_HU: {},
}

_DICE_PROB: Dict[int, float] = {}


def _build_dice_prob():
    counts: Counter = Counter()
    for d1 in range(1, 7):
        for d2 in range(1, 7):
            for d3 in range(1, 7):
                counts[d1 + d2 + d3] += 1
    total = 216
    for s, c in counts.items():
        _DICE_PROB[s] = c / total


_build_dice_prob()


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
        """)
    log.info("Database initialised.")


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


def classify_game(score: int, faces: list, game_mode: str) -> str:
    sf = sorted(faces)
    if game_mode == SICBO:
        if sf == [4, 4, 4]:
            return "BÃO"
        return "TÀI" if score > 10 else "XỈU"
    if game_mode == LC_HU:
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


class CauAnalyzer:
    @staticmethod
    def get_streak(seq: List[bool]) -> Tuple[int, bool]:
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
    def detect_type(seq: List[bool]) -> dict:
        if len(seq) < 4:
            return {
                "type": "CHƯA RÕ", "len": 0,
                "pred": None, "conf": 50,
                "desc": "Chưa đủ dữ liệu", "break_risk": 0,
            }

        streak, last_val = CauAnalyzer.get_streak(seq)

        if streak >= 8:
            return {
                "type": "BỆT SIÊU DÀI",
                "len": streak,
                "pred": not last_val,
                "conf": min(85 + (streak - 8) * 2, 94),
                "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {streak} ván ⚠️ Rất dễ gãy",
                "break_risk": 90,
            }
        if streak >= 5:
            return {
                "type": "BỆT DÀI",
                "len": streak,
                "pred": not last_val,
                "conf": min(74 + (streak - 5) * 4, 86),
                "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {streak} ván — Nguy cơ gãy cao",
                "break_risk": 70,
            }
        if streak >= 3:
            return {
                "type": "BỆT",
                "len": streak,
                "pred": last_val,
                "conf": 58 + streak * 4,
                "desc": f"Bệt {'Tài' if last_val else 'Xỉu'} {streak} ván",
                "break_risk": 30,
            }

        if len(seq) >= 6:
            alt5 = all(seq[-(i + 1)] != seq[-(i + 2)] for i in range(4))
            if alt5:
                return {
                    "type": "CẦU 1-1",
                    "len": 5,
                    "pred": not seq[-1],
                    "conf": 73,
                    "desc": "Cầu 1-1 (ping pong) đang chạy → Tiếp tục xen kẽ",
                    "break_risk": 25,
                }

        if len(seq) >= 8:
            r8 = seq[-8:]
            pairs_ok = (
                r8[0] == r8[1] and r8[1] != r8[2] and
                r8[2] == r8[3] and r8[3] != r8[4] and
                r8[4] == r8[5] and r8[5] != r8[6] and
                r8[6] == r8[7]
            )
            if pairs_ok:
                return {
                    "type": "CẦU 2-2",
                    "len": 8,
                    "pred": r8[-1],
                    "conf": 70,
                    "desc": "Cầu 2-2 → Tiếp tục theo cặp",
                    "break_risk": 20,
                }

        if len(seq) >= 12:
            r12 = seq[-12:]
            triplets_ok = all(
                r12[i * 3] == r12[i * 3 + 1] == r12[i * 3 + 2]
                and (i == 0 or r12[i * 3] != r12[(i - 1) * 3])
                for i in range(4)
            )
            if triplets_ok:
                return {
                    "type": "CẦU 3-3",
                    "len": 12,
                    "pred": r12[-1],
                    "conf": 74,
                    "desc": "Cầu 3-3 → Tiếp tục theo bộ 3",
                    "break_risk": 18,
                }

        if len(seq) >= 6:
            r6 = seq[-6:]
            zz = (
                r6[0] == r6[1] and
                r6[1] != r6[2] and
                r6[2] != r6[3] and
                r6[3] == r6[4] and
                r6[4] != r6[5]
            )
            if zz:
                return {
                    "type": "ZIGZAG",
                    "len": 6,
                    "pred": not seq[-1],
                    "conf": 65,
                    "desc": "Cầu Zigzag lệch nhịp → Dự đoán tiếp tục",
                    "break_risk": 35,
                }

        return {
            "type": "HỖN HỢP",
            "len": len(seq),
            "pred": None,
            "conf": 50,
            "desc": "Cầu hỗn hợp — khó xác định xu hướng",
            "break_risk": 50,
        }

    @staticmethod
    def history_windows(seq: List[bool]) -> dict:
        result = {}
        for w in [20, 50, 100]:
            chunk = seq[-w:] if len(seq) >= w else seq
            if not chunk:
                continue
            tai_r = sum(chunk) / len(chunk)
            result[w] = {
                "tai_ratio": tai_r,
                "tai": sum(chunk),
                "xiu": len(chunk) - sum(chunk),
                "total": len(chunk),
                "dominant": "TÀI" if tai_r > 0.5 else "XỈU",
                "strength": abs(tai_r - 0.5) * 2,
            }
        return result

    @staticmethod
    def pattern6_match(seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 13:
            return None, 50
        pattern = tuple(seq[-6:])
        matches: Counter = Counter()
        for i in range(len(seq) - 6):
            if tuple(seq[i:i + 6]) == pattern:
                if i + 6 < len(seq):
                    matches[seq[i + 6]] += 1
        total = sum(matches.values())
        if total < 2:
            return None, 50
        best, cnt = matches.most_common(1)[0]
        conf = int(cnt / total * 100)
        return best, max(conf, 50)

    @staticmethod
    def freq_score_analysis(scores: List[int], pred_tai: bool) -> int:
        if len(scores) < 10:
            return 50
        recent = scores[-30:]
        target_range = range(11, 19) if pred_tai else range(3, 11)
        freq = sum(1 for s in recent if s in target_range) / len(recent)
        return int(50 + (freq - 0.5) * 60)

    @staticmethod
    def dice_break_detect(scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 6:
            return None, 50
        last3 = scores[-3:]
        prev3 = scores[-6:-3]
        avg_last = sum(last3) / 3
        avg_prev = sum(prev3) / 3
        diff = avg_last - avg_prev
        if abs(diff) < 1.5:
            return None, 50
        if diff > 0:
            return True, min(55 + int(diff * 5), 78)
        return False, min(55 + int(abs(diff) * 5), 78)


class PredictionEngine:
    def __init__(self, game_mode: str):
        self.game_mode = game_mode
        self.weights: Dict[str, float] = {}
        self._load_weights()
        self.cau = CauAnalyzer()

    def _load_weights(self):
        try:
            with _db() as db:
                rows = db.execute(
                    "SELECT algo_name, weight FROM algo_weights WHERE game_mode=?",
                    (self.game_mode,)
                ).fetchall()
                for r in rows:
                    self.weights[r["algo_name"]] = r["weight"]
        except Exception:
            pass
        defaults = {
            "cau_detect": 6.0,
            "pattern6": 5.5,
            "markov3": 5.0,
            "markov2": 4.5,
            "markov1": 3.5,
            "streak_breaker": 3.5,
            "streak_advanced": 4.5,
            "pattern5": 3.5,
            "pattern4": 3.0,
            "pattern3": 2.5,
            "cau_dao": 4.0,
            "cau_2_1": 3.5,
            "zigzag": 3.0,
            "gap_analysis": 4.0,
            "entropy": 2.5,
            "chi_balance": 2.0,
            "history_20": 3.0,
            "history_50": 2.5,
            "history_100": 2.0,
            "score_trend": 3.0,
            "hot_cold": 2.5,
            "score_dist": 3.0,
            "linear_reg": 3.5,
            "cycle_fft": 3.0,
            "perceptron": 3.5,
            "adaptive_ma": 3.0,
            "dice_break": 2.5,
            "prob_weight": 2.0,
            "tai_xiu_freq": 2.5,
            "trend_accel": 3.0,
        }
        for k, v in defaults.items():
            if k not in self.weights:
                self.weights[k] = v

    def update_weight(self, algo: str, correct: bool):
        w = self.weights.get(algo, 1.0)
        self.weights[algo] = min(12.0, w * 1.15) if correct else max(0.4, w * 0.85)
        try:
            with _db() as db:
                row = db.execute(
                    "SELECT hits, misses FROM algo_weights WHERE game_mode=? AND algo_name=?",
                    (self.game_mode, algo)
                ).fetchone()
                h = (row["hits"] if row else 0) + (1 if correct else 0)
                m = (row["misses"] if row else 0) + (0 if correct else 1)
                db.execute(
                    "INSERT OR REPLACE INTO algo_weights (game_mode, algo_name, weight, hits, misses, updated) "
                    "VALUES (?,?,?,?,?,?)",
                    (self.game_mode, algo, self.weights[algo], h, m, datetime.now().isoformat())
                )
        except Exception:
            pass

    def w(self, name: str) -> float:
        return self.weights.get(name, 1.0)

    def _markov3(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 12:
            return None, 50
        pat = tuple(seq[-3:])
        cnt: Counter = Counter()
        for i in range(len(seq) - 3):
            if tuple(seq[i:i + 3]) == pat and i + 3 < len(seq):
                cnt[seq[i + 3]] += 1
        total = sum(cnt.values())
        if total < 3:
            return None, 50
        best, n = cnt.most_common(1)[0]
        return best, max(int(n / total * 100), 50)

    def _markov2(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 8:
            return None, 50
        pat = tuple(seq[-2:])
        cnt: Counter = Counter()
        for i in range(len(seq) - 2):
            if tuple(seq[i:i + 2]) == pat:
                cnt[seq[i + 2]] += 1
        total = sum(cnt.values())
        if total < 3:
            return None, 50
        best, n = cnt.most_common(1)[0]
        return best, max(int(n / total * 100), 50)

    def _markov1(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        last = seq[-1]
        cnt: Counter = Counter()
        for i in range(len(seq) - 1):
            if seq[i] == last:
                cnt[seq[i + 1]] += 1
        total = sum(cnt.values())
        if total < 3:
            return None, 50
        best, n = cnt.most_common(1)[0]
        return best, max(int(n / total * 100), 50)

    def _streak_breaker(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 3:
            return None, 50
        streak, last = CauAnalyzer.get_streak(seq)
        if streak >= 3:
            return not last, min(52 + streak * 7, 87)
        return None, 50

    def _streak_advanced(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        streak, last = CauAnalyzer.get_streak(seq)
        if streak >= 6:
            return not last, min(70 + (streak - 6) * 5, 95)
        return None, 50

    def _pattern_match(self, seq: List[bool], depth: int) -> Tuple[Optional[bool], int]:
        if len(seq) < depth + 2:
            return None, 50
        pat = tuple(seq[-depth:])
        cnt: Counter = Counter()
        for i in range(len(seq) - depth):
            if tuple(seq[i:i + depth]) == pat and i + depth < len(seq):
                cnt[seq[i + depth]] += 1
        total = sum(cnt.values())
        if total < 2:
            return None, 50
        best, n = cnt.most_common(1)[0]
        return best, max(int(n / total * 100), 50)

    def _cau_dao(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        r = seq[-5:]
        if all(r[i] != r[i + 1] for i in range(4)):
            return not seq[-1], 76
        return None, 50

    def _cau_2_1(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 9:
            return None, 50
        r = seq[-9:]
        a = r[0]
        expected = [a, a, not a, a, a, not a, a, a, not a]
        if r[:8] == expected[:8]:
            return not a, 78
        return None, 50

    def _zigzag(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        if all(seq[-(i + 1)] != seq[-(i + 2)] for i in range(4)):
            return not seq[-1], 70
        return None, 50

    def _gap_analysis(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 10:
            return None, 50
        tp = [i for i, v in enumerate(seq) if v]
        fp = [i for i, v in enumerate(seq) if not v]
        if len(tp) < 3 or len(fp) < 3:
            return None, 50
        tg = [tp[i + 1] - tp[i] for i in range(len(tp) - 1)]
        fg = [fp[i + 1] - fp[i] for i in range(len(fp) - 1)]
        avg_tg = sum(tg) / len(tg)
        avg_fg = sum(fg) / len(fg)
        cur = len(seq) - 1
        dt = cur - tp[-1]
        df = cur - fp[-1]
        if dt >= avg_tg * 1.5:
            return True, min(60 + int((dt - avg_tg) * 3), 85)
        if df >= avg_fg * 1.5:
            return False, min(60 + int((df - avg_fg) * 3), 85)
        return None, 50

    def _entropy(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 10:
            return None, 50
        w = seq[-10:]
        tc = sum(w)
        xc = 10 - tc
        if tc >= 8:
            return False, 72
        if xc >= 8:
            return True, 72
        return None, 50

    def _chi_balance(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        w = seq[-20:] if len(seq) >= 20 else seq
        if not w:
            return None, 50
        r = sum(w) / len(w)
        if abs(r - 0.5) < 0.1:
            return None, 50
        return r < 0.5, min(50 + int(abs(r - 0.5) * 80), 74)

    def _history_window(self, seq: List[bool], window: int) -> Tuple[Optional[bool], int]:
        chunk = seq[-window:] if len(seq) >= window else seq
        if len(chunk) < 5:
            return None, 50
        r = sum(chunk) / len(chunk)
        if abs(r - 0.5) < 0.12:
            return None, 50
        pred = r < 0.5
        conf = min(50 + int(abs(r - 0.5) * 70), 72)
        return pred, conf

    def _score_trend(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 8:
            return None, 50
        r = sum(scores[-4:]) / 4
        o = sum(scores[-8:-4]) / 4
        diff = r - o
        if abs(diff) < 0.8:
            return None, 50
        return diff > 0, min(50 + int(abs(diff) * 5), 78)

    def _hot_cold(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 15:
            return None, 50
        recent = scores[-30:]
        ht = sum(1 for s in recent if s > 13)
        hx = sum(1 for s in recent if s < 7)
        avg5 = sum(scores[-5:]) / 5
        if avg5 > 14 and ht > 10:
            return False, 68
        if avg5 < 6 and hx > 10:
            return True, 68
        return None, 50

    def _score_dist(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 12:
            return None, 50
        r = scores[-12:]
        lo = sum(1 for s in r if s <= 7)
        hi = sum(1 for s in r if s >= 13)
        if lo / 12 >= 0.58:
            return True, 66
        if hi / 12 >= 0.58:
            return False, 66
        return None, 50

    def _linear_reg(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 10:
            return None, 50
        n = min(len(scores), 20)
        y = scores[-n:]
        x = list(range(n))
        sx, sy = sum(x), sum(y)
        sxy = sum(x[i] * y[i] for i in range(n))
        sx2 = sum(i * i for i in x)
        d = n * sx2 - sx ** 2
        if d == 0:
            return None, 50
        slope = (n * sxy - sx * sy) / d
        pred_score = sum(y[-3:]) / 3 + slope * 3
        if pred_score > 12.5:
            return False, 64
        if pred_score < 8.5:
            return True, 64
        return None, 50

    def _cycle_fft(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 20:
            return None, 50
        best_lag, best_corr = None, 0.0
        for lag in range(2, min(16, len(seq) // 2)):
            corr = sum(
                1 for i in range(len(seq) - lag) if seq[i] == seq[i + lag]
            ) / (len(seq) - lag)
            if corr > best_corr:
                best_corr, best_lag = corr, lag
        if best_corr > 0.65 and best_lag and len(seq) > best_lag:
            return seq[-best_lag], int(50 + best_corr * 30)
        return None, 50

    def _perceptron(self, seq: List[bool], scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(seq) < 10:
            return None, 50
        l3 = seq[-3:]
        tr = sum(seq[-10:]) / 10
        avg_t = sum(scores[-3:]) / 3 if len(scores) >= 3 else 10.5
        feats = [
            l3[0] * 2 - 1, l3[1] * 2 - 1, l3[2] * 2 - 1,
            (tr - 0.5) * 2,
            (avg_t - 10.5) / 5,
        ]
        ws = [0.40, 0.30, 0.20, 0.50, 0.40]
        bias = 0.05
        dot = sum(w * f for w, f in zip(ws, feats)) + bias
        prob = 1 / (1 + math.exp(-dot))
        if prob > 0.55:
            return True, int(50 + prob * 20)
        if prob < 0.45:
            return False, int(50 + (1 - prob) * 20)
        return None, 50

    def _adaptive_ma(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 15:
            return None, 50
        ma5 = sum(scores[-5:]) / 5
        ma15 = sum(scores[-15:]) / 15
        diff = ma5 - ma15
        if abs(diff) < 0.5:
            return None, 50
        return diff > 0, min(50 + int(abs(diff) * 6), 78)

    def _trend_accel(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 9:
            return None, 50
        a1 = sum(scores[-3:]) / 3
        a2 = sum(scores[-6:-3]) / 3
        a3 = sum(scores[-9:-6]) / 3
        v1 = a1 - a2
        v2 = a2 - a3
        acc = v1 - v2
        if abs(acc) < 0.5:
            return None, 50
        if acc > 0:
            return True, min(54 + int(acc * 4), 76)
        return False, min(54 + int(abs(acc) * 4), 76)

    def _tai_xiu_freq(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        short = sum(seq[-6:]) / 6
        long = sum(seq[-30:]) / len(seq[-30:]) if len(seq) >= 30 else sum(seq) / len(seq)
        diff = short - long
        if abs(diff) < 0.15:
            return None, 50
        return diff < 0, min(52 + int(abs(diff) * 60), 74)

    def _prob_weight(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 8:
            return None, 50
        recent = scores[-8:]
        low_prob = sum(_DICE_PROB.get(s, 0) for s in recent if s <= 10)
        high_prob = sum(_DICE_PROB.get(s, 0) for s in recent if s > 10)
        if high_prob > low_prob * 1.3:
            return False, 58
        if low_prob > high_prob * 1.3:
            return True, 58
        return None, 50

    def predict(self, state: dict) -> dict:
        history = state["history"]
        if len(history) < 8:
            return {
                "pred": "TÀI", "vi1": 11, "vi2": 13, "vi3": 15,
                "confidence": 50, "algo_count": 0,
                "cau_type": "CHƯA ĐỦ DỮ LIỆU", "cau_desc": "",
                "note": "Chưa đủ dữ liệu",
            }

        seq: List[bool] = []
        scores: List[int] = []
        for g in reversed(list(history)):
            tx = is_tai(g["score"], g["faces"], self.game_mode)
            if tx is not None:
                seq.append(tx)
                scores.append(g["score"])

        if len(seq) < 6:
            return {
                "pred": "TÀI", "vi1": 11, "vi2": 13, "vi3": 15,
                "confidence": 50, "algo_count": 0,
                "cau_type": "CHƯA ĐỦ", "cau_desc": "",
            }

        cau_info = CauAnalyzer.detect_type(seq)
        p6, c6 = CauAnalyzer.pattern6_match(seq)
        hw = CauAnalyzer.history_windows(seq)

        algos = [
            ("cau_detect", lambda: (cau_info["pred"], cau_info["conf"])),
            ("pattern6",   lambda: (p6, c6)),
            ("markov3",    lambda: self._markov3(seq)),
            ("markov2",    lambda: self._markov2(seq)),
            ("markov1",    lambda: self._markov1(seq)),
            ("streak_breaker",  lambda: self._streak_breaker(seq)),
            ("streak_advanced", lambda: self._streak_advanced(seq)),
            ("pattern5",   lambda: self._pattern_match(seq, 5)),
            ("pattern4",   lambda: self._pattern_match(seq, 4)),
            ("pattern3",   lambda: self._pattern_match(seq, 3)),
            ("cau_dao",    lambda: self._cau_dao(seq)),
            ("cau_2_1",    lambda: self._cau_2_1(seq)),
            ("zigzag",     lambda: self._zigzag(seq)),
            ("gap_analysis", lambda: self._gap_analysis(seq)),
            ("entropy",    lambda: self._entropy(seq)),
            ("chi_balance", lambda: self._chi_balance(seq)),
            ("history_20", lambda: self._history_window(seq, 20)),
            ("history_50", lambda: self._history_window(seq, 50)),
            ("history_100", lambda: self._history_window(seq, 100)),
            ("score_trend", lambda: self._score_trend(scores)),
            ("hot_cold",   lambda: self._hot_cold(scores)),
            ("score_dist", lambda: self._score_dist(scores)),
            ("linear_reg", lambda: self._linear_reg(scores)),
            ("cycle_fft",  lambda: self._cycle_fft(seq)),
            ("perceptron", lambda: self._perceptron(seq, scores)),
            ("adaptive_ma", lambda: self._adaptive_ma(scores)),
            ("dice_break", lambda: CauAnalyzer.dice_break_detect(scores)),
            ("prob_weight", lambda: self._prob_weight(scores)),
            ("tai_xiu_freq", lambda: self._tai_xiu_freq(seq)),
            ("trend_accel", lambda: self._trend_accel(scores)),
        ]

        results = []
        for name, fn in algos:
            try:
                p, c = fn()
                if p is not None:
                    results.append((p, c, self.w(name), name))
            except Exception as e:
                log.debug("Algo %s err: %s", name, e)

        if not results:
            return {
                "pred": "TÀI" if random.random() > 0.5 else "XỈU",
                "vi1": 11, "vi2": 13, "vi3": 15,
                "confidence": 50, "algo_count": 0,
                "cau_type": cau_info["type"],
                "cau_desc": cau_info["desc"],
            }

        tai_sc = sum(c * w for p, c, w, _ in results if p)
        xiu_sc = sum(c * w for p, c, w, _ in results if not p)
        total = tai_sc + xiu_sc
        pred_bool = tai_sc >= xiu_sc
        raw = (tai_sc if pred_bool else xiu_sc) / total * 100 if total else 50
        confidence = max(54, min(96, int(raw)))

        cl = state.get("consecutive_losses", 0)
        if cl >= 4 and confidence < 72:
            return {
                "pred": "CHỜ",
                "vi1": 0, "vi2": 0, "vi3": 0,
                "confidence": 0,
                "algo_count": len(results),
                "cau_type": cau_info["type"],
                "cau_desc": cau_info["desc"],
                "note": f"🔴 Tạm dừng — Sai {cl} lần liên tiếp, đợi tín hiệu rõ hơn.",
            }

        recent20 = seq[-20:] if len(seq) >= 20 else seq
        if recent20:
            r = sum(recent20) / len(recent20)
            if (pred_bool and r > 0.72) or (not pred_bool and r < 0.28):
                confidence = max(50, confidence - 12)

        freq_adj = CauAnalyzer.freq_score_analysis(scores, pred_bool)
        confidence = max(50, min(96, int((confidence + freq_adj) / 2)))

        rcent = [s for s in scores[-50:] if (s > 10) == pred_bool]
        if len(rcent) < 5:
            rcent = list(range(11, 18)) if pred_bool else list(range(3, 11))
        cnt = Counter(rcent)
        top = [v for v, _ in cnt.most_common(8)]
        prev = state.get("prev_pred", {})
        pvs = {prev.get("vi1"), prev.get("vi2"), prev.get("vi3")}
        top2 = [v for v in top if v not in pvs] or top
        random.shuffle(top2)
        sel = top2[:3]
        while len(sel) < 3:
            e = random.randint(11, 17) if pred_bool else random.randint(3, 10)
            if e not in sel:
                sel.append(e)
        sel.sort()

        hw_str = ""
        for w_size in [20, 50, 100]:
            if w_size in hw:
                d = hw[w_size]
                hw_str += f"{w_size}v: {d['tai']}T/{d['xiu']}X  "

        return {
            "pred": "TÀI" if pred_bool else "XỈU",
            "vi1": sel[0], "vi2": sel[1], "vi3": sel[2],
            "confidence": confidence,
            "algo_count": len(results),
            "cau_type": cau_info["type"],
            "cau_desc": cau_info["desc"],
            "cau_break_risk": cau_info.get("break_risk", 0),
            "history_windows": hw_str.strip(),
        }


_engines: Dict[str, PredictionEngine] = {}


def get_engine(game_mode: str) -> PredictionEngine:
    if game_mode not in _engines:
        _engines[game_mode] = PredictionEngine(game_mode)
    return _engines[game_mode]


async def _fetch_sicbo(session: aiohttp.ClientSession) -> Optional[list]:
    for attempt in range(MAX_RETRIES):
        headers = {**SICBO_HEADERS, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                SICBO_API, headers=headers,
                timeout=aiohttp.ClientTimeout(total=6),
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    await asyncio.sleep(0.4)
                    continue
                raw = await resp.read()
                if not raw or not raw.strip():
                    continue
                text = raw.decode("utf-8", errors="replace").strip()
                if text.startswith("<"):
                    await asyncio.sleep(1)
                    continue
                data = json.loads(text)
                dc = data.get("data")
                items = None
                if isinstance(dc, dict):
                    items = (
                        dc.get("resultList") or dc.get("list") or
                        dc.get("rows") or dc.get("result")
                    )
                elif isinstance(dc, list):
                    items = dc
                if not items:
                    items = data.get("resultList") or data.get("list") or data.get("rows")
                if isinstance(items, list) and items:
                    _states[SICBO]["api_ok"] = True
                    return items
        except Exception as e:
            log.debug("SicBo fetch attempt %d: %s", attempt + 1, e)
        await asyncio.sleep(0.4 * (attempt + 1))
    _states[SICBO]["api_ok"] = False
    return None


async def _fetch_lc(session: aiohttp.ClientSession, game_mode: str) -> Optional[list]:
    url = LC_MD5_API if game_mode == LC_MD5 else LC_HU_API
    for attempt in range(MAX_RETRIES):
        headers = {**LC_HEADERS, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=8),
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    await asyncio.sleep(0.5)
                    continue
                raw = await resp.read()
                if not raw:
                    continue
                data = json.loads(raw.decode("utf-8", errors="replace"))
                items = data.get("list") or data.get("data")
                if isinstance(items, list) and items:
                    _states[game_mode]["api_ok"] = True
                    return items
        except Exception as e:
            log.debug("LC %s fetch attempt %d: %s", game_mode, attempt + 1, e)
        await asyncio.sleep(0.5 * (attempt + 1))
    _states[game_mode]["api_ok"] = False
    return None


def _parse_sicbo(raw: dict) -> dict:
    faces = raw.get("facesList") or []
    score = raw.get("score") or sum(faces)
    return {
        "game_num": str(raw.get("gameNum", "")),
        "score": int(score),
        "faces": [int(f) for f in faces],
        "type": classify_game(int(score), [int(f) for f in faces], SICBO),
        "time": datetime.now().strftime("%H:%M:%S"),
        "ts": datetime.now().isoformat(),
    }


def _parse_lc(raw: dict, game_mode: str) -> dict:
    dices = raw.get("dices") or []
    point = raw.get("point") or sum(dices)
    return {
        "game_num": str(raw.get("id", "")),
        "score": int(point),
        "faces": [int(d) for d in dices],
        "type": classify_game(int(point), [int(d) for d in dices], game_mode),
        "raw_result": raw.get("resultTruyenThong", ""),
        "time": datetime.now().strftime("%H:%M:%S"),
        "ts": datetime.now().isoformat(),
    }


async def _load_initial(session: aiohttp.ClientSession, game_mode: str):
    state = _states[game_mode]
    if game_mode == SICBO:
        raw_list = await _fetch_sicbo(session)
    else:
        raw_list = await _fetch_lc(session, game_mode)

    if not raw_list:
        log.warning("Cannot load history for %s", game_mode)
        return

    games = []
    for item in raw_list:
        try:
            if game_mode == SICBO:
                g = _parse_sicbo(item)
            else:
                g = _parse_lc(item, game_mode)
            if g["game_num"]:
                games.append(g)
        except Exception as e:
            log.debug("Parse err %s: %s", game_mode, e)

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
    state["pred"] = get_engine(game_mode).predict(state)
    log.info("✅ %s — loaded %d sessions", GAME_LABELS[game_mode], len(games))


def _conf_bar(c: int) -> str:
    filled = round(c / 10)
    bar = "█" * filled + "░" * (10 - filled)
    star = " ⭐⭐" if c >= 88 else (" ⭐" if c >= 78 else (" 🔥" if c >= 68 else ""))
    return f"{bar} <b>{c}%</b>{star}"


_TYPE_EMOJI = {
    "TÀI": "🔴", "XỈU": "🔵", "BÃO": "🌪",
    "NỔ HŨ TÀI": "🏺🔴", "NỔ HŨ XỈU": "🏺🔵",
    "CHỜ": "🟡",
}


def _te(t: str) -> str:
    return _TYPE_EMOJI.get(t, "🎲")


def _build_msg(game_mode: str, pred: dict, prev_pred: dict, curr_game: dict) -> str:
    now = datetime.now().strftime("%H:%M:%S %d/%m")
    label = GAME_LABELS[game_mode]
    state = _states[game_mode]

    result_block = ""
    outcome_block = ""
    special_block = ""

    if curr_game.get("game_num"):
        dice_str = " | ".join(f"[{d}]" for d in curr_game["faces"])
        c_type = curr_game["type"]
        c_score = curr_game["score"]

        result_block = (
            "\n\n📜 <b>PHIÊN VỪA KẾT THÚC</b>\n"
            "<blockquote>"
            f"🔢 Phiên    : <b>#{curr_game['game_num']}</b>\n"
            f"🎲 Xúc xắc : <b>{dice_str}</b>\n"
            f"💯 Tổng     : <b>{c_score}</b>\n"
            f"🏷 Kết quả  : <b>{_te(c_type)} {c_type}</b>\n"
            f"⏰ Lúc      : <b>{curr_game.get('time', '—')}</b>"
            "</blockquote>"
        )

        if game_mode == SICBO and sorted(curr_game["faces"]) == [4, 4, 4]:
            special_block = "\n\n🌪 <b>⚠️ BÃO 4-4-4 — MỌI CƯỢC THUA (TRỪ ĐẶT BÃO)!</b>"
        elif game_mode == LC_HU and sorted(curr_game["faces"]) == [1, 1, 1]:
            special_block = "\n\n🏺💥 <b>NỔ HŨ XỈU! 1-1-1 — JACKPOT XỈU!</b>"
        elif game_mode == LC_HU and sorted(curr_game["faces"]) == [6, 6, 6]:
            special_block = "\n\n🏺💥 <b>NỔ HŨ TÀI! 6-6-6 — JACKPOT TÀI!</b>"

        if prev_pred and prev_pred.get("pred") in ("TÀI", "XỈU") and c_type in ("TÀI", "XỈU"):
            if prev_pred["pred"] == c_type:
                vi_hit = any(prev_pred.get(vk) == c_score for vk in ("vi1", "vi2", "vi3"))
                outcome_block = (
                    "\n\n💎 <b>═══════ CHUẨN VỊ! 🎯 ═══════</b>"
                    if vi_hit else
                    "\n\n🏆 <b>═══════ ĐÚNG ✅ ═══════</b>"
                )
            else:
                outcome_block = "\n\n💔 <b>═══════ SAI ❌ ═══════</b>"

    p_label = pred.get("pred", "—")
    cau_type = pred.get("cau_type", "")
    cau_desc = pred.get("cau_desc", "")
    cau_risk = pred.get("cau_break_risk", 0)
    hw_str = pred.get("history_windows", "")
    api_st = "🟢" if state["api_ok"] else "🔴"

    if p_label == "CHỜ":
        pred_block = (
            "🟡 <b>TẠM DỪNG — BOT AN TOÀN</b>\n"
            "<blockquote>"
            f"⚠️ {pred.get('note', 'Sai liên tiếp, chờ tín hiệu rõ hơn')}\n"
            f"🤖 Thuật toán đã phân tích: <b>{pred.get('algo_count', 0)}</b>\n"
            f"🃏 Loại cầu: <b>{cau_type}</b>"
            "</blockquote>"
        )
    else:
        vi1, vi2, vi3 = pred.get("vi1", "—"), pred.get("vi2", "—"), pred.get("vi3", "—")
        conf = pred.get("confidence", 50)
        algos = pred.get("algo_count", 0)
        risk_bar = f"{'🔴' * min(int(cau_risk/20), 5)}{'⚪' * (5 - min(int(cau_risk/20), 5))}" if cau_risk else "⚪⚪⚪⚪⚪"

        pred_block = (
            "🔮 <b>DỰ ĐOÁN PHIÊN TIẾP THEO</b>\n"
            "<blockquote>"
            f"🎯 Dự đoán     : <b>{_te(p_label)} {p_label}</b>\n"
            f"📊 Độ tin cậy  : {_conf_bar(conf)}\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"3️⃣ <b>VỊ TIN CẬY:</b>\n"
            f"   🥇 Vị 1  : <b>{vi1}</b>\n"
            f"   🥈 Vị 2  : <b>{vi2}</b>\n"
            f"   🥉 Vị 3  : <b>{vi3}</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"🃏 Loại cầu    : <b>{cau_type}</b>\n"
            f"📝 Phân tích   : <i>{cau_desc}</i>\n"
            f"⚡ Nguy cơ gãy : {risk_bar} <b>{cau_risk}%</b>\n"
        )
        if hw_str:
            pred_block += f"📈 Lịch sử     : <i>{hw_str}</i>\n"
        pred_block += (
            f"🤖 Thuật toán  : <b>{algos} layers</b>"
            "</blockquote>"
        )

    return (
        f"🎲 <b>{label} — DỰ ĐOÁN TỰ ĐỘNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n" + pred_block +
        result_block +
        special_block +
        outcome_block +
        f"\n\n<i>🔄 {now} | {api_st} Live</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>SicBo Bot Ultra v6.0</i>"
    )


def _record_pred(game_mode: str, pred: dict, actual: dict):
    if not pred or not actual.get("game_num"):
        return
    p_type = pred.get("pred")
    a_type = actual["type"]
    a_score = actual["score"]
    outcome = None
    vi_hit = 0
    if p_type in ("TÀI", "XỈU") and a_type in ("TÀI", "XỈU", "NỔ HŨ TÀI", "NỔ HŨ XỈU"):
        actual_tai = "TÀI" in a_type
        pred_tai = p_type == "TÀI"
        outcome = "✅ ĐÚNG" if pred_tai == actual_tai else "❌ SAI"
    for vk in ("vi1", "vi2", "vi3"):
        if pred.get(vk) == a_score:
            vi_hit = 1
            break
    try:
        with _db() as db:
            db.execute(
                "INSERT OR IGNORE INTO predictions "
                "(game_mode, game_num, pred_type, pred_vi1, pred_vi2, pred_vi3, confidence, "
                "cau_type, actual_vi, actual_type, dice, outcome, vi_hit, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    game_mode, actual["game_num"], p_type,
                    pred.get("vi1"), pred.get("vi2"), pred.get("vi3"),
                    pred.get("confidence"), pred.get("cau_type"),
                    a_score, a_type,
                    "-".join(map(str, actual["faces"])),
                    outcome, vi_hit, actual["ts"],
                )
            )
    except Exception as e:
        log.debug("record_pred: %s", e)


async def _push(app: Application, game_mode: str, prev_pred: dict, curr_game: dict):
    state = _states[game_mode]
    pred = state["pred"]
    auto_msg = state["auto_msg"]
    if not auto_msg or _maintenance["active"]:
        return
    text = _build_msg(game_mode, pred, prev_pred, curr_game)
    dead = []
    for chat_id in list(auto_msg.keys()):
        try:
            old_id = auto_msg.get(chat_id)
            if old_id:
                try:
                    await app.bot.delete_message(chat_id=chat_id, message_id=old_id)
                except Exception:
                    pass
            m = await app.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=ParseMode.HTML
            )
            auto_msg[chat_id] = m.message_id
            await asyncio.sleep(0.05)
        except Forbidden:
            dead.append(chat_id)
        except Exception as e:
            log.debug("push %s %s: %s", game_mode, chat_id, e)
    for c in dead:
        auto_msg.pop(c, None)


async def _game_loop(app: Application, game_mode: str):
    state = _states[game_mode]
    interval = SICBO_INTERVAL if game_mode == SICBO else LC_INTERVAL

    connector = aiohttp.TCPConnector(ssl=False, limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _load_initial(session, game_mode)

        while True:
            if _maintenance["active"]:
                await asyncio.sleep(1)
                continue
            try:
                await asyncio.sleep(interval)
                if game_mode == SICBO:
                    raw_list = await _fetch_sicbo(session)
                else:
                    raw_list = await _fetch_lc(session, game_mode)

                if not raw_list:
                    continue

                if game_mode == SICBO:
                    latest_raw = raw_list[0]
                    new_num = str(latest_raw.get("gameNum", ""))
                else:
                    latest_raw = raw_list[0]
                    new_num = str(latest_raw.get("id", ""))

                if not new_num or new_num == state["latest"].get("game_num"):
                    continue

                prev_pred = state["pred"].copy()

                if game_mode == SICBO:
                    new_game = _parse_sicbo(latest_raw)
                else:
                    new_game = _parse_lc(latest_raw, game_mode)

                state["history"].appendleft(new_game)
                state["latest"] = new_game
                state["prev_pred"] = prev_pred

                if prev_pred.get("pred") in ("TÀI", "XỈU"):
                    pred_tai = prev_pred["pred"] == "TÀI"
                    actual_tai = is_tai(new_game["score"], new_game["faces"], game_mode)
                    if actual_tai is not None:
                        if pred_tai == actual_tai:
                            state["consecutive_losses"] = 0
                        else:
                            state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1

                state["pred"] = get_engine(game_mode).predict(state)
                _record_pred(game_mode, prev_pred, new_game)
                await _push(app, game_mode, prev_pred, new_game)

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("loop %s: %s", game_mode, e)
                await asyncio.sleep(3)


async def _start_maintenance(app: Application, minutes: int, reason: str):
    if _maintenance["active"]:
        return
    end = datetime.now() + timedelta(minutes=minutes)
    _maintenance.update({"active": True, "end_time": end, "reason": reason})
    for gm in (SICBO, LC_MD5, LC_HU):
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "🔧 <b>BẢO TRÌ HỆ THỐNG</b>\n"
                        f"⏳ <b>{minutes} phút</b>\n"
                        f"📋 Lý do: {reason}\n"
                        f"🕐 Xong lúc: <b>{end.strftime('%H:%M %d/%m')}</b>"
                    ),
                    parse_mode=ParseMode.HTML,
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
    for gm in (SICBO, LC_MD5, LC_HU):
        for chat_id in list(_states[gm]["auto_msg"].keys()):
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text="✅ <b>BẢO TRÌ HOÀN TẤT!</b> Bot hoạt động trở lại.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    name = update.effective_user.first_name or "bạn"
    role = "👑 Admin" if is_admin(uid) else ("✅ Thành viên" if is_allowed(uid) else "🔒 Chưa kích hoạt")
    await update.message.reply_html(
        "🎲 <b>SICBO &amp; LẨU CUA BOT ULTRA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Chào <b>{name}</b>! [{role}]\n\n"
        "<blockquote>"
        "🤖 Dự đoán Tài/Xỉu tự động đa nền tảng\n"
        "🎲 SicBo Sunwin | 🦀 Lẩu Cua MD5 | 🏺 Lẩu Cua Hũ\n"
        "🧠 30+ thuật toán AI thích ứng\n"
        "🃏 Phân tích cầu & lịch sử sâu\n"
        "</blockquote>\n\n"
        "📋 /help để xem lệnh\n"
        "💡 <i>/trailkey để nhận key 2 giờ miễn phí!</i>"
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    base = (
        "📖 <b>HƯỚNG DẪN SỬ DỤNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎲 <b>SicBo Sunwin:</b>\n"
        "<blockquote>"
        "/autosicbo — Bắt đầu nhận dự đoán SicBo\n"
        "/stop_sicbo — Dừng SicBo auto\n"
        "/live — Xem trực tiếp SicBo\n"
        "</blockquote>\n"
        "🦀 <b>Lẩu Cua MD5:</b>\n"
        "<blockquote>"
        "/auto_lc_md5 — Bắt đầu nhận dự đoán LC MD5\n"
        "/stop_lc_md5 — Dừng LC MD5 auto\n"
        "/live_md5 — Xem trực tiếp LC MD5\n"
        "</blockquote>\n"
        "🏺 <b>Lẩu Cua Hũ:</b>\n"
        "<blockquote>"
        "/auto_lc_hu — Bắt đầu nhận dự đoán LC Hũ\n"
        "/stop_lc_hu — Dừng LC Hũ auto\n"
        "/live_hu — Xem trực tiếp LC Hũ\n"
        "</blockquote>\n"
        "👤 <b>Chung:</b>\n"
        "<blockquote>"
        "/stop_auto — Dừng TẤT CẢ auto\n"
        "/trailkey — Key trải nghiệm 2h\n"
        "/key {key} — Kích hoạt key\n"
        "/info — Thông tin tài khoản\n"
        "/listkq — Lịch sử dự đoán\n"
        "</blockquote>"
    )
    admin_extra = ""
    if is_admin(uid):
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
            "/reset_weights — Reset AI weights\n"
            "</blockquote>"
        )
    await update.message.reply_html(base + admin_extra)


def _check_maint(update: Update) -> bool:
    if _maintenance["active"]:
        end = _maintenance["end_time"].strftime("%H:%M") if _maintenance["end_time"] else "sắp tới"
        asyncio.create_task(
            update.message.reply_html(
                f"🔧 <b>Bot đang bảo trì!</b>\n"
                f"⏳ Xong lúc <b>{end}</b>\n"
                f"📋 Lý do: {_maintenance['reason']}"
            )
        )
        return True
    return False


async def _cmd_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE, game_mode: str):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    if _check_maint(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html(
            "🔒 <b>Chưa có quyền truy cập!</b>\n"
            "<blockquote>Dùng /trailkey để nhận key 2 giờ miễn phí\n"
            "Hoặc liên hệ admin mua key chính thức</blockquote>"
        )
        return
    state = _states[game_mode]
    if not state["latest"]:
        m = await update.message.reply_html(
            f"⏳ <b>Đang kết nối {GAME_LABELS[game_mode]}...</b>\n"
            "<i>Vui lòng chờ dữ liệu được tải, thử lại sau vài giây.</i>"
        )
        state["auto_msg"][chat_id] = m.message_id
        return
    text = _build_msg(game_mode, state["pred"], state.get("prev_pred", {}), state["latest"])
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


async def cmd_autosicbo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_auto(update, ctx, SICBO)


async def cmd_auto_lc_md5(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_auto(update, ctx, LC_MD5)


async def cmd_auto_lc_hu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_auto(update, ctx, LC_HU)


async def _cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE, game_mode: str):
    chat_id = update.effective_chat.id
    state = _states[game_mode]
    if chat_id in state["auto_msg"]:
        old_id = state["auto_msg"].pop(chat_id)
        if old_id:
            try:
                await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
            except Exception:
                pass
        await update.message.reply_html(
            f"⏹ <b>Đã dừng {GAME_LABELS[game_mode]} auto.</b>"
        )
    else:
        await update.message.reply_html("ℹ️ Không có phiên auto nào đang chạy.")


async def cmd_stop_sicbo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_stop(update, ctx, SICBO)


async def cmd_stop_lc_md5(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_stop(update, ctx, LC_MD5)


async def cmd_stop_lc_hu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_stop(update, ctx, LC_HU)


async def cmd_stop_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    stopped = []
    for gm in (SICBO, LC_MD5, LC_HU):
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
            "⏹ <b>Đã dừng tất cả auto:</b>\n"
            + "\n".join(f"• {g}" for g in stopped)
        )
    else:
        await update.message.reply_html("ℹ️ Không có phiên auto nào đang chạy.")


async def _cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE, game_mode: str):
    uid = update.effective_user.id
    if _check_maint(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    state = _states[game_mode]
    if not state["latest"]:
        await update.message.reply_html("⏳ Chưa có dữ liệu. Thử lại sau.")
        return
    text = _build_msg(game_mode, state["pred"], state.get("prev_pred", {}), state["latest"])
    await update.message.reply_html(text)


async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_live(update, ctx, SICBO)


async def cmd_live_md5(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_live(update, ctx, LC_MD5)


async def cmd_live_hu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _cmd_live(update, ctx, LC_HU)


async def cmd_trailkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or ""
    name = update.effective_user.full_name or str(uid)
    if _check_maint(update):
        return
    with _db() as db:
        used = db.execute(
            "SELECT 1 FROM activation_keys WHERE used_by=? AND is_trial=1", (uid,)
        ).fetchone()
    if used:
        await update.message.reply_html(
            "⚠️ <b>Bạn đã dùng key trải nghiệm rồi!</b>\n"
            "Liên hệ admin để nâng cấp tài khoản."
        )
        return
    k = _gen_key("TRIAL")
    exp = (datetime.now() + timedelta(hours=2)).isoformat()
    with _db() as db:
        db.execute(
            "INSERT INTO activation_keys (key, created_by, created_at, expires_at, used_by, used_at, is_trial) "
            "VALUES (?,?,?,?,?,?,1)",
            (k, 0, datetime.now().isoformat(), exp, uid, datetime.now().isoformat())
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id, username, added_at, added_by) VALUES (?,?,?,?)",
            (uid, username, datetime.now().isoformat(), 0)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id, expires_at) VALUES (?,?)",
            (uid, exp)
        )
    exp_fmt = datetime.fromisoformat(exp).strftime("%H:%M %d/%m/%Y")
    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                chat_id=adm,
                text=(
                    "🔔 <b>USER MỚI NHẬN TRIAL KEY!</b>\n"
                    "<blockquote>"
                    f"👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                    f"🔑 <code>{k}</code>\n📅 Đến {exp_fmt}"
                    "</blockquote>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    await update.message.reply_html(
        "🎁 <b>KEY TRẢI NGHIỆM 2 GIỜ</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"🔑 <code>{k}</code>\n"
        f"⏳ Hết hạn: <b>{exp_fmt}</b>\n"
        "✅ Đã kích hoạt tự động!"
        "</blockquote>\n\n"
        "🚀 Dùng /autosicbo, /auto_lc_md5 hoặc /auto_lc_hu để bắt đầu!"
    )


async def cmd_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or ""
    name = update.effective_user.full_name or str(uid)
    if _check_maint(update):
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
            "UPDATE activation_keys SET used_by=?, used_at=? WHERE key=?",
            (uid, datetime.now().isoformat(), key)
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id, username, added_at, added_by) VALUES (?,?,?,?)",
            (uid, username, datetime.now().isoformat(), 0)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id, expires_at) VALUES (?,?)",
            (uid, row["expires_at"])
        )
        exp_fmt = row["expires_at"][:16].replace("T", " ")

    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                chat_id=adm,
                text=(
                    "🔔 <b>USER KÍCH HOẠT KEY!</b>\n"
                    "<blockquote>"
                    f"👤 {name}\n🆔 <code>{uid}</code>\n@{username or 'N/A'}\n"
                    f"🔑 <code>{key}</code>\n📅 Đến {exp_fmt}"
                    "</blockquote>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    await update.message.reply_html(
        "🔑 <b>KÍCH HOẠT THÀNH CÔNG!</b>\n"
        f"<blockquote>📅 Hết hạn: <b>{exp_fmt}</b></blockquote>\n"
        "🚀 Dùng /autosicbo, /auto_lc_md5 hoặc /auto_lc_hu để bắt đầu!"
    )


async def cmd_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    name = update.effective_user.full_name or str(uid)
    if is_admin(uid):
        role, exp = "👑 Admin", "♾ Vĩnh viễn"
    elif is_allowed(uid):
        with _db() as db:
            e = db.execute(
                "SELECT expires_at FROM user_expiry WHERE user_id=?", (uid,)
            ).fetchone()
        if e and e["expires_at"]:
            dt = datetime.fromisoformat(e["expires_at"])
            left = dt - datetime.now()
            hrs = max(0, int(left.total_seconds() // 3600))
            mins = max(0, int((left.total_seconds() % 3600) // 60))
            exp = f"{dt.strftime('%H:%M %d/%m/%Y')} (còn {hrs}h{mins}m)"
        else:
            exp = "Không xác định"
        role = "✅ Thành viên"
    else:
        role, exp = "❌ Chưa kích hoạt", "—"

    with _db() as db:
        total = db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        correct = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%'"
        ).fetchone()[0]
        vi_hits = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE vi_hit=1"
        ).fetchone()[0]

    acc = f"{correct / total * 100:.1f}%" if total else "—"
    vi_acc = f"{vi_hits / total * 100:.1f}%" if total else "—"

    auto_status = []
    for gm in (SICBO, LC_MD5, LC_HU):
        if uid in _states[gm]["auto_msg"] or update.effective_chat.id in _states[gm]["auto_msg"]:
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
        "📊 <b>Thống kê bot:</b>\n"
        "<blockquote>"
        f"Tổng dự đoán : <b>{total}</b>\n"
        f"✅ Đúng loại : <b>{correct}</b> ({acc})\n"
        f"🎯 Trúng vị  : <b>{vi_hits}</b> ({vi_acc})\n"
        f"🔧 Bảo trì   : {'Đang bảo trì' if _maintenance['active'] else 'Bình thường'}"
        "</blockquote>"
    )


async def cmd_listkq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _check_maint(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    gm_arg = ctx.args[0].lower() if ctx.args else "sicbo"
    gm_map = {"sicbo": SICBO, "md5": LC_MD5, "hu": LC_HU, "lc_md5": LC_MD5, "lc_hu": LC_HU}
    gm = gm_map.get(gm_arg, SICBO)
    with _db() as db:
        rows = db.execute(
            "SELECT * FROM predictions WHERE game_mode=? ORDER BY id DESC LIMIT 15",
            (gm,)
        ).fetchall()
    if not rows:
        await update.message.reply_html(f"📭 Chưa có lịch sử cho {GAME_LABELS[gm]}.")
        return
    correct = sum(1 for r in rows if r["outcome"] and "ĐÚNG" in r["outcome"])
    vi_hit = sum(1 for r in rows if r["vi_hit"])
    lines = [
        f"📜 <b>LỊCH SỬ {GAME_LABELS[gm]}</b>",
        f"<i>15 phiên • Đúng loại: {correct}/{len(rows)} ({correct/len(rows)*100:.0f}%) • Vị: {vi_hit}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in rows:
        out = r["outcome"] or "⏳"
        vm = " 🎯" if r["vi_hit"] else ""
        ct = f" [{r['cau_type']}]" if r["cau_type"] else ""
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


@_admin_only
async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/add {user_id} [hours]</code>")
        return
    try:
        tid = int(ctx.args[0])
        hours = int(ctx.args[1]) if len(ctx.args) > 1 else 720
    except ValueError:
        await update.message.reply_html("❌ Tham số không hợp lệ!")
        return
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id, added_at, added_by) VALUES (?,?,?)",
            (tid, datetime.now().isoformat(), update.effective_user.id)
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id, expires_at) VALUES (?,?)",
            (tid, exp)
        )
    await update.message.reply_html(
        f"✅ Đã thêm <code>{tid}</code>\n"
        f"📅 Hạn: {exp[:16].replace('T', ' ')} ({hours}h)"
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
        db.execute("DELETE FROM user_expiry WHERE user_id=?", (tid,))
    for gm in (SICBO, LC_MD5, LC_HU):
        _states[gm]["auto_msg"].pop(tid, None)
    await update.message.reply_html(f"✅ Đã xoá user <code>{tid}</code>!")


@_admin_only
async def cmd_luser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    with _db() as db:
        rows = db.execute(
            "SELECT u.user_id, u.username, u.added_at, e.expires_at "
            "FROM allowed_users u LEFT JOIN user_expiry e ON u.user_id=e.user_id "
            "ORDER BY u.added_at DESC"
        ).fetchall()
    if not rows:
        await update.message.reply_html("📭 Chưa có user nào.")
        return
    lines = [f"👥 <b>DANH SÁCH USER ({len(rows)})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        exp = r["expires_at"][:16].replace("T", " ") if r["expires_at"] else "∞"
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
    k = _gen_key("SUNWIN")
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute(
            "INSERT INTO activation_keys (key, created_by, created_at, expires_at) VALUES (?,?,?,?)",
            (k, update.effective_user.id, datetime.now().isoformat(), exp)
        )
    await update.message.reply_html(
        "🔑 <b>KEY MỚI TẠO</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"Key  : <code>{k}</code>\n"
        f"Hạn  : {exp[:16].replace('T', ' ')} ({hours}h)\n"
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
        used = f"✅ {r['used_by']}" if r["used_by"] else "🟡 Chưa dùng"
        trial = " [TRIAL]" if r["is_trial"] else ""
        exp = r["expires_at"][:16].replace("T", " ")
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
                chat_id=row["user_id"],
                text=(
                    "📢 <b>THÔNG BÁO TỪ ADMIN</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<blockquote>{msg}</blockquote>\n"
                    f"<i>🕐 {datetime.now().strftime('%H:%M %d/%m/%Y')}</i>"
                ),
                parse_mode=ParseMode.HTML,
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
        for gm in (SICBO, LC_MD5, LC_HU):
            total = db.execute(
                "SELECT COUNT(*) FROM predictions WHERE game_mode=?", (gm,)
            ).fetchone()[0]
            correct = db.execute(
                "SELECT COUNT(*) FROM predictions WHERE game_mode=? AND outcome LIKE '%ĐÚNG%'", (gm,)
            ).fetchone()[0]
            vi = db.execute(
                "SELECT COUNT(*) FROM predictions WHERE game_mode=? AND vi_hit=1", (gm,)
            ).fetchone()[0]
            acc = f"{correct/total*100:.1f}%" if total else "—"
            vi_acc = f"{vi/total*100:.1f}%" if total else "—"
            cl = _states[gm].get("consecutive_losses", 0)
            api = "🟢" if _states[gm]["api_ok"] else "🔴"
            lines.append(
                f"\n{GAME_LABELS[gm]} {api}\n"
                "<blockquote>"
                f"Tổng: <b>{total}</b> | Đúng: <b>{correct}</b> ({acc}) | Vị: <b>{vi}</b> ({vi_acc})\n"
                f"Sai LT: <b>{cl}</b> | History: <b>{len(_states[gm]['history'])}</b> phiên"
                "</blockquote>"
            )
    lines.append(
        f"\n🔧 Bảo trì: {'Đang bảo trì' if _maintenance['active'] else 'Bình thường'}"
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
    for gm in (SICBO, LC_MD5, LC_HU):
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
                    db.execute("DELETE FROM user_expiry WHERE user_id=?", (uid,))
                    for gm in (SICBO, LC_MD5, LC_HU):
                        _states[gm]["auto_msg"].pop(uid, None)
                db.execute(
                    "DELETE FROM activation_keys WHERE used_by IS NOT NULL AND expires_at < ?",
                    (now,)
                )
        except Exception as e:
            log.debug("cleanup: %s", e)


async def post_init(app: Application):
    asyncio.create_task(_game_loop(app, SICBO))
    asyncio.create_task(_game_loop(app, LC_MD5))
    asyncio.create_task(_game_loop(app, LC_HU))
    asyncio.create_task(_cleanup_loop())
    log.info("✅ All game loops started.")


def main():
    init_db()
    for gm in (SICBO, LC_MD5, LC_HU):
        get_engine(gm)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    handlers = [
        CommandHandler("start",          cmd_start),
        CommandHandler("help",           cmd_help),
        CommandHandler("autosicbo",      cmd_autosicbo),
        CommandHandler("auto_lc_md5",    cmd_auto_lc_md5),
        CommandHandler("auto_lc_hu",     cmd_auto_lc_hu),
        CommandHandler("stop_sicbo",     cmd_stop_sicbo),
        CommandHandler("stop_lc_md5",    cmd_stop_lc_md5),
        CommandHandler("stop_lc_hu",     cmd_stop_lc_hu),
        CommandHandler("stop_auto",      cmd_stop_auto),
        CommandHandler("live",           cmd_live),
        CommandHandler("live_md5",       cmd_live_md5),
        CommandHandler("live_hu",        cmd_live_hu),
        CommandHandler("trailkey",       cmd_trailkey),
        CommandHandler("key",            cmd_key),
        CommandHandler("info",           cmd_info),
        CommandHandler("listkq",         cmd_listkq),
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
    ]
    for h in handlers:
        app.add_handler(h)

    log.info("🎲 SicBo + LC Bot Ultra v6.0 starting...")
    app.run_polling(drop_pending_updates=True, poll_interval=1)


if __name__ == "__main__":
    main()
