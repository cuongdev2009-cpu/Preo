#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║      🎲  SICBO SUNWIN BOT  — Ultra Edition v5.0                ║
║   Bão chỉ 4-4-4 • Ensemble thích ứng • Dự đoán siêu ổn định   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import string
from collections import Counter, deque, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════
BOT_TOKEN  = "8828842195:AAGdzF60aoUbBv6PJf8_LnQ0AunYF3UN8C8"
ADMIN_IDS  = [8001225219]

API_URL = (
    "https://api.wsktnus8.net/v2/history/getLastResult"
    "?gameId=ktrng_3979&size=100&tableId=39791215743193&curPage=1"
)

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
]

BASE_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer":         "https://sunwin.gs/",
    "Origin":          "https://sunwin.gs",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
    "Sec-Fetch-Dest":  "empty",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "cross-site",
}

FETCH_INTERVAL = 2.5
DB_PATH        = "sicbo.db"
MEM_WINDOW     = 300            # mở rộng vùng nhớ cho thuật toán dài hạn
MAX_RETRIES    = 3

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ══════════════════════════════════════════════════════════════════
_history:   deque = deque(maxlen=MEM_WINDOW)   # newest first
_latest:    dict  = {}
_pred:      dict  = {}
_auto_msg:  dict  = {}
_api_ok:    bool  = False
_prev_pred: dict  = {}

# Trạng thái bảo trì
_maintenance_state = {
    "active": False,
    "end_time": None,
    "reason": "",
    "task": None
}

# Bộ đếm sai liên tiếp gần đây (dùng trong engine)
_recent_results = deque(maxlen=50)   # lưu tuple (pred_bool, actual_tai, conf)

# ══════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════
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
                game_num    TEXT UNIQUE,
                pred_type   TEXT,
                pred_vi1    INTEGER,
                pred_vi2    INTEGER,
                pred_vi3    INTEGER,
                confidence  INTEGER,
                actual_vi   INTEGER,
                actual_type TEXT,
                dice        TEXT,
                outcome     TEXT,
                vi_hit      INTEGER DEFAULT 0,
                created_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS algo_weights (
                algo_name TEXT PRIMARY KEY,
                weight    REAL DEFAULT 1.0,
                hits      INTEGER DEFAULT 0,
                misses    INTEGER DEFAULT 0,
                updated   TEXT
            );
        """)
    log.info("Database initialised.")

# ══════════════════════════════════════════════════════════════════
#  AUTH HELPERS
# ══════════════════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════════════════
#  GAME LOGIC (CHỈ BÃO 4-4-4)
# ══════════════════════════════════════════════════════════════════
def classify(score: int, faces: list) -> str:
    if faces == [4, 4, 4]:
        return "🌪 BÃO"
    # Các bộ ba khác vẫn tính Tài/Xỉu theo tổng điểm
    if score > 10:
        return "TÀI"
    if score >= 3:   # 3 <= score <= 10
        return "XỈU"
    return "XỈU"  # không thể xảy ra vì score >=3

def is_tai(score: int, faces: list) -> Optional[bool]:
    t = classify(score, faces)
    if t == "TÀI":
        return True
    if t == "XỈU":
        return False
    # BÃO 4-4-4 không phải Tài/Xỉu -> None
    return None

# ══════════════════════════════════════════════════════════════════
#  PREDICTION ENGINE (ENSEMBLE THÍCH ỨNG)
# ══════════════════════════════════════════════════════════════════
class PredictionEngine:
    def __init__(self):
        self.algo_weights: Dict[str, float] = {}
        self.algo_perf: Dict[str, dict] = defaultdict(lambda: {"hits":0,"misses":0,"total":0})
        self.load_weights()
        self.consecutive_losses = 0
        self.last_pred_type = None

    def load_weights(self):
        try:
            with _db() as db:
                rows = db.execute("SELECT algo_name, weight FROM algo_weights").fetchall()
                for r in rows:
                    self.algo_weights[r["algo_name"]] = r["weight"]
        except Exception:
            self.algo_weights = {}
        # Trọng số mặc định nếu chưa có
        defaults = {
            "markov3": 5.0, "markov2": 4.0, "markov1": 3.0,
            "streak_breaker": 3.0, "streak_advanced": 4.0,
            "pattern5": 3.0, "pattern4": 2.0, "pattern3": 2.0,
            "cau_dao": 4.0, "cau_1_1": 3.0, "cau_2_1": 3.0,
            "zigzag": 2.0, "gap_analysis": 4.0, "entropy": 2.0,
            "chi_balance": 1.0, "score_trend": 2.0, "hot_cold": 2.0,
            "score_dist": 3.0, "linear_reg": 3.5, "cycle_fft": 2.5,
            "neural_perceptron": 3.0, "adaptive_ma": 3.0,
        }
        for k, v in defaults.items():
            if k not in self.algo_weights:
                self.algo_weights[k] = v

    def update_weights(self, algo_name: str, correct: bool):
        """Cập nhật trọng số dựa trên kết quả đúng/sai."""
        current = self.algo_weights.get(algo_name, 1.0)
        if correct:
            self.algo_weights[algo_name] = min(10.0, current * 1.15)
            self.algo_perf[algo_name]["hits"] += 1
        else:
            self.algo_weights[algo_name] = max(0.5, current * 0.85)
            self.algo_perf[algo_name]["misses"] += 1
        self.algo_perf[algo_name]["total"] += 1
        # Lưu vào DB
        try:
            with _db() as db:
                db.execute(
                    "INSERT OR REPLACE INTO algo_weights (algo_name, weight, hits, misses, updated) VALUES (?,?,?,?,?)",
                    (algo_name, self.algo_weights[algo_name],
                     self.algo_perf[algo_name]["hits"],
                     self.algo_perf[algo_name]["misses"],
                     datetime.now().isoformat())
                )
        except Exception:
            pass

    def get_weight(self, algo_name: str) -> float:
        return self.algo_weights.get(algo_name, 1.0)

    def record_result(self, pred_bool: bool, actual_tai: Optional[bool]):
        """Ghi nhận kết quả để điều chỉnh chiến lược."""
        global _recent_results
        _recent_results.append((pred_bool, actual_tai, 0))
        if actual_tai is not None:
            if pred_bool == actual_tai:
                self.consecutive_losses = 0
            else:
                self.consecutive_losses += 1

    # ---------- CÁC THUẬT TOÁN (có sử dụng engine weight) ----------
    def _markov3(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 12:
            return None, 50
        pattern = (seq[-3], seq[-2], seq[-1])
        counts: Counter = Counter()
        for i in range(len(seq) - 3):
            if (seq[i], seq[i+1], seq[i+2]) == pattern:
                if i + 3 < len(seq):
                    counts[seq[i + 3]] += 1
        total = sum(counts.values())
        if total < 2:
            return None, 50
        best_val, best_cnt = counts.most_common(1)[0]
        conf = int(best_cnt / total * 100)
        return best_val, max(conf, 50)

    def _markov2(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 8:
            return None, 50
        pattern = (seq[-2], seq[-1])
        counts: Counter = Counter()
        for i in range(len(seq) - 2):
            if (seq[i], seq[i + 1]) == pattern:
                counts[seq[i + 2]] += 1
        total = sum(counts.values())
        if total < 3:
            return None, 50
        best_val, best_cnt = counts.most_common(1)[0]
        conf = int(best_cnt / total * 100)
        return best_val, conf

    def _markov1(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        last = seq[-1]
        counts: Counter = Counter()
        for i in range(len(seq) - 1):
            if seq[i] == last:
                counts[seq[i + 1]] += 1
        total = sum(counts.values())
        if total < 3:
            return None, 50
        best_val, best_cnt = counts.most_common(1)[0]
        conf = int(best_cnt / total * 100)
        return best_val, conf

    def _streak_breaker(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 3:
            return None, 50
        last = seq[-1]
        streak = 1
        for x in reversed(seq[:-1]):
            if x == last:
                streak += 1
            else:
                break
        if streak >= 3:
            conf = min(52 + streak * 7, 87)
            return not last, conf
        return None, 50

    def _streak_advanced(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        last = seq[-1]
        streak = 1
        for x in reversed(seq[:-1]):
            if x == last:
                streak += 1
            else:
                break
        if streak >= 6:
            conf = min(70 + (streak - 6) * 5, 95)
            return not last, conf
        return None, 50

    def _pattern_match(self, seq: List[bool], depth: int) -> Tuple[Optional[bool], int]:
        if len(seq) < depth + 2:
            return None, 50
        pattern = tuple(seq[-depth:])
        votes: Counter = Counter()
        for i in range(len(seq) - depth):
            if tuple(seq[i: i + depth]) == pattern:
                if i + depth < len(seq):
                    votes[seq[i + depth]] += 1
        total = sum(votes.values())
        if total < 2:
            return None, 50
        best_val, best_cnt = votes.most_common(1)[0]
        conf = int(best_cnt / total * 100)
        return best_val, max(conf, 50)

    def _cau_dao_detect(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        last5 = seq[-5:]
        expected = not last5[0]
        is_alternating = all(last5[i] == (expected if i%2==1 else not expected) for i in range(5))
        if is_alternating:
            return not seq[-1], 78
        return None, 50

    def _cau_1_1_detect(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 8:
            return None, 50
        recent = seq[-8:]
        pattern = [recent[-6], not recent[-6], recent[-6]]
        if recent[-6:] == pattern * 2:
            return not seq[-1], 82
        return None, 50

    def _cau_2_1_detect(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 9:
            return None, 50
        recent = seq[-9:]
        a = recent[-9]
        b = not a
        expected = [a, a, b, a, a, b, a, a, b]
        if recent[:8] == expected[:8]:
            return b, 80
        return None, 50

    def _zigzag_detect(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 6:
            return None, 50
        zigzag = all(seq[-(i+1)] != seq[-(i+2)] for i in range(4))
        if zigzag:
            return not seq[-1], 72
        return None, 50

    def _gap_analysis(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 10:
            return None, 50
        true_pos = [i for i, v in enumerate(seq) if v]
        false_pos = [i for i, v in enumerate(seq) if not v]
        if len(true_pos) < 3 or len(false_pos) < 3:
            return None, 50
        true_gaps = [true_pos[i+1] - true_pos[i] for i in range(len(true_pos)-1)]
        false_gaps = [false_pos[i+1] - false_pos[i] for i in range(len(false_pos)-1)]
        avg_true_gap = sum(true_gaps) / len(true_gaps)
        avg_false_gap = sum(false_gaps) / len(false_gaps)
        last_true = true_pos[-1]
        last_false = false_pos[-1]
        current = len(seq) - 1
        dist_true = current - last_true
        dist_false = current - last_false
        if dist_true >= avg_true_gap * 1.5:
            return True, min(60 + int(dist_true - avg_true_gap)*3, 85)
        if dist_false >= avg_false_gap * 1.5:
            return False, min(60 + int(dist_false - avg_false_gap)*3, 85)
        return None, 50

    def _entropy_analysis(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        if len(seq) < 10:
            return None, 50
        window = seq[-10:]
        tai_count = sum(window)
        xiu_count = len(window) - tai_count
        if tai_count >= 8:
            return False, 75
        if xiu_count >= 8:
            return True, 75
        return None, 50

    def _chi_balance(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        window = seq[-20:] if len(seq) >= 20 else seq
        if not window:
            return None, 50
        tai_pct = sum(window) / len(window)
        if abs(tai_pct - 0.5) < 0.1:
            return None, 50
        pred_tai = tai_pct < 0.5
        conf = min(50 + int(abs(tai_pct - 0.5) * 80), 74)
        return pred_tai, conf

    def _score_trend(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 8:
            return None, 50
        recent = scores[-4:]
        older  = scores[-8:-4]
        r_avg  = sum(recent) / len(recent)
        o_avg  = sum(older)  / len(older)
        diff   = r_avg - o_avg
        if abs(diff) < 0.8:
            return None, 50
        pred_tai = diff > 0
        conf = min(50 + int(abs(diff) * 5), 80)
        return pred_tai, conf

    def _hot_cold_zone(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 15:
            return None, 50
        recent = scores[-30:]
        hot_tai = sum(1 for s in recent if s > 13)
        hot_xiu = sum(1 for s in recent if s < 7)
        last5   = scores[-5:]
        avg5    = sum(last5) / len(last5)
        if avg5 > 14 and hot_tai > 10:
            return False, 68
        if avg5 < 6 and hot_xiu > 10:
            return True, 68
        return None, 50

    def _score_distribution(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        if len(scores) < 12:
            return None, 50
        recent = scores[-12:]
        low = sum(1 for s in recent if s <= 7)
        mid = sum(1 for s in recent if 8 <= s <= 12)
        high = sum(1 for s in recent if s >= 13)
        total = len(recent)
        if low / total >= 0.6:
            return True, 65
        if high / total >= 0.6:
            return False, 65
        if mid / total >= 0.6:
            return None, 50
        return None, 50

    # --- THUẬT TOÁN MỚI ---
    def _linear_regression_trend(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        """Hồi quy tuyến tính xu hướng điểm."""
        if len(scores) < 10:
            return None, 50
        n = len(scores)
        x = list(range(n))
        y = scores
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i]*y[i] for i in range(n))
        sum_x2 = sum(i*i for i in x)
        denom = n * sum_x2 - sum_x**2
        if denom == 0:
            return None, 50
        slope = (n * sum_xy - sum_x * sum_y) / denom
        # Dự đoán điểm tiếp theo = trung bình 3 điểm cuối + slope*3
        last_avg = sum(y[-3:]) / 3
        pred_score = last_avg + slope * 3
        if pred_score > 12:
            return False, 65
        elif pred_score < 9:
            return True, 65
        return None, 50

    def _cycle_fft_detect(self, seq: List[bool]) -> Tuple[Optional[bool], int]:
        """Tìm chu kỳ đơn giản bằng tự tương quan."""
        if len(seq) < 20:
            return None, 50
        # Tự tương quan cho độ trễ từ 1 đến 10
        best_lag = None
        best_corr = 0
        for lag in range(2, min(15, len(seq)//2)):
            corr = sum(1 for i in range(len(seq)-lag) if seq[i] == seq[i+lag]) / (len(seq)-lag)
            if corr > best_corr:
                best_corr = corr
                best_lag = lag
        if best_corr > 0.65 and best_lag:
            # Dự đoán dựa trên giá trị cách đây best_lag phiên
            if len(seq) > best_lag:
                return seq[-best_lag], int(50 + best_corr*30)
        return None, 50

    def _neural_perceptron(self, seq: List[bool], scores: List[int]) -> Tuple[Optional[bool], int]:
        """Perceptron đơn giản với 5 đầu vào: 3 kết quả gần nhất, trend, chênh lệch dài hạn."""
        if len(seq) < 10:
            return None, 50
        # Tính các đặc trưng
        last3 = seq[-3:]   # 0/1
        tai_ratio = sum(seq[-10:]) / 10
        trend = scores[-3:] if len(scores) >= 3 else [10]*3
        avg_trend = sum(trend) / len(trend)
        # Vector đặc trưng (5 giá trị)
        features = [
            last3[0]*2-1, last3[1]*2-1, last3[2]*2-1,  # đổi thành -1/1
            (tai_ratio - 0.5) * 2,
            (avg_trend - 10) / 5
        ]
        # Trọng số đã được "học" qua thời gian (cố định ban đầu)
        weights = [0.4, 0.3, 0.2, 0.5, 0.4]
        bias = 0.1
        dot = sum(w*f for w,f in zip(weights, features)) + bias
        prob = 1 / (1 + math.exp(-dot))  # sigmoid -> khả năng Tài
        if prob > 0.55:
            return True, int(50 + prob*20)
        elif prob < 0.45:
            return False, int(50 + (1-prob)*20)
        return None, 50

    def _adaptive_ma(self, scores: List[int]) -> Tuple[Optional[bool], int]:
        """Trung bình động thích ứng, so sánh MA ngắn và MA dài."""
        if len(scores) < 15:
            return None, 50
        ma5 = sum(scores[-5:]) / 5
        ma15 = sum(scores[-15:]) / 15
        diff = ma5 - ma15
        if abs(diff) < 0.5:
            return None, 50
        pred_tai = diff > 0
        conf = min(50 + int(abs(diff) * 6), 78)
        return pred_tai, conf

    # ---------- ENSEMBLE & DỰ ĐOÁN CHÍNH ----------
    def predict(self) -> dict:
        global _history, _prev_pred, _recent_results
        if len(_history) < 8:
            return {
                "pred": "TÀI", "vi1": 11, "vi2": 13, "vi3": 15,
                "confidence": 50, "note": "Chưa đủ dữ liệu"
            }

        seq:    List[bool] = []
        scores: List[int]  = []
        for g in reversed(list(_history)):
            tx = is_tai(g["score"], g["faces"])
            if tx is not None:
                seq.append(tx)
                scores.append(g["score"])

        if len(seq) < 6:
            return {
                "pred": "TÀI", "vi1": 11, "vi2": 13, "vi3": 15,
                "confidence": 50, "note": "Chưa đủ dữ liệu"
            }

        # Danh sách tất cả thuật toán với tên và hàm, trọng số từ engine
        algos = [
            ("markov3",           lambda: self._markov3(seq)),
            ("markov2",           lambda: self._markov2(seq)),
            ("markov1",           lambda: self._markov1(seq)),
            ("streak_breaker",    lambda: self._streak_breaker(seq)),
            ("streak_advanced",   lambda: self._streak_advanced(seq)),
            ("pattern5",          lambda: self._pattern_match(seq, 5)),
            ("pattern4",          lambda: self._pattern_match(seq, 4)),
            ("pattern3",          lambda: self._pattern_match(seq, 3)),
            ("cau_dao",           lambda: self._cau_dao_detect(seq)),
            ("cau_1_1",           lambda: self._cau_1_1_detect(seq)),
            ("cau_2_1",           lambda: self._cau_2_1_detect(seq)),
            ("zigzag",            lambda: self._zigzag_detect(seq)),
            ("gap_analysis",      lambda: self._gap_analysis(seq)),
            ("entropy",           lambda: self._entropy_analysis(seq)),
            ("chi_balance",       lambda: self._chi_balance(seq)),
            ("score_trend",       lambda: self._score_trend(scores)),
            ("hot_cold",          lambda: self._hot_cold_zone(scores)),
            ("score_dist",        lambda: self._score_distribution(scores)),
            ("linear_reg",        lambda: self._linear_regression_trend(scores)),
            ("cycle_fft",         lambda: self._cycle_fft_detect(seq)),
            ("neural_perceptron", lambda: self._neural_perceptron(seq, scores)),
            ("adaptive_ma",       lambda: self._adaptive_ma(scores)),
        ]

        results = []
        for name, func in algos:
            try:
                p, c = func()
                if p is not None:
                    w = self.get_weight(name)
                    results.append((p, c, w, name))
            except Exception as e:
                log.warning(f"Algo {name} error: {e}")

        # Nếu không có thuật toán nào đưa ra dự đoán, dùng ngẫu nhiên cân bằng
        if not results:
            pred_bool = random.random() > 0.5
            vi1 = random.randint(11, 17) if pred_bool else random.randint(4, 10)
            vi2 = min(18, vi1 + 1)      if pred_bool else max(3, vi1 - 1)
            vi3 = min(18, vi1 + 2)      if pred_bool else max(3, vi1 - 2)
            return {
                "pred": "TÀI" if pred_bool else "XỈU",
                "vi1": vi1, "vi2": vi2, "vi3": vi3,
                "confidence": 50,
                "algo_count": 0
            }

        # Tính điểm tổng hợp có trọng số
        tai_score = sum(c * w for p, c, w, _ in results if p is True)
        xiu_score = sum(c * w for p, c, w, _ in results if p is False)
        total     = tai_score + xiu_score

        pred_bool = tai_score >= xiu_score
        raw_conf  = (tai_score if pred_bool else xiu_score) / total * 100 if total else 50
        confidence = max(54, min(96, int(raw_conf)))

        # --- CƠ CHẾ AN TOÀN: nếu sai liên tiếp 3 lần, tăng ngưỡng confidence ---
        if self.consecutive_losses >= 3:
            if confidence < 70:
                # Skip (trả về dự đoán "CHỜ")
                return {
                    "pred": "CHỜ",
                    "vi1": 0, "vi2": 0, "vi3": 0,
                    "confidence": 0,
                    "algo_count": len(results),
                    "note": "Bot tạm dừng do sai liên tiếp, đợi tín hiệu rõ ràng hơn."
                }

        # --- CÂN BẰNG CHỐNG THIÊN VỊ ---
        recent20 = seq[-20:] if len(seq) >= 20 else seq
        if recent20:
            tai_ratio = sum(recent20) / len(recent20)
            if (pred_bool and tai_ratio > 0.7) or (not pred_bool and tai_ratio < 0.3):
                confidence = max(50, confidence - 15)

        # Dự đoán vị nâng cao
        recent_scores = scores[-40:]
        if pred_bool:
            candidates = [s for s in recent_scores if 11 <= s <= 17]
            if len(candidates) < 5:
                candidates = list(range(11, 18))
        else:
            candidates = [s for s in recent_scores if 4 <= s <= 10]
            if len(candidates) < 5:
                candidates = list(range(4, 11))

        cnt = Counter(candidates)
        top_candidates = [v for v, _ in cnt.most_common(8)]
        if _prev_pred:
            prev_vis = {_prev_pred.get("vi1"), _prev_pred.get("vi2"), _prev_pred.get("vi3")}
            top_candidates = [v for v in top_candidates if v not in prev_vis] or top_candidates

        random.shuffle(top_candidates)
        selected = top_candidates[:3]
        while len(selected) < 3:
            extra = random.randint(11, 17) if pred_bool else random.randint(4, 10)
            if extra not in selected:
                selected.append(extra)
        selected.sort()
        vi1, vi2, vi3 = selected[0], selected[1], selected[2]

        return {
            "pred":       "TÀI" if pred_bool else "XỈU",
            "vi1":        vi1,
            "vi2":        vi2,
            "vi3":        vi3,
            "confidence": confidence,
            "algo_count": len(results),
        }

# Khởi tạo engine toàn cục
engine = PredictionEngine()

# ══════════════════════════════════════════════════════════════════
#  API FETCHER
# ══════════════════════════════════════════════════════════════════
async def fetch_results(session: aiohttp.ClientSession) -> Optional[List[dict]]:
    global _api_ok
    for attempt in range(MAX_RETRIES):
        headers = {**BASE_HEADERS, "User-Agent": _UA_POOL[attempt % len(_UA_POOL)]}
        try:
            async with session.get(
                API_URL,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=6),
                ssl=False,
                allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    await asyncio.sleep(0.4)
                    continue
                raw = await resp.read()
                if not raw or not raw.strip():
                    await asyncio.sleep(0.4)
                    continue
                text = raw.decode("utf-8", errors="replace").strip()
                if text.startswith("<"):
                    await asyncio.sleep(1)
                    continue
                data = json.loads(text)
                items = None
                data_content = data.get("data")
                if isinstance(data_content, dict):
                    items = (
                        data_content.get("resultList")
                        or data_content.get("list")
                        or data_content.get("rows")
                        or data_content.get("result")
                        or data_content.get("results")
                    )
                elif isinstance(data_content, list):
                    items = data_content
                if not items:
                    items = (
                        data.get("resultList")
                        or data.get("list")
                        or data.get("rows")
                        or data.get("result")
                    )
                if isinstance(items, list) and items:
                    _api_ok = True
                    return items
                log.warning("API unexpected structure: %s", text[:300])
                return None
        except json.JSONDecodeError as e:
            log.warning("API JSON error (attempt %d): %s", attempt + 1, e)
        except aiohttp.ClientError as e:
            log.warning("API client error (attempt %d): %s", attempt + 1, e)
        except Exception as e:
            log.warning("API fetch error (attempt %d): %s", attempt + 1, e)
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(0.5 * (attempt + 1))
    _api_ok = False
    return None

def parse_game(raw: dict) -> dict:
    faces = raw.get("facesList") or []
    score = raw.get("score") or sum(faces)
    return {
        "game_num": raw.get("gameNum", "—"),
        "score":    int(score),
        "faces":    [int(f) for f in faces],
        "type":     classify(int(score), [int(f) for f in faces]),
        "key":      raw.get("keyR", ""),
        "md5":      raw.get("md5", ""),
        "time":     datetime.now().strftime("%H:%M:%S"),
        "ts":       datetime.now().isoformat(),
    }

# ══════════════════════════════════════════════════════════════════
#  INITIAL HISTORY LOAD
# ══════════════════════════════════════════════════════════════════
async def load_initial_history(session: aiohttp.ClientSession):
    global _latest, _pred, _prev_pred
    raw_list = await fetch_results(session)
    if not raw_list:
        log.warning("Không thể tải lịch sử ban đầu.")
        return

    games = []
    for item in raw_list:
        try:
            g = parse_game(item)
            games.append(g)
        except Exception as e:
            log.warning("Lỗi parse game: %s", e)

    if not games:
        return

    def extract_num(g):
        try:
            return int(g["game_num"].replace("#", ""))
        except:
            return 0
    games.sort(key=extract_num)

    _history.clear()
    for g in reversed(games):
        _history.appendleft(g)

    _latest = games[-1]
    _pred = engine.predict()
    _prev_pred = {}
    log.info(f"✅ Đã nạp {len(games)} phiên lịch sử từ API.")

# ══════════════════════════════════════════════════════════════════
#  AUTO-UPDATE LOOP
# ══════════════════════════════════════════════════════════════════
async def auto_loop(app: Application):
    global _latest, _pred, _prev_pred

    connector = aiohttp.TCPConnector(ssl=False, limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        await load_initial_history(session)

        log.info("Auto-loop running (interval=%.1fs)", FETCH_INTERVAL)
        while True:
            if _maintenance_state["active"]:
                await asyncio.sleep(1)
                continue

            try:
                await asyncio.sleep(FETCH_INTERVAL)
                raw_list = await fetch_results(session)
                if not raw_list:
                    continue

                latest_raw   = raw_list[0]
                new_game_num = latest_raw.get("gameNum", "")

                if new_game_num == _latest.get("game_num") or not new_game_num:
                    continue

                prev_pred   = _pred.copy()
                prev_latest = _latest.copy()

                new_game = parse_game(latest_raw)
                _history.appendleft(new_game)
                _latest = new_game
                _pred   = engine.predict()
                _prev_pred = prev_pred

                # Cập nhật kết quả cho engine
                if prev_pred.get("pred") in ("TÀI", "XỈU"):
                    pred_bool = prev_pred["pred"] == "TÀI"
                    actual_tai = is_tai(new_game["score"], new_game["faces"])
                    if actual_tai is not None:
                        engine.record_result(pred_bool, actual_tai)
                        # Cập nhật trọng số cho từng thuật toán (nếu có lưu riêng)
                        # Tạm thời không lưu riêng vì không rõ thuật toán nào đúng
                        # Có thể cải thiện sau.

                _record_prediction(prev_pred, new_game)
                await _push_new_message(app, prev_pred, prev_latest, new_game)

            except asyncio.CancelledError:
                log.info("Auto-loop cancelled.")
                return
            except Exception as e:
                log.exception("Auto-loop error: %s", e)
                await asyncio.sleep(2)

# ══════════════════════════════════════════════════════════════════
#  BẢO TRÌ
# ══════════════════════════════════════════════════════════════════
async def start_maintenance(app: Application, minutes: int, reason: str):
    global _maintenance_state
    if _maintenance_state["active"]:
        return

    end_time = datetime.now() + timedelta(minutes=minutes)
    _maintenance_state.update({
        "active": True,
        "end_time": end_time,
        "reason": reason,
    })

    dead = []
    for chat_id in list(_auto_msg.keys()):
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    "🔧 <b>BẢO TRÌ HỆ THỐNG</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏳ Thời gian dự kiến: <b>{minutes} phút</b>\n"
                    f"📋 Lý do: {reason}\n"
                    f"🕐 Kết thúc: <b>{end_time.strftime('%H:%M %d/%m/%Y')}</b>\n\n"
                    "<i>Bot sẽ tự động hoạt động lại sau bảo trì.</i>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            dead.append(chat_id)
    for c in dead:
        _auto_msg.pop(c, None)

    async def _auto_end():
        await asyncio.sleep(minutes * 60)
        await end_maintenance(app)

    if _maintenance_state["task"]:
        _maintenance_state["task"].cancel()
    _maintenance_state["task"] = asyncio.create_task(_auto_end())

    log.info(f"Bảo trì bắt đầu: {minutes} phút, lý do: {reason}")

async def end_maintenance(app: Application):
    global _maintenance_state
    if not _maintenance_state["active"]:
        return

    _maintenance_state["active"] = False
    if _maintenance_state["task"]:
        _maintenance_state["task"].cancel()
        _maintenance_state["task"] = None

    for chat_id in list(_auto_msg.keys()):
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    "✅ <b>BẢO TRÌ HOÀN TẤT</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🚀 Bot đã hoạt động trở lại. Dự đoán sẽ tiếp tục ngay.\n"
                    "<i>Dùng /autosicbo nếu cần khởi động lại.</i>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    log.info("Bảo trì kết thúc.")

# ══════════════════════════════════════════════════════════════════
#  MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════════
_TYPE_EMOJI = {"TÀI": "🔴", "XỈU": "🔵", "🌪 BÃO": "🌪", "⚡ ĐẶC BIỆT": "⚡", "CHỜ": "🟡"}

def _emoji(t: str) -> str:
    return _TYPE_EMOJI.get(t, "🎲")

def _conf_bar(c: int) -> str:
    filled = round(c / 10)
    bar    = "█" * filled + "░" * (10 - filled)
    star   = " ⭐⭐" if c >= 88 else (" ⭐" if c >= 78 else (" 🔥" if c >= 68 else ""))
    return f"{bar} <b>{c}%</b>{star}"

def _build_pred_msg(pred: dict, prev_pred: dict, prev_game: dict, curr_game: dict) -> str:
    now = datetime.now().strftime("%H:%M:%S %d/%m")

    maint_text = ""
    if _maintenance_state["active"]:
        maint_text = (
            "\n⚠️ <b>ĐANG BẢO TRÌ</b> ⚠️\n"
            f"<i>Dự đoán bị tạm dừng, quay lại lúc {_maintenance_state['end_time'].strftime('%H:%M')}</i>\n"
        )

    result_block = ""
    outcome_block = ""
    if curr_game.get("game_num"):
        dice_str = " | ".join(f"[{d}]" for d in curr_game["faces"])
        c_type   = curr_game["type"]
        c_score  = curr_game["score"]

        result_block = (
            "\n\n📜 <b>PHIÊN VỪA KẾT THÚC</b>\n"
            "<blockquote>"
            f"🔢 Phiên   : <b>#{curr_game['game_num']}</b>\n"
            f"🎲 Xúc xắc: <b>{dice_str}</b>\n"
            f"💯 Tổng    : <b>{c_score}</b>\n"
            f"🏷 Kết quả : <b>{_emoji(c_type)} {c_type}</b>\n"
            f"⏰ Lúc     : <b>{curr_game.get('time','—')}</b>"
            "</blockquote>"
        )

        if prev_pred and prev_pred.get("pred") in ("TÀI", "XỈU") and c_type in ("TÀI", "XỈU"):
            p_type = prev_pred["pred"]
            if p_type == c_type:
                vi_match = any(prev_pred.get(vk) == c_score for vk in ("vi1","vi2","vi3"))
                if vi_match:
                    outcome_block = "\n\n💎 <b>═══ CHUẨN VỊ! 🎯 ═══</b>"
                else:
                    outcome_block = "\n\n🏆 <b>═══════ ĐÚNG ✅ ═══════</b>"
            else:
                outcome_block = "\n\n💔 <b>═══════ SAI ❌ ═══════</b>"

    bao_block = ""
    if curr_game.get("faces") == [4, 4, 4]:
        bao_block = "\n\n🌪 <b>⚠️ BÃO 4-4-4 — TẤT CẢ CƯỢC THUA (TRỪ BÃO)! ⚠️</b>"

    p_label = pred.get("pred", "—")
    note = pred.get("note", "")
    if p_label == "CHỜ":
        pred_block = (
            "🟡 <b>TẠM DỪNG DỰ ĐOÁN</b>\n"
            "<blockquote>"
            "Bot phát hiện sai liên tiếp, chờ tín hiệu rõ ràng hơn.\n"
            f"📊 Số thuật toán đã phân tích: <b>{pred.get('algo_count', 0)}</b>\n"
            "</blockquote>"
        )
    else:
        p_vi1   = pred.get("vi1", "—")
        p_vi2   = pred.get("vi2", "—")
        p_vi3   = pred.get("vi3", "—")
        p_conf  = pred.get("confidence", 50)
        p_algos = pred.get("algo_count", 0)

        pred_block = (
            "🔮 <b>DỰ ĐOÁN PHIÊN TIẾP THEO</b>\n"
            "<blockquote>"
            f"🎯 Dự đoán    : <b>{_emoji(p_label)} {p_label}</b>\n"
            f"📊 Độ tin cậy : {_conf_bar(p_conf)}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"3️⃣ <b>VỊ TIN CẬY:</b>\n"
            f"   🥇 Vị 1 : <b>{p_vi1}</b>\n"
            f"   🥈 Vị 2 : <b>{p_vi2}</b>\n"
            f"   🥉 Vị 3 : <b>{p_vi3}</b>\n"
            f"🤖 Thuật toán : <b>{p_algos} layer</b>"
            "</blockquote>"
        )

    return (
        "🎲 <b>SICBO SUNWIN — DỰ ĐOÁN TỰ ĐỘNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + maint_text +
        "\n" + pred_block +
        result_block +
        bao_block +
        outcome_block +
        f"\n\n<i>🔄 {now} | ⚡ Live</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <i>Sicbo Sunwin Bot • </i>"
    )

# ══════════════════════════════════════════════════════════════════
#  RECORD & PUSH
# ══════════════════════════════════════════════════════════════════
def _record_prediction(pred: dict, actual: dict):
    if not pred or not actual.get("game_num"):
        return
    outcome  = None
    vi_hit   = 0
    p_type   = pred.get("pred")
    a_type   = actual["type"]
    a_score  = actual["score"]

    if p_type in ("TÀI", "XỈU") and a_type in ("TÀI", "XỈU"):
        outcome = "✅ ĐÚNG" if p_type == a_type else "❌ SAI"

    for vk in ("vi1", "vi2", "vi3"):
        if pred.get(vk) == a_score:
            vi_hit = 1
            break

    try:
        with _db() as db:
            db.execute(
                """INSERT OR IGNORE INTO predictions
                   (game_num, pred_type, pred_vi1, pred_vi2, pred_vi3, confidence,
                    actual_vi, actual_type, dice, outcome, vi_hit, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    actual["game_num"],
                    p_type,
                    pred.get("vi1"),
                    pred.get("vi2"),
                    pred.get("vi3"),
                    pred.get("confidence"),
                    a_score,
                    a_type,
                    "-".join(map(str, actual["faces"])),
                    outcome,
                    vi_hit,
                    actual["ts"],
                ),
            )
    except Exception as e:
        log.warning("record_prediction: %s", e)

async def _push_new_message(app, prev_pred, prev_game, new_game):
    if not _auto_msg:
        return
    if _maintenance_state["active"]:
        return

    text = _build_pred_msg(_pred, prev_pred, prev_game, new_game)
    dead = []

    for chat_id in list(_auto_msg.keys()):
        try:
            old_id = _auto_msg.get(chat_id)
            if old_id:
                try:
                    await app.bot.delete_message(chat_id=chat_id, message_id=old_id)
                except Exception:
                    pass
            m = await app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            _auto_msg[chat_id] = m.message_id
            await asyncio.sleep(0.05)
        except Forbidden:
            dead.append(chat_id)
        except Exception as e:
            log.warning("push_new_msg %s: %s", chat_id, e)

    for c in dead:
        _auto_msg.pop(c, None)

# ══════════════════════════════════════════════════════════════════
#  KEY SYSTEM
# ══════════════════════════════════════════════════════════════════
def _gen_key(prefix: str = "SUNWIN") -> str:
    body = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    return f"{prefix}-{body[:4]}-{body[4:8]}-{body[8:12]}-{body[12:]}"

def create_key(created_by: int, hours: int = 720, is_trial: bool = False) -> str:
    k   = _gen_key("TRIAL" if is_trial else "SUNWIN")
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute(
            """INSERT INTO activation_keys
               (key, created_by, created_at, expires_at, is_trial)
               VALUES (?,?,?,?,?)""",
            (k, created_by, datetime.now().isoformat(), exp, int(is_trial)),
        )
    return k

async def activate_key(
    user_id: int,
    key: str,
    username: str = "",
    full_name: str = "",
    bot=None,
) -> Tuple[bool, str]:
    with _db() as db:
        row = db.execute(
            "SELECT * FROM activation_keys WHERE key=?", (key,)
        ).fetchone()
        if not row:
            return False, "❌ Key không tồn tại!"
        if row["used_by"] and int(row["used_by"]) != user_id:
            return False, "❌ Key đã được người khác sử dụng!"
        try:
            if datetime.now() > datetime.fromisoformat(row["expires_at"]):
                return False, "❌ Key đã hết hạn!"
        except Exception:
            return False, "❌ Lỗi dữ liệu key."

        db.execute(
            "UPDATE activation_keys SET used_by=?, used_at=? WHERE key=?",
            (user_id, datetime.now().isoformat(), key),
        )
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id, username, added_at, added_by) VALUES (?,?,?,?)",
            (user_id, username, datetime.now().isoformat(), 0),
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id, expires_at) VALUES (?,?)",
            (user_id, row["expires_at"]),
        )
        exp_str = row["expires_at"][:16].replace("T", " ")

    if bot:
        trial_label = "🎁 TRIAL" if row["is_trial"] else "🔑 CHÍNH THỨC"
        admin_text = (
            "🔔 <b>USER MỚI KÍCH HOẠT KEY!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<blockquote>"
            f"👤 Tên    : <b>{full_name or 'N/A'}</b>\n"
            f"🆔 User ID: <code>{user_id}</code>\n"
            f"📱 Username: @{username or 'N/A'}\n"
            f"🔑 Key    : <code>{key}</code>\n"
            f"🏷 Loại   : <b>{trial_label}</b>\n"
            f"📅 Hết hạn: <b>{exp_str}</b>\n"
            f"⏰ Lúc    : <b>{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}</b>"
            "</blockquote>"
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=admin_text,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                log.warning("Admin notify failed %s: %s", admin_id, e)

    return True, f"✅ Kích hoạt thành công!\n📅 Hết hạn: <b>{exp_str}</b>"

# ══════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════
def _check_maintenance(update: Update) -> bool:
    if _maintenance_state["active"]:
        end_str = _maintenance_state["end_time"].strftime("%H:%M %d/%m/%Y") if _maintenance_state["end_time"] else "sắp tới"
        asyncio.create_task(
            update.message.reply_html(
                f"🔧 <b>Bot đang bảo trì!</b>\n"
                f"⏳ Dự kiến hoàn tất lúc <b>{end_str}</b>\n"
                f"📋 Lý do: {_maintenance_state['reason']}\n\n"
                "<i>Vui lòng thử lại sau.</i>"
            )
        )
        return True
    return False

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name or "bạn"
    uid  = update.effective_user.id
    role = "👑 Admin" if is_admin(uid) else ("✅ Thành viên" if is_allowed(uid) else "🔒 Chưa kích hoạt")
    await update.message.reply_html(
        "🎲 <b>SICBO SUNWIN BOT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Chào <b>{name}</b>!  [{role}]\n\n"
        "<blockquote>"
        "🤖 Bot dự đoán Tài/Xỉu tự động\n"
        "🎯 Dự đoán <b>3 vị</b> Nét\n"
        "🧠 Ensemble AI thích ứng\n"
        "⚡ Chỉ BÃO 4-4-4 là đặc biệt\n"
        "</blockquote>\n\n"
        "📋 Dùng /help để xem tất cả lệnh\n"
        "💡 <i>Dùng /trailkey để nhận key trải nghiệm 2 giờ!</i>"
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    base = (
        "📖 <b>HƯỚNG DẪN SỬ DỤNG</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>Người dùng:</b>\n"
        "<blockquote>"
        "/start — Khởi động bot\n"
        "/autosicbo — Bắt đầu nhận dự đoán tự động\n"
        "/stop_auto — Dừng nhận dự đoán\n"
        "/predict — Xem dự đoán phiên tiếp theo (1 lần)\n"
        "/live — Xem kết quả phiên vừa rồi + dự đoán mới nhất\n"
        "/key {key} — Kích hoạt key\n"
        "/trailkey — Nhận key trải nghiệm 2 giờ\n"
        "/info — Thông tin tài khoản & thống kê\n"
        "/listkq — Lịch sử dự đoán 15 phiên\n"
        "/help — Trợ giúp\n"
        "</blockquote>\n"
    )
    admin_extra = ""
    if is_admin(uid):
        admin_extra = (
            "\n👑 <b>Admin:</b>\n"
            "<blockquote>"
            "/add {id} [hours] — Thêm user thủ công\n"
            "/bo {id} — Xoá user\n"
            "/luser — Danh sách tất cả user\n"
            "/tkey [hours] — Tạo key mới\n"
            "/delkey {key} — Xoá key\n"
            "/lkey — Danh sách tất cả key\n"
            "/noti {thông báo} — Broadcast toàn bộ user\n"
            "/stat — Thống kê độ chính xác\n"
            "/baotri {phút} {lý do} — Bảo trì hệ thống\n"
            "/huybaotri — Hủy bảo trì\n"
            "/reset_weights — Reset trọng số AI về mặc định\n"
            "</blockquote>"
        )
    await update.message.reply_html(base + admin_extra)

async def cmd_autosicbo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id

    if _check_maintenance(update):
        return

    if not is_allowed(uid):
        await update.message.reply_html(
            "🔒 <b>Bạn chưa có quyền truy cập!</b>\n\n"
            "<blockquote>"
            "Dùng /trailkey để nhận key trải nghiệm 2 giờ\n"
            "Hoặc liên hệ admin để mua key chính thức"
            "</blockquote>"
        )
        return

    if not _latest:
        m = await update.message.reply_html(
            "⏳ <b>Đang kết nối API...</b>\n"
            "<i>Vui lòng chờ dữ liệu được tải, thử lại sau ít giây.</i>"
        )
        _auto_msg[chat_id] = m.message_id
        return

    text = _build_pred_msg(_pred, {}, {}, _latest)
    try:
        old_id = _auto_msg.get(chat_id)
        if old_id:
            try:
                await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
            except Exception:
                pass
        m = await update.message.reply_html(text)
        _auto_msg[chat_id] = m.message_id
        try:
            await update.message.delete()
        except Exception:
            pass
    except Exception as e:
        log.warning("autosicbo send: %s", e)
        m = await update.message.reply_html(text)
        _auto_msg[chat_id] = m.message_id

async def cmd_stop_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in _auto_msg:
        old_id = _auto_msg.pop(chat_id)
        if old_id:
            try:
                await ctx.bot.delete_message(chat_id=chat_id, message_id=old_id)
            except Exception:
                pass
        await update.message.reply_html("⏹ <b>Đã dừng nhận dự đoán tự động.</b>")
    else:
        await update.message.reply_html("ℹ️ Không có phiên auto nào đang chạy.")

async def cmd_predict(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _check_maintenance(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    if not _latest:
        await update.message.reply_html("⏳ Chưa có dữ liệu. Vui lòng thử lại sau.")
        return
    text = _build_pred_msg(_pred, _prev_pred, _latest, _latest)
    await update.message.reply_html(text)

async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _check_maintenance(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    if not _latest:
        await update.message.reply_html("⏳ Chưa có dữ liệu.")
        return
    text = _build_pred_msg(_pred, _prev_pred, _latest, _latest)
    await update.message.reply_html(text)

async def cmd_trailkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or ""
    name     = update.effective_user.full_name or str(uid)

    if _check_maintenance(update):
        return

    with _db() as db:
        used = db.execute(
            "SELECT 1 FROM activation_keys WHERE used_by=? AND is_trial=1", (uid,)
        ).fetchone()
    if used:
        await update.message.reply_html(
            "⚠️ <b>Bạn đã sử dụng key trải nghiệm rồi!</b>\n"
            "Liên hệ admin để nâng cấp tài khoản."
        )
        return

    k  = create_key(created_by=0, hours=2, is_trial=True)
    ok, msg = await activate_key(
        uid, k,
        username=username,
        full_name=name,
        bot=ctx.bot,
    )
    exp_time = (datetime.now() + timedelta(hours=2)).strftime("%H:%M %d/%m/%Y")
    await update.message.reply_html(
        "🎁 <b>KEY TRẢI NGHIỆM</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"🔑 Key  : <code>{k}</code>\n"
        f"⏳ Hạn  : 2 giờ (đến {exp_time})\n"
        f"✅ Trạng thái: Đã kích hoạt tự động!\n"
        "</blockquote>\n\n"
        "🚀 Dùng /autosicbo để bắt đầu dự đoán!"
    )

async def cmd_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    username = update.effective_user.username or ""
    name     = update.effective_user.full_name or str(uid)

    if _check_maintenance(update):
        return

    if not ctx.args:
        await update.message.reply_html(
            "❌ Thiếu key!\n"
            "Cách dùng: <code>/key YOUR_KEY_HERE</code>"
        )
        return

    key = ctx.args[0].strip()
    ok, msg = await activate_key(
        uid, key,
        username=username,
        full_name=name,
        bot=ctx.bot,
    )
    footer = "\n🚀 Dùng /autosicbo để bắt đầu!" if ok else "\n💬 Liên hệ admin để được hỗ trợ."
    await update.message.reply_html(
        f"🔑 <b>KÍCH HOẠT KEY</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<blockquote>{msg}</blockquote>"
        + footer
    )

async def cmd_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = update.effective_user.full_name or str(uid)

    if is_admin(uid):
        role, exp = "👑 Admin", "♾ Vĩnh viễn"
    elif is_allowed(uid):
        with _db() as db:
            e = db.execute(
                "SELECT expires_at FROM user_expiry WHERE user_id=?", (uid,)
            ).fetchone()
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
        correct = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%'"
        ).fetchone()[0]
        vi_hits = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE vi_hit=1"
        ).fetchone()[0]

    acc    = f"{correct / total * 100:.1f}%" if total else "—"
    vi_acc = f"{vi_hits / total * 100:.1f}%" if total else "—"
    api_status = "🟢 Online" if _api_ok else "🔴 Offline"
    maint_status = "🔧 Đang bảo trì" if _maintenance_state["active"] else "✅ Bình thường"

    await update.message.reply_html(
        f"👤 <b>THÔNG TIN TÀI KHOẢN</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<blockquote>"
        f"🪪 Tên     : <b>{name}</b>\n"
        f"🆔 ID      : <code>{uid}</code>\n"
        f"🏷 Vai trò : <b>{role}</b>\n"
        f"📅 Hết hạn : <b>{exp}</b>\n"
        f"</blockquote>\n"
        f"📊 <b>Thống kê bot:</b>\n"
        f"<blockquote>"
        f"Tổng dự đoán : <b>{total}</b>\n"
        f"✅ Đúng loại : <b>{correct}</b> ({acc})\n"
        f"🎯 Trúng vị  : <b>{vi_hits}</b> ({vi_acc})\n"
        f"API Status   : {api_status}\n"
        f"Trạng thái   : {maint_status}\n"
        f"Lịch sử RAM  : <b>{len(_history)}</b> phiên\n"
        f"</blockquote>"
    )

async def cmd_listkq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _check_maintenance(update):
        return
    if not is_allowed(uid):
        await update.message.reply_html("🔒 Bạn chưa có quyền truy cập!")
        return
    with _db() as db:
        rows = db.execute(
            "SELECT * FROM predictions ORDER BY id DESC LIMIT 15"
        ).fetchall()
    if not rows:
        await update.message.reply_html("📭 Chưa có lịch sử dự đoán.")
        return

    correct = sum(1 for r in rows if r["outcome"] and "ĐÚNG" in r["outcome"])
    vi_hit  = sum(1 for r in rows if r["vi_hit"])
    acc_str = f"{correct}/{len(rows)} ({correct/len(rows)*100:.0f}%)"

    lines = [
        f"📜 <b>LỊCH SỬ DỰ ĐOÁN</b>",
        f"<i>15 phiên gần nhất • Đúng loại: {acc_str} • Trúng vị: {vi_hit}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in rows:
        outcome   = r["outcome"] or "⏳"
        vi_marker = " 🎯" if r["vi_hit"] else ""
        lines.append(
            f"<blockquote>"
            f"📌 <b>#{r['game_num']}</b> {outcome}{vi_marker}\n"
            f"🎯 Dự đoán: {r['pred_type']} | Vị: {r['pred_vi1']} / {r['pred_vi2']} / {r['pred_vi3']}\n"
            f"🎲 Kết quả: {r['dice'] or '—'} = <b>{r['actual_vi']}</b> {r['actual_type'] or ''}"
            f"</blockquote>"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n<i>...</i>"
    await update.message.reply_html(text)

# ══════════════════════════════════════════════════════════════════
#  ADMIN COMMANDS
# ══════════════════════════════════════════════════════════════════
def _admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_html("⛔ Chỉ admin mới dùng được lệnh này!")
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper

@_admin_only
async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/add {user_id} [hours]</code>")
        return
    try:
        tid   = int(ctx.args[0])
        hours = int(ctx.args[1]) if len(ctx.args) > 1 else 720
    except ValueError:
        await update.message.reply_html("❌ ID hoặc giờ không hợp lệ!")
        return
    exp = (datetime.now() + timedelta(hours=hours)).isoformat()
    with _db() as db:
        db.execute(
            "INSERT OR IGNORE INTO allowed_users (user_id, added_at, added_by) VALUES (?,?,?)",
            (tid, datetime.now().isoformat(), update.effective_user.id),
        )
        db.execute(
            "INSERT OR REPLACE INTO user_expiry (user_id, expires_at) VALUES (?,?)",
            (tid, exp),
        )
    await update.message.reply_html(
        f"✅ Đã thêm user <code>{tid}</code>\n"
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
        db.execute("DELETE FROM user_expiry WHERE user_id=?", (tid,))
    _auto_msg.pop(tid, None)
    await update.message.reply_html(f"✅ Đã xoá user <code>{tid}</code>!")

@_admin_only
async def cmd_luser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    with _db() as db:
        rows = db.execute(
            """SELECT u.user_id, u.username, u.added_at, e.expires_at
               FROM allowed_users u
               LEFT JOIN user_expiry e ON u.user_id = e.user_id
               ORDER BY u.added_at DESC"""
        ).fetchall()
    if not rows:
        await update.message.reply_html("📭 Chưa có user nào.")
        return
    lines = [f"👥 <b>DANH SÁCH USER ({len(rows)})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for r in rows:
        exp  = r["expires_at"][:16].replace("T"," ") if r["expires_at"] else "∞"
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
    k   = create_key(update.effective_user.id, hours=hours)
    exp = (datetime.now() + timedelta(hours=hours)).strftime("%H:%M %d/%m/%Y")
    await update.message.reply_html(
        f"🔑 <b>KEY MỚI TẠO</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<blockquote>"
        f"Key  : <code>{k}</code>\n"
        f"Hạn  : {exp} ({hours}h)\n"
        f"Dùng : 1 lần\n"
        f"</blockquote>"
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
        exp   = r["expires_at"][:16].replace("T", " ")
        lines.append(
            f"<blockquote>"
            f"🔑 <code>{r['key']}</code>{trial}\n"
            f"📅 {exp} | {used}"
            f"</blockquote>"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n<i>...và nhiều hơn nữa</i>"
    await update.message.reply_html(text)

@_admin_only
async def cmd_noti(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html("Dùng: <code>/noti {thông báo của bạn}</code>")
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
        f"📢 <b>Gửi thông báo hoàn tất</b>\n"
        f"<blockquote>"
        f"✅ Thành công : {sent}\n"
        f"❌ Thất bại  : {fail}\n"
        f"📊 Tổng      : {sent + fail}"
        f"</blockquote>"
    )

@_admin_only
async def cmd_stat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    with _db() as db:
        total   = db.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        correct = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%'"
        ).fetchone()[0]
        wrong   = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%SAI%'"
        ).fetchone()[0]
        vi_hits = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE vi_hit=1"
        ).fetchone()[0]
        last7d  = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE created_at >= datetime('now','-7 days')"
        ).fetchone()[0]
        correct7 = db.execute(
            "SELECT COUNT(*) FROM predictions WHERE outcome LIKE '%ĐÚNG%' AND created_at >= datetime('now','-7 days')"
        ).fetchone()[0]

    acc    = f"{correct / total * 100:.1f}%" if total else "—"
    vi_acc = f"{vi_hits / total * 100:.1f}%" if total else "—"
    acc7   = f"{correct7 / last7d * 100:.1f}%" if last7d else "—"

    await update.message.reply_html(
        "📊 <b>THỐNG KÊ ĐỘ CHÍNH XÁC</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<blockquote>"
        f"📌 Tổng dự đoán  : <b>{total}</b>\n"
        f"✅ Đúng loại     : <b>{correct}</b> ({acc})\n"
        f"❌ Sai           : <b>{wrong}</b>\n"
        f"🎯 Trúng vị      : <b>{vi_hits}</b> ({vi_acc})\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📅 7 ngày qua    : <b>{last7d}</b> phiên\n"
        f"✅ Đúng (7 ngày) : <b>{correct7}</b> ({acc7})\n"
        f"🤖 History   : <b>{len(_history)}</b> phiên\n"
        f"🌐 API Status    : {'🟢 Online' if _api_ok else '🔴 Offline'}\n"
        f"🔧 Bảo trì       : {'Đang bảo trì' if _maintenance_state['active'] else 'Bình thường'}\n"
        f"🧠 AI Mode       : {'An toàn (skip)' if engine.consecutive_losses >= 3 else 'Bình thường'}\n"
        "</blockquote>"
    )

@_admin_only
async def cmd_baotri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_html(
            "🔧 <b>BẢO TRÌ HỆ THỐNG</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Dùng: <code>/baotri &lt;số phút&gt; &lt;lý do&gt;</code>\n"
            "Ví dụ: <code>/baotri 30 Nâng cấp server</code>"
        )
        return
    try:
        minutes = int(ctx.args[0])
    except ValueError:
        await update.message.reply_html("❌ Số phút không hợp lệ!")
        return
    reason = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else "Bảo trì định kỳ"

    if _maintenance_state["active"]:
        await update.message.reply_html("⚠️ Bot đang trong quá trình bảo trì rồi!")
        return

    await start_maintenance(ctx.application, minutes, reason)
    await update.message.reply_html(
        f"🔧 <b>ĐÃ KÍCH HOẠT BẢO TRÌ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Thời gian: <b>{minutes} phút</b>\n"
        f"📋 Lý do: {reason}\n"
        f"🕐 Kết thúc: <b>{_maintenance_state['end_time'].strftime('%H:%M %d/%m/%Y')}</b>\n\n"
        "<i>Bot sẽ tự động hoạt động lại. Dùng /huybaotri để hủy.</i>"
    )

@_admin_only
async def cmd_huybaotri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _maintenance_state["active"]:
        await update.message.reply_html("ℹ️ Hiện không có bảo trì nào đang diễn ra.")
        return
    await end_maintenance(ctx.application)
    await update.message.reply_html("✅ <b>Đã hủy bảo trì!</b> Bot hoạt động trở lại.")

@_admin_only
async def cmd_reset_weights(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Reset trọng số AI về mặc định."""
    global engine
    engine.algo_weights = {}
    engine.load_weights()
    with _db() as db:
        db.execute("DELETE FROM algo_weights")
    await update.message.reply_html("🔄 <b>Đã reset trọng số AI về mặc định.</b>")

# ══════════════════════════════════════════════════════════════════
#  MAINTENANCE TASKS
# ══════════════════════════════════════════════════════════════════
async def cleanup_expired_users():
    with _db() as db:
        now = datetime.now().isoformat()
        expired = db.execute(
            "SELECT user_id FROM user_expiry WHERE expires_at < ?", (now,)
        ).fetchall()
        for row in expired:
            uid = row["user_id"]
            db.execute("DELETE FROM allowed_users WHERE user_id=?", (uid,))
            db.execute("DELETE FROM user_expiry WHERE user_id=?", (uid,))
            _auto_msg.pop(uid, None)
        db.execute(
            "DELETE FROM activation_keys WHERE used_by IS NOT NULL AND expires_at < ?",
            (now,)
        )
        db.commit()

async def maintenance_loop(app: Application):
    while True:
        await asyncio.sleep(1800)
        await cleanup_expired_users()

# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
async def post_init(app: Application):
    asyncio.create_task(auto_loop(app))
    asyncio.create_task(maintenance_loop(app))
    log.info("✅ Auto-loop & maintenance tasks created.")

def main():
    init_db()
    # Load weights cho engine
    engine.load_weights()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    handlers = [
        CommandHandler("start",      cmd_start),
        CommandHandler("help",       cmd_help),
        CommandHandler("autosicbo",  cmd_autosicbo),
        CommandHandler("stop_auto",  cmd_stop_auto),
        CommandHandler("predict",    cmd_predict),
        CommandHandler("live",       cmd_live),
        CommandHandler("trailkey",   cmd_trailkey),
        CommandHandler("key",        cmd_key),
        CommandHandler("info",       cmd_info),
        CommandHandler("listkq",     cmd_listkq),
        CommandHandler("add",        cmd_add),
        CommandHandler("bo",         cmd_bo),
        CommandHandler("luser",      cmd_luser),
        CommandHandler("tkey",       cmd_tkey),
        CommandHandler("delkey",     cmd_delkey),
        CommandHandler("lkey",       cmd_lkey),
        CommandHandler("noti",       cmd_noti),
        CommandHandler("stat",       cmd_stat),
        CommandHandler("baotri",     cmd_baotri),
        CommandHandler("huybaotri",  cmd_huybaotri),
        CommandHandler("reset_weights", cmd_reset_weights),
    ]
    for h in handlers:
        app.add_handler(h)

    log.info("🎲 Sicbo Sunwin Bot Ultra v5.0 starting…")
    app.run_polling(drop_pending_updates=True, poll_interval=1)

if __name__ == "__main__":
    main()
