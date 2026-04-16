import os
import io
import json
import logging
import asyncio
import re
import sqlite3
import shutil
import tempfile
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAnimation,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode, ChatAction
from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest,
    DeletePhotosRequest,
)
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

# ╔══════════════════════════════════════════════════════════════╗
# ║                    CONFIGURATION                             ║
# ╚══════════════════════════════════════════════════════════════╝

BOT_TOKEN = "YOUR_BOT_TOKEN"
API_ID = 123456
API_HASH = "YOUR_API_HASH"
ADMIN_IDS = [123456789]
SUPPORT_USERNAME = "@itsukiarai"
SUPPORT_URL = "https://t.me/itsukiarai"
BOT_NAME = "SkullAutomation"
BOT_VERSION = "3.0.0"
DB_FILE = "skull_automation.db"
BACKUP_DIR = "backups"
MEDIA_DIR = "media_cache"
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024

# Plan Limits Configuration
PLAN_CONFIG = {
    "free": {
        "name": "🆓 Free",
        "max_keywords": 10,
        "max_filters": 5,
        "max_forwards": 2,
        "max_scheduled": 5,
        "max_blocked_words": 15,
        "max_whitelist": 10,
        "max_templates": 3,
        "media_in_replies": False,
        "auto_react": False,
        "working_hours": False,
        "recurring_schedule": False,
        "regex_keywords": False,
        "multi_media": False,
        "custom_commands": False,
        "priority_support": False,
        "backup_export": False,
        "advanced_stats": False,
        "broadcast_receive": True,
    },
    "premium": {
        "name": "⭐ Premium",
        "max_keywords": 50,
        "max_filters": 25,
        "max_forwards": 10,
        "max_scheduled": 25,
        "max_blocked_words": 50,
        "max_whitelist": 50,
        "max_templates": 15,
        "media_in_replies": True,
        "auto_react": True,
        "working_hours": True,
        "recurring_schedule": True,
        "regex_keywords": True,
        "multi_media": False,
        "custom_commands": True,
        "priority_support": True,
        "backup_export": True,
        "advanced_stats": True,
        "broadcast_receive": True,
    },
    "vip": {
        "name": "👑 VIP",
        "max_keywords": 200,
        "max_filters": 100,
        "max_forwards": 30,
        "max_scheduled": 100,
        "max_blocked_words": 200,
        "max_whitelist": 200,
        "max_templates": 50,
        "media_in_replies": True,
        "auto_react": True,
        "working_hours": True,
        "recurring_schedule": True,
        "regex_keywords": True,
        "multi_media": True,
        "custom_commands": True,
        "priority_support": True,
        "backup_export": True,
        "advanced_stats": True,
        "broadcast_receive": True,
    },
}

# Reaction Emoji Options
REACTION_EMOJIS = [
    "👍", "❤️", "🔥", "🥰", "👏", "😁", "🤔", "🤯",
    "😱", "🎉", "⚡", "🏆", "💯", "😍", "🤗", "🫡",
    "👎", "😢", "💔", "🤮", "💩", "🤡", "👀", "🦴",
]

# Days of week for working hours
DAYS_OF_WEEK = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

# ╔══════════════════════════════════════════════════════════════╗
# ║                  CONVERSATION STATES                         ║
# ╚══════════════════════════════════════════════════════════════╝

(
    ST_PHONE, ST_OTP, ST_2FA,
    ST_WELCOME_MSG, ST_WELCOME_MEDIA,
    ST_AWAY_MSG, ST_AWAY_MEDIA,
    ST_KW_TRIGGER, ST_KW_RESPONSE, ST_KW_MEDIA,
    ST_FILTER_NAME, ST_FILTER_RESP, ST_FILTER_MEDIA,
    ST_BIO, ST_NAME, ST_USERNAME,
    ST_PROFILE_PIC,
    ST_SCHED_TARGET, ST_SCHED_MSG, ST_SCHED_TIME, ST_SCHED_MEDIA,
    ST_FWD_SOURCE, ST_FWD_DEST,
    ST_BLOCK_WORD, ST_WHITELIST,
    ST_PM_MSG, ST_PM_MEDIA,
    ST_SPAM_LIMIT, ST_SPAM_MSG,
    ST_ADMIN_BROADCAST, ST_ADMIN_BROADCAST_MEDIA,
    ST_ADMIN_SEARCH,
    ST_ADMIN_UPLOAD_DB,
    ST_TEMPLATE_NAME, ST_TEMPLATE_CONTENT, ST_TEMPLATE_MEDIA,
    ST_WORKING_HOURS,
    ST_REACT_EMOJI,
    ST_ADMIN_SET_PLAN, ST_ADMIN_SET_PLAN_DAYS,
    ST_ADMIN_BAN_REASON,
    ST_EXPORT_CONFIRM,
    ST_IMPORT_FILE,
    ST_CUSTOM_CMD_NAME, ST_CUSTOM_CMD_RESP, ST_CUSTOM_CMD_MEDIA,
    ST_ADMIN_ANNOUNCE,
    ST_FEEDBACK,
    ST_NOTE_TITLE, ST_NOTE_CONTENT, ST_NOTE_MEDIA,
    ST_AUTO_REPLY_DELAY,
) = range(52)

# ╔══════════════════════════════════════════════════════════════╗
# ║                      LOGGING                                ║
# ╚══════════════════════════════════════════════════════════════╝

os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{BOT_NAME.lower()}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(BOT_NAME)

# ╔══════════════════════════════════════════════════════════════╗
# ║                      DATABASE                               ║
# ╚══════════════════════════════════════════════════════════════╝


class Database:
    """SQLite database manager with full feature support."""

    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init()

    def conn(self):
        c = sqlite3.connect(self.db_file, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        return c

    def _init(self):
        with self.conn() as cx:
            cx.executescript("""
            -- Users table
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT,
                first_name    TEXT,
                last_name     TEXT,
                phone         TEXT,
                session_str   TEXT,
                joined_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                last_active   TEXT DEFAULT CURRENT_TIMESTAMP,
                is_banned     INTEGER DEFAULT 0,
                ban_reason    TEXT,
                plan          TEXT DEFAULT 'free',
                plan_until    TEXT,
                referral_code TEXT,
                referred_by   INTEGER,
                language      TEXT DEFAULT 'en',
                timezone      TEXT DEFAULT 'UTC'
            );

            -- Settings table
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER,
                key     TEXT,
                value   TEXT,
                PRIMARY KEY (user_id, key)
            );

            -- Keywords with full media support
            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                trigger_text TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                match_type  TEXT DEFAULT 'contains',
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                reply_delay INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Filters with media
            CREATE TABLE IF NOT EXISTS filters (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                name        TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Blocked words
            CREATE TABLE IF NOT EXISTS blocked_words (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                word    TEXT,
                action  TEXT DEFAULT 'warn',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Whitelist
            CREATE TABLE IF NOT EXISTS whitelist (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                target_user TEXT,
                target_name TEXT,
                added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Scheduled messages with media
            CREATE TABLE IF NOT EXISTS scheduled (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                target       TEXT,
                message      TEXT,
                media_file_id TEXT,
                media_type   TEXT,
                send_at      TEXT,
                is_sent      INTEGER DEFAULT 0,
                sent_at      TEXT,
                recurring    INTEGER DEFAULT 0,
                interval_hr  INTEGER DEFAULT 0,
                max_repeats  INTEGER DEFAULT 0,
                repeat_count INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Auto-forward rules
            CREATE TABLE IF NOT EXISTS auto_forward (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                source   TEXT,
                dest     TEXT,
                active   INTEGER DEFAULT 1,
                filter_text TEXT,
                forward_media INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- PM permit
            CREATE TABLE IF NOT EXISTS pm_permit (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                approved     INTEGER,
                approved_name TEXT,
                approved_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                auto_approved INTEGER DEFAULT 0,
                UNIQUE(user_id, approved)
            );

            -- Stats
            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER,
                key     TEXT,
                value   INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, key)
            );

            -- Daily stats for analytics
            CREATE TABLE IF NOT EXISTS daily_stats (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date    TEXT,
                key     TEXT,
                value   INTEGER DEFAULT 0,
                UNIQUE(user_id, date, key)
            );

            -- Logs
            CREATE TABLE IF NOT EXISTS logs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                action    TEXT,
                detail    TEXT,
                category  TEXT DEFAULT 'general',
                ts        TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Spam tracking
            CREATE TABLE IF NOT EXISTS spam_track (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                sender   INTEGER,
                count    INTEGER DEFAULT 0,
                last_ts  TEXT,
                blocked  INTEGER DEFAULT 0,
                UNIQUE(user_id, sender)
            );

            -- Templates
            CREATE TABLE IF NOT EXISTS templates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                name        TEXT,
                content     TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                category    TEXT DEFAULT 'general',
                is_global   INTEGER DEFAULT 0,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Working hours
            CREATE TABLE IF NOT EXISTS working_hours (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                day      INTEGER,
                start_hr INTEGER DEFAULT 9,
                start_min INTEGER DEFAULT 0,
                end_hr   INTEGER DEFAULT 17,
                end_min  INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                UNIQUE(user_id, day)
            );

            -- Custom commands
            CREATE TABLE IF NOT EXISTS custom_commands (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                command     TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Notes / saved messages
            CREATE TABLE IF NOT EXISTS notes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                title       TEXT,
                content     TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_pinned   INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Media attachments (for multi-media support)
            CREATE TABLE IF NOT EXISTS media_attachments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                parent_type TEXT,
                parent_id   INTEGER,
                file_id     TEXT,
                media_type  TEXT,
                caption     TEXT,
                position    INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Feedback
            CREATE TABLE IF NOT EXISTS feedback (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                message   TEXT,
                status    TEXT DEFAULT 'pending',
                admin_reply TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                replied_at TEXT
            );

            -- Plan history
            CREATE TABLE IF NOT EXISTS plan_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                old_plan  TEXT,
                new_plan  TEXT,
                days      INTEGER,
                changed_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Announcements
            CREATE TABLE IF NOT EXISTS announcements (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                title     TEXT,
                content   TEXT,
                media_file_id TEXT,
                media_type TEXT,
                target    TEXT DEFAULT 'all',
                created_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_keywords_user ON keywords(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_filters_user ON filters(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_scheduled_pending ON scheduled(is_sent, send_at);
            CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id, ts);
            CREATE INDEX IF NOT EXISTS idx_spam_track ON spam_track(user_id, sender);
            CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(user_id, date);
            """)

    # ═══════════════════ USER METHODS ═══════════════════

    def add_user(self, uid, username=None, first_name=None, last_name=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO users(user_id, username, first_name, last_name, joined_at, last_active)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username=COALESCE(excluded.username, users.username),
                       first_name=COALESCE(excluded.first_name, users.first_name),
                       last_name=COALESCE(excluded.last_name, users.last_name),
                       last_active=excluded.last_active""",
                (uid, username, first_name, last_name,
                 datetime.now().isoformat(), datetime.now().isoformat()),
            )

    def get_user(self, uid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE user_id=?", (uid,)
            ).fetchone()

    def all_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users ORDER BY last_active DESC"
            ).fetchall()

    def users_with_sessions(self):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM users
                   WHERE session_str IS NOT NULL
                   AND session_str != ''
                   AND is_banned=0"""
            ).fetchall()

    def users_by_plan(self, plan: str):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan=? ORDER BY last_active DESC",
                (plan,),
            ).fetchall()

    def active_users(self, days: int = 7):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE last_active > ? ORDER BY last_active DESC",
                (cutoff,),
            ).fetchall()

    def update_user(self, uid, **kw):
        if not kw:
            return
        allowed = {
            "username", "first_name", "last_name", "phone",
            "session_str", "is_banned", "ban_reason", "plan",
            "plan_until", "referral_code", "referred_by",
            "language", "timezone", "last_active",
        }
        sets = []
        vals = []
        for k, v in kw.items():
            if k in allowed:
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        vals.append(uid)
        with self.conn() as cx:
            cx.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE user_id=?",
                vals,
            )

    def touch_user(self, uid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE users SET last_active=? WHERE user_id=?",
                (datetime.now().isoformat(), uid),
            )

    def delete_user_data(self, uid):
        tables = [
            "settings", "keywords", "filters", "blocked_words",
            "whitelist", "scheduled", "auto_forward", "pm_permit",
            "stats", "daily_stats", "logs", "spam_track", "templates",
            "working_hours", "custom_commands", "notes",
            "media_attachments", "feedback",
        ]
        with self.conn() as cx:
            for t in tables:
                cx.execute(f"DELETE FROM {t} WHERE user_id=?", (uid,))

    def full_delete_user(self, uid):
        self.delete_user_data(uid)
        with self.conn() as cx:
            cx.execute("DELETE FROM users WHERE user_id=?", (uid,))

    # ═══════════════════ BAN METHODS ═══════════════════

    def ban_user(self, uid, reason=""):
        self.update_user(uid, is_banned=1, ban_reason=reason)
        self.log(uid, "banned", reason, "admin")

    def unban_user(self, uid):
        self.update_user(uid, is_banned=0, ban_reason=None)
        self.log(uid, "unbanned", "", "admin")

    def is_banned(self, uid):
        u = self.get_user(uid)
        return bool(u and u["is_banned"])

    def banned_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE is_banned=1"
            ).fetchall()

    # ═══════════════════ PLAN METHODS ═══════════════════

    def get_plan(self, uid) -> str:
        u = self.get_user(uid)
        if not u:
            return "free"
        plan = u["plan"] or "free"
        plan_until = u["plan_until"]
        if plan != "free" and plan_until:
            try:
                if datetime.fromisoformat(plan_until) < datetime.now():
                    self.set_plan(uid, "free", admin_id=0, auto=True)
                    return "free"
            except (ValueError, TypeError):
                self.set_plan(uid, "free", admin_id=0, auto=True)
                return "free"
        return plan

    def get_plan_config(self, uid) -> dict:
        plan = self.get_plan(uid)
        return PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])

    def get_plan_expiry(self, uid) -> Optional[str]:
        u = self.get_user(uid)
        if u and u["plan_until"]:
            return u["plan_until"]
        return None

    def set_plan(self, uid, plan: str, days: int = 0,
                 admin_id: int = 0, auto: bool = False):
        old_plan = self.get_plan(uid)
        expiry = None
        if plan != "free" and days > 0:
            expiry = (datetime.now() + timedelta(days=days)).isoformat()

        self.update_user(uid, plan=plan, plan_until=expiry)

        with self.conn() as cx:
            cx.execute(
                """INSERT INTO plan_history
                   (user_id, old_plan, new_plan, days, changed_by)
                   VALUES (?,?,?,?,?)""",
                (uid, old_plan, plan, days, admin_id),
            )

        if not auto:
            self.log(
                uid, "plan_change",
                f"{old_plan} → {plan} ({days}d) by {admin_id}",
                "plan",
            )

    def plan_check(self, uid, feature: str) -> bool:
        config = self.get_plan_config(uid)
        return config.get(feature, False)

    def plan_limit(self, uid, feature: str) -> int:
        config = self.get_plan_config(uid)
        return config.get(feature, 0)

    def premium_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan IN ('premium', 'vip')"
            ).fetchall()

    def vip_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan='vip'"
            ).fetchall()

    def expiring_plans(self, days: int = 3):
        cutoff = (datetime.now() + timedelta(days=days)).isoformat()
        now = datetime.now().isoformat()
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM users
                   WHERE plan != 'free'
                   AND plan_until IS NOT NULL
                   AND plan_until BETWEEN ? AND ?""",
                (now, cutoff),
            ).fetchall()

    def plan_history(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM plan_history
                   WHERE user_id=?
                   ORDER BY created_at DESC LIMIT 20""",
                (uid,),
            ).fetchall()

    # ═══════════════════ SESSION METHODS ═══════════════════

    def save_session(self, uid, phone, session_str):
        with self.conn() as cx:
            cx.execute(
                "UPDATE users SET phone=?, session_str=? WHERE user_id=?",
                (phone, session_str, uid),
            )

    def get_session(self, uid):
        u = self.get_user(uid)
        return u["session_str"] if u else None

    def remove_session(self, uid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE users
                   SET session_str=NULL, phone=NULL
                   WHERE user_id=?""",
                (uid,),
            )

    # ═══════════════════ SETTINGS METHODS ═══════════════════

    def set_setting(self, uid, key, value):
        with self.conn() as cx:
            cx.execute(
                """INSERT OR REPLACE INTO settings(user_id, key, value)
                   VALUES(?,?,?)""",
                (uid, key, str(value)),
            )

    def get_setting(self, uid, key, default=None):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT value FROM settings WHERE user_id=? AND key=?",
                (uid, key),
            ).fetchone()
        return r["value"] if r else default

    def all_settings(self, uid):
        with self.conn() as cx:
            rows = cx.execute(
                "SELECT key, value FROM settings WHERE user_id=?",
                (uid,),
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def del_setting(self, uid, key):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM settings WHERE user_id=? AND key=?",
                (uid, key),
            )

    def bulk_set_settings(self, uid, settings_dict: dict):
        with self.conn() as cx:
            for key, value in settings_dict.items():
                cx.execute(
                    """INSERT OR REPLACE INTO settings(user_id, key, value)
                       VALUES(?,?,?)""",
                    (uid, key, str(value)),
                )

    # ═══════════════════ KEYWORD METHODS ═══════════════════

    def add_keyword(self, uid, trigger, response, match_type="contains",
                    media_file_id=None, media_type=None, reply_delay=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO keywords
                   (user_id, trigger_text, response, match_type,
                    media_file_id, media_type, reply_delay)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, trigger.lower().strip(), response, match_type,
                 media_file_id, media_type, reply_delay),
            )
            return cx.execute("SELECT last_insert_rowid()").fetchone()[0]

    def update_keyword(self, uid, kid, **kw):
        allowed = {
            "trigger_text", "response", "match_type",
            "media_file_id", "media_type", "is_active",
            "reply_delay",
        }
        sets = []
        vals = []
        for k, v in kw.items():
            if k in allowed:
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        sets.append("updated_at=?")
        vals.append(datetime.now().isoformat())
        vals.extend([kid, uid])
        with self.conn() as cx:
            cx.execute(
                f"UPDATE keywords SET {', '.join(sets)} WHERE id=? AND user_id=?",
                vals,
            )

    def get_keywords(self, uid, active_only=True):
        q = "SELECT * FROM keywords WHERE user_id=?"
        if active_only:
            q += " AND is_active=1"
        q += " ORDER BY id DESC"
        with self.conn() as cx:
            return cx.execute(q, (uid,)).fetchall()

    def get_keyword(self, uid, kid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            ).fetchone()

    def del_keyword(self, uid, kid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            )

    def toggle_keyword(self, uid, kid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT is_active FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            ).fetchone()
            if r:
                new_val = 0 if r["is_active"] else 1
                cx.execute(
                    """UPDATE keywords SET is_active=?, updated_at=?
                       WHERE id=? AND user_id=?""",
                    (new_val, datetime.now().isoformat(), kid, uid),
                )

    def clear_keywords(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM keywords WHERE user_id=?", (uid,))

    def kw_inc(self, kid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE keywords SET used_count=used_count+1 WHERE id=?",
                (kid,),
            )

    def keyword_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM keywords WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ FILTER METHODS ═══════════════════

    def add_filter(self, uid, name, response,
                   media_file_id=None, media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO filters
                   (user_id, name, response, media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, name.lower().strip(), response,
                 media_file_id, media_type),
            )
            return cx.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_filters(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM filters
                   WHERE user_id=? AND is_active=1
                   ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def get_filter(self, uid, fid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM filters WHERE id=? AND user_id=?",
                (fid, uid),
            ).fetchone()

    def del_filter(self, uid, fid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM filters WHERE id=? AND user_id=?",
                (fid, uid),
            )

    def clear_filters(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM filters WHERE user_id=?", (uid,))

    def filter_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM filters WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def filter_inc(self, fid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE filters SET used_count=used_count+1 WHERE id=?",
                (fid,),
            )

    # ═══════════════════ BLOCKED WORDS ═══════════════════

    def add_blocked(self, uid, word, action="warn"):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO blocked_words(user_id, word, action)
                   VALUES(?,?,?)""",
                (uid, word.lower().strip(), action),
            )

    def get_blocked(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM blocked_words
                   WHERE user_id=? ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_blocked(self, uid, bid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM blocked_words WHERE id=? AND user_id=?",
                (bid, uid),
            )

    def clear_blocked(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM blocked_words WHERE user_id=?", (uid,))

    def blocked_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM blocked_words WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ WHITELIST ═══════════════════

    def add_whitelist(self, uid, target, target_name=""):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO whitelist(user_id, target_user, target_name)
                   VALUES(?,?,?)""",
                (uid, target, target_name),
            )

    def get_whitelist(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM whitelist
                   WHERE user_id=? ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_whitelist(self, uid, wid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM whitelist WHERE id=? AND user_id=?",
                (wid, uid),
            )

    def clear_whitelist(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM whitelist WHERE user_id=?", (uid,))

    def whitelist_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM whitelist WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def is_whitelisted(self, uid, sender_id, sender_username=None):
        wl = self.get_whitelist(uid)
        wl_ids = set()
        for w in wl:
            wl_ids.add(str(w["target_user"]))
        if str(sender_id) in wl_ids:
            return True
        if sender_username:
            clean = sender_username.lower().lstrip("@")
            for w in wl:
                t = str(w["target_user"]).lower().lstrip("@")
                if t == clean:
                    return True
        return False

    # ═══════════════════ SCHEDULED ═══════════════════

    def add_scheduled(self, uid, target, message, send_at,
                      media_file_id=None, media_type=None,
                      recurring=False, interval_hr=0, max_repeats=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO scheduled
                   (user_id, target, message, send_at, media_file_id,
                    media_type, recurring, interval_hr, max_repeats)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (uid, target, message, send_at, media_file_id,
                 media_type, int(recurring), interval_hr, max_repeats),
            )

    def pending_scheduled(self):
        now = datetime.now().isoformat()
        with self.conn() as cx:
            return cx.execute(
                """SELECT s.*, u.session_str FROM scheduled s
                   JOIN users u ON s.user_id=u.user_id
                   WHERE s.is_sent=0
                   AND s.send_at<=?
                   AND u.session_str IS NOT NULL
                   AND u.session_str != ''
                   AND u.is_banned=0""",
                (now,),
            ).fetchall()

    def mark_sent(self, sid, recurring=False, interval_hr=0,
                  max_repeats=0, repeat_count=0):
        with self.conn() as cx:
            if recurring and interval_hr > 0:
                new_count = repeat_count + 1
                if max_repeats > 0 and new_count >= max_repeats:
                    cx.execute(
                        """UPDATE scheduled
                           SET is_sent=1, sent_at=?, repeat_count=?
                           WHERE id=?""",
                        (datetime.now().isoformat(), new_count, sid),
                    )
                else:
                    new_at = (
                        datetime.now() + timedelta(hours=interval_hr)
                    ).isoformat()
                    cx.execute(
                        """UPDATE scheduled
                           SET send_at=?, repeat_count=?
                           WHERE id=?""",
                        (new_at, new_count, sid),
                    )
            else:
                cx.execute(
                    """UPDATE scheduled
                       SET is_sent=1, sent_at=?
                       WHERE id=?""",
                    (datetime.now().isoformat(), sid),
                )

    def user_scheduled(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM scheduled
                   WHERE user_id=? AND is_sent=0
                   ORDER BY send_at ASC""",
                (uid,),
            ).fetchall()

    def del_scheduled(self, uid, sid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM scheduled WHERE id=? AND user_id=?",
                (sid, uid),
            )

    def scheduled_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT COUNT(*) FROM scheduled
                   WHERE user_id=? AND is_sent=0""",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ AUTO-FORWARD ═══════════════════

    def add_forward(self, uid, source, dest, filter_text="",
                    forward_media=True):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO auto_forward
                   (user_id, source, dest, filter_text, forward_media)
                   VALUES(?,?,?,?,?)""",
                (uid, source, dest, filter_text, int(forward_media)),
            )

    def get_forwards(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM auto_forward
                   WHERE user_id=? AND active=1
                   ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_forward(self, uid, fid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM auto_forward WHERE id=? AND user_id=?",
                (fid, uid),
            )

    def clear_forwards(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM auto_forward WHERE user_id=?", (uid,)
            )

    def forward_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT COUNT(*) FROM auto_forward
                   WHERE user_id=? AND active=1""",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ PM PERMIT ═══════════════════

    def approve_pm(self, uid, sender, sender_name="", auto=False):
        with self.conn() as cx:
            cx.execute(
                """INSERT OR IGNORE INTO pm_permit
                   (user_id, approved, approved_name, auto_approved)
                   VALUES(?,?,?,?)""",
                (uid, sender, sender_name, int(auto)),
            )

    def is_pm_approved(self, uid, sender):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT 1 FROM pm_permit WHERE user_id=? AND approved=?",
                (uid, sender),
            ).fetchone()
        return r is not None

    def get_approved(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM pm_permit
                   WHERE user_id=?
                   ORDER BY approved_at DESC""",
                (uid,),
            ).fetchall()

    def revoke_pm(self, uid, sender):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM pm_permit WHERE user_id=? AND approved=?",
                (uid, sender),
            )

    def approved_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM pm_permit WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ STATS ═══════════════════

    def inc_stat(self, uid, key, n=1):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO stats(user_id, key, value)
                   VALUES(?,?,?)
                   ON CONFLICT(user_id, key)
                   DO UPDATE SET value=value+?""",
                (uid, key, n, n),
            )
        today = datetime.now().strftime("%Y-%m-%d")
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO daily_stats(user_id, date, key, value)
                   VALUES(?,?,?,?)
                   ON CONFLICT(user_id, date, key)
                   DO UPDATE SET value=value+?""",
                (uid, today, key, n, n),
            )

    def get_stat(self, uid, key):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT value FROM stats WHERE user_id=? AND key=?",
                (uid, key),
            ).fetchone()
        return r["value"] if r else 0

    def all_stats(self, uid):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT key, value FROM stats
                   WHERE user_id=? ORDER BY key ASC""",
                (uid,),
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def daily_stats(self, uid, days=7):
        cutoff = (
            datetime.now() - timedelta(days=days)
        ).strftime("%Y-%m-%d")
        with self.conn() as cx:
            return cx.execute(
                """SELECT date, key, value FROM daily_stats
                   WHERE user_id=? AND date>=?
                   ORDER BY date DESC, key ASC""",
                (uid, cutoff),
            ).fetchall()

    def reset_stats(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM stats WHERE user_id=?", (uid,))
            cx.execute(
                "DELETE FROM daily_stats WHERE user_id=?", (uid,)
            )

    def global_stats(self):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT key, SUM(value) as total
                   FROM stats GROUP BY key ORDER BY total DESC"""
            ).fetchall()
        return {r["key"]: r["total"] for r in rows}

    # ═══════════════════ LOGS ═══════════════════

    def log(self, uid, action, detail="", category="general"):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO logs(user_id, action, detail, category)
                   VALUES(?,?,?,?)""",
                (uid, action, detail[:500], category),
            )

    def get_logs(self, uid, limit=30, category=None):
        q = "SELECT * FROM logs WHERE user_id=?"
        params = [uid]
        if category:
            q += " AND category=?"
            params.append(category)
        q += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def all_logs(self, limit=50, category=None):
        q = "SELECT * FROM logs"
        params = []
        if category:
            q += " WHERE category=?"
            params.append(category)
        q += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def clear_logs(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM logs WHERE user_id=?", (uid,))

    def clear_all_logs(self):
        with self.conn() as cx:
            cx.execute("DELETE FROM logs")

    # ═══════════════════ SPAM TRACKING ═══════════════════

    def check_spam(self, uid, sender, limit=5):
        now = datetime.now()
        min_ago = (now - timedelta(minutes=1)).isoformat()
        with self.conn() as cx:
            r = cx.execute(
                """SELECT * FROM spam_track
                   WHERE user_id=? AND sender=? AND last_ts>?""",
                (uid, sender, min_ago),
            ).fetchone()
            if r:
                new_cnt = r["count"] + 1
                if new_cnt >= limit:
                    cx.execute(
                        """UPDATE spam_track
                           SET count=?, blocked=1, last_ts=?
                           WHERE id=?""",
                        (new_cnt, now.isoformat(), r["id"]),
                    )
                    return True
                cx.execute(
                    "UPDATE spam_track SET count=?, last_ts=? WHERE id=?",
                    (new_cnt, now.isoformat(), r["id"]),
                )
            else:
                cx.execute(
                    """INSERT INTO spam_track
                       (user_id, sender, count, last_ts)
                       VALUES(?,?,1,?)""",
                    (uid, sender, now.isoformat()),
                )
        return False

    def is_spam_blocked(self, uid, sender):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT blocked FROM spam_track
                   WHERE user_id=? AND sender=?""",
                (uid, sender),
            ).fetchone()
        return bool(r and r["blocked"])

    def unblock_spam(self, uid, sender):
        with self.conn() as cx:
            cx.execute(
                """UPDATE spam_track
                   SET blocked=0, count=0
                   WHERE user_id=? AND sender=?""",
                (uid, sender),
            )

    # ═══════════════════ TEMPLATES ═══════════════════

    def add_template(self, uid, name, content, category="general",
                     media_file_id=None, media_type=None,
                     is_global=False):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO templates
                   (user_id, name, content, category,
                    media_file_id, media_type, is_global)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, name, content, category,
                 media_file_id, media_type, int(is_global)),
            )

    def get_templates(self, uid, include_global=True):
        with self.conn() as cx:
            if include_global:
                return cx.execute(
                    """SELECT * FROM templates
                       WHERE user_id=? OR is_global=1
                       ORDER BY name ASC""",
                    (uid,),
                ).fetchall()
            return cx.execute(
                """SELECT * FROM templates
                   WHERE user_id=?
                   ORDER BY name ASC""",
                (uid,),
            ).fetchall()

    def get_template(self, uid, tid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM templates
                   WHERE id=? AND (user_id=? OR is_global=1)""",
                (tid, uid),
            ).fetchone()

    def del_template(self, uid, tid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM templates WHERE id=? AND user_id=?",
                (tid, uid),
            )

    def clear_templates(self, uid):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM templates
                   WHERE user_id=? AND is_global=0""",
                (uid,),
            )

    def template_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM templates WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def template_inc(self, tid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE templates
                   SET used_count=used_count+1
                   WHERE id=?""",
                (tid,),
            )

    # ═══════════════════ WORKING HOURS ═══════════════════

    def set_working_hours(self, uid, day, start_hr, start_min,
                          end_hr, end_min, is_active=True):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO working_hours
                   (user_id, day, start_hr, start_min,
                    end_hr, end_min, is_active)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(user_id, day)
                   DO UPDATE SET
                       start_hr=excluded.start_hr,
                       start_min=excluded.start_min,
                       end_hr=excluded.end_hr,
                       end_min=excluded.end_min,
                       is_active=excluded.is_active""",
                (uid, day, start_hr, start_min,
                 end_hr, end_min, int(is_active)),
            )

    def get_working_hours(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM working_hours
                   WHERE user_id=?
                   ORDER BY day ASC""",
                (uid,),
            ).fetchall()

    def is_working_hours(self, uid):
        if self.get_setting(uid, "working_hours", "false") != "true":
            return True

        now = datetime.now()
        day = now.weekday()
        with self.conn() as cx:
            r = cx.execute(
                """SELECT * FROM working_hours
                   WHERE user_id=? AND day=? AND is_active=1""",
                (uid, day),
            ).fetchone()

        if not r:
            return True

        current_mins = now.hour * 60 + now.minute
        start_mins = r["start_hr"] * 60 + r["start_min"]
        end_mins = r["end_hr"] * 60 + r["end_min"]
        return start_mins <= current_mins <= end_mins

    def clear_working_hours(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM working_hours WHERE user_id=?", (uid,)
            )

    # ═══════════════════ CUSTOM COMMANDS ═══════════════════

    def add_custom_cmd(self, uid, command, response,
                       media_file_id=None, media_type=None):
        cmd = command.lower().strip().lstrip("/")
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO custom_commands
                   (user_id, command, response,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, cmd, response, media_file_id, media_type),
            )

    def get_custom_cmds(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM custom_commands
                   WHERE user_id=? AND is_active=1
                   ORDER BY command ASC""",
                (uid,),
            ).fetchall()

    def del_custom_cmd(self, uid, cid):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM custom_commands
                   WHERE id=? AND user_id=?""",
                (cid, uid),
            )

    def clear_custom_cmds(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM custom_commands WHERE user_id=?", (uid,)
            )

    def custom_cmd_inc(self, cid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE custom_commands
                   SET used_count=used_count+1
                   WHERE id=?""",
                (cid,),
            )

    # ═══════════════════ NOTES ═══════════════════

    def add_note(self, uid, title, content,
                 media_file_id=None, media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO notes
                   (user_id, title, content,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, title, content, media_file_id, media_type),
            )

    def get_notes(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM notes
                   WHERE user_id=?
                   ORDER BY is_pinned DESC, updated_at DESC""",
                (uid,),
            ).fetchall()

    def get_note(self, uid, nid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            ).fetchone()

    def del_note(self, uid, nid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            )

    def toggle_pin_note(self, uid, nid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT is_pinned FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            ).fetchone()
            if r:
                cx.execute(
                    """UPDATE notes SET is_pinned=?
                       WHERE id=? AND user_id=?""",
                    (0 if r["is_pinned"] else 1, nid, uid),
                )

    def clear_notes(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM notes WHERE user_id=?", (uid,))

    # ═══════════════════ MEDIA ATTACHMENTS ═══════════════════

    def add_media_attachment(self, uid, parent_type, parent_id,
                             file_id, media_type, caption="",
                             position=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO media_attachments
                   (user_id, parent_type, parent_id,
                    file_id, media_type, caption, position)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, parent_type, parent_id,
                 file_id, media_type, caption, position),
            )

    def get_media_attachments(self, uid, parent_type, parent_id):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM media_attachments
                   WHERE user_id=? AND parent_type=? AND parent_id=?
                   ORDER BY position ASC""",
                (uid, parent_type, parent_id),
            ).fetchall()

    def del_media_attachments(self, uid, parent_type, parent_id):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM media_attachments
                   WHERE user_id=? AND parent_type=? AND parent_id=?""",
                (uid, parent_type, parent_id),
            )

    # ═══════════════════ FEEDBACK ═══════════════════

    def add_feedback(self, uid, message):
        with self.conn() as cx:
            cx.execute(
                "INSERT INTO feedback(user_id, message) VALUES(?,?)",
                (uid, message),
            )

    def get_all_feedback(self, status=None):
        q = "SELECT f.*, u.username, u.first_name FROM feedback f LEFT JOIN users u ON f.user_id=u.user_id"
        params = []
        if status:
            q += " WHERE f.status=?"
            params.append(status)
        q += " ORDER BY f.created_at DESC"
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def reply_feedback(self, fid, reply):
        with self.conn() as cx:
            cx.execute(
                """UPDATE feedback
                   SET status='replied', admin_reply=?, replied_at=?
                   WHERE id=?""",
                (reply, datetime.now().isoformat(), fid),
            )

    def user_feedback(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM feedback
                   WHERE user_id=?
                   ORDER BY created_at DESC""",
                (uid,),
            ).fetchall()

    # ═══════════════════ ANNOUNCEMENTS ═══════════════════

    def add_announcement(self, title, content, created_by,
                         target="all", media_file_id=None,
                         media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO announcements
                   (title, content, created_by, target,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?,?)""",
                (title, content, created_by, target,
                 media_file_id, media_type),
            )

    def get_announcements(self, limit=10):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM announcements
                   ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()

    # ═══════════════════ ADMIN HELPERS ═══════════════════

    def total_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT COUNT(*) FROM users"
            ).fetchone()[0]

    def active_sessions_count(self):
        with self.conn() as cx:
            return cx.execute(
                """SELECT COUNT(*) FROM users
                   WHERE session_str IS NOT NULL
                   AND session_str != ''"""
            ).fetchone()[0]

    def users_by_plan_count(self):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT plan, COUNT(*) as cnt
                   FROM users GROUP BY plan"""
            ).fetchall()
        return {r["plan"]: r["cnt"] for r in rows}

    def db_size(self):
        if not os.path.exists(self.db_file):
            return "0 KB"
        size = os.path.getsize(self.db_file)
        if size < 1024:
            return f"{size} B"
        if size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        return f"{size / (1024 * 1024):.1f} MB"

    def cleanup(self):
        week_ago = (
            datetime.now() - timedelta(days=7)
        ).isoformat()
        day_ago = (
            datetime.now() - timedelta(days=1)
        ).isoformat()
        month_ago = (
            datetime.now() - timedelta(days=30)
        ).isoformat()
        with self.conn() as cx:
            cx.execute("DELETE FROM logs WHERE ts<?", (week_ago,))
            cx.execute(
                "DELETE FROM spam_track WHERE last_ts<?", (day_ago,)
            )
            cx.execute("DELETE FROM scheduled WHERE is_sent=1")
            cx.execute(
                "DELETE FROM daily_stats WHERE date<?",
                (month_ago[:10],),
            )
            cx.execute("VACUUM")

    # ═══════════════════ EXPORT / IMPORT ═══════════════════

    def export_user_data(self, uid) -> dict:
        data = {
            "version": BOT_VERSION,
            "exported_at": datetime.now().isoformat(),
            "user_id": uid,
            "settings": self.all_settings(uid),
            "keywords": [],
            "filters": [],
            "blocked_words": [],
            "whitelist": [],
            "templates": [],
            "custom_commands": [],
            "notes": [],
            "working_hours": [],
        }

        for kw in self.get_keywords(uid, active_only=False):
            data["keywords"].append({
                "trigger": kw["trigger_text"],
                "response": kw["response"],
                "match_type": kw["match_type"],
                "media_file_id": kw["media_file_id"],
                "media_type": kw["media_type"],
                "is_active": kw["is_active"],
                "reply_delay": kw["reply_delay"],
            })

        for f in self.get_filters(uid):
            data["filters"].append({
                "name": f["name"],
                "response": f["response"],
                "media_file_id": f["media_file_id"],
                "media_type": f["media_type"],
            })

        for bw in self.get_blocked(uid):
            data["blocked_words"].append({
                "word": bw["word"],
                "action": bw["action"],
            })

        for w in self.get_whitelist(uid):
            data["whitelist"].append({
                "target": w["target_user"],
                "name": w["target_name"],
            })

        for t in self.get_templates(uid, include_global=False):
            data["templates"].append({
                "name": t["name"],
                "content": t["content"],
                "category": t["category"],
                "media_file_id": t["media_file_id"],
                "media_type": t["media_type"],
            })

        for cmd in self.get_custom_cmds(uid):
            data["custom_commands"].append({
                "command": cmd["command"],
                "response": cmd["response"],
                "media_file_id": cmd["media_file_id"],
                "media_type": cmd["media_type"],
            })

        for n in self.get_notes(uid):
            data["notes"].append({
                "title": n["title"],
                "content": n["content"],
                "media_file_id": n["media_file_id"],
                "media_type": n["media_type"],
                "is_pinned": n["is_pinned"],
            })

        for wh in self.get_working_hours(uid):
            data["working_hours"].append({
                "day": wh["day"],
                "start_hr": wh["start_hr"],
                "start_min": wh["start_min"],
                "end_hr": wh["end_hr"],
                "end_min": wh["end_min"],
                "is_active": wh["is_active"],
            })

        return data

    def import_user_data(self, uid, data: dict):
        if "settings" in data:
            self.bulk_set_settings(uid, data["settings"])

        if "keywords" in data:
            for kw in data["keywords"]:
                self.add_keyword(
                    uid,
                    kw["trigger"],
                    kw["response"],
                    kw.get("match_type", "contains"),
                    kw.get("media_file_id"),
                    kw.get("media_type"),
                    kw.get("reply_delay", 0),
                )

        if "filters" in data:
            for f in data["filters"]:
                self.add_filter(
                    uid,
                    f["name"],
                    f["response"],
                    f.get("media_file_id"),
                    f.get("media_type"),
                )

        if "blocked_words" in data:
            for bw in data["blocked_words"]:
                self.add_blocked(
                    uid,
                    bw["word"],
                    bw.get("action", "warn"),
                )

        if "whitelist" in data:
            for w in data["whitelist"]:
                self.add_whitelist(
                    uid,
                    w["target"],
                    w.get("name", ""),
                )

        if "templates" in data:
            for t in data["templates"]:
                self.add_template(
                    uid,
                    t["name"],
                    t["content"],
                    t.get("category", "general"),
                    t.get("media_file_id"),
                    t.get("media_type"),
                )

        if "custom_commands" in data:
            for cmd in data["custom_commands"]:
                self.add_custom_cmd(
                    uid,
                    cmd["command"],
                    cmd["response"],
                    cmd.get("media_file_id"),
                    cmd.get("media_type"),
                )

        if "notes" in data:
            for n in data["notes"]:
                self.add_note(
                    uid,
                    n["title"],
                    n["content"],
                    n.get("media_file_id"),
                    n.get("media_type"),
                )

        if "working_hours" in data:
            for wh in data["working_hours"]:
                self.set_working_hours(
                    uid,
                    wh["day"],
                    wh["start_hr"],
                    wh["start_min"],
                    wh["end_hr"],
                    wh["end_min"],
                    wh.get("is_active", True),
                )

        self.log(uid, "data_import", "Settings imported", "system")


# Initialize database
db = Database()

# ╔══════════════════════════════════════════════════════════════╗
# ║                   ACTIVE CLIENTS                             ║
# ╚══════════════════════════════════════════════════════════════╝

active_clients: dict[int, TelegramClient] = {}
client_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# ╔══════════════════════════════════════════════════════════════╗
# ║                     HELPERS                                  ║
# ╚══════════════════════════════════════════════════════════════╝


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def admin_only(fn):
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                      *args, **kwargs):
        user = update.effective_user
        if not user or not is_admin(user.id):
            target = update.effective_message
            if target:
                await target.reply_text("⛔ Admin only.")
            return
        return await fn(update, ctx, *args, **kwargs)
    return wrapper


def fmt_bool(val) -> str:
    return "🟢" if val in ("true", True, 1, "1") else "🔴"


def fmt_plan(plan: str) -> str:
    config = PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])
    return config["name"]


def back_btn(cb="main_menu"):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("◀️ Back", callback_data=cb)]]
    )


def confirm_btns(yes_cb, no_cb="main_menu", yes_text="✅ Yes",
                  no_text="❌ Cancel"):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(yes_text, callback_data=yes_cb),
        InlineKeyboardButton(no_text, callback_data=no_cb),
    ]])


def substitute_vars(text: str, sender=None) -> str:
    if not text:
        return ""
    now = datetime.now()
    replacements = {
        "{time}": now.strftime("%H:%M:%S"),
        "{date}": now.strftime("%Y-%m-%d"),
        "{day}": now.strftime("%A"),
        "{bot}": BOT_NAME,
    }
    if sender:
        username = getattr(sender, "username", None)
        replacements.update({
            "{name}": getattr(sender, "first_name", "") or "",
            "{lastname}": getattr(sender, "last_name", "") or "",
            "{fullname}": (
                f"{getattr(sender, 'first_name', '') or ''} "
                f"{getattr(sender, 'last_name', '') or ''}"
            ).strip(),
            "{username}": f"@{username}" if username else "",
            "{id}": str(getattr(sender, "id", "")),
            "{mention}": (
                f"[{getattr(sender, 'first_name', 'User')}]"
                f"(tg://user?id={getattr(sender, 'id', 0)})"
            ),
        })
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text


def parse_bool(v, default=False):
    if v is None:
        return default
    return str(v).lower() in {"1", "true", "yes", "on"}


def truncate(text: str, max_len: int = 50) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def get_media_info(message) -> Tuple[Optional[str], Optional[str]]:
    """Extract media file_id and type from a telegram message."""
    if not message:
        return None, None

    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.video:
        return message.video.file_id, "video"
    if message.animation:
        return message.animation.file_id, "animation"
    if message.document:
        return message.document.file_id, "document"
    if message.voice:
        return message.voice.file_id, "voice"
    if message.audio:
        return message.audio.file_id, "audio"
    if message.video_note:
        return message.video_note.file_id, "video_note"
    if message.sticker:
        return message.sticker.file_id, "sticker"
    return None, None


async def send_media_message(bot_or_client, chat_id, text=None,
                             media_file_id=None, media_type=None,
                             reply_to=None, parse_mode="Markdown",
                             is_telethon=False):
    """Universal media sender for both bot and telethon client."""
    if is_telethon:
        return await send_media_telethon(
            bot_or_client, chat_id, text,
            media_file_id, media_type, reply_to,
        )
    return await send_media_bot(
        bot_or_client, chat_id, text,
        media_file_id, media_type, reply_to, parse_mode,
    )


async def send_media_bot(bot, chat_id, text=None,
                         media_file_id=None, media_type=None,
                         reply_to=None, parse_mode="Markdown"):
    """Send media via telegram bot API."""
    try:
        if not media_file_id or not media_type:
            return await bot.send_message(
                chat_id=chat_id,
                text=text or "​",
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )

        caption = text[:MAX_CAPTION_LENGTH] if text else None
        methods = {
            "photo": bot.send_photo,
            "video": bot.send_video,
            "animation": bot.send_animation,
            "document": bot.send_document,
            "voice": bot.send_voice,
            "audio": bot.send_audio,
            "video_note": bot.send_video_note,
            "sticker": bot.send_sticker,
        }

        method = methods.get(media_type)
        if not method:
            return await bot.send_message(
                chat_id=chat_id,
                text=text or "​",
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )

        kwargs = {
            "chat_id": chat_id,
            media_type: media_file_id,
            "reply_to_message_id": reply_to,
        }

        if media_type not in ("sticker", "video_note"):
            kwargs["caption"] = caption
            kwargs["parse_mode"] = parse_mode

        return await method(**kwargs)
    except Exception as exc:
        logger.error("send_media_bot error: %s", exc)
        if text:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )


async def send_media_telethon(client, chat_id, text=None,
                              media_file_id=None, media_type=None,
                              reply_to=None):
    """Send media via telethon client."""
    try:
        if media_file_id and media_type:
            try:
                return await client.send_file(
                    chat_id,
                    media_file_id,
                    caption=text or "",
                    reply_to=reply_to,
                )
            except Exception:
                pass

        if text:
            return await client.send_message(
                chat_id,
                text,
                reply_to=reply_to,
            )
    except Exception as exc:
        logger.error("send_media_telethon error: %s", exc)


def plan_limit_text(uid: int, feature: str, current: int) -> str:
    """Get formatted limit info."""
    limit = db.plan_limit(uid, feature)
    plan = db.get_plan(uid)
    if current >= limit:
        return (
            f"❌ You've reached the limit ({current}/{limit}) "
            f"for your {fmt_plan(plan)} plan.\n"
            f"Upgrade to unlock more! Contact {SUPPORT_USERNAME}"
        )
    return ""


# ╔══════════════════════════════════════════════════════════════╗
# ║              KEYBOARD BUILDERS                               ║
# ╚══════════════════════════════════════════════════════════════╝


def main_kb(uid: int) -> InlineKeyboardMarkup:
    logged_in = bool(db.get_session(uid))
    plan = db.get_plan(uid)
    plan_icon = {"free": "🆓", "premium": "⭐", "vip": "👑"}.get(
        plan, "🆓"
    )

    rows = []
    if logged_in:
        rows = [
            [
                InlineKeyboardButton(
                    "📱 My Account", callback_data="account"
                ),
                InlineKeyboardButton(
                    f"⚙️ Settings {plan_icon}",
                    callback_data="settings_menu",
                ),
            ],
            [
                InlineKeyboardButton(
                    "💬 Welcome", callback_data="welcome_menu"
                ),
                InlineKeyboardButton(
                    "🔑 Keywords", callback_data="kw_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🤖 Away Mode", callback_data="away_menu"
                ),
                InlineKeyboardButton(
                    "📝 Filters", callback_data="filter_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🛡️ PM Permit", callback_data="pm_menu"
                ),
                InlineKeyboardButton(
                    "🔇 Anti-Spam", callback_data="spam_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🚫 Blocked Words", callback_data="bw_menu"
                ),
                InlineKeyboardButton(
                    "📋 Whitelist", callback_data="wl_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "⏰ Scheduled", callback_data="sched_menu"
                ),
                InlineKeyboardButton(
                    "↗️ Auto-Forward", callback_data="fwd_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "👤 Profile", callback_data="profile_menu"
                ),
                InlineKeyboardButton(
                    "📑 Templates", callback_data="tmpl_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "⏰ Work Hours", callback_data="wh_menu"
                ),
                InlineKeyboardButton(
                    "😍 Auto-React", callback_data="react_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📒 Notes", callback_data="notes_menu"
                ),
                InlineKeyboardButton(
                    "🤖 Custom Cmds", callback_data="ccmd_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📊 Stats", callback_data="stats_menu"
                ),
                InlineKeyboardButton(
                    "📜 Logs", callback_data="logs_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💎 My Plan", callback_data="plan_menu"
                ),
                InlineKeyboardButton(
                    "💬 Feedback", callback_data="feedback_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📥 Backup", callback_data="backup_menu"
                ),
                InlineKeyboardButton(
                    "❓ Help", callback_data="help_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔄 Reconnect", callback_data="reconnect"
                ),
                InlineKeyboardButton(
                    "🚪 Logout", callback_data="logout_ask"
                ),
            ],
        ]
    else:
        rows = [
            [
                InlineKeyboardButton(
                    "🔐 Login", callback_data="login_start"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💎 Plans", callback_data="plan_info"
                ),
                InlineKeyboardButton(
                    "❓ Help", callback_data="help_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💬 Support", url=SUPPORT_URL
                ),
            ],
        ]
    if is_admin(uid):
        rows.append([
            InlineKeyboardButton(
                "👑 Admin Panel", callback_data="admin_home"
            ),
        ])
    return InlineKeyboardMarkup(rows)


async def show_main(target, uid: int):
    plan = db.get_plan(uid)
    plan_text = fmt_plan(plan)
    await target.edit_message_text(
        f"🦴 *{BOT_NAME}* — Main Menu\n\n"
        f"Your Plan: {plan_text}",
        reply_markup=main_kb(uid),
        parse_mode="Markdown",
    )


async def ask_state(q, ctx, state, prompt, extra_kb=None):
    ctx.user_data["state"] = state
    kb = extra_kb
    if not kb:
        cancel_row = [
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_state")
        ]
        kb = InlineKeyboardMarkup([cancel_row])
    await q.edit_message_text(
        f"{prompt}\n\n_Send /cancel or tap Cancel to abort._",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def ask_state_msg(message, ctx, state, prompt):
    ctx.user_data["state"] = state
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_state")
    ]])
    await message.reply_text(
        f"{prompt}\n\n_Send /cancel to abort._",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║              TELETHON CLIENT MANAGEMENT                      ║
# ╚══════════════════════════════════════════════════════════════╝


def register_handlers(client: TelegramClient, uid: int):
    """Register all event handlers for a user's client."""

    @client.on(events.NewMessage(
        incoming=True, func=lambda e: e.is_private
    ))
    async def on_pm(event):
        try:
            await handle_pm(event, uid, client)
        except Exception as exc:
            logger.exception(
                "PM handler error uid=%s: %s", uid, exc
            )

    @client.on(events.NewMessage(
        incoming=True, func=lambda e: not e.is_private
    ))
    async def on_group(event):
        try:
            await handle_group(event, uid, client)
        except Exception as exc:
            logger.exception(
                "Group handler error uid=%s: %s", uid, exc
            )


async def start_client(uid: int) -> Optional[TelegramClient]:
    """Start or restart a user's Telethon client."""
    sess = db.get_session(uid)
    if not sess:
        return None

    async with client_locks[uid]:
        old = active_clients.pop(uid, None)
        if old:
            try:
                await old.disconnect()
            except Exception:
                pass

        try:
            client = TelegramClient(
                StringSession(sess),
                API_ID,
                API_HASH,
                device_model=BOT_NAME,
                system_version="2.0",
                app_version=BOT_VERSION,
                flood_sleep_threshold=60,
            )
            await client.connect()

            if not await client.is_user_authorized():
                db.remove_session(uid)
                db.log(uid, "auth_fail", "Session expired", "system")
                return None

            register_handlers(client, uid)
            active_clients[uid] = client
            db.log(uid, "client_start", "Userbot connected", "system")
            db.touch_user(uid)
            return client

        except (AuthKeyUnregisteredError, UserDeactivatedBanError):
            db.remove_session(uid)
            db.log(uid, "session_invalid", "Session revoked", "system")
            return None
        except Exception as exc:
            logger.exception(
                "start_client uid=%s failed: %s", uid, exc
            )
            db.log(uid, "client_error", str(exc)[:200], "system")
            return None


async def stop_client(uid: int):
    """Stop a user's Telethon client."""
    async with client_locks[uid]:
        client = active_clients.pop(uid, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
    db.log(uid, "client_stop", "Userbot disconnected", "system")


def get_client(uid: int) -> Optional[TelegramClient]:
    """Get active client for a user."""
    client = active_clients.get(uid)
    if client and client.is_connected():
        return client
    return None


# ╔══════════════════════════════════════════════════════════════╗
# ║            TELETHON RUNTIME HANDLERS                         ║
# ╚══════════════════════════════════════════════════════════════╝


async def reply_with_media(event, text, media_file_id, media_type,
                           client):
    """Reply to a telethon event with optional media."""
    if media_file_id and media_type:
        try:
            await client.send_file(
                event.chat_id,
                media_file_id,
                caption=text or "",
                reply_to=event.id,
            )
            return
        except Exception as exc:
            logger.debug("Media reply fallback: %s", exc)

    if text:
        await event.reply(text)


async def handle_pm(event, uid: int, client: TelegramClient):
    """Handle incoming private messages for a user."""
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return
    if sender.id == uid:
        return

    sid = sender.id
    text = event.raw_text or ""
    db.inc_stat(uid, "messages_received")
    db.touch_user(uid)

    # Check if within working hours
    if not db.is_working_hours(uid):
        wh_msg = db.get_setting(
            uid, "wh_message",
            "🕐 I'm currently outside working hours. "
            "I'll respond when I'm back!"
        )
        wh_once = db.get_setting(uid, "wh_notify_once", "true")
        if wh_once == "true":
            k = f"wh_notified_{sid}"
            if db.get_stat(uid, k) == 0:
                await event.reply(substitute_vars(wh_msg, sender))
                db.inc_stat(uid, k)
        else:
            await event.reply(substitute_vars(wh_msg, sender))
        return

    # Check whitelist
    s_uname = getattr(sender, "username", None)
    in_wl = db.is_whitelisted(uid, sid, s_uname)

    # Anti-spam check
    if (
        not in_wl
        and db.get_setting(uid, "anti_spam", "false") == "true"
    ):
        try:
            limit = int(db.get_setting(uid, "spam_limit", "5"))
        except ValueError:
            limit = 5

        if db.is_spam_blocked(uid, sid):
            return

        if db.check_spam(uid, sid, limit):
            spam_msg = db.get_setting(
                uid, "spam_msg",
                "⚠️ Spam detected. Your messages are being ignored."
            )
            spam_media = db.get_setting(uid, "spam_media_id")
            spam_media_type = db.get_setting(uid, "spam_media_type")
            await reply_with_media(
                event, spam_msg, spam_media, spam_media_type, client
            )
            db.log(uid, "spam_block", f"sender={sid}", "spam")
            db.inc_stat(uid, "spam_blocked")
            return

    # Blocked words check
    for bw in db.get_blocked(uid):
        if bw["word"] in text.lower() and not in_wl:
            action = bw["action"] or "warn"
            if action == "delete":
                try:
                    await event.delete()
                except Exception:
                    await event.reply(
                        "⚠️ Your message contained a blocked word."
                    )
            elif action == "mute":
                await event.reply(
                    "🔇 You've been muted for using a blocked word."
                )
            else:
                await event.reply(
                    "⚠️ Your message contained a blocked word."
                )
            db.log(
                uid, "blocked_word",
                f"word={bw['word']} sender={sid}",
                "moderation",
            )
            db.inc_stat(uid, "blocked_word_triggered")
            return

    # PM Permit check
    if (
        not in_wl
        and db.get_setting(uid, "pm_permit", "false") == "true"
    ):
        if not db.is_pm_approved(uid, sid):
            pm_msg = db.get_setting(
                uid, "pm_msg",
                "⚠️ You are not approved to PM me. Please wait."
            )
            pm_media = db.get_setting(uid, "pm_media_id")
            pm_media_type = db.get_setting(uid, "pm_media_type")

            try:
                limit = int(db.get_setting(uid, "pm_limit", "3"))
            except ValueError:
                limit = 3

            stat_k = f"pm_warn_{sid}"
            warns = db.get_stat(uid, stat_k)
            if warns >= limit:
                pm_block_msg = db.get_setting(
                    uid, "pm_block_msg",
                    "🚫 You have reached the PM permit limit."
                )
                await event.reply(pm_block_msg)
                db.log(uid, "pm_block", f"sender={sid}", "pm")
                db.inc_stat(uid, "pm_blocked")
                return

            db.inc_stat(uid, stat_k)
            remaining = max(limit - warns - 1, 0)
            msg = (
                f"{substitute_vars(pm_msg, sender)}\n\n"
                f"⚠️ {remaining} warning(s) left."
            )
            await reply_with_media(
                event, msg, pm_media, pm_media_type, client
            )
            db.inc_stat(uid, "pm_warnings_sent")
            return

    # Check custom commands (if user typed /something)
    if text.startswith("/"):
        cmd_text = text[1:].split()[0].lower()
        for cmd in db.get_custom_cmds(uid):
            if cmd["command"] == cmd_text:
                resp = substitute_vars(cmd["response"], sender)
                await reply_with_media(
                    event, resp,
                    cmd["media_file_id"], cmd["media_type"],
                    client,
                )
                db.custom_cmd_inc(cmd["id"])
                db.inc_stat(uid, "custom_cmd_used")
                return

    # Keyword matching
    for kw in db.get_keywords(uid):
        matched = False
        trigger = kw["trigger_text"]
        mt = kw["match_type"]
        tl = text.lower()

        if mt == "exact" and tl == trigger:
            matched = True
        elif mt == "contains" and trigger in tl:
            matched = True
        elif mt == "startswith" and tl.startswith(trigger):
            matched = True
        elif mt == "endswith" and tl.endswith(trigger):
            matched = True
        elif mt == "regex":
            if db.plan_check(uid, "regex_keywords"):
                try:
                    matched = bool(re.search(trigger, text, re.I))
                except re.error:
                    matched = False

        if matched:
            delay = kw["reply_delay"] or 0
            if delay > 0:
                await asyncio.sleep(delay)

            resp = substitute_vars(kw["response"], sender)
            await reply_with_media(
                event, resp,
                kw["media_file_id"], kw["media_type"],
                client,
            )
            db.kw_inc(kw["id"])
            db.inc_stat(uid, "keyword_replies")
            db.log(
                uid, "kw_reply",
                f"trigger={trigger}", "keyword",
            )
            return

    # Filter matching
    for filt in db.get_filters(uid):
        if filt["name"] in text.lower():
            resp = substitute_vars(filt["response"], sender)
            await reply_with_media(
                event, resp,
                filt["media_file_id"], filt["media_type"],
                client,
            )
            db.filter_inc(filt["id"])
            db.inc_stat(uid, "filter_replies")
            db.log(
                uid, "filter_reply",
                f"name={filt['name']}", "filter",
            )
            return

    # Welcome message
    if db.get_setting(uid, "welcome", "false") == "true":
        w_msg = db.get_setting(
            uid, "welcome_msg",
            "👋 Hi {name}! Thanks for messaging me."
        )
        w_media = db.get_setting(uid, "welcome_media_id")
        w_media_type = db.get_setting(uid, "welcome_media_type")
        w_mode = db.get_setting(uid, "welcome_mode", "first_time")
        msg = substitute_vars(w_msg, sender)

        if w_mode == "always":
            await reply_with_media(
                event, msg, w_media, w_media_type, client
            )
            db.inc_stat(uid, "welcome_sent")
        else:
            k = f"welcomed_{sid}"
            if db.get_stat(uid, k) == 0:
                await reply_with_media(
                    event, msg, w_media, w_media_type, client
                )
                db.inc_stat(uid, k)
                db.inc_stat(uid, "welcome_sent")

    # Away message
    if db.get_setting(uid, "away", "false") == "true":
        a_msg = db.get_setting(
            uid, "away_msg",
            "🌙 I'm currently away. I'll reply when I'm back!"
        )
        a_media = db.get_setting(uid, "away_media_id")
        a_media_type = db.get_setting(uid, "away_media_type")
        msg = substitute_vars(a_msg, sender)
        await reply_with_media(
            event, msg, a_media, a_media_type, client
        )
        db.inc_stat(uid, "away_sent")

    # Auto-react
    if (
        db.plan_check(uid, "auto_react")
        and db.get_setting(uid, "auto_react", "false") == "true"
    ):
        emoji = db.get_setting(uid, "react_emoji", "👍")
        try:
            await client(SendReactionRequest(
                peer=event.chat_id,
                msg_id=event.id,
                reaction=[ReactionEmoji(emoticon=emoji)],
            ))
            db.inc_stat(uid, "auto_reacted")
        except Exception:
            pass


async def handle_group(event, uid: int, client: TelegramClient):
    """Handle incoming group messages for auto-forwarding."""
    for rule in db.get_forwards(uid):
        try:
            chat = await event.get_chat()
            cid_str = str(chat.id)
            uname = getattr(chat, "username", "") or ""
            src = str(rule["source"])

            match = src in (
                cid_str, uname, f"@{uname}",
                str(-100) + cid_str,
            )
            if not match and uname:
                match = src.lower().lstrip("@") == uname.lower()

            if match:
                # Apply text filter if set
                ft = rule["filter_text"]
                if ft and ft.lower() not in (
                    event.raw_text or ""
                ).lower():
                    continue

                dest = rule["dest"]
                try:
                    dest = int(dest)
                except (ValueError, TypeError):
                    pass

                if rule["forward_media"] or not event.media:
                    await client.forward_messages(
                        dest, event.message
                    )
                else:
                    if event.raw_text:
                        await client.send_message(
                            dest, event.raw_text
                        )

                db.inc_stat(uid, "messages_forwarded")

        except Exception as exc:
            logger.debug(
                "Forward error uid=%s: %s", uid, exc
            )

# ══════════════════════════════════════════════════════════════
# PART 2 OF 3 — Bot Commands, Menus, Callback Router
# Place this code directly after Part 1 in the same file
# ══════════════════════════════════════════════════════════════


# ╔══════════════════════════════════════════════════════════════╗
# ║                    BOT COMMANDS                              ║
# ╚══════════════════════════════════════════════════════════════╝


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    user = update.effective_user
    if not user:
        return
    db.add_user(user.id, user.username, user.first_name,
                getattr(user, "last_name", None))

    if db.is_banned(user.id):
        await update.message.reply_text(
            "⛔ You are banned from using this bot.\n"
            f"Contact {SUPPORT_USERNAME} for help."
        )
        return

    plan = db.get_plan(user.id)
    plan_text = fmt_plan(plan)

    text = (
        f"🦴 *Welcome to {BOT_NAME} v{BOT_VERSION}!*\n\n"
        f"Hello {user.first_name or 'there'}! 👋\n\n"
        f"Your Plan: {plan_text}\n\n"
        f"*🔥 What can this bot do?*\n\n"
        f"*📬 Messaging:*\n"
        f"• 💬 Welcome messages with media\n"
        f"• 🌙 Away/AFK messages with media\n"
        f"• 🔑 Keyword auto-replies (text + media)\n"
        f"• 📝 Smart filters with media\n"
        f"• ⏰ Scheduled messages (one-time & recurring)\n"
        f"• 📑 Reusable message templates\n\n"
        f"*🛡️ Protection:*\n"
        f"• 🛡️ PM Permit system\n"
        f"• 🔇 Anti-Spam with auto-block\n"
        f"• 🚫 Blocked words (warn/delete/mute)\n"
        f"• 📋 User whitelist\n\n"
        f"*⚡ Automation:*\n"
        f"• ↗️ Auto-forward between chats\n"
        f"• 😍 Auto-react with emoji\n"
        f"• 🤖 Custom commands\n"
        f"• ⏰ Working hours\n\n"
        f"*👤 Profile:*\n"
        f"• Edit bio, name, profile photo\n"
        f"• 📒 Personal notes\n"
        f"• 📊 Usage statistics\n\n"
        f"*💎 Plans:*\n"
        f"• 🆓 Free — Basic features\n"
        f"• ⭐ Premium — Media, reactions, regex\n"
        f"• 👑 VIP — Unlimited everything\n\n"
        f"Press *🔐 Login* to get started!\n"
        f"Support: {SUPPORT_USERNAME}"
    )
    await update.message.reply_text(
        text, reply_markup=main_kb(user.id), parse_mode="Markdown"
    )

    # Auto-reconnect if session exists
    if db.get_session(user.id) and user.id not in active_clients:
        asyncio.create_task(start_client(user.id))


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    uid = update.effective_user.id
    text = (
        f"❓ *{BOT_NAME} — Complete Help Guide*\n\n"
        f"═══ *Getting Started* ═══\n"
        f"1️⃣ Use /start to open the main menu\n"
        f"2️⃣ Tap *🔐 Login* and enter your phone number\n"
        f"3️⃣ Enter the OTP code sent to your Telegram\n"
        f"4️⃣ If you have 2FA, enter your password\n"
        f"5️⃣ You're connected! Configure features from the menu\n\n"
        f"═══ *Commands* ═══\n"
        f"/start — Open main menu\n"
        f"/help — Show this help\n"
        f"/status — Bot & connection status\n"
        f"/cancel — Cancel current action\n"
        f"/plan — View your plan details\n"
        f"/stats — View your statistics\n"
        f"/export — Export your settings\n"
        f"/feedback — Send feedback to admin\n\n"
        f"═══ *Message Variables* ═══\n"
        f"`{{name}}` — Sender's first name\n"
        f"`{{lastname}}` — Sender's last name\n"
        f"`{{fullname}}` — Full name\n"
        f"`{{username}}` — @username\n"
        f"`{{id}}` — User ID\n"
        f"`{{mention}}` — Clickable mention\n"
        f"`{{time}}` — Current time\n"
        f"`{{date}}` — Current date\n"
        f"`{{day}}` — Day of week\n"
        f"`{{bot}}` — Bot name\n\n"
        f"═══ *Media Support* ═══\n"
        f"You can attach media to:\n"
        f"• Welcome & Away messages\n"
        f"• Keyword replies\n"
        f"• Filter responses\n"
        f"• Scheduled messages\n"
        f"• Templates\n"
        f"• PM Permit messages\n\n"
        f"Supported: Photos, Videos, GIFs, Documents, "
        f"Voice, Audio, Stickers\n\n"
        f"═══ *Keyword Match Types* ═══\n"
        f"• `contains` — Trigger found anywhere\n"
        f"• `exact` — Exact match only\n"
        f"• `startswith` — Message starts with trigger\n"
        f"• `endswith` — Message ends with trigger\n"
        f"• `regex` — Regular expression (Premium+)\n\n"
        f"═══ *Plan Features* ═══\n"
        f"🆓 *Free:* 10 keywords, 5 filters, basic features\n"
        f"⭐ *Premium:* 50 keywords, media replies, "
        f"auto-react, regex\n"
        f"👑 *VIP:* 200 keywords, unlimited media, "
        f"all features\n\n"
        f"Contact {SUPPORT_USERNAME} to upgrade!\n\n"
        f"═══ *Tips* ═══\n"
        f"💡 Use templates to save frequently used messages\n"
        f"💡 Set working hours to auto-reply outside work time\n"
        f"💡 Whitelist important contacts to bypass filters\n"
        f"💡 Export your settings regularly as backup"
    )
    await update.message.reply_text(
        text, reply_markup=main_kb(uid), parse_mode="Markdown"
    )


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel command."""
    tmp = ctx.user_data.pop("tmp_client", None)
    if tmp:
        try:
            await tmp.disconnect()
        except Exception:
            pass
    ctx.user_data.clear()
    uid = update.effective_user.id
    await update.message.reply_text(
        "❌ Action cancelled.",
        reply_markup=main_kb(uid),
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    uid = update.effective_user.id
    sess = db.get_session(uid)
    cli = get_client(uid)
    connected = bool(sess and cli)
    settings = db.all_settings(uid)
    plan = db.get_plan(uid)
    plan_config = db.get_plan_config(uid)
    kw_count = db.keyword_count(uid)
    filter_count = db.filter_count(uid)
    fwd_count = db.forward_count(uid)
    sched_count = db.scheduled_count(uid)
    approved = db.approved_count(uid)

    plan_expiry = db.get_plan_expiry(uid)
    expiry_text = ""
    if plan_expiry and plan != "free":
        try:
            exp = datetime.fromisoformat(plan_expiry)
            days_left = (exp - datetime.now()).days
            expiry_text = f"\n⏳ Expires in: *{days_left}* days"
        except (ValueError, TypeError):
            pass

    text = (
        f"📊 *{BOT_NAME} Status*\n\n"
        f"{'🟢 Connected' if connected else '🔴 Disconnected'}\n"
        f"💎 Plan: {fmt_plan(plan)}{expiry_text}\n\n"
        f"═══ *Features* ═══\n"
        f"{fmt_bool(settings.get('welcome', 'false'))} Welcome\n"
        f"{fmt_bool(settings.get('away', 'false'))} Away Mode\n"
        f"{fmt_bool(settings.get('pm_permit', 'false'))} PM Permit\n"
        f"{fmt_bool(settings.get('anti_spam', 'false'))} Anti-Spam\n"
        f"{fmt_bool(settings.get('auto_react', 'false'))} Auto-React\n"
        f"{fmt_bool(settings.get('working_hours', 'false'))} Working Hours\n\n"
        f"═══ *Counts* ═══\n"
        f"🔑 Keywords: {kw_count}/{plan_config['max_keywords']}\n"
        f"📝 Filters: {filter_count}/{plan_config['max_filters']}\n"
        f"↗️ Forwards: {fwd_count}/{plan_config['max_forwards']}\n"
        f"⏰ Scheduled: {sched_count}/{plan_config['max_scheduled']}\n"
        f"👥 PM Approved: {approved}"
    )
    await update.message.reply_text(
        text, reply_markup=main_kb(uid), parse_mode="Markdown"
    )


async def cmd_plan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /plan command."""
    uid = update.effective_user.id
    await show_plan_info(update.message, uid, is_edit=False)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command."""
    uid = update.effective_user.id
    stats = db.all_stats(uid)
    if not stats:
        body = "_No stats recorded yet. Start using features!_"
    else:
        lines = []
        for k, v in sorted(stats.items()):
            if k.startswith("pm_warn_") or k.startswith("welcomed_") or k.startswith("wh_notified_"):
                continue
            display_name = k.replace("_", " ").title()
            lines.append(f"• {display_name}: *{v}*")
        body = "\n".join(lines) if lines else "_No meaningful stats yet._"

    await update.message.reply_text(
        f"📊 *Your Statistics*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=main_kb(uid),
    )


async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /export command."""
    uid = update.effective_user.id
    if not db.plan_check(uid, "backup_export"):
        await update.message.reply_text(
            f"⭐ Export is a Premium feature.\n"
            f"Contact {SUPPORT_USERNAME} to upgrade!",
            reply_markup=main_kb(uid),
        )
        return

    await update.message.reply_text("📤 Preparing export...")
    try:
        data = db.export_user_data(uid)
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        bio = io.BytesIO(json_str.encode("utf-8"))
        bio.name = f"skull_backup_{uid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        await update.message.reply_document(
            document=bio,
            caption="📥 Your settings backup.\nUse the Import feature to restore.",
        )
        db.log(uid, "export", "Settings exported", "backup")
    except Exception as exc:
        await update.message.reply_text(f"❌ Export failed: {exc}")


async def cmd_feedback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /feedback command."""
    ctx.user_data["state"] = ST_FEEDBACK
    await update.message.reply_text(
        "💬 *Send Feedback*\n\n"
        "Type your message, suggestion, or bug report.\n"
        "The admin will review it.\n\n"
        "_/cancel to abort._",
        parse_mode="Markdown",
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║                    PLAN INFO DISPLAY                         ║
# ╚══════════════════════════════════════════════════════════════╝


async def show_plan_info(target, uid, is_edit=True):
    """Show plan comparison."""
    current = db.get_plan(uid)
    expiry = db.get_plan_expiry(uid)
    expiry_text = ""
    if expiry and current != "free":
        try:
            exp = datetime.fromisoformat(expiry)
            days_left = (exp - datetime.now()).days
            expiry_text = f"\n⏳ Expires: `{expiry[:10]}` ({days_left} days)"
        except (ValueError, TypeError):
            pass

    text = (
        f"💎 *Plan Information*\n\n"
        f"Your Plan: {fmt_plan(current)}{expiry_text}\n\n"
        f"═══ *Plan Comparison* ═══\n\n"
        f"🆓 *Free Plan:*\n"
        f"• 10 keywords, 5 filters\n"
        f"• 2 auto-forwards, 5 scheduled\n"
        f"• Basic text replies\n"
        f"• No media in replies\n"
        f"• No auto-react\n\n"
        f"⭐ *Premium Plan:*\n"
        f"• 50 keywords, 25 filters\n"
        f"• 10 auto-forwards, 25 scheduled\n"
        f"• ✅ Media in all replies\n"
        f"• ✅ Auto-react with emoji\n"
        f"• ✅ Working hours\n"
        f"• ✅ Regex keywords\n"
        f"• ✅ Recurring schedules\n"
        f"• ✅ Backup/Export\n"
        f"• ✅ Advanced stats\n"
        f"• ✅ Priority support\n\n"
        f"👑 *VIP Plan:*\n"
        f"• 200 keywords, 100 filters\n"
        f"• 30 auto-forwards, 100 scheduled\n"
        f"• ✅ Everything in Premium\n"
        f"• ✅ Multi-media per reply\n"
        f"• ✅ Custom commands\n"
        f"• ✅ All future features\n\n"
        f"Contact {SUPPORT_USERNAME} to upgrade!"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "💬 Contact Support", url=SUPPORT_URL
        )],
        [InlineKeyboardButton("◀️ Back", callback_data="main_menu")],
    ])

    if is_edit:
        await target.edit_message_text(
            text, parse_mode="Markdown", reply_markup=kb
        )
    else:
        await target.reply_text(
            text, parse_mode="Markdown", reply_markup=kb
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║                     ALL MENUS                                ║
# ╚══════════════════════════════════════════════════════════════╝


async def welcome_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("welcome", "false") == "true"
    mode = s.get("welcome_mode", "first_time")
    has_media = bool(s.get("welcome_media_id"))
    media_icon = "🖼️" if has_media else ""

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="welcome_toggle",
        )],
        [
            InlineKeyboardButton(
                "✏️ Set Message", callback_data="welcome_set"
            ),
            InlineKeyboardButton(
                "📋 View", callback_data="welcome_view"
            ),
        ],
        [
            InlineKeyboardButton(
                f"📎 {'Change' if has_media else 'Add'} Media {media_icon}",
                callback_data="welcome_media",
            ),
            InlineKeyboardButton(
                "🗑️ Remove Media",
                callback_data="welcome_rm_media",
            ),
        ],
        [InlineKeyboardButton(
            f"Mode: {mode.replace('_', ' ').title()}",
            callback_data="welcome_mode_tog",
        )],
        [
            InlineKeyboardButton(
                "🗑️ Delete All", callback_data="welcome_del"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"💬 *Welcome Message*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n"
        f"Mode: {mode.replace('_', ' ').title()}\n"
        f"Media: {'✅ Attached' if has_media else '❌ None'}\n\n"
        f"*Variables:* `{{name}}` `{{username}}` `{{id}}` "
        f"`{{time}}` `{{date}}` `{{mention}}` `{{fullname}}`",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def away_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("away", "false") == "true"
    has_media = bool(s.get("away_media_id"))

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="away_toggle",
        )],
        [
            InlineKeyboardButton(
                "✏️ Set Message", callback_data="away_set"
            ),
            InlineKeyboardButton(
                "📋 View", callback_data="away_view"
            ),
        ],
        [
            InlineKeyboardButton(
                f"📎 {'Change' if has_media else 'Add'} Media",
                callback_data="away_media",
            ),
            InlineKeyboardButton(
                "🗑️ Remove Media",
                callback_data="away_rm_media",
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Delete All", callback_data="away_del"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"🌙 *Away Mode*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n"
        f"Media: {'✅ Attached' if has_media else '❌ None'}\n\n"
        f"When enabled, auto-replies to all PMs.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def kw_menu(q, uid):
    kws = db.get_keywords(uid, active_only=False)
    total = len(kws)
    active = sum(1 for k in kws if k["is_active"])
    plan_config = db.get_plan_config(uid)
    max_kw = plan_config["max_keywords"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add Keyword", callback_data="kw_add"
            ),
            InlineKeyboardButton(
                "📋 List All", callback_data="kw_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔍 Active Only", callback_data="kw_list_active"
            ),
            InlineKeyboardButton(
                "📊 Usage Stats", callback_data="kw_stats"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="kw_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"🔑 *Keywords*\n\n"
        f"Total: {total}/{max_kw} | Active: {active}\n\n"
        f"*Match Types:* contains, exact, startswith, "
        f"endswith, regex{'✅' if db.plan_check(uid, 'regex_keywords') else '🔒'}\n\n"
        f"*Media:* {'✅ Supported' if db.plan_check(uid, 'media_in_replies') else '🔒 Premium+'}\n\n"
        f"_Tip: Add media to keyword replies for richer responses!_",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def filter_menu(q, uid):
    fs = db.get_filters(uid)
    plan_config = db.get_plan_config(uid)
    max_f = plan_config["max_filters"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add Filter", callback_data="filter_add"
            ),
            InlineKeyboardButton(
                "📋 List", callback_data="filter_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="filter_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"📝 *Filters*\n\n"
        f"Total: {len(fs)}/{max_f}\n\n"
        f"Filters match words anywhere in the message "
        f"and auto-reply.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def pm_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("pm_permit", "false") == "true"
    limit = s.get("pm_limit", "3")
    approved = db.approved_count(uid)
    has_media = bool(s.get("pm_media_id"))

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="pm_toggle",
        )],
        [
            InlineKeyboardButton(
                "✏️ Set Message", callback_data="pm_set_msg"
            ),
            InlineKeyboardButton(
                "👥 Approved", callback_data="pm_approved"
            ),
        ],
        [
            InlineKeyboardButton(
                f"📎 {'Change' if has_media else 'Add'} Media",
                callback_data="pm_media",
            ),
            InlineKeyboardButton(
                f"⚠️ Limit: {limit}",
                callback_data="pm_limit_menu",
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"🛡️ *PM Permit*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n"
        f"Warn Limit: {limit}\n"
        f"Approved Users: {approved}\n"
        f"Media: {'✅' if has_media else '❌'}\n\n"
        f"When enabled, unapproved users get warned "
        f"before being blocked.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def spam_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("anti_spam", "false") == "true"
    limit = s.get("spam_limit", "5")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="spam_toggle",
        )],
        [
            InlineKeyboardButton(
                f"📊 Limit: {limit}/min",
                callback_data="spam_set_limit",
            ),
            InlineKeyboardButton(
                "✏️ Warning Msg",
                callback_data="spam_set_msg",
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"🔇 *Anti-Spam*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n"
        f"Limit: {limit} messages/minute\n\n"
        f"Automatically blocks users who spam your PMs.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def bw_menu(q, uid):
    words = db.get_blocked(uid)
    plan_config = db.get_plan_config(uid)
    max_bw = plan_config["max_blocked_words"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add Word", callback_data="bw_add"
            ),
            InlineKeyboardButton(
                "📋 List", callback_data="bw_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="bw_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"🚫 *Blocked Words*\n\n"
        f"Total: {len(words)}/{max_bw}\n\n"
        f"*Actions per word:*\n"
        f"• `warn` — Send warning message\n"
        f"• `delete` — Delete the message\n"
        f"• `mute` — Mute notification\n\n"
        f"Format: `word | action`\n"
        f"Example: `spam | delete`",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def wl_menu(q, uid):
    wl = db.get_whitelist(uid)
    plan_config = db.get_plan_config(uid)
    max_wl = plan_config["max_whitelist"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add User", callback_data="wl_add"
            ),
            InlineKeyboardButton(
                "📋 List", callback_data="wl_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="wl_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"📋 *Whitelist*\n\n"
        f"Users: {len(wl)}/{max_wl}\n\n"
        f"Whitelisted users bypass:\n"
        f"• PM Permit\n"
        f"• Anti-Spam\n"
        f"• Blocked Words\n\n"
        f"Send user ID or @username to add.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def sched_menu(q, uid):
    items = db.user_scheduled(uid)
    plan_config = db.get_plan_config(uid)
    max_s = plan_config["max_scheduled"]
    can_recur = db.plan_check(uid, "recurring_schedule")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "➕ New Schedule", callback_data="sched_add"
        )],
        [InlineKeyboardButton(
            "📋 View Pending", callback_data="sched_list"
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"⏰ *Scheduled Messages*\n\n"
        f"Pending: {len(items)}/{max_s}\n"
        f"Recurring: {'✅ Available' if can_recur else '🔒 Premium+'}\n\n"
        f"*How to schedule:*\n"
        f"1️⃣ Tap ➕ New Schedule\n"
        f"2️⃣ Enter target (username or ID)\n"
        f"3️⃣ Type your message\n"
        f"4️⃣ Set date and time\n"
        f"5️⃣ Optionally attach media",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def fwd_menu(q, uid):
    rules = db.get_forwards(uid)
    plan_config = db.get_plan_config(uid)
    max_f = plan_config["max_forwards"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add Rule", callback_data="fwd_add"
            ),
            InlineKeyboardButton(
                "📋 View Rules", callback_data="fwd_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="fwd_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"↗️ *Auto-Forward*\n\n"
        f"Active Rules: {len(rules)}/{max_f}\n\n"
        f"Automatically forward messages from one chat to another.\n\n"
        f"*Steps:*\n"
        f"1️⃣ Set source chat (ID or @username)\n"
        f"2️⃣ Set destination chat\n"
        f"3️⃣ Messages will be forwarded automatically",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def profile_menu(q, uid):
    cli = get_client(uid)
    status = "🟢 Connected" if cli else "🔴 Not Connected"

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✏️ Set Bio", callback_data="profile_bio"
            ),
            InlineKeyboardButton(
                "📛 Set Name", callback_data="profile_name"
            ),
        ],
        [
            InlineKeyboardButton(
                "🖼️ Set Photo", callback_data="profile_pic"
            ),
            InlineKeyboardButton(
                "🗑️ Remove Photo",
                callback_data="profile_rmpic",
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"👤 *Profile Tools*\n\n"
        f"Status: {status}\n\n"
        f"Manage your Telegram profile directly from the bot.\n\n"
        f"• Set Bio (max 70 characters)\n"
        f"• Set Name: `FirstName | LastName`\n"
        f"• Upload or remove profile photo",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def tmpl_menu(q, uid):
    templates = db.get_templates(uid, include_global=False)
    plan_config = db.get_plan_config(uid)
    max_t = plan_config["max_templates"]

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Create Template", callback_data="tmpl_add"
            ),
            InlineKeyboardButton(
                "📋 My Templates", callback_data="tmpl_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🌐 Global Templates",
                callback_data="tmpl_global",
            ),
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="tmpl_clear"
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"📑 *Templates*\n\n"
        f"Your Templates: {len(templates)}/{max_t}\n\n"
        f"Save frequently used messages as templates "
        f"for quick access.\n"
        f"Templates support media attachments.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def react_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("auto_react", "false") == "true"
    emoji = s.get("react_emoji", "👍")
    can_react = db.plan_check(uid, "auto_react")

    if not can_react:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💎 Upgrade to Premium", url=SUPPORT_URL
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ])
        await q.edit_message_text(
            f"😍 *Auto-React*\n\n"
            f"🔒 This feature requires *Premium* or *VIP* plan.\n\n"
            f"Auto-react adds emoji reactions to incoming PMs.\n\n"
            f"Contact {SUPPORT_USERNAME} to upgrade!",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    emoji_rows = []
    row = []
    for i, em in enumerate(REACTION_EMOJIS):
        selected = "✅" if em == emoji else ""
        row.append(InlineKeyboardButton(
            f"{em}{selected}",
            callback_data=f"react_set_{em}",
        ))
        if len(row) == 6:
            emoji_rows.append(row)
            row = []
    if row:
        emoji_rows.append(row)

    rows = [
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="react_toggle",
        )],
    ] + emoji_rows + [
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ]

    await q.edit_message_text(
        f"😍 *Auto-React*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n"
        f"Current Emoji: {emoji}\n\n"
        f"Tap an emoji to select it:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def wh_menu(q, uid):
    s = db.all_settings(uid)
    on = s.get("working_hours", "false") == "true"
    can_wh = db.plan_check(uid, "working_hours")

    if not can_wh:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💎 Upgrade to Premium", url=SUPPORT_URL
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ])
        await q.edit_message_text(
            f"⏰ *Working Hours*\n\n"
            f"🔒 Requires *Premium* or *VIP* plan.\n\n"
            f"Set working hours so the bot auto-replies "
            f"outside those hours.\n\n"
            f"Contact {SUPPORT_USERNAME} to upgrade!",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    hours = db.get_working_hours(uid)
    hours_text = ""
    if hours:
        for h in hours:
            day_name = DAYS_OF_WEEK[h["day"]] if h["day"] < 7 else "?"
            status = "🟢" if h["is_active"] else "🔴"
            hours_text += (
                f"{status} {day_name}: "
                f"{h['start_hr']:02d}:{h['start_min']:02d} - "
                f"{h['end_hr']:02d}:{h['end_min']:02d}\n"
            )
    else:
        hours_text = "_Not configured yet._"

    rows = [
        [InlineKeyboardButton(
            f"{'🟢 ON' if on else '🔴 OFF'} — Toggle",
            callback_data="wh_toggle",
        )],
    ]
    # Day buttons
    day_row1 = []
    day_row2 = []
    for i, day in enumerate(DAYS_OF_WEEK):
        btn = InlineKeyboardButton(
            day[:3], callback_data=f"wh_day_{i}"
        )
        if i < 4:
            day_row1.append(btn)
        else:
            day_row2.append(btn)
    rows.append(day_row1)
    rows.append(day_row2)
    rows.append([
        InlineKeyboardButton(
            "✏️ Set Outside-Hours Msg",
            callback_data="wh_set_msg",
        ),
    ])
    rows.append([
        InlineKeyboardButton(
            "🗑️ Clear All", callback_data="wh_clear"
        ),
        InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        ),
    ])

    await q.edit_message_text(
        f"⏰ *Working Hours*\n\n"
        f"Status: {'🟢 On' if on else '🔴 Off'}\n\n"
        f"{hours_text}\n\n"
        f"Tap a day to set hours.\n"
        f"Format: `HH:MM-HH:MM`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def notes_menu(q, uid):
    notes = db.get_notes(uid)
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ New Note", callback_data="note_add"
            ),
            InlineKeyboardButton(
                "📋 View Notes", callback_data="note_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="note_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"📒 *Notes*\n\n"
        f"Total: {len(notes)}\n\n"
        f"Save personal notes, snippets, and reminders.\n"
        f"Notes support media attachments.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def ccmd_menu(q, uid):
    can_ccmd = db.plan_check(uid, "custom_commands")
    cmds = db.get_custom_cmds(uid) if can_ccmd else []

    if not can_ccmd:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💎 Upgrade to Premium", url=SUPPORT_URL
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ])
        await q.edit_message_text(
            f"🤖 *Custom Commands*\n\n"
            f"🔒 Requires *Premium* or *VIP* plan.\n\n"
            f"Create custom `/commands` for your userbot.\n\n"
            f"Contact {SUPPORT_USERNAME} to upgrade!",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "➕ Add Command", callback_data="ccmd_add"
            ),
            InlineKeyboardButton(
                "📋 List", callback_data="ccmd_list"
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear All", callback_data="ccmd_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        f"🤖 *Custom Commands*\n\n"
        f"Total: {len(cmds)}\n\n"
        f"Create custom slash commands that work in your PMs.\n"
        f"Example: `/hello` → replies with your custom message.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def stats_menu(q, uid):
    stats = db.all_stats(uid)
    if not stats:
        body = "_No stats recorded yet._"
    else:
        lines = []
        display_stats = {
            "messages_received": "📨 Messages Received",
            "keyword_replies": "🔑 Keyword Replies",
            "filter_replies": "📝 Filter Replies",
            "welcome_sent": "💬 Welcomes Sent",
            "away_sent": "🌙 Away Replies",
            "pm_warnings_sent": "⚠️ PM Warnings",
            "pm_blocked": "🚫 PM Blocked",
            "spam_blocked": "🔇 Spam Blocked",
            "messages_forwarded": "↗️ Messages Forwarded",
            "auto_reacted": "😍 Auto Reactions",
            "blocked_word_triggered": "🚫 Blocked Words Hit",
            "custom_cmd_used": "🤖 Custom Cmds Used",
        }
        for key, label in display_stats.items():
            val = stats.get(key, 0)
            if val > 0:
                lines.append(f"{label}: *{val}*")
        body = "\n".join(lines) if lines else "_No meaningful stats yet._"

    can_advanced = db.plan_check(uid, "advanced_stats")
    rows = [
        [InlineKeyboardButton(
            "🔄 Reset Stats", callback_data="stats_reset"
        )],
    ]
    if can_advanced:
        rows.insert(0, [InlineKeyboardButton(
            "📈 Daily Analytics", callback_data="stats_daily"
        )])
    rows.append([InlineKeyboardButton(
        "◀️ Back", callback_data="main_menu"
    )])

    await q.edit_message_text(
        f"📊 *Your Statistics*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def logs_menu(q, uid):
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📋 All Logs", callback_data="logs_view"
            ),
            InlineKeyboardButton(
                "🔑 Keyword Logs",
                callback_data="logs_kw",
            ),
        ],
        [
            InlineKeyboardButton(
                "🛡️ PM Logs", callback_data="logs_pm"
            ),
            InlineKeyboardButton(
                "🔇 Spam Logs",
                callback_data="logs_spam",
            ),
        ],
        [
            InlineKeyboardButton(
                "🗑️ Clear Logs", callback_data="logs_clear"
            ),
            InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            ),
        ],
    ])
    await q.edit_message_text(
        "📜 *Activity Logs*\n\n"
        "View recent bot activity by category.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def help_menu(q, uid):
    text = (
        f"❓ *{BOT_NAME} Quick Help*\n\n"
        f"═══ *Getting Started* ═══\n"
        f"1. Login with your phone number\n"
        f"2. Enter OTP (and 2FA if enabled)\n"
        f"3. Configure features from the menu\n\n"
        f"═══ *Key Features* ═══\n"
        f"💬 *Welcome* — Auto-greet new PMers\n"
        f"🌙 *Away* — Auto-reply when absent\n"
        f"🔑 *Keywords* — Smart auto-replies\n"
        f"📝 *Filters* — Content-based replies\n"
        f"🛡️ *PM Permit* — Control who can PM\n"
        f"🔇 *Anti-Spam* — Block spammers\n"
        f"⏰ *Schedule* — Send messages later\n"
        f"↗️ *Forward* — Auto-forward messages\n"
        f"😍 *React* — Auto-add reactions\n"
        f"📑 *Templates* — Reusable messages\n\n"
        f"═══ *Commands* ═══\n"
        f"/start — Main menu\n"
        f"/help — This help page\n"
        f"/status — Connection status\n"
        f"/cancel — Cancel action\n"
        f"/plan — Plan details\n"
        f"/stats — Usage stats\n"
        f"/export — Backup settings\n"
        f"/feedback — Send feedback\n\n"
        f"Support: {SUPPORT_USERNAME}"
    )
    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📖 Full Guide", callback_data="help_full"
            )],
            [InlineKeyboardButton(
                "💬 Contact Support", url=SUPPORT_URL
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ]),
    )


async def settings_menu(q, uid):
    s = db.all_settings(uid)
    plan = db.get_plan(uid)

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{fmt_bool(s.get('welcome', 'false'))} Welcome",
                callback_data="welcome_menu",
            ),
            InlineKeyboardButton(
                f"{fmt_bool(s.get('away', 'false'))} Away",
                callback_data="away_menu",
            ),
        ],
        [
            InlineKeyboardButton(
                f"{fmt_bool(s.get('pm_permit', 'false'))} PM Permit",
                callback_data="pm_menu",
            ),
            InlineKeyboardButton(
                f"{fmt_bool(s.get('anti_spam', 'false'))} Anti-Spam",
                callback_data="spam_menu",
            ),
        ],
        [
            InlineKeyboardButton(
                f"{fmt_bool(s.get('auto_react', 'false'))} Auto-React",
                callback_data="react_menu",
            ),
            InlineKeyboardButton(
                f"{fmt_bool(s.get('working_hours', 'false'))} Work Hours",
                callback_data="wh_menu",
            ),
        ],
        [
            InlineKeyboardButton(
                "🔑 Keywords", callback_data="kw_menu"
            ),
            InlineKeyboardButton(
                "📝 Filters", callback_data="filter_menu"
            ),
        ],
        [
            InlineKeyboardButton(
                "🚫 Blocked", callback_data="bw_menu"
            ),
            InlineKeyboardButton(
                "📋 Whitelist", callback_data="wl_menu"
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"⚙️ *Settings Overview*\n\n"
        f"Plan: {fmt_plan(plan)}\n"
        f"🟢 = Enabled | 🔴 = Disabled\n\n"
        f"Tap a feature to configure it.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def account_menu(q, uid):
    cli = get_client(uid)
    if not cli:
        await q.edit_message_text(
            "❌ Not connected. Use *🔄 Reconnect* from the menu.",
            parse_mode="Markdown",
            reply_markup=back_btn(),
        )
        return
    try:
        me = await cli.get_me()
        plan = db.get_plan(uid)
        text = (
            f"📱 *Account Info*\n\n"
            f"👤 Name: {me.first_name or ''} {me.last_name or ''}\n"
            f"📛 Username: @{me.username or 'not set'}\n"
            f"🆔 ID: `{me.id}`\n"
            f"📞 Phone: `+{me.phone}`\n"
            f"✅ TG Premium: "
            f"{'Yes' if getattr(me, 'premium', False) else 'No'}\n"
            f"💎 Bot Plan: {fmt_plan(plan)}\n"
            f"🟢 Status: Connected"
        )
    except Exception as exc:
        text = f"❌ Error loading account: `{exc}`"

    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "🔄 Refresh", callback_data="account"
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ]),
    )


async def backup_menu(q, uid):
    can_backup = db.plan_check(uid, "backup_export")
    if not can_backup:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💎 Upgrade", url=SUPPORT_URL
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="main_menu"
            )],
        ])
        await q.edit_message_text(
            f"📥 *Backup & Restore*\n\n"
            f"🔒 Requires *Premium* or *VIP* plan.\n\n"
            f"Export your settings, keywords, filters, etc.\n"
            f"Import them later to restore.\n\n"
            f"Contact {SUPPORT_USERNAME} to upgrade!",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📤 Export Settings", callback_data="backup_export"
        )],
        [InlineKeyboardButton(
            "📥 Import Settings", callback_data="backup_import"
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"📥 *Backup & Restore*\n\n"
        f"• Export saves all your settings as a JSON file\n"
        f"• Import restores settings from a JSON file\n\n"
        f"⚠️ Import adds to existing settings (doesn't overwrite).",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def feedback_menu(q, uid):
    fb = db.user_feedback(uid)
    pending = sum(1 for f in fb if f["status"] == "pending")
    replied = sum(1 for f in fb if f["status"] == "replied")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✏️ Send Feedback", callback_data="fb_send"
        )],
        [InlineKeyboardButton(
            "📋 My Feedback", callback_data="fb_list"
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])
    await q.edit_message_text(
        f"💬 *Feedback*\n\n"
        f"Pending: {pending}\n"
        f"Replied: {replied}\n\n"
        f"Send feedback, suggestions, or bug reports to the admin.",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║                     ADMIN MENUS                              ║
# ╚══════════════════════════════════════════════════════════════╝


async def admin_home(q, uid):
    if not is_admin(uid):
        await q.edit_message_text(
            "⛔ Admin access denied.", reply_markup=back_btn()
        )
        return

    total = db.total_users()
    sessions = db.active_sessions_count()
    banned = len(db.banned_users())
    plans = db.users_by_plan_count()

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"👥 Users ({total})",
                callback_data="admin_users",
            ),
            InlineKeyboardButton(
                "📊 Stats", callback_data="admin_stats"
            ),
        ],
        [
            InlineKeyboardButton(
                "📢 Broadcast", callback_data="admin_broadcast"
            ),
            InlineKeyboardButton(
                "🔍 Search", callback_data="admin_search"
            ),
        ],
        [
            InlineKeyboardButton(
                f"💎 Plans", callback_data="admin_plans"
            ),
            InlineKeyboardButton(
                f"🚫 Banned ({banned})",
                callback_data="admin_banned",
            ),
        ],
        [
            InlineKeyboardButton(
                "⏳ Expiring Plans",
                callback_data="admin_expiring",
            ),
            InlineKeyboardButton(
                "💬 Feedback", callback_data="admin_feedback"
            ),
        ],
        [
            InlineKeyboardButton(
                "📢 Announcement",
                callback_data="admin_announce",
            ),
            InlineKeyboardButton(
                "📜 Logs", callback_data="admin_logs"
            ),
        ],
        [
            InlineKeyboardButton(
                "🧹 Cleanup", callback_data="admin_cleanup"
            ),
            InlineKeyboardButton(
                "📤 Upload DB", callback_data="admin_upload_db"
            ),
        ],
        [
            InlineKeyboardButton(
                "📥 Download DB",
                callback_data="admin_download_db",
            ),
            InlineKeyboardButton(
                "🌐 Global Templates",
                callback_data="admin_global_tmpl",
            ),
        ],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="main_menu"
        )],
    ])

    free_cnt = plans.get("free", 0)
    prem_cnt = plans.get("premium", 0)
    vip_cnt = plans.get("vip", 0)

    await q.edit_message_text(
        f"👑 *Admin Panel*\n\n"
        f"*Users:* {total}\n"
        f"*Sessions:* {sessions}\n"
        f"*Banned:* {banned}\n\n"
        f"*Plans:*\n"
        f"🆓 Free: {free_cnt}\n"
        f"⭐ Premium: {prem_cnt}\n"
        f"👑 VIP: {vip_cnt}\n\n"
        f"*DB Size:* {db.db_size()}",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def admin_users_menu(q, uid):
    users = db.all_users()[:50]
    if not users:
        await q.edit_message_text(
            "👥 No users yet.",
            reply_markup=back_btn("admin_home"),
        )
        return

    text = "👥 *All Users (Latest 50)*\n\n"
    for u in users[:25]:
        plan_icon = {
            "free": "🆓", "premium": "⭐", "vip": "👑"
        }.get(u["plan"] or "free", "🆓")
        banned_icon = "🚫" if u["is_banned"] else ""
        session_icon = "🟢" if u["session_str"] else "🔴"
        text += (
            f"{session_icon}{plan_icon}{banned_icon} "
            f"`{u['user_id']}` "
            f"@{u['username'] or 'none'} — "
            f"{u['first_name'] or 'N/A'}\n"
        )

    if len(users) > 25:
        text += f"\n_... and {len(users) - 25} more_"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "🔍 Search User", callback_data="admin_search"
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="admin_home"
        )],
    ])
    await q.edit_message_text(
        text, parse_mode="Markdown", reply_markup=kb
    )


async def admin_stats_menu(q, uid):
    g_stats = db.global_stats()
    total = db.total_users()
    sessions = db.active_sessions_count()
    active_7d = len(db.active_users(7))
    active_1d = len(db.active_users(1))

    stats_text = ""
    display = {
        "messages_received": "📨 Total Messages",
        "keyword_replies": "🔑 Keyword Replies",
        "filter_replies": "📝 Filter Replies",
        "welcome_sent": "💬 Welcomes Sent",
        "away_sent": "🌙 Away Replies",
        "spam_blocked": "🔇 Spam Blocked",
        "pm_blocked": "🚫 PM Blocked",
        "messages_forwarded": "↗️ Forwarded",
        "auto_reacted": "😍 Reactions",
    }
    for key, label in display.items():
        val = g_stats.get(key, 0)
        if val:
            stats_text += f"{label}: *{val}*\n"

    if not stats_text:
        stats_text = "_No global stats yet._"

    text = (
        f"📊 *Global Statistics*\n\n"
        f"*Users:*\n"
        f"• Total: *{total}*\n"
        f"• Active (24h): *{active_1d}*\n"
        f"• Active (7d): *{active_7d}*\n"
        f"• Sessions: *{sessions}*\n\n"
        f"*Activity:*\n{stats_text}\n"
        f"*DB Size:* {db.db_size()}"
    )
    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=back_btn("admin_home"),
    )


async def admin_plans_menu(q, uid):
    premium = db.premium_users()
    vip = db.vip_users()

    text = "💎 *Plan Management*\n\n"
    if premium:
        text += "*⭐ Premium Users:*\n"
        for u in premium[:15]:
            exp = u["plan_until"] or "N/A"
            text += f"• `{u['user_id']}` @{u['username'] or 'none'} — until `{exp[:10]}`\n"
    else:
        text += "_No premium users._\n"

    text += "\n"
    if vip:
        text += "*👑 VIP Users:*\n"
        for u in vip[:15]:
            exp = u["plan_until"] or "N/A"
            text += f"• `{u['user_id']}` @{u['username'] or 'none'} — until `{exp[:10]}`\n"
    else:
        text += "_No VIP users._\n"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "🔍 Set Plan for User",
            callback_data="admin_set_plan",
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="admin_home"
        )],
    ])
    await q.edit_message_text(
        text, parse_mode="Markdown", reply_markup=kb
    )


async def admin_banned_menu(q, uid):
    users = db.banned_users()
    if not users:
        body = "_No banned users._"
    else:
        body = "\n".join(
            f"• `{u['user_id']}` @{u['username'] or 'none'} "
            f"— {u['ban_reason'] or 'No reason'}"
            for u in users[:30]
        )
    await q.edit_message_text(
        f"🚫 *Banned Users*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=back_btn("admin_home"),
    )


async def admin_expiring_menu(q, uid):
    users = db.expiring_plans(7)
    if not users:
        body = "_No plans expiring in 7 days._"
    else:
        body = "\n".join(
            f"• `{u['user_id']}` @{u['username'] or 'none'} "
            f"— {u['plan']} expires `{(u['plan_until'] or '')[:10]}`"
            for u in users[:30]
        )

    await q.edit_message_text(
        f"⏳ *Expiring Plans (7 days)*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=back_btn("admin_home"),
    )


async def admin_feedback_menu(q, uid):
    feedback = db.get_all_feedback("pending")
    if not feedback:
        body = "_No pending feedback._"
    else:
        body = ""
        for fb in feedback[:15]:
            body += (
                f"*#{fb['id']}* from `{fb['user_id']}` "
                f"(@{fb['username'] or 'none'})\n"
                f"📝 {truncate(fb['message'], 80)}\n"
                f"📅 {(fb['created_at'] or '')[:16]}\n\n"
            )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📋 All Feedback", callback_data="admin_fb_all"
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="admin_home"
        )],
    ])
    await q.edit_message_text(
        f"💬 *Pending Feedback*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def admin_logs_menu(q, uid):
    rows = db.all_logs(30)
    if not rows:
        body = "_No logs._"
    else:
        body = "\n".join(
            f"• `{r['ts'][:16]}` | `{r['user_id']}` | "
            f"*{r['action']}* | {truncate(r['detail'] or '', 30)}"
            for r in rows
        )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "🗑️ Clear All Logs",
            callback_data="admin_clrlogs",
        )],
        [InlineKeyboardButton(
            "◀️ Back", callback_data="admin_home"
        )],
    ])
    await q.edit_message_text(
        f"📜 *Admin Logs (Latest 30)*\n\n{body}",
        parse_mode="Markdown",
        reply_markup=kb,
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║                  CALLBACK ROUTER                             ║
# ╚══════════════════════════════════════════════════════════════╝


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Master callback query handler."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    uid = q.from_user.id
    data = q.data

    if db.is_banned(uid) and data != "main_menu":
        await q.edit_message_text(
            f"⛔ You are banned.\nContact {SUPPORT_USERNAME}"
        )
        return

    db.touch_user(uid)

    # ═══════════ NAVIGATION ═══════════

    if data == "main_menu":
        ctx.user_data.clear()
        await show_main(q, uid)
        return

    if data == "cancel_state":
        tmp = ctx.user_data.pop("tmp_client", None)
        if tmp:
            try:
                await tmp.disconnect()
            except Exception:
                pass
        ctx.user_data.clear()
        await show_main(q, uid)
        return

    # ═══════════ LOGIN ═══════════

    if data == "login_start":
        if db.get_session(uid):
            await q.edit_message_text(
                "✅ You're already logged in!",
                reply_markup=back_btn(),
            )
        else:
            await ask_state(
                q, ctx, ST_PHONE,
                "🔐 *Login*\n\n"
                "Send your phone number with country code.\n"
                "Example: `+1234567890`",
            )
        return

    if data == "logout_ask":
        await q.edit_message_text(
            "🚪 *Logout Confirmation*\n\n"
            "This will disconnect your account from the bot.\n"
            "All your settings will be preserved.\n\n"
            "Are you sure?",
            parse_mode="Markdown",
            reply_markup=confirm_btns("logout_confirm", "main_menu"),
        )
        return

    if data == "logout_confirm":
        await stop_client(uid)
        db.remove_session(uid)
        db.log(uid, "logout", "", "auth")
        await q.edit_message_text(
            "✅ Logged out successfully.\n"
            "Your settings are saved.",
            reply_markup=back_btn(),
        )
        return

    if data == "reconnect":
        await q.edit_message_text("🔄 Reconnecting...")
        client = await start_client(uid)
        if client:
            await q.edit_message_text(
                "✅ Successfully reconnected!",
                reply_markup=back_btn(),
            )
        else:
            await q.edit_message_text(
                "❌ Reconnection failed.\n"
                "Try logging in again.",
                reply_markup=back_btn(),
            )
        return

    if data == "account":
        await account_menu(q, uid)
        return

    if data == "settings_menu":
        await settings_menu(q, uid)
        return

    if data == "help_menu":
        await help_menu(q, uid)
        return

    if data == "help_full":
        await cmd_help.__wrapped__(update, ctx) if hasattr(cmd_help, '__wrapped__') else None
        text = (
            f"📖 *Complete {BOT_NAME} Guide*\n\n"
            f"═══ *Admin Instructions* ═══\n"
            f"(for bot owner: {SUPPORT_USERNAME})\n\n"
            f"*Admin Commands:*\n"
            f"• Search users by ID or username\n"
            f"• Ban/Unban users with reasons\n"
            f"• Set plans (Free/Premium/VIP)\n"
            f"• Broadcast to all or filtered users\n"
            f"• View global stats and logs\n"
            f"• Manage feedback\n"
            f"• Database backup/restore\n"
            f"• Create global templates\n\n"
            f"═══ *User Instructions* ═══\n\n"
            f"*1. Login:*\n"
            f"Enter your phone number → OTP → 2FA (if any)\n\n"
            f"*2. Welcome Message:*\n"
            f"Auto-greet users who PM you for the first time.\n"
            f"Supports text + media. Use variables like {{name}}.\n\n"
            f"*3. Keywords:*\n"
            f"Set trigger words and auto-reply messages.\n"
            f"Match types: contains, exact, startswith, endswith, regex.\n"
            f"Attach photos/videos/docs to replies.\n\n"
            f"*4. PM Permit:*\n"
            f"Only approved users can PM you.\n"
            f"Others get warned then blocked.\n\n"
            f"*5. Anti-Spam:*\n"
            f"Auto-block users sending too many messages.\n\n"
            f"*6. Scheduled Messages:*\n"
            f"Schedule messages for specific date/time.\n"
            f"Premium: recurring schedules.\n\n"
            f"*7. Auto-Forward:*\n"
            f"Copy messages from one chat to another automatically.\n\n"
            f"*8. Working Hours:*\n"
            f"Set business hours. Auto-reply outside hours.\n\n"
            f"*9. Templates:*\n"
            f"Save reusable message snippets.\n\n"
            f"*10. Backup:*\n"
            f"Export all settings as JSON. Import later."
        )
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("help_menu"),
        )
        return

    # ═══════════ PLAN ═══════════

    if data == "plan_menu" or data == "plan_info":
        await show_plan_info(q, uid)
        return

    # ═══════════ WELCOME ═══════════

    if data == "welcome_menu":
        await welcome_menu(q, uid)
        return

    if data == "welcome_toggle":
        cur = db.get_setting(uid, "welcome", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "welcome", new_val)
        db.log(uid, "welcome_toggle", new_val, "settings")
        await welcome_menu(q, uid)
        return

    if data == "welcome_mode_tog":
        cur = db.get_setting(uid, "welcome_mode", "first_time")
        new_val = "always" if cur == "first_time" else "first_time"
        db.set_setting(uid, "welcome_mode", new_val)
        await welcome_menu(q, uid)
        return

    if data == "welcome_set":
        await ask_state(
            q, ctx, ST_WELCOME_MSG,
            "✏️ *Set Welcome Message*\n\n"
            "Send your welcome message text.\n\n"
            "*Variables:*\n"
            "`{name}` `{username}` `{id}` `{mention}`\n"
            "`{fullname}` `{time}` `{date}` `{day}`",
        )
        return

    if data == "welcome_view":
        msg = db.get_setting(uid, "welcome_msg", "_Not set_")
        has_media = bool(db.get_setting(uid, "welcome_media_id"))
        media_text = "\n\n📎 Media: ✅ Attached" if has_media else ""
        await q.edit_message_text(
            f"📋 *Welcome Message:*\n\n{msg}{media_text}",
            parse_mode="Markdown",
            reply_markup=back_btn("welcome_menu"),
        )
        return

    if data == "welcome_media":
        if not db.plan_check(uid, "media_in_replies"):
            await q.edit_message_text(
                f"🔒 Media in replies requires *Premium+*.\n"
                f"Contact {SUPPORT_USERNAME} to upgrade!",
                parse_mode="Markdown",
                reply_markup=back_btn("welcome_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_WELCOME_MEDIA,
            "📎 *Attach Welcome Media*\n\n"
            "Send a photo, video, GIF, document, or voice message.\n"
            "This will be sent along with your welcome text.",
        )
        return

    if data == "welcome_rm_media":
        db.del_setting(uid, "welcome_media_id")
        db.del_setting(uid, "welcome_media_type")
        await q.edit_message_text(
            "✅ Welcome media removed.",
            reply_markup=back_btn("welcome_menu"),
        )
        return

    if data == "welcome_del":
        db.del_setting(uid, "welcome_msg")
        db.del_setting(uid, "welcome_media_id")
        db.del_setting(uid, "welcome_media_type")
        db.set_setting(uid, "welcome", "false")
        await q.edit_message_text(
            "🗑️ Welcome message and media deleted.",
            reply_markup=back_btn("welcome_menu"),
        )
        return

    # ═══════════ AWAY ═══════════

    if data == "away_menu":
        await away_menu(q, uid)
        return

    if data == "away_toggle":
        cur = db.get_setting(uid, "away", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "away", new_val)
        db.log(uid, "away_toggle", new_val, "settings")
        await away_menu(q, uid)
        return

    if data == "away_set":
        await ask_state(
            q, ctx, ST_AWAY_MSG,
            "✏️ *Set Away Message*\n\n"
            "Send your away/AFK message.\n\n"
            "Variables: `{name}` `{username}` `{time}` `{date}`",
        )
        return

    if data == "away_view":
        msg = db.get_setting(uid, "away_msg", "_Not set_")
        has_media = bool(db.get_setting(uid, "away_media_id"))
        media_text = "\n\n📎 Media: ✅ Attached" if has_media else ""
        await q.edit_message_text(
            f"📋 *Away Message:*\n\n{msg}{media_text}",
            parse_mode="Markdown",
            reply_markup=back_btn("away_menu"),
        )
        return

    if data == "away_media":
        if not db.plan_check(uid, "media_in_replies"):
            await q.edit_message_text(
                f"🔒 Media requires *Premium+*.\n"
                f"Contact {SUPPORT_USERNAME}",
                parse_mode="Markdown",
                reply_markup=back_btn("away_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_AWAY_MEDIA,
            "📎 *Attach Away Media*\n\n"
            "Send a photo, video, GIF, document, or voice.",
        )
        return

    if data == "away_rm_media":
        db.del_setting(uid, "away_media_id")
        db.del_setting(uid, "away_media_type")
        await q.edit_message_text(
            "✅ Away media removed.",
            reply_markup=back_btn("away_menu"),
        )
        return

    if data == "away_del":
        db.del_setting(uid, "away_msg")
        db.del_setting(uid, "away_media_id")
        db.del_setting(uid, "away_media_type")
        db.set_setting(uid, "away", "false")
        await q.edit_message_text(
            "🗑️ Away message and media deleted.",
            reply_markup=back_btn("away_menu"),
        )
        return

    # ═══════════ KEYWORDS ═══════════

    if data == "kw_menu":
        await kw_menu(q, uid)
        return

    if data == "kw_add":
        limit = db.plan_limit(uid, "max_keywords")
        current = db.keyword_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_keywords", current),
                reply_markup=back_btn("kw_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_KW_TRIGGER,
            "➕ *Add Keyword*\n\n"
            "Send: `keyword | match_type`\n\n"
            "*Match types:*\n"
            "• `contains` — found anywhere (default)\n"
            "• `exact` — exact match\n"
            "• `startswith` — starts with trigger\n"
            "• `endswith` — ends with trigger\n"
            "• `regex` — regular expression (Premium+)\n\n"
            "*Examples:*\n"
            "`hello | contains`\n"
            "`hi there | exact`\n"
            "`price | startswith`",
        )
        return

    if data == "kw_list" or data == "kw_list_active":
        active_only = data == "kw_list_active"
        kws = db.get_keywords(uid, active_only=active_only)
        if not kws:
            label = "active keywords" if active_only else "keywords"
            await q.edit_message_text(
                f"📋 No {label} found.",
                reply_markup=back_btn("kw_menu"),
            )
            return

        rows = []
        text = "📋 *Keywords* — tap to toggle | 🗑️ delete\n\n"
        for kw in kws[:20]:
            icon = "🟢" if kw["is_active"] else "🔴"
            media = " 📎" if kw["media_file_id"] else ""
            text += (
                f"{icon} `{kw['trigger_text']}` "
                f"({kw['match_type']}) → "
                f"{truncate(kw['response'], 30)}{media}\n"
                f"   Used: {kw['used_count']} times\n\n"
            )
            rows.append([
                InlineKeyboardButton(
                    f"{icon} {truncate(kw['trigger_text'], 15)}",
                    callback_data=f"kw_tog_{kw['id']}",
                ),
                InlineKeyboardButton(
                    "🗑️", callback_data=f"kw_del_{kw['id']}"
                ),
            ])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="kw_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "kw_stats":
        kws = db.get_keywords(uid, active_only=False)
        if not kws:
            await q.edit_message_text(
                "📊 No keyword stats yet.",
                reply_markup=back_btn("kw_menu"),
            )
            return
        text = "📊 *Keyword Usage Stats*\n\n"
        sorted_kws = sorted(
            kws, key=lambda x: x["used_count"], reverse=True
        )
        for kw in sorted_kws[:20]:
            icon = "🟢" if kw["is_active"] else "🔴"
            text += (
                f"{icon} `{kw['trigger_text']}` — "
                f"*{kw['used_count']}* uses\n"
            )
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("kw_menu"),
        )
        return

    if data == "kw_clear":
        await q.edit_message_text(
            "⚠️ *Clear ALL keywords?*\n\n"
            "This cannot be undone!",
            parse_mode="Markdown",
            reply_markup=confirm_btns("kw_clear_ok", "kw_menu"),
        )
        return

    if data == "kw_clear_ok":
        db.clear_keywords(uid)
        db.log(uid, "keywords_cleared", "", "keyword")
        await q.edit_message_text(
            "✅ All keywords cleared.",
            reply_markup=back_btn("kw_menu"),
        )
        return

    if data.startswith("kw_tog_"):
        kid = int(data[7:])
        db.toggle_keyword(uid, kid)
        # Re-show list
        kws = db.get_keywords(uid, active_only=False)
        rows = []
        text = "📋 *Keywords* — tap to toggle | 🗑️ delete\n\n"
        for kw in kws[:20]:
            icon = "🟢" if kw["is_active"] else "🔴"
            media = " 📎" if kw["media_file_id"] else ""
            text += (
                f"{icon} `{kw['trigger_text']}` "
                f"({kw['match_type']}) → "
                f"{truncate(kw['response'], 30)}{media}\n\n"
            )
            rows.append([
                InlineKeyboardButton(
                    f"{icon} {truncate(kw['trigger_text'], 15)}",
                    callback_data=f"kw_tog_{kw['id']}",
                ),
                InlineKeyboardButton(
                    "🗑️", callback_data=f"kw_del_{kw['id']}"
                ),
            ])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="kw_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("kw_del_"):
        kid = int(data[7:])
        db.del_keyword(uid, kid)
        db.log(uid, "keyword_deleted", f"id={kid}", "keyword")
        await kw_menu(q, uid)
        return

    # ═══════════ FILTERS ═══════════

    if data == "filter_menu":
        await filter_menu(q, uid)
        return

    if data == "filter_add":
        limit = db.plan_limit(uid, "max_filters")
        current = db.filter_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_filters", current),
                reply_markup=back_btn("filter_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_FILTER_NAME,
            "➕ *Add Filter*\n\n"
            "Send the filter trigger word.\n"
            "When this word appears in a message, "
            "the filter response will be sent.",
        )
        return

    if data == "filter_list":
        fs = db.get_filters(uid)
        if not fs:
            await q.edit_message_text(
                "📋 No filters yet.",
                reply_markup=back_btn("filter_menu"),
            )
            return
        rows = []
        text = "📋 *Filters:*\n\n"
        for f in fs[:20]:
            media = " 📎" if f["media_file_id"] else ""
            text += (
                f"• `{f['name']}` → "
                f"{truncate(f['response'], 40)}{media}\n"
                f"  Used: {f['used_count']} times\n\n"
            )
            rows.append([InlineKeyboardButton(
                f"🗑️ {f['name']}",
                callback_data=f"fdel_{f['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="filter_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "filter_clear":
        db.clear_filters(uid)
        await q.edit_message_text(
            "✅ All filters cleared.",
            reply_markup=back_btn("filter_menu"),
        )
        return

    if data.startswith("fdel_"):
        fid = int(data[5:])
        db.del_filter(uid, fid)
        await filter_menu(q, uid)
        return

    # ═══════════ PM PERMIT ═══════════

    if data == "pm_menu":
        await pm_menu(q, uid)
        return

    if data == "pm_toggle":
        cur = db.get_setting(uid, "pm_permit", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "pm_permit", new_val)
        db.log(uid, "pm_toggle", new_val, "settings")
        await pm_menu(q, uid)
        return

    if data == "pm_set_msg":
        await ask_state(
            q, ctx, ST_PM_MSG,
            "✏️ *Set PM Permit Message*\n\n"
            "This message is sent to unapproved users.\n\n"
            "Variables: `{name}` `{username}` `{id}` `{mention}`",
        )
        return

    if data == "pm_media":
        if not db.plan_check(uid, "media_in_replies"):
            await q.edit_message_text(
                f"🔒 Media requires *Premium+*.",
                parse_mode="Markdown",
                reply_markup=back_btn("pm_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_PM_MEDIA,
            "📎 *Attach PM Permit Media*\n\n"
            "Send media to attach to PM permit messages.",
        )
        return

    if data == "pm_approved":
        approved = db.get_approved(uid)
        if not approved:
            await q.edit_message_text(
                "👥 No approved users.",
                reply_markup=back_btn("pm_menu"),
            )
            return
        rows = []
        text = "👥 *Approved Users:*\n\n"
        for a in approved[:20]:
            auto = " (auto)" if a["auto_approved"] else ""
            text += (
                f"• `{a['approved']}` "
                f"{a['approved_name'] or ''}{auto}\n"
            )
            rows.append([InlineKeyboardButton(
                f"❌ {a['approved']}",
                callback_data=f"pm_rev_{a['approved']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="pm_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "pm_limit_menu":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "1", callback_data="pm_lim_1"
                ),
                InlineKeyboardButton(
                    "3", callback_data="pm_lim_3"
                ),
                InlineKeyboardButton(
                    "5", callback_data="pm_lim_5"
                ),
                InlineKeyboardButton(
                    "10", callback_data="pm_lim_10"
                ),
            ],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="pm_menu"
            )],
        ])
        await q.edit_message_text(
            "⚠️ *Select Warning Limit*\n\n"
            "How many warnings before blocking?",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if data.startswith("pm_lim_"):
        db.set_setting(uid, "pm_limit", data[7:])
        await pm_menu(q, uid)
        return

    if data.startswith("pm_rev_"):
        try:
            sender_id = int(data[7:])
            db.revoke_pm(uid, sender_id)
        except ValueError:
            pass
        await pm_menu(q, uid)
        return

    # ═══════════ ANTI-SPAM ═══════════

    if data == "spam_menu":
        await spam_menu(q, uid)
        return

    if data == "spam_toggle":
        cur = db.get_setting(uid, "anti_spam", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "anti_spam", new_val)
        db.log(uid, "spam_toggle", new_val, "settings")
        await spam_menu(q, uid)
        return

    if data == "spam_set_limit":
        await ask_state(
            q, ctx, ST_SPAM_LIMIT,
            "📊 *Set Spam Limit*\n\n"
            "How many messages per minute before blocking?\n"
            "Send a number (1-50).",
        )
        return

    if data == "spam_set_msg":
        await ask_state(
            q, ctx, ST_SPAM_MSG,
            "✏️ *Set Spam Warning*\n\n"
            "Send the message shown to spammers.",
        )
        return

    # ═══════════ BLOCKED WORDS ═══════════

    if data == "bw_menu":
        await bw_menu(q, uid)
        return

    if data == "bw_add":
        limit = db.plan_limit(uid, "max_blocked_words")
        current = db.blocked_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_blocked_words", current),
                reply_markup=back_btn("bw_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_BLOCK_WORD,
            "➕ *Add Blocked Words*\n\n"
            "Format: `word | action`\n\n"
            "*Actions:*\n"
            "• `warn` — send warning (default)\n"
            "• `delete` — delete the message\n"
            "• `mute` — mute notification\n\n"
            "*Examples:*\n"
            "`spam | delete`\n"
            "`badword | warn`\n"
            "`scam, phishing | delete`\n\n"
            "Separate multiple words with commas.",
        )
        return

    if data == "bw_list":
        words = db.get_blocked(uid)
        if not words:
            await q.edit_message_text(
                "📋 No blocked words.",
                reply_markup=back_btn("bw_menu"),
            )
            return
        rows = []
        text = "📋 *Blocked Words:*\n\n"
        for w in words[:20]:
            text += f"• `{w['word']}` — action: {w['action']}\n"
            rows.append([InlineKeyboardButton(
                f"❌ {w['word']}",
                callback_data=f"bw_del_{w['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="bw_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "bw_clear":
        db.clear_blocked(uid)
        await q.edit_message_text(
            "✅ All blocked words cleared.",
            reply_markup=back_btn("bw_menu"),
        )
        return

    if data.startswith("bw_del_"):
        bid = int(data[7:])
        db.del_blocked(uid, bid)
        await bw_menu(q, uid)
        return

    # ═══════════ WHITELIST ═══════════

    if data == "wl_menu":
        await wl_menu(q, uid)
        return

    if data == "wl_add":
        limit = db.plan_limit(uid, "max_whitelist")
        current = db.whitelist_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_whitelist", current),
                reply_markup=back_btn("wl_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_WHITELIST,
            "➕ *Add to Whitelist*\n\n"
            "Send user ID or @username.\n"
            "Whitelisted users bypass all restrictions.",
        )
        return

    if data == "wl_list":
        wl = db.get_whitelist(uid)
        if not wl:
            await q.edit_message_text(
                "📋 Whitelist is empty.",
                reply_markup=back_btn("wl_menu"),
            )
            return
        rows = []
        text = "📋 *Whitelist:*\n\n"
        for w in wl[:20]:
            name = f" ({w['target_name']})" if w['target_name'] else ""
            text += f"• {w['target_user']}{name}\n"
            rows.append([InlineKeyboardButton(
                f"❌ {w['target_user']}",
                callback_data=f"wl_del_{w['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="wl_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "wl_clear":
        db.clear_whitelist(uid)
        await q.edit_message_text(
            "✅ Whitelist cleared.",
            reply_markup=back_btn("wl_menu"),
        )
        return

    if data.startswith("wl_del_"):
        wid = int(data[7:])
        db.del_whitelist(uid, wid)
        await wl_menu(q, uid)
        return

    # ═══════════ SCHEDULED ═══════════

    if data == "sched_menu":
        await sched_menu(q, uid)
        return

    if data == "sched_add":
        limit = db.plan_limit(uid, "max_scheduled")
        current = db.scheduled_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_scheduled", current),
                reply_markup=back_btn("sched_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_SCHED_TARGET,
            "➕ *Schedule Message — Step 1/3*\n\n"
            "Send the target (username or chat ID).\n\n"
            "Examples:\n"
            "`@username`\n"
            "`123456789`",
        )
        return

    if data == "sched_list":
        items = db.user_scheduled(uid)
        if not items:
            await q.edit_message_text(
                "📋 No pending scheduled messages.",
                reply_markup=back_btn("sched_menu"),
            )
            return
        rows = []
        text = "📋 *Scheduled Messages:*\n\n"
        for item in items[:15]:
            icon = "🔁" if item["recurring"] else "📨"
            media = " 📎" if item["media_file_id"] else ""
            text += (
                f"{icon} *{(item['send_at'] or '')[:16]}*{media}\n"
                f"To: `{item['target']}`\n"
                f"Msg: {truncate(item['message'] or '', 40)}\n\n"
            )
            rows.append([InlineKeyboardButton(
                f"🗑️ #{item['id']}",
                callback_data=f"sdel_{item['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="sched_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("sdel_"):
        sid = int(data[5:])
        db.del_scheduled(uid, sid)
        await sched_menu(q, uid)
        return

    # ═══════════ FORWARD ═══════════

    if data == "fwd_menu":
        await fwd_menu(q, uid)
        return

    if data == "fwd_add":
        limit = db.plan_limit(uid, "max_forwards")
        current = db.forward_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_forwards", current),
                reply_markup=back_btn("fwd_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_FWD_SOURCE,
            "↗️ *Add Auto-Forward — Step 1/2*\n\n"
            "Send the *source* chat ID or @username.\n"
            "Messages from this chat will be forwarded.",
        )
        return

    if data == "fwd_list":
        rules = db.get_forwards(uid)
        if not rules:
            await q.edit_message_text(
                "📋 No auto-forward rules.",
                reply_markup=back_btn("fwd_menu"),
            )
            return
        rows = []
        text = "📋 *Auto-Forward Rules:*\n\n"
        for r in rules:
            ft = f" (filter: {r['filter_text']})" if r['filter_text'] else ""
            text += f"↗️ `{r['source']}` → `{r['dest']}`{ft}\n"
            rows.append([InlineKeyboardButton(
                f"🗑️ #{r['id']}",
                callback_data=f"fdl_{r['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="fwd_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "fwd_clear":
        db.clear_forwards(uid)
        await q.edit_message_text(
            "✅ All forward rules cleared.",
            reply_markup=back_btn("fwd_menu"),
        )
        return

    if data.startswith("fdl_"):
        fid = int(data[4:])
        db.del_forward(uid, fid)
        await fwd_menu(q, uid)
        return

    # ═══════════ PROFILE ═══════════

    if data == "profile_menu":
        await profile_menu(q, uid)
        return

    if data == "profile_bio":
        await ask_state(
            q, ctx, ST_BIO,
            "✏️ *Set Bio*\n\n"
            "Send your new bio text (max 70 characters).",
        )
        return

    if data == "profile_name":
        await ask_state(
            q, ctx, ST_NAME,
            "📛 *Set Name*\n\n"
            "Send: `FirstName | LastName`\n"
            "Example: `John | Doe`\n\n"
            "Leave LastName empty for first name only:\n"
            "`John`",
        )
        return

    if data == "profile_pic":
        await ask_state(
            q, ctx, ST_PROFILE_PIC,
            "🖼️ *Set Profile Photo*\n\n"
            "Send a photo to use as your profile picture.",
        )
        return

    if data == "profile_rmpic":
        await remove_profile_photo(q, uid)
        return

    # ═══════════ TEMPLATES ═══════════

    if data == "tmpl_menu":
        await tmpl_menu(q, uid)
        return

    if data == "tmpl_add":
        limit = db.plan_limit(uid, "max_templates")
        current = db.template_count(uid)
        if current >= limit:
            await q.edit_message_text(
                plan_limit_text(uid, "max_templates", current),
                reply_markup=back_btn("tmpl_menu"),
            )
            return
        await ask_state(
            q, ctx, ST_TEMPLATE_NAME,
            "➕ *Create Template — Step 1/2*\n\n"
            "Send the template name.\n"
            "Example: `greeting`",
        )
        return

    if data == "tmpl_list":
        templates = db.get_templates(uid, include_global=False)
        if not templates:
            await q.edit_message_text(
                "📋 No templates yet.",
                reply_markup=back_btn("tmpl_menu"),
            )
            return
        rows = []
        text = "📋 *Your Templates:*\n\n"
        for t in templates[:20]:
            media = " 📎" if t["media_file_id"] else ""
            text += (
                f"• `{t['name']}`{media}\n"
                f"  {truncate(t['content'], 50)}\n"
                f"  Used: {t['used_count']} times\n\n"
            )
            rows.append([InlineKeyboardButton(
                f"🗑️ {t['name']}",
                callback_data=f"tdel_{t['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="tmpl_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "tmpl_global":
        templates = db.get_templates(uid, include_global=True)
        global_t = [t for t in templates if t["is_global"]]
        if not global_t:
            await q.edit_message_text(
                "🌐 No global templates available.",
                reply_markup=back_btn("tmpl_menu"),
            )
            return
        text = "🌐 *Global Templates:*\n\n"
        for t in global_t[:20]:
            text += (
                f"• `{t['name']}` — "
                f"{truncate(t['content'], 50)}\n"
            )
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("tmpl_menu"),
        )
        return

    if data == "tmpl_clear":
        db.clear_templates(uid)
        await q.edit_message_text(
            "✅ All templates cleared.",
            reply_markup=back_btn("tmpl_menu"),
        )
        return

    if data.startswith("tdel_"):
        tid = int(data[5:])
        db.del_template(uid, tid)
        await tmpl_menu(q, uid)
        return

    # ═══════════ AUTO-REACT ═══════════

    if data == "react_menu":
        await react_menu(q, uid)
        return

    if data == "react_toggle":
        if not db.plan_check(uid, "auto_react"):
            return
        cur = db.get_setting(uid, "auto_react", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "auto_react", new_val)
        db.log(uid, "react_toggle", new_val, "settings")
        await react_menu(q, uid)
        return

    if data.startswith("react_set_"):
        emoji = data[10:]
        db.set_setting(uid, "react_emoji", emoji)
        await react_menu(q, uid)
        return

    # ═══════════ WORKING HOURS ═══════════

    if data == "wh_menu":
        await wh_menu(q, uid)
        return

    if data == "wh_toggle":
        if not db.plan_check(uid, "working_hours"):
            return
        cur = db.get_setting(uid, "working_hours", "false")
        new_val = "false" if cur == "true" else "true"
        db.set_setting(uid, "working_hours", new_val)
        db.log(uid, "wh_toggle", new_val, "settings")
        await wh_menu(q, uid)
        return

    if data.startswith("wh_day_"):
        day = int(data[7:])
        ctx.user_data["wh_day"] = day
        await ask_state(
            q, ctx, ST_WORKING_HOURS,
            f"⏰ *Set Hours for {DAYS_OF_WEEK[day]}*\n\n"
            f"Send time range: `HH:MM-HH:MM`\n\n"
            f"Example: `09:00-17:00`\n"
            f"Send `off` to disable this day.",
        )
        return

    if data == "wh_set_msg":
        await ask_state(
            q, ctx, ST_WORKING_HOURS,
            "✏️ *Outside-Hours Message*\n\n"
            "Send the message shown outside working hours.\n\n"
            "Variables: `{name}` `{time}` `{date}`",
        )
        ctx.user_data["wh_set_msg"] = True
        return

    if data == "wh_clear":
        db.clear_working_hours(uid)
        db.set_setting(uid, "working_hours", "false")
        await q.edit_message_text(
            "✅ Working hours cleared.",
            reply_markup=back_btn("wh_menu"),
        )
        return

    # ═══════════ NOTES ═══════════

    if data == "notes_menu":
        await notes_menu(q, uid)
        return

    if data == "note_add":
        await ask_state(
            q, ctx, ST_NOTE_TITLE,
            "➕ *New Note — Step 1/2*\n\n"
            "Send the note title.",
        )
        return

    if data == "note_list":
        notes = db.get_notes(uid)
        if not notes:
            await q.edit_message_text(
                "📋 No notes yet.",
                reply_markup=back_btn("notes_menu"),
            )
            return
        rows = []
        text = "📒 *Your Notes:*\n\n"
        for n in notes[:20]:
            pin = "📌" if n["is_pinned"] else ""
            media = " 📎" if n["media_file_id"] else ""
            text += (
                f"{pin} *{n['title']}*{media}\n"
                f"  {truncate(n['content'] or '', 50)}\n\n"
            )
            rows.append([
                InlineKeyboardButton(
                    f"📌 {truncate(n['title'], 12)}",
                    callback_data=f"npin_{n['id']}",
                ),
                InlineKeyboardButton(
                    "🗑️", callback_data=f"ndel_{n['id']}"
                ),
            ])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="notes_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "note_clear":
        db.clear_notes(uid)
        await q.edit_message_text(
            "✅ All notes cleared.",
            reply_markup=back_btn("notes_menu"),
        )
        return

    if data.startswith("npin_"):
        nid = int(data[5:])
        db.toggle_pin_note(uid, nid)
        # Re-trigger note list
        await q.edit_message_text(
            "📌 Note pin toggled.",
            reply_markup=back_btn("notes_menu"),
        )
        return

    if data.startswith("ndel_"):
        nid = int(data[5:])
        db.del_note(uid, nid)
        await notes_menu(q, uid)
        return

    # ═══════════ CUSTOM COMMANDS ═══════════

    if data == "ccmd_menu":
        await ccmd_menu(q, uid)
        return

    if data == "ccmd_add":
        if not db.plan_check(uid, "custom_commands"):
            return
        await ask_state(
            q, ctx, ST_CUSTOM_CMD_NAME,
            "➕ *Add Custom Command — Step 1/2*\n\n"
            "Send the command name (without /).\n"
            "Example: `hello`\n\n"
            "Users can then use `/hello` in your PMs.",
        )
        return

    if data == "ccmd_list":
        cmds = db.get_custom_cmds(uid)
        if not cmds:
            await q.edit_message_text(
                "📋 No custom commands yet.",
                reply_markup=back_btn("ccmd_menu"),
            )
            return
        rows = []
        text = "🤖 *Custom Commands:*\n\n"
        for cmd in cmds[:20]:
            media = " 📎" if cmd["media_file_id"] else ""
            text += (
                f"• `/{cmd['command']}`{media}\n"
                f"  → {truncate(cmd['response'], 50)}\n"
                f"  Used: {cmd['used_count']} times\n\n"
            )
            rows.append([InlineKeyboardButton(
                f"🗑️ /{cmd['command']}",
                callback_data=f"cdel_{cmd['id']}",
            )])
        rows.append([InlineKeyboardButton(
            "◀️ Back", callback_data="ccmd_menu"
        )])
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "ccmd_clear":
        db.clear_custom_cmds(uid)
        await q.edit_message_text(
            "✅ All custom commands cleared.",
            reply_markup=back_btn("ccmd_menu"),
        )
        return

    if data.startswith("cdel_"):
        cid = int(data[5:])
        db.del_custom_cmd(uid, cid)
        await ccmd_menu(q, uid)
        return

    # ═══════════ STATS / LOGS ═══════════

    if data == "stats_menu":
        await stats_menu(q, uid)
        return

    if data == "stats_reset":
        await q.edit_message_text(
            "⚠️ Reset all statistics?",
            reply_markup=confirm_btns(
                "stats_reset_ok", "stats_menu"
            ),
        )
        return

    if data == "stats_reset_ok":
        db.reset_stats(uid)
        await q.edit_message_text(
            "✅ Stats reset.",
            reply_markup=back_btn("stats_menu"),
        )
        return

    if data == "stats_daily":
        daily = db.daily_stats(uid, 7)
        if not daily:
            await q.edit_message_text(
                "📈 No daily stats available yet.",
                reply_markup=back_btn("stats_menu"),
            )
            return
        by_date = {}
        for row in daily:
            d = row["date"]
            if d not in by_date:
                by_date[d] = {}
            by_date[d][row["key"]] = row["value"]

        text = "📈 *Daily Analytics (7 days)*\n\n"
        for date_str in sorted(by_date.keys(), reverse=True)[:7]:
            data_d = by_date[date_str]
            text += f"*{date_str}:*\n"
            for k, v in sorted(data_d.items()):
                if not k.startswith("pm_warn_") and not k.startswith("welcomed_"):
                    text += f"  • {k.replace('_', ' ').title()}: {v}\n"
            text += "\n"

        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("stats_menu"),
        )
        return

    if data == "logs_menu":
        await logs_menu(q, uid)
        return

    if data == "logs_view":
        rows_data = db.get_logs(uid, 25)
        if not rows_data:
            body = "_No logs yet._"
        else:
            body = "\n".join(
                f"• `{(r['ts'] or '')[:16]}` | "
                f"*{r['action']}* | "
                f"{truncate(r['detail'] or '', 30)}"
                for r in rows_data
            )
        await q.edit_message_text(
            f"📜 *Your Logs*\n\n{body}",
            parse_mode="Markdown",
            reply_markup=back_btn("logs_menu"),
        )
        return

    if data in ("logs_kw", "logs_pm", "logs_spam"):
        cat_map = {
            "logs_kw": "keyword",
            "logs_pm": "pm",
            "logs_spam": "spam",
        }
        cat = cat_map[data]
        rows_data = db.get_logs(uid, 25, category=cat)
        if not rows_data:
            body = f"_No {cat} logs._"
        else:
            body = "\n".join(
                f"• `{(r['ts'] or '')[:16]}` | "
                f"*{r['action']}* | "
                f"{truncate(r['detail'] or '', 30)}"
                for r in rows_data
            )
        await q.edit_message_text(
            f"📜 *{cat.title()} Logs*\n\n{body}",
            parse_mode="Markdown",
            reply_markup=back_btn("logs_menu"),
        )
        return

    if data == "logs_clear":
        db.clear_logs(uid)
        await q.edit_message_text(
            "✅ Logs cleared.",
            reply_markup=back_btn("logs_menu"),
        )
        return

    # ═══════════ FEEDBACK ═══════════

    if data == "feedback_menu":
        await feedback_menu(q, uid)
        return

    if data == "fb_send":
        await ask_state(
            q, ctx, ST_FEEDBACK,
            "✏️ *Send Feedback*\n\n"
            "Type your feedback, suggestion, or bug report.",
        )
        return

    if data == "fb_list":
        fb = db.user_feedback(uid)
        if not fb:
            await q.edit_message_text(
                "📋 No feedback sent yet.",
                reply_markup=back_btn("feedback_menu"),
            )
            return
        text = "💬 *Your Feedback:*\n\n"
        for f in fb[:10]:
            status_icon = {
                "pending": "⏳",
                "replied": "✅",
                "closed": "🔒",
            }.get(f["status"], "❓")
            text += (
                f"{status_icon} *#{f['id']}* — {f['status']}\n"
                f"📝 {truncate(f['message'], 60)}\n"
            )
            if f["admin_reply"]:
                text += f"💬 Reply: {truncate(f['admin_reply'], 60)}\n"
            text += "\n"
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("feedback_menu"),
        )
        return

    # ═══════════ BACKUP ═══════════

    if data == "backup_menu":
        await backup_menu(q, uid)
        return

    if data == "backup_export":
        if not db.plan_check(uid, "backup_export"):
            return
        await q.edit_message_text("📤 Preparing export...")
        try:
            export_data = db.export_user_data(uid)
            json_str = json.dumps(
                export_data, indent=2, ensure_ascii=False
            )
            bio = io.BytesIO(json_str.encode("utf-8"))
            bio.name = (
                f"skull_backup_{uid}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            await q.message.reply_document(
                document=bio,
                caption=(
                    "📥 Your settings backup.\n"
                    "Use *Import* to restore."
                ),
                parse_mode="Markdown",
            )
            db.log(uid, "export", "Settings exported", "backup")
            await q.edit_message_text(
                "✅ Export complete!",
                reply_markup=back_btn("backup_menu"),
            )
        except Exception as exc:
            await q.edit_message_text(
                f"❌ Export failed: {exc}",
                reply_markup=back_btn("backup_menu"),
            )
        return

    if data == "backup_import":
        if not db.plan_check(uid, "backup_export"):
            return
        await ask_state(
            q, ctx, ST_IMPORT_FILE,
            "📥 *Import Settings*\n\n"
            "Send a JSON backup file previously exported.\n\n"
            "⚠️ This will ADD to your existing settings.",
        )
        return

    # ═══════════ ADMIN ═══════════

    if data == "admin_home":
        await admin_home(q, uid)
        return

    if data == "admin_users":
        if not is_admin(uid):
            return
        await admin_users_menu(q, uid)
        return

    if data == "admin_stats":
        if not is_admin(uid):
            return
        await admin_stats_menu(q, uid)
        return

    if data == "admin_broadcast":
        if not is_admin(uid):
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📢 All Users",
                callback_data="admin_bc_all",
            )],
            [InlineKeyboardButton(
                "⭐ Premium Only",
                callback_data="admin_bc_premium",
            )],
            [InlineKeyboardButton(
                "👑 VIP Only",
                callback_data="admin_bc_vip",
            )],
            [InlineKeyboardButton(
                "🟢 Connected Only",
                callback_data="admin_bc_connected",
            )],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="admin_home"
            )],
        ])
        await q.edit_message_text(
            "📢 *Broadcast*\n\n"
            "Select target audience:",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if data.startswith("admin_bc_"):
        if not is_admin(uid):
            return
        target = data[9:]
        ctx.user_data["bc_target"] = target
        await ask_state(
            q, ctx, ST_ADMIN_BROADCAST,
            f"📢 *Broadcast to: {target}*\n\n"
            f"Send the broadcast message.\n"
            f"You can also send media (photo/video/doc) "
            f"with a caption.",
        )
        return

    if data == "admin_search":
        if not is_admin(uid):
            return
        await ask_state(
            q, ctx, ST_ADMIN_SEARCH,
            "🔍 *Search User*\n\n"
            "Send user ID or @username.",
        )
        return

    if data == "admin_plans":
        if not is_admin(uid):
            return
        await admin_plans_menu(q, uid)
        return

    if data == "admin_banned":
        if not is_admin(uid):
            return
        await admin_banned_menu(q, uid)
        return

    if data == "admin_expiring":
        if not is_admin(uid):
            return
        await admin_expiring_menu(q, uid)
        return

    if data == "admin_feedback":
        if not is_admin(uid):
            return
        await admin_feedback_menu(q, uid)
        return

    if data == "admin_fb_all":
        if not is_admin(uid):
            return
        feedback = db.get_all_feedback()
        if not feedback:
            await q.edit_message_text(
                "💬 No feedback.",
                reply_markup=back_btn("admin_home"),
            )
            return
        text = "💬 *All Feedback:*\n\n"
        for fb in feedback[:20]:
            status_icon = {"pending": "⏳", "replied": "✅"}.get(
                fb["status"], "❓"
            )
            text += (
                f"{status_icon} *#{fb['id']}* `{fb['user_id']}` "
                f"@{fb['username'] or 'none'}\n"
                f"📝 {truncate(fb['message'], 60)}\n\n"
            )
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=back_btn("admin_home"),
        )
        return

    if data == "admin_announce":
        if not is_admin(uid):
            return
        await ask_state(
            q, ctx, ST_ADMIN_ANNOUNCE,
            "📢 *Create Announcement*\n\n"
            "Send the announcement message.\n"
            "This will be sent to ALL users.",
        )
        return

    if data == "admin_logs":
        if not is_admin(uid):
            return
        await admin_logs_menu(q, uid)
        return

    if data == "admin_clrlogs":
        if not is_admin(uid):
            return
        db.clear_all_logs()
        await q.edit_message_text(
            "✅ All logs cleared.",
            reply_markup=back_btn("admin_home"),
        )
        return

    if data == "admin_cleanup":
        if not is_admin(uid):
            return
        db.cleanup()
        await q.edit_message_text(
            "✅ Database cleanup complete.\n"
            f"DB Size: {db.db_size()}",
            reply_markup=back_btn("admin_home"),
        )
        return

    if data == "admin_upload_db":
        if not is_admin(uid):
            return
        await ask_state(
            q, ctx, ST_ADMIN_UPLOAD_DB,
            "📤 *Upload Database*\n\n"
            "Send a SQLite `.db` file to replace the "
            "current database.\n\n"
            "⚠️ Current DB will be backed up first.",
        )
        return

    if data == "admin_download_db":
        if not is_admin(uid):
            return
        try:
            with open(DB_FILE, "rb") as f:
                await q.message.reply_document(
                    document=f,
                    filename=f"skull_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                    caption="📥 Current database file.",
                )
            await q.edit_message_text(
                "✅ Database sent.",
                reply_markup=back_btn("admin_home"),
            )
        except Exception as exc:
            await q.edit_message_text(
                f"❌ Failed: {exc}",
                reply_markup=back_btn("admin_home"),
            )
        return

    if data == "admin_global_tmpl":
        if not is_admin(uid):
            return
        await ask_state(
            q, ctx, ST_TEMPLATE_NAME,
            "🌐 *Create Global Template*\n\n"
            "Send the template name.\n"
            "This template will be available to all users.",
        )
        ctx.user_data["tmpl_global"] = True
        return

    if data == "admin_set_plan":
        if not is_admin(uid):
            return
        await ask_state(
            q, ctx, ST_ADMIN_SET_PLAN,
            "💎 *Set User Plan*\n\n"
            "Send: `user_id | plan | days`\n\n"
            "Plans: `free`, `premium`, `vip`\n\n"
            "Examples:\n"
            "`123456789 | premium | 30`\n"
            "`987654321 | vip | 90`\n"
            "`123456789 | free | 0`",
        )
        return

    if data.startswith("au_ban_"):
        if not is_admin(uid):
            return
        target_uid = int(data[7:])
        ctx.user_data["ban_target"] = target_uid
        await ask_state(
            q, ctx, ST_ADMIN_BAN_REASON,
            f"🚫 *Ban User `{target_uid}`*\n\n"
            f"Send the ban reason (or 'none').",
        )
        return

    if data.startswith("au_unban_"):
        if not is_admin(uid):
            return
        target_uid = int(data[9:])
        db.unban_user(target_uid)
        await q.edit_message_text(
            f"✅ Unbanned `{target_uid}`.",
            parse_mode="Markdown",
            reply_markup=back_btn("admin_home"),
        )
        return

    if data.startswith("au_plan_"):
        if not is_admin(uid):
            return
        parts = data.split("_")
        if len(parts) >= 5:
            target_uid = int(parts[2])
            plan = parts[3]
            days = int(parts[4])
            db.set_plan(target_uid, plan, days, admin_id=uid)
            await q.edit_message_text(
                f"✅ Set `{target_uid}` to "
                f"*{fmt_plan(plan)}* for *{days}* days.",
                parse_mode="Markdown",
                reply_markup=back_btn("admin_home"),
            )
        return

    if data.startswith("au_del_"):
        if not is_admin(uid):
            return
        target_uid = int(data[7:])
        db.full_delete_user(target_uid)
        if target_uid in active_clients:
            await stop_client(target_uid)
        await q.edit_message_text(
            f"✅ User `{target_uid}` fully deleted.",
            parse_mode="Markdown",
            reply_markup=back_btn("admin_home"),
        )
        return

    logger.warning("Unhandled callback: %s from uid=%s", data, uid)


# ╔══════════════════════════════════════════════════════════════╗
# ║                  LOGIN HELPERS                               ║
# ╚══════════════════════════════════════════════════════════════╝


async def login_start_phone(update: Update,
                            ctx: ContextTypes.DEFAULT_TYPE,
                            phone: str):
    """Begin the login process with phone number."""
    uid = update.effective_user.id
    client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        device_model=BOT_NAME,
        system_version="2.0",
        app_version=BOT_VERSION,
    )
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
        ctx.user_data["tmp_client"] = client
        ctx.user_data["state"] = ST_OTP
        ctx.user_data["phone"] = phone
        ctx.user_data["phone_code_hash"] = sent.phone_code_hash
        await update.message.reply_text(
            "📩 *OTP Sent!*\n\n"
            "Check your Telegram app for the code.\n"
            "Send it here exactly as received.\n\n"
            "Example: `12345`\n\n"
            "_/cancel to abort._",
            parse_mode="Markdown",
        )
    except PhoneNumberInvalidError:
        await client.disconnect()
        await update.message.reply_text(
            "❌ Invalid phone number.\n"
            "Make sure it starts with `+` and country code.",
            parse_mode="Markdown",
        )
    except FloodWaitError as exc:
        await client.disconnect()
        await update.message.reply_text(
            f"⏳ Please wait *{exc.seconds}* seconds "
            f"before trying again.",
            parse_mode="Markdown",
        )
    except Exception as exc:
        await client.disconnect()
        logger.exception("Login error: %s", exc)
        await update.message.reply_text(
            f"❌ Login error: `{exc}`",
            parse_mode="Markdown",
        )


async def finish_login(update: Update,
                       ctx: ContextTypes.DEFAULT_TYPE,
                       password: Optional[str] = None):
    """Complete the login process."""
    uid = update.effective_user.id
    phone = ctx.user_data.get("phone")
    client: TelegramClient = ctx.user_data.get("tmp_client")

    if not client or not phone:
        ctx.user_data.clear()
        await update.message.reply_text(
            "❌ Login session expired. Start again.",
            reply_markup=main_kb(uid),
        )
        return

    try:
        if password is not None:
            await client.sign_in(password=password)

        me = await client.get_me()
        session_str = client.session.save()

        db.add_user(
            uid,
            update.effective_user.username,
            update.effective_user.first_name,
            getattr(update.effective_user, "last_name", None),
        )
        db.save_session(uid, phone, session_str)
        db.log(uid, "login", f"connected as {me.id}", "auth")

        try:
            await client.disconnect()
        except Exception:
            pass

        ctx.user_data.clear()
        started = await start_client(uid)

        await update.message.reply_text(
            f"✅ *Login Successful!*\n\n"
            f"👤 {me.first_name or ''} {me.last_name or ''}\n"
            f"📛 @{me.username or 'none'}\n"
            f"🆔 `{me.id}`\n\n"
            f"{'🟢 Userbot connected!' if started else '⚠️ Session saved but auto-connect failed. Try Reconnect.'}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
    except Exception as exc:
        try:
            await client.disconnect()
        except Exception:
            pass
        ctx.user_data.clear()
        logger.exception("Login final step error: %s", exc)
        await update.message.reply_text(
            f"❌ Login failed: `{exc}`\n\n"
            f"Try /start and login again.",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║                PROFILE ACTIONS                               ║
# ╚══════════════════════════════════════════════════════════════╝


async def remove_profile_photo(q, uid):
    """Remove user's profile photo via Telethon."""
    client = get_client(uid)
    if not client:
        await q.edit_message_text(
            "❌ Not connected.",
            reply_markup=back_btn("profile_menu"),
        )
        return
    try:
        photos = await client.get_profile_photos("me")
        if not photos:
            await q.edit_message_text(
                "ℹ️ No profile photo to remove.",
                reply_markup=back_btn("profile_menu"),
            )
            return
        await client(DeletePhotosRequest(
            id=[p for p in await client.get_profile_photos("me", limit=1)]
        ))
        db.log(uid, "profile_photo_remove", "", "profile")
        await q.edit_message_text(
            "✅ Profile photo removed.",
            reply_markup=back_btn("profile_menu"),
        )
    except Exception as exc:
        await q.edit_message_text(
            f"❌ Failed: `{exc}`",
            parse_mode="Markdown",
            reply_markup=back_btn("profile_menu"),
        )
# ══════════════════════════════════════════════════════════════
# PART 3 OF 3 — Message Handlers, Background Tasks, Main
# Place this code directly after Part 2 in the same file
# ══════════════════════════════════════════════════════════════


# ╔══════════════════════════════════════════════════════════════╗
# ║              MESSAGE HANDLERS — TEXT                         ║
# ╚══════════════════════════════════════════════════════════════╝


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming text messages based on conversation state."""
    if not update.message or not update.effective_user:
        return

    user = update.effective_user
    uid = user.id
    text = update.message.text or ""
    state = ctx.user_data.get("state")

    db.add_user(uid, user.username, user.first_name,
                getattr(user, "last_name", None))

    if db.is_banned(uid):
        await update.message.reply_text(
            f"⛔ You are banned.\nContact {SUPPORT_USERNAME}"
        )
        return

    db.touch_user(uid)

    # ═══════════ LOGIN FLOW ═══════════

    if state == ST_PHONE:
        phone = text.strip()
        if not phone.startswith("+"):
            await update.message.reply_text(
                "❌ Phone must start with `+`\n"
                "Example: `+1234567890`",
                parse_mode="Markdown",
            )
            return
        if len(phone) < 8 or len(phone) > 16:
            await update.message.reply_text(
                "❌ Invalid phone number length."
            )
            return
        await login_start_phone(update, ctx, phone)
        return

    if state == ST_OTP:
        client: TelegramClient = ctx.user_data.get("tmp_client")
        phone = ctx.user_data.get("phone")
        phone_code_hash = ctx.user_data.get("phone_code_hash")

        if not client or not phone or not phone_code_hash:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Login session expired. Start again.",
                reply_markup=main_kb(uid),
            )
            return

        code = re.sub(r"\D", "", text)
        if not code:
            await update.message.reply_text(
                "❌ Please send the numeric OTP code."
            )
            return

        try:
            await client.sign_in(
                phone=phone,
                code=code,
                phone_code_hash=phone_code_hash,
            )
            await finish_login(update, ctx)
        except SessionPasswordNeededError:
            ctx.user_data["state"] = ST_2FA
            await update.message.reply_text(
                "🔐 *Two-Step Verification*\n\n"
                "Your account has 2FA enabled.\n"
                "Send your password.\n\n"
                "_/cancel to abort._",
                parse_mode="Markdown",
            )
        except PhoneCodeInvalidError:
            await update.message.reply_text(
                "❌ Invalid OTP code. Try again."
            )
        except PhoneCodeExpiredError:
            await update.message.reply_text(
                "⌛ OTP expired. Start login again with /start"
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            ctx.user_data.clear()
        except Exception as exc:
            await update.message.reply_text(
                f"❌ OTP error: `{exc}`",
                parse_mode="Markdown",
            )
        return

    if state == ST_2FA:
        await finish_login(update, ctx, password=text)
        return

    # ═══════════ WELCOME ═══════════

    if state == ST_WELCOME_MSG:
        db.set_setting(uid, "welcome_msg", text)
        db.log(uid, "welcome_msg_set", "", "settings")
        ctx.user_data.clear()
        await update.message.reply_text(
            "✅ Welcome message saved!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ AWAY ═══════════

    if state == ST_AWAY_MSG:
        db.set_setting(uid, "away_msg", text)
        db.log(uid, "away_msg_set", "", "settings")
        ctx.user_data.clear()
        await update.message.reply_text(
            "✅ Away message saved!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ PM PERMIT ═══════════

    if state == ST_PM_MSG:
        db.set_setting(uid, "pm_msg", text)
        db.log(uid, "pm_msg_set", "", "settings")
        ctx.user_data.clear()
        await update.message.reply_text(
            "✅ PM permit message saved!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ ANTI-SPAM ═══════════

    if state == ST_SPAM_MSG:
        db.set_setting(uid, "spam_msg", text)
        ctx.user_data.clear()
        await update.message.reply_text(
            "✅ Spam warning message saved!",
            reply_markup=main_kb(uid),
        )
        return

    if state == ST_SPAM_LIMIT:
        try:
            value = int(text.strip())
            if value < 1 or value > 50:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Send a number between 1 and 50."
            )
            return
        db.set_setting(uid, "spam_limit", str(value))
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Spam limit set to {value} messages/minute.",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ BLOCKED WORDS ═══════════

    if state == ST_BLOCK_WORD:
        entries = [e.strip() for e in text.split(",") if e.strip()]
        if not entries:
            await update.message.reply_text(
                "❌ No valid words provided."
            )
            return

        added = 0
        limit = db.plan_limit(uid, "max_blocked_words")
        current = db.blocked_count(uid)

        for entry in entries:
            if current + added >= limit:
                break
            parts = [p.strip() for p in entry.split("|", 1)]
            word = parts[0].lower()
            action = "warn"
            if len(parts) == 2:
                act = parts[1].lower()
                if act in ("warn", "delete", "mute"):
                    action = act
            if word:
                db.add_blocked(uid, word, action)
                added += 1

        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Added {added} blocked word(s).",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ WHITELIST ═══════════

    if state == ST_WHITELIST:
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "❌ Send a user ID or @username."
            )
            return
        db.add_whitelist(uid, target)
        db.log(uid, "whitelist_add", target, "settings")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ `{target}` added to whitelist.",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ KEYWORDS ═══════════

    if state == ST_KW_TRIGGER:
        parts = [p.strip() for p in text.split("|", 1)]
        trigger = parts[0].lower()
        match_type = "contains"

        if len(parts) == 2:
            mt = parts[1].lower().strip()
            valid_types = {
                "contains", "exact", "startswith",
                "endswith", "regex",
            }
            if mt in valid_types:
                match_type = mt
            else:
                await update.message.reply_text(
                    f"❌ Invalid match type `{mt}`.\n"
                    f"Valid: contains, exact, startswith, "
                    f"endswith, regex",
                    parse_mode="Markdown",
                )
                return

        if match_type == "regex" and not db.plan_check(uid, "regex_keywords"):
            await update.message.reply_text(
                f"🔒 Regex requires *Premium+*.\n"
                f"Contact {SUPPORT_USERNAME} to upgrade!",
                parse_mode="Markdown",
            )
            return

        if match_type == "regex":
            try:
                re.compile(trigger)
            except re.error as exc:
                await update.message.reply_text(
                    f"❌ Invalid regex pattern: `{exc}`",
                    parse_mode="Markdown",
                )
                return

        if not trigger:
            await update.message.reply_text(
                "❌ Trigger cannot be empty."
            )
            return

        ctx.user_data["kw_trigger"] = trigger
        ctx.user_data["kw_match_type"] = match_type
        ctx.user_data["state"] = ST_KW_RESPONSE
        await update.message.reply_text(
            f"✅ Trigger: `{trigger}` ({match_type})\n\n"
            f"✏️ Now send the *response text*.\n\n"
            f"Variables: `{{name}}` `{{username}}` `{{id}}` "
            f"`{{mention}}` `{{time}}` `{{date}}`\n\n"
            f"_You can also send media (photo/video/doc) "
            f"as the response._",
            parse_mode="Markdown",
        )
        return

    if state == ST_KW_RESPONSE:
        trigger = ctx.user_data.get("kw_trigger")
        match_type = ctx.user_data.get("kw_match_type", "contains")
        if not trigger:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Keyword flow expired. Start again."
            )
            return

        kid = db.add_keyword(uid, trigger, text, match_type=match_type)
        db.log(uid, "keyword_add", f"{trigger} ({match_type})", "keyword")
        ctx.user_data.clear()

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📎 Add Media to This Keyword",
                callback_data=f"kw_add_media_{kid}",
            )],
            [InlineKeyboardButton(
                "✅ Done", callback_data="kw_menu"
            )],
        ])

        can_media = db.plan_check(uid, "media_in_replies")
        if can_media:
            await update.message.reply_text(
                f"✅ Keyword added!\n\n"
                f"Trigger: `{trigger}`\n"
                f"Match: {match_type}\n"
                f"Response: {truncate(text, 50)}\n\n"
                f"Want to attach media?",
                parse_mode="Markdown",
                reply_markup=kb,
            )
        else:
            await update.message.reply_text(
                f"✅ Keyword added!\n\n"
                f"Trigger: `{trigger}`\n"
                f"Match: {match_type}",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        return

    # ═══════════ FILTERS ═══════════

    if state == ST_FILTER_NAME:
        name = text.strip().lower()
        if not name:
            await update.message.reply_text(
                "❌ Filter name cannot be empty."
            )
            return
        ctx.user_data["filter_name"] = name
        ctx.user_data["state"] = ST_FILTER_RESP
        await update.message.reply_text(
            f"✅ Filter trigger: `{name}`\n\n"
            f"✏️ Now send the *response text*.\n\n"
            f"_You can also send media as the response._",
            parse_mode="Markdown",
        )
        return

    if state == ST_FILTER_RESP:
        name = ctx.user_data.get("filter_name")
        if not name:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Filter flow expired."
            )
            return
        fid = db.add_filter(uid, name, text)
        db.log(uid, "filter_add", name, "filter")
        ctx.user_data.clear()

        can_media = db.plan_check(uid, "media_in_replies")
        if can_media:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "📎 Add Media",
                    callback_data=f"flt_add_media_{fid}",
                )],
                [InlineKeyboardButton(
                    "✅ Done", callback_data="filter_menu"
                )],
            ])
            await update.message.reply_text(
                f"✅ Filter `{name}` added!\n\n"
                f"Want to attach media?",
                parse_mode="Markdown",
                reply_markup=kb,
            )
        else:
            await update.message.reply_text(
                f"✅ Filter `{name}` added!",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        return

    # ═══════════ SCHEDULED MESSAGES ═══════════

    if state == ST_SCHED_TARGET:
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "❌ Target cannot be empty."
            )
            return
        ctx.user_data["sched_target"] = target
        ctx.user_data["state"] = ST_SCHED_MSG
        await update.message.reply_text(
            f"✅ Target: `{target}`\n\n"
            f"✏️ *Step 2/3:* Send the message text.\n\n"
            f"_You can also send media with caption._",
            parse_mode="Markdown",
        )
        return

    if state == ST_SCHED_MSG:
        ctx.user_data["sched_msg"] = text
        ctx.user_data["state"] = ST_SCHED_TIME
        await update.message.reply_text(
            "✅ Message saved.\n\n"
            "⏰ *Step 3/3:* Send the date and time.\n\n"
            "Format: `YYYY-MM-DD HH:MM`\n"
            "Example: `2025-12-31 10:00`\n\n"
            "_Time is in your local timezone._",
            parse_mode="Markdown",
        )
        return

    if state == ST_SCHED_TIME:
        target = ctx.user_data.get("sched_target")
        message = ctx.user_data.get("sched_msg")
        media_id = ctx.user_data.get("sched_media_id")
        media_type = ctx.user_data.get("sched_media_type")

        if not target or not message:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Schedule flow expired."
            )
            return

        dt_text = text.strip()
        try:
            send_at = datetime.strptime(dt_text, "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid format. Use `YYYY-MM-DD HH:MM`",
                parse_mode="Markdown",
            )
            return

        if send_at <= datetime.now():
            await update.message.reply_text(
                "❌ Time must be in the future."
            )
            return

        db.add_scheduled(
            uid, target, message, send_at.isoformat(),
            media_file_id=media_id, media_type=media_type,
        )
        db.log(uid, "schedule_add", f"to={target} at={dt_text}", "schedule")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ *Message Scheduled!*\n\n"
            f"📨 To: `{target}`\n"
            f"⏰ At: `{dt_text}`\n"
            f"📝 Msg: {truncate(message, 50)}\n"
            f"{'📎 Media attached' if media_id else ''}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ AUTO-FORWARD ═══════════

    if state == ST_FWD_SOURCE:
        source = text.strip()
        if not source:
            await update.message.reply_text(
                "❌ Source cannot be empty."
            )
            return
        ctx.user_data["fwd_source"] = source
        ctx.user_data["state"] = ST_FWD_DEST
        await update.message.reply_text(
            f"✅ Source: `{source}`\n\n"
            f"↗️ *Step 2/2:* Send the *destination* "
            f"chat ID or @username.",
            parse_mode="Markdown",
        )
        return

    if state == ST_FWD_DEST:
        source = ctx.user_data.get("fwd_source")
        dest = text.strip()
        if not source or not dest:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Forward flow expired."
            )
            return
        db.add_forward(uid, source, dest)
        db.log(uid, "forward_add", f"{source} → {dest}", "forward")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ *Auto-Forward Rule Added!*\n\n"
            f"↗️ `{source}` → `{dest}`",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ PROFILE ═══════════

    if state == ST_BIO:
        client = get_client(uid)
        if not client:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Not connected.",
                reply_markup=main_kb(uid),
            )
            return
        bio_text = text[:70]
        try:
            await client(UpdateProfileRequest(about=bio_text))
            db.log(uid, "bio_update", bio_text[:30], "profile")
            await update.message.reply_text(
                f"✅ Bio updated!\n\n`{bio_text}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        except Exception as exc:
            await update.message.reply_text(
                f"❌ Failed: `{exc}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        ctx.user_data.clear()
        return

    if state == ST_NAME:
        client = get_client(uid)
        if not client:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Not connected.",
                reply_markup=main_kb(uid),
            )
            return
        parts = [p.strip() for p in text.split("|", 1)]
        first = parts[0][:64]
        last = parts[1][:64] if len(parts) > 1 else ""
        try:
            await client(UpdateProfileRequest(
                first_name=first, last_name=last
            ))
            db.log(uid, "name_update", f"{first} {last}".strip(), "profile")
            await update.message.reply_text(
                f"✅ Name updated to: *{first} {last}*",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        except Exception as exc:
            await update.message.reply_text(
                f"❌ Failed: `{exc}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        ctx.user_data.clear()
        return

    # ═══════════ TEMPLATES ═══════════

    if state == ST_TEMPLATE_NAME:
        name = text.strip()
        if not name:
            await update.message.reply_text(
                "❌ Template name cannot be empty."
            )
            return
        ctx.user_data["tmpl_name"] = name
        ctx.user_data["state"] = ST_TEMPLATE_CONTENT
        await update.message.reply_text(
            f"✅ Template name: `{name}`\n\n"
            f"✏️ *Step 2/2:* Send the template content.\n\n"
            f"_You can also send media with caption._",
            parse_mode="Markdown",
        )
        return

    if state == ST_TEMPLATE_CONTENT:
        name = ctx.user_data.get("tmpl_name")
        is_global = ctx.user_data.get("tmpl_global", False)
        if not name:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Template flow expired."
            )
            return
        db.add_template(
            uid, name, text,
            category="general",
            is_global=is_global,
        )
        db.log(uid, "template_add", name, "template")
        ctx.user_data.clear()
        global_text = " (Global)" if is_global else ""
        await update.message.reply_text(
            f"✅ Template `{name}`{global_text} created!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ NOTES ═══════════

    if state == ST_NOTE_TITLE:
        title = text.strip()
        if not title:
            await update.message.reply_text(
                "❌ Title cannot be empty."
            )
            return
        ctx.user_data["note_title"] = title
        ctx.user_data["state"] = ST_NOTE_CONTENT
        await update.message.reply_text(
            f"✅ Title: `{title}`\n\n"
            f"✏️ *Step 2/2:* Send the note content.\n\n"
            f"_You can also send media._",
            parse_mode="Markdown",
        )
        return

    if state == ST_NOTE_CONTENT:
        title = ctx.user_data.get("note_title")
        if not title:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Note flow expired."
            )
            return
        media_id = ctx.user_data.get("note_media_id")
        media_type = ctx.user_data.get("note_media_type")
        db.add_note(uid, title, text, media_id, media_type)
        db.log(uid, "note_add", title, "notes")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Note `{title}` saved!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ CUSTOM COMMANDS ═══════════

    if state == ST_CUSTOM_CMD_NAME:
        cmd_name = text.strip().lower().lstrip("/")
        if not cmd_name:
            await update.message.reply_text(
                "❌ Command name cannot be empty."
            )
            return
        if not re.match(r'^[a-z0-9_]+$', cmd_name):
            await update.message.reply_text(
                "❌ Command can only contain a-z, 0-9, underscore."
            )
            return
        ctx.user_data["ccmd_name"] = cmd_name
        ctx.user_data["state"] = ST_CUSTOM_CMD_RESP
        await update.message.reply_text(
            f"✅ Command: `/{cmd_name}`\n\n"
            f"✏️ Now send the response text.\n\n"
            f"_You can also send media._",
            parse_mode="Markdown",
        )
        return

    if state == ST_CUSTOM_CMD_RESP:
        cmd_name = ctx.user_data.get("ccmd_name")
        if not cmd_name:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Command flow expired."
            )
            return
        media_id = ctx.user_data.get("ccmd_media_id")
        media_type = ctx.user_data.get("ccmd_media_type")
        db.add_custom_cmd(uid, cmd_name, text, media_id, media_type)
        db.log(uid, "custom_cmd_add", cmd_name, "custom_cmd")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Custom command `/{cmd_name}` created!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ WORKING HOURS ═══════════

    if state == ST_WORKING_HOURS:
        if ctx.user_data.get("wh_set_msg"):
            db.set_setting(uid, "wh_message", text)
            ctx.user_data.clear()
            await update.message.reply_text(
                "✅ Outside-hours message saved!",
                reply_markup=main_kb(uid),
            )
            return

        day = ctx.user_data.get("wh_day")
        if day is None:
            ctx.user_data.clear()
            await update.message.reply_text(
                "❌ Working hours flow expired."
            )
            return

        input_text = text.strip().lower()
        if input_text == "off":
            db.set_working_hours(uid, day, 0, 0, 0, 0, is_active=False)
            ctx.user_data.clear()
            await update.message.reply_text(
                f"✅ {DAYS_OF_WEEK[day]} disabled.",
                reply_markup=main_kb(uid),
            )
            return

        match = re.match(
            r'^(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$',
            input_text,
        )
        if not match:
            await update.message.reply_text(
                "❌ Invalid format. Use `HH:MM-HH:MM`\n"
                "Example: `09:00-17:00`\n"
                "Or send `off` to disable.",
                parse_mode="Markdown",
            )
            return

        sh, sm, eh, em = (
            int(match.group(1)), int(match.group(2)),
            int(match.group(3)), int(match.group(4)),
        )
        if not (0 <= sh <= 23 and 0 <= sm <= 59
                and 0 <= eh <= 23 and 0 <= em <= 59):
            await update.message.reply_text(
                "❌ Invalid time values."
            )
            return

        db.set_working_hours(uid, day, sh, sm, eh, em, is_active=True)
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ {DAYS_OF_WEEK[day]}: "
            f"{sh:02d}:{sm:02d} - {eh:02d}:{em:02d}",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ FEEDBACK ═══════════

    if state == ST_FEEDBACK:
        if not text.strip():
            await update.message.reply_text(
                "❌ Feedback cannot be empty."
            )
            return
        db.add_feedback(uid, text)
        db.log(uid, "feedback_sent", truncate(text, 50), "feedback")
        ctx.user_data.clear()

        # Notify admin
        try:
            admin_text = (
                f"💬 *New Feedback*\n\n"
                f"From: `{uid}` @{user.username or 'none'}\n"
                f"Name: {user.first_name or 'N/A'}\n\n"
                f"📝 {text}"
            )
            await ctx.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_text,
                parse_mode="Markdown",
            )
        except Exception:
            pass

        await update.message.reply_text(
            "✅ Feedback sent!\n"
            "The admin will review it. Thank you! 🙏",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ ADMIN: BROADCAST ═══════════

    if state == ST_ADMIN_BROADCAST:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        bc_target = ctx.user_data.get("bc_target", "all")
        media_id = ctx.user_data.get("bc_media_id")
        media_type = ctx.user_data.get("bc_media_type")

        # Get target users
        if bc_target == "premium":
            users = db.premium_users()
        elif bc_target == "vip":
            users = db.vip_users()
        elif bc_target == "connected":
            users = db.users_with_sessions()
        else:
            users = db.all_users()

        await update.message.reply_text(
            f"📢 Broadcasting to {len(users)} users..."
        )

        sent = 0
        failed = 0
        for u in users:
            try:
                if media_id and media_type:
                    await send_media_bot(
                        ctx.bot, u["user_id"], text,
                        media_id, media_type,
                    )
                else:
                    await ctx.bot.send_message(
                        chat_id=u["user_id"], text=text
                    )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1

        db.log(
            uid, "broadcast",
            f"target={bc_target} sent={sent} failed={failed}",
            "admin",
        )
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ *Broadcast Complete*\n\n"
            f"Target: {bc_target}\n"
            f"✅ Sent: {sent}\n"
            f"❌ Failed: {failed}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ ADMIN: ANNOUNCE ═══════════

    if state == ST_ADMIN_ANNOUNCE:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        media_id = ctx.user_data.get("announce_media_id")
        media_type = ctx.user_data.get("announce_media_type")

        db.add_announcement(
            "Announcement", text, uid,
            media_file_id=media_id, media_type=media_type,
        )

        users = db.all_users()
        await update.message.reply_text(
            f"📢 Sending announcement to {len(users)} users..."
        )

        sent = 0
        failed = 0
        announce_text = f"📢 *{BOT_NAME} Announcement*\n\n{text}"
        for u in users:
            try:
                if media_id and media_type:
                    await send_media_bot(
                        ctx.bot, u["user_id"],
                        announce_text, media_id, media_type,
                    )
                else:
                    await ctx.bot.send_message(
                        chat_id=u["user_id"],
                        text=announce_text,
                        parse_mode="Markdown",
                    )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1

        db.log(uid, "announcement", f"sent={sent}", "admin")
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ *Announcement Sent*\n\n"
            f"✅ Delivered: {sent}\n"
            f"❌ Failed: {failed}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ ADMIN: SEARCH ═══════════

    if state == ST_ADMIN_SEARCH:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        target = text.strip()
        result = None

        if target.lstrip("-").isdigit():
            result = db.get_user(int(target))
        if result is None:
            clean = target.lower().lstrip("@")
            for u in db.all_users():
                if (u["username"] or "").lower() == clean:
                    result = u
                    break

        ctx.user_data.clear()

        if not result:
            await update.message.reply_text(
                "❌ User not found.",
                reply_markup=main_kb(uid),
            )
            return

        target_uid = result["user_id"]
        plan = result["plan"] or "free"
        expiry = result["plan_until"] or "N/A"

        stats = db.all_stats(target_uid)
        stats_text = ""
        for k, v in sorted(stats.items()):
            if not k.startswith("pm_warn_") and not k.startswith("welcomed_"):
                stats_text += f"  • {k}: {v}\n"

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🚫 Ban",
                    callback_data=f"au_ban_{target_uid}",
                ),
                InlineKeyboardButton(
                    "✅ Unban",
                    callback_data=f"au_unban_{target_uid}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "⭐ Premium 30d",
                    callback_data=f"au_plan_{target_uid}_premium_30",
                ),
                InlineKeyboardButton(
                    "⭐ Premium 90d",
                    callback_data=f"au_plan_{target_uid}_premium_90",
                ),
            ],
            [
                InlineKeyboardButton(
                    "👑 VIP 30d",
                    callback_data=f"au_plan_{target_uid}_vip_30",
                ),
                InlineKeyboardButton(
                    "👑 VIP 90d",
                    callback_data=f"au_plan_{target_uid}_vip_90",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🆓 Set Free",
                    callback_data=f"au_plan_{target_uid}_free_0",
                ),
                InlineKeyboardButton(
                    "🗑️ Delete User",
                    callback_data=f"au_del_{target_uid}",
                ),
            ],
            [InlineKeyboardButton(
                "◀️ Back", callback_data="admin_home"
            )],
        ])

        info = (
            f"👤 *User Details*\n\n"
            f"🆔 ID: `{result['user_id']}`\n"
            f"📛 Username: @{result['username'] or 'none'}\n"
            f"👤 Name: {result['first_name'] or ''} "
            f"{result['last_name'] or ''}\n"
            f"📞 Phone: `{result['phone'] or 'N/A'}`\n"
            f"🟢 Session: {'Yes' if result['session_str'] else 'No'}\n"
            f"🚫 Banned: {'Yes' if result['is_banned'] else 'No'}\n"
            f"{'📝 Ban Reason: ' + (result['ban_reason'] or 'N/A') if result['is_banned'] else ''}\n"
            f"💎 Plan: {fmt_plan(plan)}\n"
            f"⏳ Expires: `{expiry[:10] if expiry != 'N/A' else 'N/A'}`\n"
            f"📅 Joined: `{(result['joined_at'] or '')[:10]}`\n"
            f"🕐 Last Active: `{(result['last_active'] or '')[:16]}`\n"
        )
        if stats_text:
            info += f"\n📊 *Stats:*\n{stats_text}"

        await update.message.reply_text(
            info, parse_mode="Markdown", reply_markup=kb
        )
        return

    # ═══════════ ADMIN: SET PLAN ═══════════

    if state == ST_ADMIN_SET_PLAN:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        parts = [p.strip() for p in text.split("|")]
        if len(parts) != 3:
            await update.message.reply_text(
                "❌ Format: `user_id | plan | days`\n"
                "Example: `123456789 | premium | 30`",
                parse_mode="Markdown",
            )
            return

        try:
            target_uid = int(parts[0])
            plan = parts[1].lower()
            days = int(parts[2])
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid format. Check user_id and days."
            )
            return

        if plan not in ("free", "premium", "vip"):
            await update.message.reply_text(
                "❌ Plan must be `free`, `premium`, or `vip`.",
                parse_mode="Markdown",
            )
            return

        target_user = db.get_user(target_uid)
        if not target_user:
            await update.message.reply_text(
                "❌ User not found."
            )
            return

        db.set_plan(target_uid, plan, days, admin_id=uid)
        ctx.user_data.clear()

        # Notify the user
        try:
            notify_text = (
                f"💎 *Plan Updated!*\n\n"
                f"Your plan: {fmt_plan(plan)}\n"
                f"{'Duration: ' + str(days) + ' days' if days > 0 else 'No expiry'}\n\n"
                f"Enjoy your new features! 🎉"
            )
            await ctx.bot.send_message(
                chat_id=target_uid,
                text=notify_text,
                parse_mode="Markdown",
            )
        except Exception:
            pass

        await update.message.reply_text(
            f"✅ Set `{target_uid}` to "
            f"*{fmt_plan(plan)}* for *{days}* days.",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ ADMIN: BAN REASON ═══════════

    if state == ST_ADMIN_BAN_REASON:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        target_uid = ctx.user_data.get("ban_target")
        if not target_uid:
            ctx.user_data.clear()
            return

        reason = text.strip()
        if reason.lower() == "none":
            reason = ""

        db.ban_user(target_uid, reason)

        # Stop their client if active
        if target_uid in active_clients:
            await stop_client(target_uid)

        # Notify the user
        try:
            ban_text = (
                f"⛔ *You have been banned*\n\n"
                f"{'Reason: ' + reason if reason else 'No reason provided.'}\n\n"
                f"Contact {SUPPORT_USERNAME} to appeal."
            )
            await ctx.bot.send_message(
                chat_id=target_uid,
                text=ban_text,
                parse_mode="Markdown",
            )
        except Exception:
            pass

        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Banned `{target_uid}`.\n"
            f"Reason: {reason or 'None'}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ NO STATE — FALLBACK ═══════════

    if state is not None:
        ctx.user_data.clear()
        await update.message.reply_text(
            "❓ Unknown state. Use /start to begin.",
            reply_markup=main_kb(uid),
        )
        return

    await update.message.reply_text(
        "Use /start to open the menu. 🦴",
        reply_markup=main_kb(uid),
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║            MESSAGE HANDLERS — MEDIA (PHOTO/VIDEO/DOC)        ║
# ╚══════════════════════════════════════════════════════════════╝


async def on_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming media messages (photo, video, doc, etc.)."""
    if not update.message or not update.effective_user:
        return

    uid = update.effective_user.id
    state = ctx.user_data.get("state")
    message = update.message
    caption = message.caption or ""

    if db.is_banned(uid):
        await message.reply_text(
            f"⛔ You are banned.\nContact {SUPPORT_USERNAME}"
        )
        return

    db.touch_user(uid)

    # Extract media info
    file_id, media_type = get_media_info(message)
    if not file_id:
        await message.reply_text(
            "❌ Could not process this media."
        )
        return

    # ═══════════ WELCOME MEDIA ═══════════

    if state == ST_WELCOME_MEDIA:
        db.set_setting(uid, "welcome_media_id", file_id)
        db.set_setting(uid, "welcome_media_type", media_type)
        db.log(uid, "welcome_media_set", media_type, "settings")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Welcome media ({media_type}) attached!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ AWAY MEDIA ═══════════

    if state == ST_AWAY_MEDIA:
        db.set_setting(uid, "away_media_id", file_id)
        db.set_setting(uid, "away_media_type", media_type)
        db.log(uid, "away_media_set", media_type, "settings")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Away media ({media_type}) attached!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ PM MEDIA ═══════════

    if state == ST_PM_MEDIA:
        db.set_setting(uid, "pm_media_id", file_id)
        db.set_setting(uid, "pm_media_type", media_type)
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ PM permit media ({media_type}) attached!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ KEYWORD RESPONSE WITH MEDIA ═══════════

    if state == ST_KW_RESPONSE:
        trigger = ctx.user_data.get("kw_trigger")
        match_type = ctx.user_data.get("kw_match_type", "contains")
        if not trigger:
            ctx.user_data.clear()
            await message.reply_text("❌ Keyword flow expired.")
            return

        if not db.plan_check(uid, "media_in_replies"):
            await message.reply_text(
                f"🔒 Media in replies requires *Premium+*.\n"
                f"Saving text-only response.\n"
                f"Contact {SUPPORT_USERNAME} to upgrade!",
                parse_mode="Markdown",
            )
            db.add_keyword(uid, trigger, caption or "Media reply",
                           match_type=match_type)
            ctx.user_data.clear()
            return

        response_text = caption or ""
        db.add_keyword(
            uid, trigger, response_text,
            match_type=match_type,
            media_file_id=file_id,
            media_type=media_type,
        )
        db.log(uid, "keyword_add_media",
               f"{trigger} ({media_type})", "keyword")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Keyword added with media!\n\n"
            f"Trigger: `{trigger}`\n"
            f"Match: {match_type}\n"
            f"Media: {media_type}\n"
            f"Caption: {truncate(response_text, 40) if response_text else 'None'}",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ KEYWORD MEDIA ADD (after creation) ═══════════

    if state == ST_KW_MEDIA:
        kid = ctx.user_data.get("kw_media_id")
        if not kid:
            ctx.user_data.clear()
            await message.reply_text("❌ Flow expired.")
            return
        db.update_keyword(
            uid, kid,
            media_file_id=file_id,
            media_type=media_type,
        )
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Media ({media_type}) attached to keyword!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ FILTER RESPONSE WITH MEDIA ═══════════

    if state == ST_FILTER_RESP:
        name = ctx.user_data.get("filter_name")
        if not name:
            ctx.user_data.clear()
            await message.reply_text("❌ Filter flow expired.")
            return

        if not db.plan_check(uid, "media_in_replies"):
            db.add_filter(uid, name, caption or "Media reply")
            ctx.user_data.clear()
            await message.reply_text(
                f"✅ Filter `{name}` added (text only).\n"
                f"🔒 Media requires Premium+.",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
            return

        db.add_filter(
            uid, name, caption or "",
            media_file_id=file_id,
            media_type=media_type,
        )
        db.log(uid, "filter_add_media",
               f"{name} ({media_type})", "filter")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Filter `{name}` added with media!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ FILTER MEDIA ADD ═══════════

    if state == ST_FILTER_MEDIA:
        fid = ctx.user_data.get("filter_media_id")
        if not fid:
            ctx.user_data.clear()
            await message.reply_text("❌ Flow expired.")
            return
        with db.conn() as cx:
            cx.execute(
                "UPDATE filters SET media_file_id=?, media_type=? "
                "WHERE id=? AND user_id=?",
                (file_id, media_type, fid, uid),
            )
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Media ({media_type}) attached to filter!",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ SCHEDULED MESSAGE MEDIA ═══════════

    if state == ST_SCHED_MSG:
        ctx.user_data["sched_msg"] = caption or "Scheduled media"
        ctx.user_data["sched_media_id"] = file_id
        ctx.user_data["sched_media_type"] = media_type
        ctx.user_data["state"] = ST_SCHED_TIME
        await message.reply_text(
            f"✅ Message with media ({media_type}) saved.\n\n"
            f"⏰ *Step 3/3:* Send the date and time.\n"
            f"Format: `YYYY-MM-DD HH:MM`",
            parse_mode="Markdown",
        )
        return

    # ═══════════ TEMPLATE CONTENT WITH MEDIA ═══════════

    if state == ST_TEMPLATE_CONTENT:
        name = ctx.user_data.get("tmpl_name")
        is_global = ctx.user_data.get("tmpl_global", False)
        if not name:
            ctx.user_data.clear()
            await message.reply_text("❌ Template flow expired.")
            return
        db.add_template(
            uid, name, caption or "",
            category="general",
            media_file_id=file_id,
            media_type=media_type,
            is_global=is_global,
        )
        db.log(uid, "template_add_media", name, "template")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Template `{name}` created with media!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ NOTE CONTENT WITH MEDIA ═══════════

    if state == ST_NOTE_CONTENT:
        title = ctx.user_data.get("note_title")
        if not title:
            ctx.user_data.clear()
            await message.reply_text("❌ Note flow expired.")
            return
        db.add_note(uid, title, caption or "", file_id, media_type)
        db.log(uid, "note_add_media", title, "notes")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Note `{title}` saved with media!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ CUSTOM CMD RESPONSE WITH MEDIA ═══════════

    if state == ST_CUSTOM_CMD_RESP:
        cmd_name = ctx.user_data.get("ccmd_name")
        if not cmd_name:
            ctx.user_data.clear()
            await message.reply_text("❌ Command flow expired.")
            return
        db.add_custom_cmd(
            uid, cmd_name, caption or "Media response",
            file_id, media_type,
        )
        db.log(uid, "custom_cmd_add_media", cmd_name, "custom_cmd")
        ctx.user_data.clear()
        await message.reply_text(
            f"✅ Command `/{cmd_name}` created with media!",
            parse_mode="Markdown",
            reply_markup=main_kb(uid),
        )
        return

    # ═══════════ BROADCAST WITH MEDIA ═══════════

    if state == ST_ADMIN_BROADCAST:
        if not is_admin(uid):
            ctx.user_data.clear()
            return
        ctx.user_data["bc_media_id"] = file_id
        ctx.user_data["bc_media_type"] = media_type

        # If there's caption, proceed with broadcast
        if caption:
            # Simulate text handler with caption
            update.message.text = caption
            await on_text(update, ctx)
            return

        await message.reply_text(
            "📎 Media received. Now send the broadcast text.\n"
            "Or send just a dot `.` to broadcast media only.",
        )
        return

    # ═══════════ ANNOUNCE WITH MEDIA ═══════════

    if state == ST_ADMIN_ANNOUNCE:
        if not is_admin(uid):
            ctx.user_data.clear()
            return
        ctx.user_data["announce_media_id"] = file_id
        ctx.user_data["announce_media_type"] = media_type
        if caption:
            update.message.text = caption
            await on_text(update, ctx)
            return
        await message.reply_text(
            "📎 Media received. Now send announcement text."
        )
        return

    # ═══════════ PROFILE PHOTO ═══════════

    if state == ST_PROFILE_PIC:
        client = get_client(uid)
        if not client:
            ctx.user_data.clear()
            await message.reply_text(
                "❌ Not connected.",
                reply_markup=main_kb(uid),
            )
            return

        if not message.photo:
            await message.reply_text(
                "❌ Please send a *photo* for profile picture.",
                parse_mode="Markdown",
            )
            return

        photo = message.photo[-1]
        tg_file = await photo.get_file()

        with tempfile.NamedTemporaryFile(
            suffix=".jpg", delete=False
        ) as tmp:
            temp_path = tmp.name

        try:
            await tg_file.download_to_drive(temp_path)

            uploaded = await client.upload_file(temp_path)
            await client(UploadProfilePhotoRequest(file=uploaded))

            db.log(uid, "profile_photo_set", "", "profile")
            await message.reply_text(
                "✅ Profile photo updated!",
                reply_markup=main_kb(uid),
            )
        except Exception as exc:
            await message.reply_text(
                f"❌ Failed: `{exc}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        finally:
            ctx.user_data.clear()
            try:
                os.remove(temp_path)
            except Exception:
                pass
        return

    # ═══════════ IMPORT FILE ═══════════

    if state == ST_IMPORT_FILE:
        if not message.document:
            await message.reply_text(
                "❌ Send a JSON file."
            )
            return

        doc = message.document
        if not doc.file_name.lower().endswith(".json"):
            await message.reply_text(
                "❌ File must be a `.json` file.",
                parse_mode="Markdown",
            )
            return

        try:
            tg_file = await doc.get_file()
            file_bytes = await tg_file.download_as_bytearray()
            data = json.loads(file_bytes.decode("utf-8"))

            if not isinstance(data, dict):
                raise ValueError("Invalid backup format")

            db.import_user_data(uid, data)
            db.log(uid, "data_import", "From JSON file", "backup")
            ctx.user_data.clear()
            await message.reply_text(
                "✅ Settings imported successfully!\n\n"
                "Your keywords, filters, and settings "
                "have been restored.",
                reply_markup=main_kb(uid),
            )
        except json.JSONDecodeError:
            await message.reply_text(
                "❌ Invalid JSON file."
            )
        except Exception as exc:
            await message.reply_text(
                f"❌ Import failed: `{exc}`",
                parse_mode="Markdown",
            )
        return
    # ═══════════ ADMIN: UPLOAD DB ═══════════

    if state == ST_ADMIN_UPLOAD_DB:
        if not is_admin(uid):
            ctx.user_data.clear()
            return

        if not message.document:
            await message.reply_text("❌ Send a database file.")
            return

        doc = message.document
        fname = doc.file_name.lower()
        if not fname.endswith((".db", ".sqlite", ".sqlite3")):
            await message.reply_text(
                "❌ Send a SQLite `.db` file."
            )
            return

        tg_file = await doc.get_file()
        backup_name = (
            f"{DB_FILE}.backup_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        incoming = f"{DB_FILE}.incoming"

        try:
            await tg_file.download_to_drive(incoming)

            # Validate SQLite
            with sqlite3.connect(incoming) as test_conn:
                test_conn.execute(
                    "SELECT name FROM sqlite_master LIMIT 1"
                )

            # Backup current
            if os.path.exists(DB_FILE):
                shutil.copy2(DB_FILE, backup_name)
                shutil.copy2(
                    DB_FILE,
                    os.path.join(
                        BACKUP_DIR, os.path.basename(backup_name)
                    ),
                )

            os.replace(incoming, DB_FILE)

            # Reinitialize database without global keyword
            _reload_database()

            db.log(uid, "db_upload", f"backup={backup_name}", "admin")
            ctx.user_data.clear()
            await message.reply_text(
                f"✅ Database replaced!\n"
                f"Backup: `{backup_name}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        except Exception as exc:
            ctx.user_data.clear()
            try:
                os.remove(incoming)
            except Exception:
                pass
            await message.reply_text(
                f"❌ Failed: `{exc}`",
                parse_mode="Markdown",
                reply_markup=main_kb(uid),
            )
        return

    # ═══════════ NO STATE — FALLBACK ═══════════

    await message.reply_text(
        "ℹ️ No media action is pending.\n"
        "Use /start to open the menu.",
        reply_markup=main_kb(uid),
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║            ADDITIONAL CALLBACK HANDLERS                      ║
# ╚══════════════════════════════════════════════════════════════╝


async def on_callback_media(update: Update,
                            ctx: ContextTypes.DEFAULT_TYPE):
    """Handle media-related callbacks that need state setup."""
    q = update.callback_query
    if not q:
        return
    await q.answer()
    uid = q.from_user.id
    data = q.data

    # Keyword media add (after creation)
    if data.startswith("kw_add_media_"):
        kid = int(data[13:])
        if not db.plan_check(uid, "media_in_replies"):
            await q.edit_message_text(
                f"🔒 Media requires *Premium+*.",
                parse_mode="Markdown",
                reply_markup=back_btn("kw_menu"),
            )
            return
        ctx.user_data["state"] = ST_KW_MEDIA
        ctx.user_data["kw_media_id"] = kid
        await q.edit_message_text(
            "📎 *Attach Media to Keyword*\n\n"
            "Send a photo, video, GIF, document, or voice.\n\n"
            "_/cancel to skip._",
            parse_mode="Markdown",
        )
        return

    # Filter media add (after creation)
    if data.startswith("flt_add_media_"):
        fid = int(data[14:])
        if not db.plan_check(uid, "media_in_replies"):
            await q.edit_message_text(
                f"🔒 Media requires *Premium+*.",
                parse_mode="Markdown",
                reply_markup=back_btn("filter_menu"),
            )
            return
        ctx.user_data["state"] = ST_FILTER_MEDIA
        ctx.user_data["filter_media_id"] = fid
        await q.edit_message_text(
            "📎 *Attach Media to Filter*\n\n"
            "Send a photo, video, GIF, document, or voice.\n\n"
            "_/cancel to skip._",
            parse_mode="Markdown",
        )
        return


# ╔══════════════════════════════════════════════════════════════╗
# ║                  BACKGROUND TASKS                            ║
# ╚══════════════════════════════════════════════════════════════╝


async def scheduled_worker(app: Application):
    """Background task to send scheduled messages."""
    logger.info("Scheduled worker started")
    await asyncio.sleep(5)

    while True:
        try:
            pending = db.pending_scheduled()
            for item in pending:
                uid = item["user_id"]
                sess = item["session_str"]
                if not sess:
                    continue

                temp_client = TelegramClient(
                    StringSession(sess),
                    API_ID,
                    API_HASH,
                    device_model=BOT_NAME,
                    system_version="2.0",
                    app_version=BOT_VERSION,
                )
                try:
                    await temp_client.connect()

                    if not await temp_client.is_user_authorized():
                        db.log(
                            uid, "sched_auth_fail",
                            f"id={item['id']}", "schedule",
                        )
                        continue

                    target = item["target"]
                    try:
                        target = int(target)
                    except (ValueError, TypeError):
                        pass

                    msg = item["message"] or ""
                    mid = item["media_file_id"]
                    mtype = item["media_type"]

                    if mid and mtype:
                        try:
                            await temp_client.send_file(
                                target, mid, caption=msg
                            )
                        except Exception:
                            await temp_client.send_message(
                                target, msg
                            )
                    else:
                        await temp_client.send_message(target, msg)

                    db.mark_sent(
                        item["id"],
                        bool(item["recurring"]),
                        int(item["interval_hr"] or 0),
                        int(item["max_repeats"] or 0),
                        int(item["repeat_count"] or 0),
                    )
                    db.inc_stat(uid, "scheduled_sent")
                    db.log(
                        uid, "scheduled_sent",
                        f"id={item['id']} to={item['target']}",
                        "schedule",
                    )

                    # Notify user
                    try:
                        await app.bot.send_message(
                            chat_id=uid,
                            text=(
                                f"✅ Scheduled message sent!\n"
                                f"To: `{item['target']}`\n"
                                f"Msg: {truncate(msg, 50)}"
                            ),
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass

                except Exception as exc:
                    db.log(
                        uid, "scheduled_error",
                        f"id={item['id']} err={str(exc)[:100]}",
                        "schedule",
                    )
                    logger.error(
                        "Scheduled send error uid=%s: %s",
                        uid, exc,
                    )
                finally:
                    try:
                        await temp_client.disconnect()
                    except Exception:
                        pass

        except Exception as exc:
            logger.exception("scheduled_worker error: %s", exc)

        await asyncio.sleep(15)


async def plan_expiry_checker(app: Application):
    """Check and handle expiring plans."""
    logger.info("Plan expiry checker started")
    await asyncio.sleep(30)

    while True:
        try:
            # Check for expired plans
            now = datetime.now().isoformat()
            with db.conn() as cx:
                expired = cx.execute(
                    """SELECT * FROM users
                       WHERE plan != 'free'
                       AND plan_until IS NOT NULL
                       AND plan_until < ?""",
                    (now,),
                ).fetchall()

            for user in expired:
                uid = user["user_id"]
                old_plan = user["plan"]
                db.set_plan(uid, "free", admin_id=0, auto=True)
                logger.info(
                    "Plan expired for uid=%s (%s → free)",
                    uid, old_plan,
                )

                # Notify user
                try:
                    await app.bot.send_message(
                        chat_id=uid,
                        text=(
                            f"⚠️ *Plan Expired*\n\n"
                            f"Your {fmt_plan(old_plan)} plan has expired.\n"
                            f"You've been moved to the 🆓 Free plan.\n\n"
                            f"Contact {SUPPORT_USERNAME} to renew!"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            # Notify users with plans expiring in 3 days
            expiring = db.expiring_plans(3)
            for user in expiring:
                uid = user["user_id"]
                try:
                    exp = datetime.fromisoformat(user["plan_until"])
                    days_left = (exp - datetime.now()).days
                    if days_left in (3, 1):
                        notif_key = f"expiry_notified_{days_left}d"
                        if db.get_stat(uid, notif_key) == 0:
                            await app.bot.send_message(
                                chat_id=uid,
                                text=(
                                    f"⏳ *Plan Expiring Soon!*\n\n"
                                    f"Your {fmt_plan(user['plan'])} plan "
                                    f"expires in *{days_left}* day(s).\n\n"
                                    f"Renew now! Contact {SUPPORT_USERNAME}"
                                ),
                                parse_mode="Markdown",
                            )
                            db.inc_stat(uid, notif_key)
                except Exception:
                    pass

        except Exception as exc:
            logger.exception("plan_expiry_checker error: %s", exc)

        await asyncio.sleep(3600)


async def cleanup_worker():
    """Periodic database cleanup."""
    logger.info("Cleanup worker started")
    await asyncio.sleep(60)

    while True:
        try:
            db.cleanup()
            logger.info("Database cleanup completed")
        except Exception as exc:
            logger.exception("cleanup_worker error: %s", exc)
        await asyncio.sleep(3600 * 6)


async def health_check_worker():
    """Check active client connections periodically."""
    logger.info("Health check worker started")
    await asyncio.sleep(120)

    while True:
        try:
            disconnected = []
            for uid, client in list(active_clients.items()):
                try:
                    if not client.is_connected():
                        disconnected.append(uid)
                except Exception:
                    disconnected.append(uid)

            for uid in disconnected:
                logger.info("Reconnecting uid=%s", uid)
                try:
                    await start_client(uid)
                except Exception as exc:
                    logger.error(
                        "Health reconnect failed uid=%s: %s",
                        uid, exc,
                    )

        except Exception as exc:
            logger.exception("health_check error: %s", exc)

        await asyncio.sleep(300)


async def reconnect_saved_clients():
    """Reconnect all saved sessions on startup."""
    logger.info("Reconnecting saved clients...")
    users = db.users_with_sessions()
    connected = 0
    failed = 0

    for user in users:
        uid = user["user_id"]
        try:
            client = await start_client(uid)
            if client:
                connected += 1
            else:
                failed += 1
        except Exception as exc:
            logger.error(
                "Reconnect failed uid=%s: %s", uid, exc
            )
            failed += 1
        await asyncio.sleep(1)

    logger.info(
        "Reconnect complete: %d connected, %d failed",
        connected, failed,
    )


async def post_init(app: Application):
    """Initialize background tasks after bot starts."""
    logger.info("%s v%s initializing...", BOT_NAME, BOT_VERSION)

    asyncio.create_task(reconnect_saved_clients())
    asyncio.create_task(scheduled_worker(app))
    asyncio.create_task(plan_expiry_checker(app))
    asyncio.create_task(cleanup_worker())
    asyncio.create_task(health_check_worker())

    logger.info("All background tasks started")

    # Notify admin
    try:
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🟢 *{BOT_NAME} v{BOT_VERSION} Started*\n\n"
                f"Users: {db.total_users()}\n"
                f"Sessions: {db.active_sessions_count()}\n"
                f"DB Size: {db.db_size()}"
            ),
            parse_mode="Markdown",
        )
    except Exception:
        pass


# ╔══════════════════════════════════════════════════════════════╗
# ║                    APPLICATION BUILDER                       ║
# ╚══════════════════════════════════════════════════════════════╝


def build_application() -> Application:
    """Build and configure the bot application."""
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .build()
    )

    # Command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("feedback", cmd_feedback))

    # Callback handlers
    app.add_handler(CallbackQueryHandler(
        on_callback_media,
        pattern=r"^(kw_add_media_|flt_add_media_)",
    ))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Media handlers (must be before text handler)
    media_filter = (
        filters.PHOTO
        | filters.VIDEO
        | filters.ANIMATION
        | filters.Document.ALL
        | filters.VOICE
        | filters.AUDIO
        | filters.VIDEO_NOTE
        | filters.Sticker.ALL
    )
    app.add_handler(MessageHandler(media_filter, on_media))

    # Text handler (last)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, on_text
    ))

    return app


# ╔══════════════════════════════════════════════════════════════╗
# ║                       MAIN                                   ║
# ╚══════════════════════════════════════════════════════════════╝


def main():
    """Main entry point."""
    # Validate configuration
    errors = []
    if not BOT_TOKEN:
        errors.append("BOT_TOKEN is not set")
    if not API_ID:
        errors.append("API_ID is not set")
    if not API_HASH:
        errors.append("API_HASH is not set")
    if not ADMIN_ID:
        errors.append("ADMIN_ID is not set")

    if errors:
        for err in errors:
            logger.error("❌ Configuration: %s", err)
        raise RuntimeError(
            "Missing required configuration. "
            "Set BOT_TOKEN, API_ID, API_HASH, and ADMIN_ID."
        )

    # Create directories
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)

    # Build and run
    app = build_application()

    logger.info("=" * 50)
    logger.info("%s v%s", BOT_NAME, BOT_VERSION)
    logger.info("Admin: %s", ADMIN_ID)
    logger.info("Support: %s", SUPPORT_USERNAME)
    logger.info("Database: %s (%s)", DB_FILE, db.db_size())
    logger.info("Users: %s", db.total_users())
    logger.info("Sessions: %s", db.active_sessions_count())
    logger.info("=" * 50)
    logger.info("Starting polling...")

    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
