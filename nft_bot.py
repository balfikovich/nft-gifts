"""
NFT Gift Viewer Bot
===================
Зависимости:
    pip install aiogram aiohttp python-dotenv pillow beautifulsoup4 lxml
    apt install ffmpeg

Переменные окружения (.env):
    BOT_TOKEN=xxx
    LOG_FILE=bot.log   (опционально)
"""

import asyncio
import hashlib
import io
import logging
import multiprocessing
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from collections import OrderedDict
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    MessageEntity,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InputTextMessageContent,
    PreCheckoutQuery,
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv

# ── Конфиг ───────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан! Создай .env: BOT_TOKEN=xxx")

ADMIN_ID = 5479063264

# ══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════

LOG_FILE = os.environ.get("LOG_FILE", "bot.log")
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_ch, _fh])
logger   = logging.getLogger(__name__)
user_log = logging.getLogger("user_events")


def _u(user) -> str:
    if user is None:
        return "unknown"
    name  = (user.full_name or "").strip() or "NoName"
    uname = f" @{user.username}" if user.username else " (без username)"
    return f"{name}{uname} (id={user.id})"


def _chat(chat) -> str:
    if chat is None:
        return "unknown"
    if chat.type == "private":
        return f"личка [private] (id={chat.id})"
    title = (getattr(chat, "title", None) or "NoTitle").strip()
    return f'"{title}" [{chat.type}] (id={chat.id})'


# ══════════════════════════════════════════════════════════════════════════════
#  КОНСТАНТЫ
# ══════════════════════════════════════════════════════════════════════════════

FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
FRAGMENT_TGS_URL   = "https://nft.fragment.com/gift/{slug}.tgs"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=30)

CB_NO_COMPRESS       = "nc:"
CB_NO_ANIM           = "na:"
CB_SEND_STICKER      = "sk:"
CB_NO_COMPRESS_VIDEO = "ncv:"
CB_SEND_GIF          = "gif:"
CB_DONATE            = "donate"

ANTISPAM_SECONDS  = 1.5
ANTISPAM_SLUG_SEC = 120

# Custom Emoji IDs
E_GIFT      = "5408829285685291820"
E_MODEL     = "5408894951440279259"
E_BACK      = "5411585799990830248"
E_SYMBOL    = "5409189019261103031"
E_LINK      = "5409143419593321597"
E_WARN      = "5409124594751660992"
E_ERR       = "5408930028438188841"
E_START     = "6028495398941759268"
E_DONATE    = "5309759985192832914"
E_FLOOR_GEM = "5409321884074419506"   # 💎 для Floorprice
E_FLOOR_TON = "5316802593391916971"   # ❤️ для цены TON

# ══════════════════════════════════════════════════════════════════════════════
#  СЛОВАРЬ ПРАВИЛЬНЫХ НАЗВАНИЙ ПОДАРКОВ
# ══════════════════════════════════════════════════════════════════════════════

_GIFT_NAMES: set[str] = {
    "Santa Hat", "Signet Ring", "Precious Peach", "Plush Pepe", "Spiced Wine",
    "Jelly Bunny", "Durov's Cap", "Perfume Bottle", "Eternal Rose", "Berry Box",
    "Vintage Cigar", "Magic Potion", "Kissed Frog", "Hex Pot", "Evil Eye",
    "Sharp Tongue", "Trapped Heart", "Skull Flower", "Scared Cat", "Spy Agaric",
    "Homemade Cake", "Genie Lamp", "Lunar Snake", "Party Sparkler", "Jester Hat",
    "Witch Hat", "Hanging Star", "Love Candle", "Cookie Heart", "Desk Calendar",
    "Jingle Bells", "Snow Mittens", "Voodoo Doll", "Mad Pumpkin", "Hypno Lollipop",
    "B-Day Candle", "Bunny Muffin", "Astral Shard", "Flying Broom", "Crystal Ball",
    "Eternal Candle", "Swiss Watch", "Ginger Cookie", "Mini Oscar", "Lol Pop",
    "Ion Gem", "Star Notepad", "Loot Bag", "Love Potion", "Toy Bear", "Diamond Ring",
    "Sakura Flower", "Sleigh Bell", "Top Hat", "Record Player", "Winter Wreath",
    "Snow Globe", "Electric Skull", "Tama Gadget", "Candy Cane", "Neko Helmet",
    "Jack-in-the-Box", "Easter Egg", "Bonded Ring", "Pet Snake", "Snake Box",
    "Xmas Stocking", "Big Year", "Holiday Drink", "Gem Signet", "Light Sword",
    "Restless Jar", "Nail Bracelet", "Heroic Helmet", "Bow Tie", "Heart Locket",
    "Lush Bouquet", "Whip Cupcake", "Joyful Bundle", "Cupid Charm", "Valentine Box",
    "Snoop Dogg", "Swag Bag", "Snoop Cigar", "Low Rider", "Westside Sign",
    "Stellar Rocket", "Jolly Chimp", "Moon Pendant", "Ionic Dryer", "Input Key",
    "Mighty Arm", "Artisan Brick", "Clover Pin", "Sky Stilettos", "Fresh Socks",
    "Happy Brownie", "Ice Cream", "Spring Basket", "Instant Ramen", "Faith Amulet",
    "Mousse Cake", "Bling Binky", "Money Pot", "Pretty Posy", "Khabib's Papakha",
    "UFC Strike", "Victory Medal", "Rare Bird",
}

def _slug_key(name: str) -> str:
    """Ключ для сравнения slug с названием: lowercase, убираем всё кроме букв и цифр."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


_GIFT_NAME_MAP: dict[str, str] = {n.lower(): n for n in _GIFT_NAMES}
_GIFT_NAME_MAP_NOSPACE: dict[str, str] = {
    n.lower().replace(" ", ""): n for n in _GIFT_NAMES
}
# Ключ без ЛЮБЫХ спецсимволов (апострофы, дефисы и т.д.) — для матчинга slug
_GIFT_NAME_MAP_SLUG: dict[str, str] = {
    _slug_key(n): n for n in _GIFT_NAMES
}


def normalize_gift_name(raw_name: str) -> str:
    key = raw_name.lower().strip()
    # 1. Точное совпадение с пробелами
    if key in _GIFT_NAME_MAP:
        return _GIFT_NAME_MAP[key]
    # 2. Убираем пробелы/дефисы/подчёркивания
    key_nospace = re.sub(r"[\s\-_]+", "", key)
    if key_nospace in _GIFT_NAME_MAP_NOSPACE:
        return _GIFT_NAME_MAP_NOSPACE[key_nospace]
    # 3. Slug-ключ: убираем ВСЕ спецсимволы включая апострофы (khabibspapakha → Khabib's Papakha)
    key_slug = _slug_key(raw_name)
    if key_slug in _GIFT_NAME_MAP_SLUG:
        return _GIFT_NAME_MAP_SLUG[key_slug]
    # 4. Фоллбэк: CamelCase разбивка
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw_name.strip())
    spaced = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", spaced)
    return " ".join(w.capitalize() for w in re.split(r"[\s_\-]+", spaced) if w)


# ══════════════════════════════════════════════════════════════════════════════
#  АНТИСПАМ И СОСТОЯНИЯ
# ══════════════════════════════════════════════════════════════════════════════

_last_request:           dict[int, float]      = {}
_last_slug:              dict[str, float]       = {}
_cb_locks:               dict[int, asyncio.Lock] = {}   # asyncio.Lock на пользователя
_used_no_compress:       set[str]               = set()
_used_no_anim:           set[str]               = set()
_used_sticker:           set[str]               = set()
_used_no_compress_video: set[str]               = set()
_used_gif:               set[str]               = set()
_awaiting_donate:        set[int]               = set()
_last_instr:             dict[int, float]       = {}
_last_button:            dict[str, float]       = {}


def _get_cb_lock(uid: int) -> asyncio.Lock:
    """Возвращает asyncio.Lock для конкретного пользователя (создаёт если нет)."""
    if uid not in _cb_locks:
        _cb_locks[uid] = asyncio.Lock()
    return _cb_locks[uid]

ANTISPAM_INSTR_SEC  = 300
ANTISPAM_BUTTON_SEC = 90.0

BOT_USERNAME: str = ""

# ── Прогрессивный антиспам ────────────────────────────────────────────────────
# Храним историю запросов: uid -> list[timestamp]
_spam_history:   dict[int, list[float]] = {}
# Когда пользователь в муте: uid -> until (monotonic)
_spam_muted:     dict[int, float]       = {}
# Когда последний раз предупреждали: uid -> timestamp
_spam_warned:    dict[int, float]       = {}
# Когда последний раз отправили сообщение о муте: uid -> timestamp
_spam_mute_notified: dict[int, float]   = {}

SPAM_WINDOW_SHORT = 30.0   # секунд — окно для подсчёта быстрых запросов
SPAM_WINDOW_LONG  = 60.0   # секунд — окно для жёсткого бана
SPAM_WARN_THRESH  = 5      # запросов за SPAM_WINDOW_SHORT → предупреждение
SPAM_MUTE_THRESH  = 10     # запросов за SPAM_WINDOW_SHORT → мут 5 минут
SPAM_BAN_THRESH   = 20     # запросов за SPAM_WINDOW_LONG  → мут 1 час
SPAM_MUTE_SHORT   = 300.0  # 5 минут
SPAM_MUTE_LONG    = 3600.0 # 1 час
SPAM_IDLE_NOTIFY  = 60.0   # секунд тишины → отправить уведомление о муте

# ── Семафор конвертации (не более 5 ffmpeg одновременно) ─────────────────────
_convert_semaphore = asyncio.Semaphore(5)

# ── Лимит параллельных генераций в чате ──────────────────────────────────────
_chat_active: dict[int, int] = {}   # chat_id -> кол-во активных генераций
CHAT_MAX_PARALLEL = 5


def _chat_acquire(chat_id: int) -> bool:
    """Возвращает True если можно начать генерацию в чате, иначе False."""
    count = _chat_active.get(chat_id, 0)
    if count >= CHAT_MAX_PARALLEL:
        return False
    _chat_active[chat_id] = count + 1
    return True


def _chat_release(chat_id: int) -> None:
    count = _chat_active.get(chat_id, 0)
    _chat_active[chat_id] = max(0, count - 1)


# ── Кэш сконвертированных видео на диске — LRU, 20 слотов ────────────────────

_VIDEO_CACHE_DIR   = os.path.join(tempfile.gettempdir(), "nft_video_cache")
_VIDEO_CACHE_MAX   = 20      # максимум видео в кэше одновременно
_VIDEO_CACHE_TTL   = 1200.0  # 20 минут
# OrderedDict: cache_key -> slug (порядок = порядок добавления, старое в начале)
_video_cache_lru:  OrderedDict[str, str]   = OrderedDict()
_video_cache_time: dict[str, float]        = {}  # cache_key -> время добавления

os.makedirs(_VIDEO_CACHE_DIR, exist_ok=True)


def _video_cache_key(slug: str) -> str:
    return hashlib.md5(slug.lower().encode()).hexdigest()


def _video_cache_get(slug: str) -> Optional[bytes]:
    key  = _video_cache_key(slug)
    path = os.path.join(_VIDEO_CACHE_DIR, f"{key}.mp4")
    if key not in _video_cache_lru or not os.path.exists(path):
        _video_cache_lru.pop(key, None)
        _video_cache_time.pop(key, None)
        return None
    # Проверяем TTL
    now = time.monotonic()
    added = _video_cache_time.get(key, 0.0)
    if now - added > _VIDEO_CACHE_TTL:
        _video_cache_lru.pop(key, None)
        _video_cache_time.pop(key, None)
        try:
            os.remove(path)
        except Exception:
            pass
        return None
    # Продлеваем TTL при обращении и двигаем в конец LRU
    _video_cache_time[key] = now
    _video_cache_lru.move_to_end(key)
    try:
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def _video_cache_put(slug: str, data: bytes) -> None:
    key  = _video_cache_key(slug)
    path = os.path.join(_VIDEO_CACHE_DIR, f"{key}.mp4")
    try:
        with open(path, "wb") as f:
            f.write(data)
    except Exception as e:
        logger.warning("video_cache_put error: %s", e)
        return

    now = time.monotonic()
    _video_cache_time[key] = now

    if key in _video_cache_lru:
        _video_cache_lru.move_to_end(key)
        return

    _video_cache_lru[key] = slug

    # Если превысили лимит — удаляем самое старое
    while len(_video_cache_lru) > _VIDEO_CACHE_MAX:
        old_key, old_slug = _video_cache_lru.popitem(last=False)
        _video_cache_time.pop(old_key, None)
        old_path = os.path.join(_VIDEO_CACHE_DIR, f"{old_key}.mp4")
        try:
            os.remove(old_path)
            logger.info("🗑 Видео вытеснено из кэша: %s", old_slug)
        except Exception:
            pass


def _video_cache_cleanup() -> None:
    """Удаляет файлы кэша которых нет в LRU (мусор после перезапуска)."""
    known = {f"{k}.mp4" for k in _video_cache_lru}
    try:
        for fname in os.listdir(_VIDEO_CACHE_DIR):
            if fname not in known:
                try:
                    os.remove(os.path.join(_VIDEO_CACHE_DIR, fname))
                except Exception:
                    pass
    except Exception:
        pass


# ── Кэш атрибутов NFT ─────────────────────────────────────────────────────────
_attrs_cache:     dict[str, tuple[object, float]] = {}  # slug -> (NftAttrs, timestamp)
_ATTRS_CACHE_TTL = 600.0   # 10 минут


def _attrs_cache_get(slug: str) -> Optional[object]:
    entry = _attrs_cache.get(slug)
    if entry is None:
        return None
    attrs, ts = entry
    if time.monotonic() - ts > _ATTRS_CACHE_TTL:
        del _attrs_cache[slug]
        return None
    return attrs


def _attrs_cache_put(slug: str, attrs: object) -> None:
    _attrs_cache[slug] = (attrs, time.monotonic())


# ── Фоновая очистка (каждый час) ─────────────────────────────────────────────
async def _background_cleanup() -> None:
    """Периодически чистит все словари состояний от устаревших записей."""
    while True:
        await asyncio.sleep(3600)
        now = time.monotonic()
        try:
            # _last_request — записи старше 10 минут
            for uid in list(_last_request):
                if now - _last_request[uid] > 600:
                    del _last_request[uid]
            # _last_slug — старше 15 минут
            for k in list(_last_slug):
                if now - _last_slug[k] > 900:
                    del _last_slug[k]
            # _last_instr
            for k in list(_last_instr):
                if now - _last_instr[k] > 600:
                    del _last_instr[k]
            # _last_button
            for k in list(_last_button):
                if now - _last_button[k] > 600:
                    del _last_button[k]
            # _spam_history — убираем старые окна
            for uid in list(_spam_history):
                _spam_history[uid] = [t for t in _spam_history[uid] if now - t < SPAM_WINDOW_LONG + 10]
                if not _spam_history[uid]:
                    del _spam_history[uid]
            # _spam_muted — истёкшие муты
            for uid in list(_spam_muted):
                if now >= _spam_muted[uid]:
                    del _spam_muted[uid]
            # _attrs_cache
            for slug in list(_attrs_cache):
                _, ts = _attrs_cache[slug]
                if now - ts > _ATTRS_CACHE_TTL:
                    del _attrs_cache[slug]
            # video cache
            _video_cache_cleanup()
            # _used_* sets — очищаем если накопилось много
            for s in (_used_no_compress, _used_no_anim, _used_sticker,
                      _used_no_compress_video, _used_gif):
                if len(s) > 5000:
                    s.clear()
            # _cb_locks — удаляем незанятые локи для освобождения памяти
            for uid in list(_cb_locks):
                if not _cb_locks[uid].locked():
                    del _cb_locks[uid]
            logger.info("🧹 Очистка завершена | spam_muted=%d | attrs_cache=%d | video_cache=%d",
                        len(_spam_muted), len(_attrs_cache), len(_video_cache_meta))
        except Exception as e:
            logger.error("_background_cleanup error: %s", e)


# ── Прогрессивная антиспам проверка ──────────────────────────────────────────

def check_spam_progressive(uid: int) -> Optional[str]:
    """
    Проверяет только мут — не записывает запрос в историю.
    Для записи используй record_spam_event(uid).
    """
    now = time.monotonic()
    muted_until = _spam_muted.get(uid)
    if muted_until is not None:
        if now < muted_until:
            return "muted"
        else:
            del _spam_muted[uid]
    return None


def record_spam_event(uid: int) -> Optional[str]:
    """
    Записывает событие запроса и возвращает результат проверки:
    None — OK, "warn" — предупреждение, "mute" — 5 мин, "ban" — 1 час.
    Вызывается ТОЛЬКО при реальных запросах к боту (текст + кнопки).
    """
    now = time.monotonic()

    # Если уже в муте — просто возвращаем статус без записи
    muted_until = _spam_muted.get(uid)
    if muted_until is not None:
        if now < muted_until:
            return "muted"
        else:
            del _spam_muted[uid]

    history = _spam_history.setdefault(uid, [])
    history.append(now)

    recent_short = [t for t in history if now - t <= SPAM_WINDOW_SHORT]
    recent_long  = [t for t in history if now - t <= SPAM_WINDOW_LONG]
    _spam_history[uid] = recent_long

    count_short = len(recent_short)
    count_long  = len(recent_long)

    if count_long >= SPAM_BAN_THRESH:
        _spam_muted[uid] = now + SPAM_MUTE_LONG
        return "ban"

    if count_short >= SPAM_MUTE_THRESH:
        _spam_muted[uid] = now + SPAM_MUTE_SHORT
        return "mute"

    if count_short >= SPAM_WARN_THRESH:
        last_warn = _spam_warned.get(uid, 0.0)
        if now - last_warn > SPAM_WINDOW_SHORT:
            _spam_warned[uid] = now
            return "warn"

    return None


def get_spam_mute_remaining(uid: int) -> Optional[int]:
    """Возвращает сколько секунд осталось в муте, или None."""
    muted_until = _spam_muted.get(uid)
    if muted_until is None:
        return None
    remaining = int(muted_until - time.monotonic())
    return remaining if remaining > 0 else None


def check_antispam(user_id: int) -> float:
    now  = time.monotonic()
    last = _last_request.get(user_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_SECONDS:
        return round(ANTISPAM_SECONDS - diff, 1)
    _last_request[user_id] = now
    return 0.0


def check_slug_antispam(chat_id: int, slug: str) -> float:
    key  = f"{chat_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_slug.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_SLUG_SEC:
        return int(ANTISPAM_SLUG_SEC - diff)
    _last_slug[key] = now
    return 0.0


def check_instr_antispam(chat_id: int) -> float:
    now  = time.monotonic()
    last = _last_instr.get(chat_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_INSTR_SEC:
        return int(ANTISPAM_INSTR_SEC - diff)
    _last_instr[chat_id] = now
    return 0.0


def check_button_antispam(user_id: int, prefix: str) -> float:
    key  = f"{user_id}:{prefix}"
    now  = time.monotonic()
    last = _last_button.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_BUTTON_SEC:
        return round(ANTISPAM_BUTTON_SEC - diff, 1)
    _last_button[key] = now
    return 0.0


# ── HTTP-сессия ───────────────────────────────────────────────────────────────
http_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
    return http_session


# ══════════════════════════════════════════════════════════════════════════════
#  ПАРСИНГ SLUG
# ══════════════════════════════════════════════════════════════════════════════

_RE_LINK = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)
_RE_SLUG  = re.compile(r"^([A-Za-z][A-Za-z0-9]*)[-](\d+)$", re.IGNORECASE)
_RE_WORDS = re.compile(
    r"^([A-Za-z][A-Za-z0-9]*(?:\s+[A-Za-z][A-Za-z0-9]*)*)\s+(\d+)$",
    re.IGNORECASE,
)


def extract_nft_slug(raw: str) -> Optional[str]:
    text = raw.strip()
    m = _RE_LINK.search(text)
    if m:
        return m.group(1)
    m = _RE_SLUG.match(text)
    if m:
        name_raw  = m.group(1)
        number    = m.group(2)
        canonical = _GIFT_NAME_MAP_NOSPACE.get(name_raw.lower().replace(" ", ""))
        slug_name = canonical.replace(" ", "") if canonical else name_raw
        return f"{slug_name}-{number}"
    m = _RE_WORDS.match(text)
    if m:
        name_raw    = m.group(1)
        number      = m.group(2)
        key_nospace = name_raw.lower().replace(" ", "")
        key_spaced  = name_raw.lower()
        if key_spaced in _GIFT_NAME_MAP:
            canonical = _GIFT_NAME_MAP[key_spaced]
        elif key_nospace in _GIFT_NAME_MAP_NOSPACE:
            canonical = _GIFT_NAME_MAP_NOSPACE[key_nospace]
        else:
            canonical = name_raw.title()
        slug_name = canonical.replace(" ", "")
        return f"{slug_name}-{number}"
    return None


def split_slug(slug: str) -> tuple[str, str]:
    parts = slug.rsplit("-", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (slug, "")


# ══════════════════════════════════════════════════════════════════════════════
#  АТРИБУТЫ NFT
# ══════════════════════════════════════════════════════════════════════════════

class NftAttrs:
    __slots__ = ("model", "model_rarity", "backdrop", "backdrop_rarity",
                 "symbol", "symbol_rarity")

    def __init__(self) -> None:
        self.model = self.backdrop = self.symbol = "—"
        self.model_rarity = self.backdrop_rarity = self.symbol_rarity = ""


def _set_attr(a: NftAttrs, label: str, value: str, rarity: str) -> None:
    label = label.lower().strip()
    value = value.strip()
    if not value or value == "—":
        return
    if "model" in label and a.model == "—":
        a.model, a.model_rarity = value, rarity
    elif ("backdrop" in label or "background" in label) and a.backdrop == "—":
        a.backdrop, a.backdrop_rarity = value, rarity
    elif "symbol" in label and a.symbol == "—":
        a.symbol, a.symbol_rarity = value, rarity


def _extract_rarity(cell) -> tuple[str, str]:
    import copy
    vc = copy.copy(cell)
    rarity = ""
    mark = vc.find("mark")
    if mark:
        rarity = mark.get_text(strip=True)
        mark.decompose()
    value = vc.get_text(separator=" ", strip=True)
    value = re.sub(r'\s+', ' ', value).strip()
    return value, rarity


async def fetch_nft_attrs(slug: str) -> NftAttrs:
    # Проверяем кэш
    cached = _attrs_cache_get(slug)
    if cached is not None:
        return cached  # type: ignore

    attrs = NftAttrs()
    url   = f"https://t.me/nft/{slug}"
    hdrs  = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        async with get_session().get(url, headers=hdrs) as resp:
            if resp.status != 200:
                logger.warning("fetch_attrs HTTP %s | slug=%s", resp.status, slug)
                return attrs
            html = await resp.text()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        for row in soup.select("div.tgme_gift_table_wrap tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            mark_tag = td.find("mark")
            rarity = mark_tag.get_text(strip=True) if mark_tag else ""
            if mark_tag:
                mark_tag.decompose()
            value = td.get_text(separator=" ", strip=True)
            value = re.sub(r'\s+', ' ', value).strip()
            _set_attr(attrs, label, value, rarity)

        if attrs.model == "—":
            for el in soup.find_all(attrs={"data-trait": True}):
                label  = str(el.get("data-trait", ""))
                value  = str(el.get("data-value", el.get_text(strip=True)))
                rarity = str(el.get("data-rarity", ""))
                _set_attr(attrs, label, value, rarity)

        if attrs.model == "—":
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd:
                    value, rarity = _extract_rarity(dd)
                    _set_attr(attrs, dt.get_text(strip=True), value, rarity)

        if attrs.model == "—":
            meta = soup.find("meta", attrs={"property": "og:description"})
            if meta:
                content = str(meta.get("content", ""))
                for sep in ("·", "\n", ","):
                    if sep in content:
                        parts = content.split(sep)
                        break
                else:
                    parts = [content]
                for part in parts:
                    if ":" in part:
                        k, _, v = part.strip().partition(":")
                        _set_attr(attrs, k.strip(), v.strip(), "")

        if attrs.model == "—":
            for line in soup.get_text(separator="\n").splitlines():
                if ":" in line:
                    k, _, v = line.strip().partition(":")
                    if k.strip().lower() in ("model", "backdrop", "background", "symbol") and v.strip():
                        _set_attr(attrs, k.strip(), v.strip(), "")

    except Exception as e:
        logger.warning("fetch_attrs error | slug=%s | %s", slug, e)

    _attrs_cache_put(slug, attrs)
    return attrs


# ══════════════════════════════════════════════════════════════════════════════
#  FLOOR PRICE (giftstat.app)
# ══════════════════════════════════════════════════════════════════════════════

FLOOR_API_URL = "https://apiv2.giftstat.app/current/collections/floor"

# Кэш: коллекция -> (floor_price, ton_rate, timestamp)
_floor_cache: dict[str, tuple[float, float, float]] = {}
_floor_cache_ttl: float = 60.0   # секунд — обновляем не чаще 1 раза в минуту

# Полный список коллекций из API (обновляется при каждом запросе)
_floor_data_all: list[dict] = []
_floor_data_ts: float = 0.0


async def _refresh_floor_data() -> bool:
    """Загружает/обновляет полный список floor prices с API. Возвращает True при успехе."""
    global _floor_data_all, _floor_data_ts
    now = time.monotonic()
    if now - _floor_data_ts < _floor_cache_ttl and _floor_data_all:
        return True
    try:
        async with get_session().get(FLOOR_API_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                logger.warning("floor_api HTTP %s", resp.status)
                return False
            payload = await resp.json(content_type=None)
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                logger.warning("floor_api: unexpected format")
                return False
            _floor_data_all = data
            _floor_data_ts  = now
            return True
    except asyncio.TimeoutError:
        logger.warning("floor_api timeout")
        return False
    except Exception as e:
        logger.warning("floor_api error: %s", e)
        return False


def _format_usd(value: float) -> str:
    """Красиво форматирует USD: 11.8 или 11.88, без лишних нулей."""
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        # оставляем 1 знак, но убираем лишний ноль
        s = f"{value:.1f}"
        return s.rstrip("0").rstrip(".")
    # до 10 — 2 знака
    s = f"{value:.2f}"
    return s.rstrip("0").rstrip(".")


async def fetch_floor_price(collection_name: str) -> tuple[Optional[float], Optional[float]]:
    """
    Возвращает (floor_price, ton_rate) для коллекции по имени.
    Сначала точное совпадение, потом slug-сравнение (без спецсимволов).
    При любой ошибке возвращает (None, None).
    """
    ok = await _refresh_floor_data()
    if not ok:
        return None, None

    name_lower = collection_name.lower().strip()
    name_slug  = _slug_key(collection_name)

    best = None
    for item in _floor_data_all:
        cname = str(item.get("collection", "")).lower().strip()
        if cname == name_lower:
            best = item
            break
        if best is None and _slug_key(cname) == name_slug:
            best = item

    if best is None:
        return None, None

    fp = best.get("floor_price")
    tr = best.get("ton_rate")
    if fp is not None and tr is not None:
        try:
            return float(fp), float(tr)
        except (ValueError, TypeError):
            pass
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗКА ФАЙЛОВ
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_url(url: str) -> tuple[bool, Optional[bytes], Optional[str]]:
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                if not data:
                    return False, None, "Пустой ответ"
                return True, data, None
            elif resp.status == 404:
                return False, None, None
            return False, None, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Таймаут (30 сек)"
    except aiohttp.ClientConnectionError as e:
        return False, None, f"Ошибка соединения: {e}"
    except Exception as e:
        return False, None, f"Ошибка: {e}"


async def fetch_nft_image(slug: str) -> tuple[bool, Optional[bytes], Optional[str]]:
    return await _fetch_url(FRAGMENT_IMAGE_URL.format(slug=slug))


async def fetch_nft_tgs(slug: str) -> tuple[bool, Optional[bytes], Optional[str]]:
    return await _fetch_url(FRAGMENT_TGS_URL.format(slug=slug))


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.error("webp_to_png: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  TGS → MP4 / GIF
# ══════════════════════════════════════════════════════════════════════════════

def _check_ffmpeg() -> bool:
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def tgs_to_mp4(tgs_bytes: bytes, size: int = 720) -> Optional[bytes]:
    """
    Конвертирует TGS → MP4. Баланс качество/скорость:
    720px рендер, CRF 14, preset faster, High Profile.
    """
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image
    except ImportError as e:
        logger.error("tgs_to_mp4: не хватает библиотеки: %s", e)
        return None

    tmp_dir    = tempfile.mkdtemp(prefix="nft_mp4_")
    tgs_path   = os.path.join(tmp_dir, "anim.tgs")
    mp4_path   = os.path.join(tmp_dir, "out.mp4")
    frames_dir = os.path.join(tmp_dir, "frames")

    try:
        with open(tgs_path, "wb") as f:
            f.write(tgs_bytes)

        anim        = LottieAnimation.from_tgs(tgs_path)
        frame_count = anim.lottie_animation_get_totalframe()
        fps         = anim.lottie_animation_get_framerate()

        if frame_count == 0:
            logger.error("tgs_to_mp4: 0 кадров")
            return None

        fps = max(fps, 1)
        os.makedirs(frames_dir, exist_ok=True)

        for i in range(frame_count):
            frame_img = anim.render_pillow_frame(frame_num=i)
            if frame_img is None:
                continue

            if frame_img.size != (size, size):
                frame_img = frame_img.resize((size, size), Image.LANCZOS)

            if frame_img.mode == "RGBA":
                bg = Image.new("RGB", (size, size), (0, 0, 0))
                bg.paste(frame_img, mask=frame_img.split()[3])
                frame_img = bg
            elif frame_img.mode != "RGB":
                frame_img = frame_img.convert("RGB")

            frame_img.save(
                os.path.join(frames_dir, f"frame_{i:05d}.png"),
                format="PNG", compress_level=0
            )

        cpu_count = multiprocessing.cpu_count()

        cmd = [
            "ffmpeg", "-y",
            "-threads", str(cpu_count),
            "-framerate", str(fps),
            "-i", os.path.join(frames_dir, "frame_%05d.png"),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-crf", "14",
            "-preset", "faster",
            "-tune", "animation",
            "-profile:v", "high",
            "-level", "4.1",
            "-movflags", "+faststart",
            mp4_path,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=180)
        if result.returncode != 0:
            logger.error("ffmpeg mp4 error: %s", result.stderr.decode(errors="replace")[-800:])
            return None

        with open(mp4_path, "rb") as f:
            mp4_data = f.read()

        logger.info("tgs_to_mp4 OK: %d кадров, %.1f fps, %dx%d, %d байт",
                    frame_count, fps, size, size, len(mp4_data))
        return mp4_data

    except subprocess.TimeoutExpired:
        logger.error("tgs_to_mp4: ffmpeg timeout")
        return None
    except Exception as e:
        logger.error("tgs_to_mp4 error: %s", e, exc_info=True)
        return None
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


def tgs_to_gif(tgs_bytes: bytes, size: int = 800) -> Optional[bytes]:
    """
    Конвертирует TGS → GIF через ffmpeg с оптимальной палитрой.
    Pillow GIF даёт 256 цветов без дизеринга — ужасное качество.
    ffmpeg palettegen + paletteuse с dithering даёт максимум для формата GIF.
    Рендер: 800px RGBA → PNG-кадры → ffmpeg palettegen → paletteuse (sierra2_4a dither).
    """
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image
    except ImportError as e:
        logger.error("tgs_to_gif: не хватает библиотеки: %s", e)
        return None

    tmp_dir    = tempfile.mkdtemp(prefix="nft_gif_")
    tgs_path   = os.path.join(tmp_dir, "anim.tgs")
    palette    = os.path.join(tmp_dir, "palette.png")
    gif_path   = os.path.join(tmp_dir, "out.gif")
    frames_dir = os.path.join(tmp_dir, "frames")

    try:
        with open(tgs_path, "wb") as f:
            f.write(tgs_bytes)

        anim        = LottieAnimation.from_tgs(tgs_path)
        frame_count = anim.lottie_animation_get_totalframe()
        fps         = anim.lottie_animation_get_framerate()

        if frame_count == 0:
            return None

        fps = max(fps, 1)
        os.makedirs(frames_dir, exist_ok=True)

        for i in range(frame_count):
            frame_img = anim.render_pillow_frame(frame_num=i)
            if frame_img is None:
                continue
            if frame_img.size != (size, size):
                frame_img = frame_img.resize((size, size), Image.LANCZOS)

            # Сохраняем RGBA — ffmpeg сам обработает прозрачность
            frame_img.save(
                os.path.join(frames_dir, f"frame_{i:05d}.png"),
                format="PNG", compress_level=0
            )

        frames_pattern = os.path.join(frames_dir, "frame_%05d.png")

        # Шаг 1: генерируем оптимальную палитру из всех кадров
        cmd_palette = [
            "ffmpeg", "-y",
            "-framerate", str(fps),
            "-i", frames_pattern,
            "-vf", f"scale={size}:{size}:flags=lanczos,palettegen=max_colors=256:reserve_transparent=1:stats_mode=full",
            palette,
        ]
        r1 = subprocess.run(cmd_palette, capture_output=True, timeout=120)
        if r1.returncode != 0:
            logger.error("ffmpeg palettegen error: %s", r1.stderr.decode(errors="replace")[-500:])
            return None

        # Шаг 2: кодируем GIF с палитрой и дизерингом sierra2_4a (лучшее для анимации)
        cmd_gif = [
            "ffmpeg", "-y",
            "-framerate", str(fps),
            "-i", frames_pattern,
            "-i", palette,
            "-lavfi", f"scale={size}:{size}:flags=lanczos[s];[s][1:v]paletteuse=dither=sierra2_4a:diff_mode=rectangle",
            gif_path,
        ]
        r2 = subprocess.run(cmd_gif, capture_output=True, timeout=180)
        if r2.returncode != 0:
            logger.error("ffmpeg gif encode error: %s", r2.stderr.decode(errors="replace")[-500:])
            return None

        with open(gif_path, "rb") as f:
            gif_data = f.read()

        logger.info("tgs_to_gif OK: %d кадров, %.1f fps, %dx%d, %d байт",
                    frame_count, fps, size, size, len(gif_data))
        return gif_data

    except subprocess.TimeoutExpired:
        logger.error("tgs_to_gif: ffmpeg timeout")
        return None
    except Exception as e:
        logger.error("tgs_to_gif error: %s", e, exc_info=True)
        return None
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  КРАФТОВЫЕ МОДЕЛИ И РЕДКОСТИ
# ══════════════════════════════════════════════════════════════════════════════

RARITY_EMOJI_IDS: dict[str, list[str]] = {
    "legendary": ["5273884071829739701", "5272019325878833357", "5273763473443035122"],
    "epic":      ["5271774839160478445", "5273779343347193556"],
    "rare":      ["5273722645483917391", "5273923675723175516"],
    "uncommon":  ["5273971740702185834", "5273715820780882978", "5271507249813034911"],
}

_DESK_CALENDAR_CRAFTED: dict[str, str] = {
    "day of mars": "legendary", "óðinsdagr": "legendary", "loki's day": "legendary",
    "celestial map": "legendary", "may the fourth": "legendary",
    "selena": "epic", "aphrodite": "epic", "ton core": "epic", "frjádagr": "epic",
    "þórsdagr": "epic", "týsdagr": "epic", "kronos": "epic", "sol invictus": "epic",
    "shinto shrine": "epic", "royal flush": "epic", "payday": "epic",
    "lucky day": "epic", "mánadagr": "epic", "treasure map": "epic", "glam day": "epic",
    "frog day": "rare", "first date": "rare", "space era": "rare", "weekly set": "rare",
    "grimoire": "rare", "cat seasons": "rare", "artwork": "rare", "ghost party": "rare",
    "zeus": "rare", "samhain": "rare", "outlaw": "rare", "hermes": "rare",
    "count dracula": "rare",
    "wedding": "uncommon", "sekhmet": "uncommon", "cyberpunk": "uncommon",
    "anniversary": "uncommon", "crunch time": "uncommon", "mesozoic": "uncommon",
    "orchestra": "uncommon", "daily bread": "uncommon", "helios": "uncommon",
    "god of wine": "uncommon", "steampunk": "uncommon", "launch date": "uncommon",
    "shuffle": "uncommon", "time spin": "uncommon", "holy month": "uncommon",
    "anno domini": "uncommon", "new year": "uncommon", "vacation": "uncommon",
    "shopping list": "uncommon", "april fools": "uncommon", "women's day": "uncommon",
    "vintage": "uncommon", "yoga time": "uncommon", "toy calendar": "uncommon",
}

_JINGLE_BELLS_CRAFTED: dict[str, str] = {
    "dragon lantern": "legendary", "maneki neko": "legendary", "hot cherry": "legendary",
    "golden dice": "legendary",
    "krampus": "epic", "little gifts": "epic", "cash bags": "epic", "stranding": "epic",
    "lucky bell": "epic", "white owl": "epic", "duality": "epic", "silver maces": "epic",
    "hedgehogs": "epic",
    "mushrooms": "rare", "tinker bell": "rare", "black gold": "rare",
    "jungle bloom": "rare", "circus": "rare", "dolls": "rare", "nutcracker": "rare",
    "cash machine": "rare", "bullfinch": "rare", "love song": "rare", "grinch": "rare",
    "royal call": "rare", "wind chimes": "rare",
    "candy houses": "uncommon", "noble pearl": "uncommon", "ice queen": "uncommon",
    "fabergé": "uncommon", "crystal": "uncommon", "blue sapphire": "uncommon",
    "santa claus": "uncommon", "pharaoh": "uncommon", "sleigh bells": "uncommon",
    "red lotus": "uncommon", "sylvan echo": "uncommon", "sarcophagus": "uncommon",
    "peonies": "uncommon", "royal charm": "uncommon", "festive night": "uncommon",
    "pink bow": "uncommon", "spring knell": "uncommon", "royal hour": "uncommon",
    "festive duo": "uncommon", "cozy winter": "uncommon", "flashlights": "uncommon",
    "purple jingle": "uncommon", "steampunk": "uncommon", "reindeer": "uncommon",
    "orchestra": "uncommon",
}

_CRAFTED_MAP: dict[str, dict[str, str]] = {
    "deskcalendar": _DESK_CALENDAR_CRAFTED,
    "jinglebells":  _JINGLE_BELLS_CRAFTED,
}


def get_craft_rarity(collection_slug_name: str, model_name: str) -> Optional[str]:
    key = collection_slug_name.lower().replace(" ", "").replace("-", "")
    crafted = _CRAFTED_MAP.get(key)
    if crafted is None:
        return None
    return crafted.get(model_name.lower().strip())


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION (MessageEntity)
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def make_caption(slug: str, attrs: NftAttrs,
                 floor_price: Optional[float] = None,
                 ton_rate: Optional[float] = None) -> tuple[str, list[MessageEntity]]:
    name, number = split_slug(slug)
    nice = normalize_gift_name(name)

    craft_rarity = get_craft_rarity(name, attrs.model)
    is_crafted   = craft_rarity is not None

    title_text = f"{nice} (Crafted) #{number}" if is_crafted else f"{nice} #{number}"

    SEP = "━━━━━━━━━━━━━━━━━━━━"
    entities: list[MessageEntity] = []
    buf = [""]

    def _len() -> int:
        return _utf16_len(buf[0])

    def ce(ch: str, eid: str) -> None:
        entities.append(MessageEntity(type="custom_emoji",
                                      offset=_len(), length=_utf16_len(ch),
                                      custom_emoji_id=eid))
        buf[0] += ch

    def bold(s: str) -> None:
        entities.append(MessageEntity(type="bold", offset=_len(), length=_utf16_len(s)))
        buf[0] += s

    def italic(s: str) -> None:
        entities.append(MessageEntity(type="italic", offset=_len(), length=_utf16_len(s)))
        buf[0] += s

    def bold_italic(s: str) -> None:
        entities.append(MessageEntity(type="bold", offset=_len(), length=_utf16_len(s)))
        entities.append(MessageEntity(type="italic", offset=_len(), length=_utf16_len(s)))
        buf[0] += s

    def code(s: str) -> None:
        entities.append(MessageEntity(type="code", offset=_len(), length=_utf16_len(s)))
        buf[0] += s

    def lnk(s: str, url: str) -> None:
        entities.append(MessageEntity(type="text_link", offset=_len(),
                                      length=_utf16_len(s), url=url))
        buf[0] += s

    def p(s: str) -> None:
        buf[0] += s

    def rarity_emojis(rarity: str) -> None:
        ids = RARITY_EMOJI_IDS.get(rarity.lower(), [])
        for eid in ids:
            ch = "⭐"
            entities.append(MessageEntity(
                type="custom_emoji",
                offset=_len(),
                length=_utf16_len(ch),
                custom_emoji_id=eid,
            ))
            buf[0] += ch

    # ── Заголовок ─────────────────────────────────────────────────────────────
    ce("🎁", E_GIFT); p(" "); bold(title_text); p("\n")

    # ── Floor price строка (сразу под заголовком) ─────────────────────────────
    try:
        if floor_price is not None and ton_rate is not None:
            usd_val = floor_price * ton_rate
            usd_str = _format_usd(usd_val)
            # Форматируем TON цену: красиво, без лишних нулей
            if floor_price == int(floor_price):
                ton_str = str(int(floor_price))
            else:
                ton_str = f"{floor_price:.2f}".rstrip("0").rstrip(".")
            # Строка: bold "[gem] Floorprice collection — [ton] {цена}" italic "(~${usd})"
            ce("💎", E_FLOOR_GEM)
            bold(" Floorprice collection — ")
            ce("❤️", E_FLOOR_TON)
            bold(ton_str)
            bold(" (")
            bold_italic(f"~${usd_str}")
            bold(")")
            p("\n")
        else:
            # Не удалось получить данные — ставим прочерк тихо
            ce("💎", E_FLOOR_GEM)
            bold(" Floorprice collection — —")
            p("\n")
    except Exception:
        pass  # В крайнем случае строку просто не выводим

    code(SEP); p("\n")

    ce("🪄", E_MODEL); p(" "); bold("Модель:"); p(f" {attrs.model}")
    if is_crafted:
        p(" · ")
        rarity_emojis(craft_rarity)
    elif attrs.model_rarity:
        p(f" · {attrs.model_rarity}")
    p("\n")

    r_back = f" · {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym  = f" · {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""
    ce("🎨", E_BACK);  p(" "); bold("Фон:");    p(f" {attrs.backdrop}{r_back}\n")
    ce("✨", E_SYMBOL); p(" "); bold("Символ:"); p(f" {attrs.symbol}{r_sym}\n")

    code(SEP); p("\n")
    ce("🔗", E_LINK); p(" "); lnk("Открыть в Telegram", f"https://t.me/nft/{slug}")

    return buf[0], entities


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def make_keyboard_static(slug: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📤 Отправить без сжатия",
                             callback_data=f"{CB_NO_COMPRESS}{slug}")
    ]])


def make_keyboard_video(slug: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎞 Отправить как GIF",
                              callback_data=f"{CB_SEND_GIF}{slug}")],
        [InlineKeyboardButton(text="🖼 Без анимации (PNG)",
                              callback_data=f"{CB_NO_ANIM}{slug}")],
        [InlineKeyboardButton(text="🎭 Отправить стикер (TGS)",
                              callback_data=f"{CB_SEND_STICKER}{slug}")],
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  ТЕКСТЫ
# ══════════════════════════════════════════════════════════════════════════════

def get_group_instruction() -> str:
    return (
        "📖 <b>Инструкция NFT Gift Viewer</b>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>Форматы запросов:</b>\n\n"
        "🖼 <b>Статичная картинка (с подписью):</b>\n"
        "<code>превью Plush Pepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимация MP4 (с подписью):</b>\n"
        "<code>+а превью Plush Pepe 22</code>\n"
        "<code>+а превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎞 <b>Только GIF — без подписи и кнопок:</b>\n"
        "<code>+гиф превью Plush Pepe 22</code>\n"
        "<code>+гиф превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎭 <b>Скачать TGS файл (для импорта стикера):</b>\n"
        "<code>+тгс превью Plush Pepe 22</code>\n"
        "<code>+тгс превью t.me/nft/PlushPepe-22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Кнопки под превью — только 1 раз каждая\n"
        "• <code>превью инструкция</code> — не чаще 1 раза в 5 минут\n\n"
        "<b>❓ Нужна помощь?</b>\n"
        "Пиши автору: <a href='https://t.me/balfikovich'>@balfikovich</a>"
    )


def get_group_welcome(chat_title: str) -> str:
    return (
        f"👋 <b>Привет, {chat_title}!</b>\n\n"
        "Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📌 Как пользоваться:</b>\n\n"
        "🖼 <b>Статичная:</b> <code>превью Plush Pepe 22</code>\n"
        "🎬 <b>Анимация MP4:</b> <code>+а превью Plush Pepe 22</code>\n"
        "🎞 <b>Только GIF:</b> <code>+гиф превью Plush Pepe 22</code>\n"
        "🎭 <b>TGS файл:</b> <code>+тгс превью Plush Pepe 22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Кнопки под превью — только 1 раз каждая\n\n"
        "⚡ Картинка ~1–2 сек | Видео/GIF ~3–6 сек\n\n"
        "<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


def get_start_text() -> str:
    return (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Показываю <b>видео-карточку</b> любого Telegram NFT-подарка.\n\n"
        "<b>📨 Как пользоваться в личке:</b>\n"
        "Отправь ссылку или название — получишь MP4 видео с подписью.\n\n"
        "<b>✅ Форматы:</b>\n"
        "<code>https://t.me/nft/PlushPepe-22</code>\n"
        "<code>t.me/nft/PlushPepe-22</code>\n"
        "<code>PlushPepe-22</code>\n"
        "<code>PlushPepe 22</code>\n"
        "<code>Plush Pepe 22</code>\n\n"
        "Под видео — кнопки <b>«Без анимации»</b> (PNG) и <b>«Скачать TGS»</b>.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>👥 В группе:</b>\n"
        "🖼 <b>Статичная:</b> <code>превью Plush Pepe 22</code>\n"
        "🎬 <b>Анимация MP4:</b> <code>+а превью Plush Pepe 22</code>\n"
        "🎞 <b>Только GIF:</b> <code>+гиф превью Plush Pepe 22</code>\n"
        "🎭 <b>TGS файл:</b> <code>+тгс превью Plush Pepe 22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>📋 Правила пользования:</b>\n\n"
        "⏱ <b>Повторный показ</b> одного подарка — не чаще <b>1 раза в 2 минуты</b>\n"
        "🔘 <b>Кнопки</b> под превью — только <b>1 раз каждая</b>\n"
        "👥 <b>В чате</b> — не более <b>5 генераций</b> одновременно\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Видео ~3–6 сек | Картинка ~1–2 сек\n\n"
        "<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ОТПРАВКИ
# ══════════════════════════════════════════════════════════════════════════════

async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


async def remove_keyboard_button(msg: Message, remove_prefix: str) -> None:
    try:
        kbd = msg.reply_markup
        if kbd is None:
            return
        new_rows = []
        for row in kbd.inline_keyboard:
            new_row = [
                btn for btn in row
                if not (btn.callback_data and btn.callback_data.startswith(remove_prefix))
            ]
            if new_row:
                new_rows.append(new_row)
        new_kbd = InlineKeyboardMarkup(inline_keyboard=new_rows) if new_rows else None
        await msg.edit_reply_markup(reply_markup=new_kbd)
    except Exception:
        pass


async def send_static_photo(message: Message, png: bytes,
                            slug: str, attrs: NftAttrs,
                            floor_price: Optional[float] = None,
                            ton_rate: Optional[float] = None) -> bool:
    caption, ents = make_caption(slug, attrs, floor_price, ton_rate)
    kbd  = make_keyboard_static(slug)
    file = BufferedInputFile(png, filename=f"{slug}.png")
    try:
        await message.answer_photo(
            photo=file,
            caption=caption,
            caption_entities=ents,
            parse_mode=None,
            reply_markup=kbd,
        )
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(png, filename=f"{slug}.png")
            await message.answer_photo(
                photo=file, caption=caption, caption_entities=ents,
                parse_mode=None, reply_markup=kbd,
            )
            return True
        except Exception as ex:
            logger.error("send_static_photo retry: %s", ex)
            return False
    except TelegramBadRequest as e:
        logger.error("send_static_photo BadRequest: %s", e)
        return False
    except Exception as e:
        logger.error("send_static_photo: %s", e)
        return False


async def send_video(message: Message, mp4: bytes,
                     slug: str, attrs: NftAttrs,
                     floor_price: Optional[float] = None,
                     ton_rate: Optional[float] = None) -> bool:
    caption, ents = make_caption(slug, attrs, floor_price, ton_rate)
    kbd  = make_keyboard_video(slug)
    file = BufferedInputFile(mp4, filename=f"{slug}.mp4")
    try:
        await message.answer_video(
            video=file,
            caption=caption,
            caption_entities=ents,
            parse_mode=None,
            reply_markup=kbd,
            supports_streaming=True,
        )
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(mp4, filename=f"{slug}.mp4")
            await message.answer_video(
                video=file, caption=caption, caption_entities=ents,
                parse_mode=None, reply_markup=kbd, supports_streaming=True,
            )
            return True
        except Exception as ex:
            logger.error("send_video retry: %s", ex)
            return False
    except TelegramBadRequest as e:
        logger.error("send_video BadRequest: %s", e)
        return False
    except Exception as e:
        logger.error("send_video: %s", e)
        return False


async def send_document(send_fn, data: bytes, filename: str) -> None:
    file = BufferedInputFile(data, filename=filename)
    try:
        await send_fn(document=file)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(data, filename=filename)
        try:
            await send_fn(document=file)
        except Exception as ex:
            logger.error("send_document retry: %s", ex)
    except Exception as e:
        logger.error("send_document: %s", e)


async def send_tgs_sticker(message: Message, tgs_data: bytes, slug: str) -> bool:
    """
    Отправляет TGS как анимированный стикер через answer_sticker.
    Файл передаём с именем slug.tgs — Telegram определяет его как стикер по расширению.
    """
    file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
    try:
        await message.answer_sticker(sticker=file)
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
            await message.answer_sticker(sticker=file)
            return True
        except Exception as ex:
            logger.error("send_tgs_sticker retry: %s", ex)
            return False
    except TelegramBadRequest as e:
        logger.error("send_tgs_sticker BadRequest: %s | slug=%s", e, slug)
        return False
    except Exception as e:
        logger.error("send_tgs_sticker error: %s | slug=%s", e, slug)
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  BOT & DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── Бот добавлен / удалён из группы ─────────────────────────────────────────
@dp.my_chat_member()
async def on_bot_chat_member(event: ChatMemberUpdated) -> None:
    if event.chat.type not in ("group", "supergroup"):
        return

    old = event.old_chat_member.status
    new = event.new_chat_member.status

    was_out = old in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")
    now_in  = new in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR,
                      "member", "administrator")
    now_out = new in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")

    if was_out and now_in:
        role = ("администратором"
                if new in (ChatMemberStatus.ADMINISTRATOR, "administrator")
                else "участником")
        user_log.info("➕ БОТ ДОБАВЛЕН | кто=%s | чат=%s | роль=%s",
                      _u(event.from_user), _chat(event.chat), role)
        try:
            await bot.send_message(
                event.chat.id,
                get_group_welcome(event.chat.title or "чат"),
                parse_mode=ParseMode.HTML,
            )
        except TelegramForbiddenError:
            logger.warning("on_bot_added: нет прав писать | chat_id=%s", event.chat.id)
        except Exception as e:
            logger.error("on_bot_added welcome: %s", e)

    elif now_out:
        act = "ВЫГНАН" if new in (ChatMemberStatus.KICKED, "kicked") else "УДАЛЁН"
        user_log.info("➖ БОТ %s | кто=%s | чат=%s", act, _u(event.from_user), _chat(event.chat))


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if message.chat.type != "private":
        return
    user_log.info("▶  /start | %s", _u(message.from_user))
    buttons = []
    if BOT_USERNAME:
        buttons.append([InlineKeyboardButton(
            text="➕ Добавить в группу",
            url=f"https://t.me/{BOT_USERNAME}?startgroup",
        )])
    buttons.append([InlineKeyboardButton(
        text="⭐ Поддержать автора",
        callback_data=CB_DONATE,
    )])
    await message.answer(
        get_start_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


# ── /cancel_donate ────────────────────────────────────────────────────────────
@dp.message(Command("cancel_donate"))
async def cmd_cancel_donate(message: Message) -> None:
    if message.chat.type != "private":
        return
    uid = message.from_user.id
    if uid in _awaiting_donate:
        _awaiting_donate.discard(uid)
        user_log.info("❌ ДОНАТ ОТМЕНЁН | %s", _u(message.from_user))
        await message.answer("✅ Донат отменён. Возвращайся когда захочешь! 😊")
    else:
        await message.answer("Нет активного ожидания оплаты. Всё в порядке! 😊")


@dp.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    """Статистика бота — только для администратора."""
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return

    now = time.monotonic()

    # Активные муты
    active_mutes = sum(1 for until in _spam_muted.values() if now < until)

    # Видео кэш
    video_cached = len(_video_cache_lru)

    # Атрибуты кэш
    attrs_cached = len(_attrs_cache)

    # Активные задачи asyncio
    tasks = len(asyncio.all_tasks())

    # Семафор — свободных слотов
    sem_free = _convert_semaphore._value

    # Активные чаты
    active_chats = sum(1 for v in _chat_active.values() if v > 0)

    # Cb locks
    active_locks = sum(1 for lock in _cb_locks.values() if lock.locked())

    import sys
    mem_mb = 0
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    except Exception:
        pass

    text = (
        "📊 <b>Статистика бота</b>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"🎬 <b>Видео кэш:</b> <code>{video_cached}/{_VIDEO_CACHE_MAX}</code> слотов\n"
        f"📦 <b>Attrs кэш:</b> <code>{attrs_cached}</code> записей\n"
        f"⚙️ <b>Семафор:</b> <code>{sem_free}/5</code> свободных слотов\n"
        f"🚫 <b>Активных мутов:</b> <code>{active_mutes}</code>\n"
        f"👥 <b>Активных чатов:</b> <code>{active_chats}</code>\n"
        f"🔒 <b>Активных lock'ов:</b> <code>{active_locks}</code>\n"
        f"⚡ <b>Asyncio задач:</b> <code>{tasks}</code>\n"
        f"💾 <b>RAM (RSS):</b> <code>{mem_mb} MB</code>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"<i>_used sets:</i> nc={len(_used_no_compress)} na={len(_used_no_anim)} "
        f"sk={len(_used_sticker)} gif={len(_used_gif)}"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


# ── Callback: «Поддержать автора» ────────────────────────────────────────────
@dp.callback_query(F.data == CB_DONATE)
async def callback_donate(callback: CallbackQuery) -> None:
    await callback.answer()
    uid = callback.from_user.id
    user_log.info("💛 ДОНАТ — ДИАЛОГ | %s", _u(callback.from_user))
    _awaiting_donate.add(uid)
    await callback.message.answer(
        f'<tg-emoji emoji-id="{E_DONATE}">⭐</tg-emoji> <b>Поддержка проекта</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Привет! 👋 Этот бот полностью <b>бесплатен</b>.\n\n"
        "Если хочешь поддержать автора — буду очень благодарен! 🙏\n\n"
        "<b>Напиши число</b> — сколько ⭐ звёзд хочешь отправить.\n"
        "Минимум — <code>1</code>, максимум — <code>2500</code>\n\n"
        "<b>Например:</b> <code>10</code> или <code>50</code> или <code>100</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "💡 Передумал — напиши <code>/cancel_donate</code>",
        parse_mode=ParseMode.HTML,
    )


# ── PreCheckout ───────────────────────────────────────────────────────────────
@dp.pre_checkout_query()
async def pre_checkout_handler(query: PreCheckoutQuery) -> None:
    user_log.info("💳 PRE_CHECKOUT | %s | %s ⭐", _u(query.from_user), query.total_amount)
    await query.answer(ok=True)


# ── Успешный платёж ───────────────────────────────────────────────────────────
@dp.message(F.successful_payment)
async def payment_handler(message: Message) -> None:
    payment = message.successful_payment
    stars   = payment.total_amount
    user    = message.from_user
    uname   = f"@{user.username}" if user.username else f"без username (id={user.id})"

    user_log.info("✅ ДОНАТ ПОЛУЧЕН | %s | %s ⭐", _u(user), stars)

    await message.answer(
        f'<tg-emoji emoji-id="{E_DONATE}">⭐</tg-emoji> <b>Огромное спасибо!</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Ты отправил <b>{stars} ⭐</b> — это очень приятно! 🚀\n\n"
        "Я обязательно напишу тебе лично, чтобы поблагодарить! 🙏\n\n"
        f"<i>С уважением, <a href='https://t.me/balfikovich'>@balfikovich</a></i>",
        parse_mode=ParseMode.HTML,
    )
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🔔 <b>Новый донат!</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"👤 <b>Имя:</b> {user.full_name or 'NoName'}\n"
            f"📎 <b>Username:</b> {uname}\n"
            f"⭐ <b>Сумма:</b> {stars} звёзд\n"
            f"🆔 <b>User ID:</b> <code>{user.id}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error("уведомление о донате: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_wait(wait: float) -> str:
    mins = int(wait) // 60; secs = int(wait) % 60
    return f"{mins} мин {secs} сек" if mins else f"{secs} сек"


@dp.callback_query(F.data.startswith(CB_NO_COMPRESS_VIDEO))
async def callback_no_compress_video(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_COMPRESS_VIDEO):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    # Проверяем спам на кнопки
    spam = record_spam_event(uid)
    if spam in ("muted", "ban", "mute"):
        await callback.answer("⏳ Слишком много запросов. Подожди немного.", show_alert=True)
        return

    if key in _used_no_compress_video:
        await callback.answer("❌ Видео без сжатия уже было отправлено!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_NO_COMPRESS_VIDEO)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {_fmt_wait(wait)}", show_alert=False)
        return

    lock = _get_cb_lock(uid)
    if lock.locked():
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    async with lock:
        await callback.answer("⏳ Загружаю оригинал…")
        try:
            # Берём из кэша если есть
            mp4_data = _video_cache_get(slug)
            if not mp4_data:
                found, tgs_data, err = await fetch_nft_tgs(slug)
                if err or not found:
                    await callback.message.answer(f"❌ Не удалось загрузить: {err or 'файл не найден'}")
                    return
                wm = await callback.message.answer("⚙️ Конвертирую в видео…")
                try:
                    async with _convert_semaphore:
                        mp4_data = await asyncio.wait_for(
                            asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=180.0)
                    if mp4_data:
                        _video_cache_put(slug, mp4_data)
                except asyncio.TimeoutError:
                    mp4_data = None
                finally:
                    await safe_delete(wm)

            if not mp4_data:
                await callback.message.answer("❌ Не удалось конвертировать видео.")
                return

            _used_no_compress_video.add(key)
            await remove_keyboard_button(callback.message, CB_NO_COMPRESS_VIDEO)
            await send_document(callback.message.answer_document, mp4_data, f"{slug}.mp4")
            user_log.info("📤 БЕЗ СЖАТИЯ ВИДЕО | slug=%s | %s", slug, _u(callback.from_user))
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error("callback_no_compress_video: %s", e, exc_info=True)


@dp.callback_query(F.data.startswith(CB_SEND_GIF))
async def callback_send_gif(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_SEND_GIF):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    spam = record_spam_event(uid)
    if spam in ("muted", "ban", "mute"):
        await callback.answer("⏳ Слишком много запросов. Подожди немного.", show_alert=True)
        return

    if key in _used_gif:
        await callback.answer("❌ GIF уже был отправлен!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_SEND_GIF)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {_fmt_wait(wait)}", show_alert=False)
        return

    lock = _get_cb_lock(uid)
    if lock.locked():
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    async with lock:
        await callback.answer("⏳ Загружаю…")
        try:
            mp4_data = _video_cache_get(slug)
            if not mp4_data:
                found, tgs_data, err = await fetch_nft_tgs(slug)
                if err or not found:
                    await callback.message.answer(f"❌ Не удалось загрузить: {err or 'файл не найден'}")
                    return
                wm = await callback.message.answer("⚙️ Конвертирую…")
                try:
                    async with _convert_semaphore:
                        mp4_data = await asyncio.wait_for(
                            asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=180.0)
                    if mp4_data:
                        _video_cache_put(slug, mp4_data)
                except asyncio.TimeoutError:
                    mp4_data = None
                finally:
                    await safe_delete(wm)

            if not mp4_data:
                await callback.message.answer("❌ Не удалось конвертировать.")
                return

            _used_gif.add(key)
            await remove_keyboard_button(callback.message, CB_SEND_GIF)
            file = BufferedInputFile(mp4_data, filename=f"{slug}.mp4")
            await callback.message.answer_animation(animation=file)
            user_log.info("🎞 GIF | slug=%s | %s", slug, _u(callback.from_user))
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error("callback_send_gif: %s", e, exc_info=True)


@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_COMPRESS):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    spam = record_spam_event(uid)
    if spam in ("muted", "ban", "mute"):
        await callback.answer("⏳ Слишком много запросов. Подожди немного.", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_NO_COMPRESS)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {_fmt_wait(wait)}", show_alert=False)
        return

    if key in _used_no_compress:
        await callback.answer("❌ Оригинал уже был отправлен!", show_alert=True)
        return

    lock = _get_cb_lock(uid)
    if lock.locked():
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    async with lock:
        await callback.answer("⏳ Загружаю оригинал…")
        try:
            found, webp, err = await fetch_nft_image(slug)
            if err or not found:
                await callback.message.answer(f"❌ Не удалось загрузить: {err or 'подарок не найден'}")
                return

            png = webp_to_png(webp)
            if not png:
                await send_document(callback.message.answer_document, webp, f"{slug}.webp")
                return

            _used_no_compress.add(key)
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass

            await send_document(callback.message.answer_document, png, f"{slug}.png")
            user_log.info("📤 БЕЗ СЖАТИЯ (PNG) | slug=%s | %s", slug, _u(callback.from_user))
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error("callback_no_compress: %s", e, exc_info=True)


@dp.callback_query(F.data.startswith(CB_NO_ANIM))
async def callback_no_anim(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_ANIM):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    spam = record_spam_event(uid)
    if spam in ("muted", "ban", "mute"):
        await callback.answer("⏳ Слишком много запросов. Подожди немного.", show_alert=True)
        return

    if key in _used_no_anim:
        await callback.answer("❌ Картинка уже была отправлена!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_NO_ANIM)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {_fmt_wait(wait)}", show_alert=False)
        return

    lock = _get_cb_lock(uid)
    if lock.locked():
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    async with lock:
        await callback.answer("⏳ Загружаю картинку…")
        try:
            (found, webp, err), attrs = await asyncio.gather(
                fetch_nft_image(slug),
                fetch_nft_attrs(slug),
            )
            if err or not found:
                await callback.message.answer(f"❌ Не удалось загрузить: {err or 'подарок не найден'}")
                return

            png = webp_to_png(webp)
            if not png:
                await send_document(callback.message.answer_document, webp, f"{slug}.webp")
                return

            name_part, _ = split_slug(slug)
            nice_name = normalize_gift_name(name_part)
            floor_price, ton_rate = await fetch_floor_price(nice_name)

            _used_no_anim.add(key)
            await remove_keyboard_button(callback.message, CB_NO_ANIM)
            ok = await send_static_photo(callback.message, png, slug, attrs, floor_price, ton_rate)
            if not ok:
                await send_document(callback.message.answer_document, png, f"{slug}.png")
            user_log.info("🖼 БЕЗ АНИМАЦИИ | slug=%s | %s", slug, _u(callback.from_user))
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error("callback_no_anim: %s", e, exc_info=True)


@dp.callback_query(F.data.startswith(CB_SEND_STICKER))
async def callback_send_sticker(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_SEND_STICKER):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    spam = record_spam_event(uid)
    if spam in ("muted", "ban", "mute"):
        await callback.answer("⏳ Слишком много запросов. Подожди немного.", show_alert=True)
        return

    if key in _used_sticker:
        await callback.answer("❌ Стикер уже был отправлен!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_SEND_STICKER)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {_fmt_wait(wait)}", show_alert=False)
        return

    lock = _get_cb_lock(uid)
    if lock.locked():
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    async with lock:
        await callback.answer("⏳ Загружаю стикер…")
        try:
            found, tgs_data, err = await fetch_nft_tgs(slug)
            if err or not found:
                await callback.message.answer(f"❌ Не удалось загрузить стикер: {err or 'не найден'}")
                return

            _used_sticker.add(key)
            await remove_keyboard_button(callback.message, CB_SEND_STICKER)
            ok = await send_tgs_sticker(callback.message, tgs_data, slug)
            if not ok:
                await callback.message.answer("❌ Не удалось отправить стикер.")
            user_log.info("🎭 СТИКЕР | ok=%s | slug=%s | %s", ok, slug, _u(callback.from_user))
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error("callback_send_sticker: %s", e, exc_info=True)


# ── Вспомогательная функция обработки результата спам-проверки ───────────────

async def _handle_spam_result(spam: Optional[str], uid: int, message: Message) -> bool:
    """
    Возвращает True если запрос нужно заблокировать (бот ответил или промолчал).
    Возвращает False если запрос разрешён.
    """
    if spam is None:
        return False
    if spam == "muted":
        now = time.monotonic()
        last_notif = _spam_mute_notified.get(uid, 0.0)
        if now - last_notif >= SPAM_IDLE_NOTIFY:
            _spam_mute_notified[uid] = now
            remaining = get_spam_mute_remaining(uid)
            if remaining:
                mins, secs = remaining // 60, remaining % 60
                ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
                try:
                    await message.answer(
                        f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                        f"<b>Спам обнаружен.</b>\n\nПовторите запрос через <code>{ts}</code>.",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
        return True
    if spam == "ban":
        user_log.warning("🚫 БАН (1ч) | uid=%d", uid)
        _spam_mute_notified[uid] = time.monotonic()
        try:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                "<b>Спам обнаружен.</b>\n\nПовторите запрос через <code>1 час</code>.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return True
    if spam == "mute":
        user_log.warning("🔇 МУТ (5мин) | uid=%d", uid)
        _spam_mute_notified[uid] = time.monotonic()
        try:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                "<b>Спам обнаружен.</b>\n\nПовторите запрос через <code>5 минут</code>.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return True
    if spam == "warn":
        try:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                "<b>Слишком много запросов!</b> Сбавь темп.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return False  # при предупреждении запрос всё равно пропускаем
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  ОСНОВНОЙ ОБРАБОТЧИК ТЕКСТА
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text)
async def handle_text(message: Message) -> None:
    if not message.from_user:
        return

    raw        = (message.text or "").strip()
    is_private = message.chat.type == "private"
    uid        = message.from_user.id

    # ── ПРОГРЕССИВНЫЙ АНТИСПАМ ────────────────────────────────────────────────
    # Сначала только проверяем мут (без записи в историю)
    if not (is_private and uid in _awaiting_donate):
        mute_check = check_spam_progressive(uid)
        if mute_check == "muted":
            now = time.monotonic()
            last_notif = _spam_mute_notified.get(uid, 0.0)
            if now - last_notif >= SPAM_IDLE_NOTIFY:
                _spam_mute_notified[uid] = now
                remaining = get_spam_mute_remaining(uid)
                if remaining:
                    mins, secs = remaining // 60, remaining % 60
                    ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
                    try:
                        await message.answer(
                            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                            f"<b>Спам обнаружен.</b>\n\nПовторите запрос через <code>{ts}</code>.",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass
            return

    # ── ДОНАТ: перехватываем ввод суммы ──────────────────────────────────────
    if is_private and uid in _awaiting_donate:
        s = raw.strip()
        if s.isdigit():
            amount = int(s)
            if amount < 1:
                await message.answer("⚠️ Минимум — <b>1 звезда ⭐</b>.", parse_mode=ParseMode.HTML)
                return
            if amount > 2500:
                await message.answer("⚠️ Максимум — <b>2500 звёзд</b> за один раз.", parse_mode=ParseMode.HTML)
                return
            _awaiting_donate.discard(uid)
            user_log.info("💛 ЧЕК | %s | %s ⭐", _u(message.from_user), amount)
            try:
                await bot.send_invoice(
                    chat_id=message.chat.id,
                    title="⭐ Поддержка автора",
                    description=f"Донат автору бота NFT Gift Viewer — {amount} звёзд. Спасибо! 🙏",
                    payload=f"donate_{uid}_{amount}",
                    currency="XTR",
                    prices=[LabeledPrice(label="Звёзды", amount=amount)],
                    provider_token="",
                )
            except Exception as e:
                logger.error("send_invoice: %s", e)
                _awaiting_donate.add(uid)
                await message.answer("❌ Не удалось создать счёт. Попробуй ещё раз.", parse_mode=ParseMode.HTML)
        else:
            await message.answer(
                "⚠️ Введи <b>число</b> — количество звёзд.\n\n"
                "Например: <code>10</code>\n\nПередумал? — <code>/cancel_donate</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── ГРУППА ────────────────────────────────────────────────────────────────
    if not is_private:
        lower = raw.lower()

        # Инструкция
        if lower.strip() in ("превью инструкция", "preview инструкция",
                             "превью instruction", "preview instruction"):
            wait = check_instr_antispam(message.chat.id)
            if wait > 0:
                await safe_delete(message)
                return
            user_log.info("📖 ИНСТРУКЦИЯ | %s | %s", _u(message.from_user), _chat(message.chat))
            await message.answer(get_group_instruction(), parse_mode=ParseMode.HTML)
            return

        # +гиф превью — только GIF без подписи
        if (lower.startswith("+гиф превью") or lower.startswith("+гиф preview")
                or lower.startswith("+gif превью") or lower.startswith("+gif preview")):
            for prefix in ("+гиф превью", "+гиф preview", "+gif превью", "+gif preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            spam = record_spam_event(uid)
            if _handle_spam_result(spam, uid, message):
                return
            await _handle_group_gif_only(message, raw)
            return

        # +тгс превью — только TGS стикер
        if (lower.startswith("+тгс превью") or lower.startswith("+тгс preview")
                or lower.startswith("+tgs превью") or lower.startswith("+tgs preview")):
            for prefix in ("+тгс превью", "+тгс preview", "+tgs превью", "+tgs preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            spam = record_spam_event(uid)
            if _handle_spam_result(spam, uid, message):
                return
            await _handle_group_tgs_only(message, raw)
            return

        # +а превью — анимированное MP4 с подписью
        if lower.startswith("+а превью") or lower.startswith("+а preview"):
            for prefix in ("+а превью", "+а preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            spam = record_spam_event(uid)
            if _handle_spam_result(spam, uid, message):
                return
            await _handle_group_video(message, raw)
            return

        # превью — статичная PNG с подписью
        if lower.startswith("превью") or lower.startswith("preview"):
            for prefix in ("превью", "preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            spam = record_spam_event(uid)
            if _handle_spam_result(spam, uid, message):
                return
            await _handle_group_static(message, raw)
            return

        return

    # ── ЛИЧКА ─────────────────────────────────────────────────────────────────
    spam = record_spam_event(uid)
    if _handle_spam_result(spam, uid, message):
        return
    await _handle_private_video(message, raw)


# ── Только GIF без подписи (группа) ──────────────────────────────────────────
async def _handle_group_gif_only(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>+гиф превью Plush Pepe 22</code>\n"
            "<code>+гиф превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎞 GIF-ONLY ЗАПРОС (группа) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    wait = check_slug_antispam(message.chat.id, slug)
    if wait > 0:
        mins, secs = wait // 60, wait % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Этот подарок уже был показан.</b>\nПовтор через <code>{ts}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    wm = await message.answer("⏳ Загружаю…")
    found, tgs_data, err = await fetch_nft_tgs(slug)

    if err or not found:
        await safe_delete(wm)
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n<code>{slug}</code>\n<i>{err or 'не найден'}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    await safe_delete(wm)
    wm = await message.answer("⚙️ Конвертирую в GIF…")

    mp4_data = _video_cache_get(slug)
    if mp4_data:
        logger.info("🎯 VIDEO CACHE HIT (gif) | slug=%s", slug)
    else:
        try:
            async with _convert_semaphore:
                mp4_data = await asyncio.wait_for(
                    asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=180.0)
            if mp4_data:
                _video_cache_put(slug, mp4_data)
        except asyncio.TimeoutError:
            mp4_data = None

    await safe_delete(wm)

    if not mp4_data:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> Не удалось конвертировать.',
            parse_mode=ParseMode.HTML,
        )
        return

    # Отправляем как animation (зацикленный GIF/MP4) — без подписи и кнопок
    file = BufferedInputFile(mp4_data, filename=f"{slug}.mp4")
    try:
        await message.answer_animation(animation=file)
        user_log.info("✅ GIF-ONLY | slug=%s | %s", slug, _u(message.from_user))
    except Exception as e:
        logger.error("_handle_group_gif_only send: %s", e)
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> Не удалось отправить GIF.',
            parse_mode=ParseMode.HTML,
        )


# ── Только TGS стикер (группа) ───────────────────────────────────────────────
async def _handle_group_tgs_only(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>+тгс превью Plush Pepe 22</code>\n"
            "<code>+тгс превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎭 TGS-ONLY ЗАПРОС (группа) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    wait = check_slug_antispam(message.chat.id, slug)
    if wait > 0:
        mins, secs = wait // 60, wait % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Этот подарок уже был показан.</b>\nПовтор через <code>{ts}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    wm = await message.answer("⏳ Загружаю стикер…")
    found, tgs_data, err = await fetch_nft_tgs(slug)
    await safe_delete(wm)

    if err or not found:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n<code>{slug}</code>\n<i>{err or 'не найден'}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Отправляем TGS файл с нормальным именем
    await send_tgs_sticker(message, tgs_data, slug)
    user_log.info("✅ TGS-ONLY | slug=%s | %s", slug, _u(message.from_user))


# ── Статичная PNG (группа) ────────────────────────────────────────────────────
async def _handle_group_static(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (группа) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>превью Plush Pepe 22</code>\n"
            "<code>превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🖼 ЗАПРОС (группа) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    wait = check_slug_antispam(message.chat.id, slug)
    if wait > 0:
        mins, secs = wait // 60, wait % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Этот подарок уже был показан.</b>\nПовтор через <code>{ts}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not _chat_acquire(message.chat.id):
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Чат занят.</b> Сейчас обрабатывается {CHAT_MAX_PARALLEL} запросов. Попробуй чуть позже.",
            parse_mode=ParseMode.HTML,
        )
        return

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю…", parse_mode=ParseMode.HTML)

    name_part, _ = split_slug(extract_nft_slug(raw) or raw)
    nice_name = normalize_gift_name(name_part)

    try:
        (found, webp, err), attrs, (floor_price, ton_rate) = await asyncio.gather(
            fetch_nft_image(slug),
            fetch_nft_attrs(slug),
            fetch_floor_price(nice_name),
        )
    finally:
        _chat_release(message.chat.id)
    elapsed = round(time.monotonic() - t0, 2)
    await safe_delete(wm)

    if err:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            f"<b>Подарок не найден</b>\n\n<code>{slug}</code>\n\n"
            "<b>Возможные причины:</b>\n"
            "• Такого номера не существует\n"
            "• Подарок сожжён 🔥\n"
            "• Опечатка в названии",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("✅ СТАТИК (группа) | slug=%s | модель=%s | %s | %.2fс",
                  slug, attrs.model, _u(message.from_user), elapsed)

    png = webp_to_png(webp)
    if png:
        ok = await send_static_photo(message, png, slug, attrs, floor_price, ton_rate)
        if not ok:
            await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await send_document(message.answer_document, webp, f"{slug}.webp")


# ── Анимированная MP4 (группа) ───────────────────────────────────────────────
async def _handle_group_video(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>+а превью Plush Pepe 22</code>\n"
            "<code>+а превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎬 ЗАПРОС (группа аним) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    wait = check_slug_antispam(message.chat.id, slug)
    if wait > 0:
        mins, secs = wait // 60, wait % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Этот подарок уже был показан.</b>\nПовтор через <code>{ts}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not _chat_acquire(message.chat.id):
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Чат занят.</b> Сейчас обрабатывается {CHAT_MAX_PARALLEL} запросов. Попробуй чуть позже.",
            parse_mode=ParseMode.HTML,
        )
        return

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю данные…")

    name_part, _ = split_slug(slug)
    nice_name = normalize_gift_name(name_part)

    try:
        (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs, (floor_price, ton_rate) = await asyncio.gather(
            fetch_nft_image(slug),
            fetch_nft_tgs(slug),
            fetch_nft_attrs(slug),
            fetch_floor_price(nice_name),
        )
    finally:
        _chat_release(message.chat.id)

    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                f"<b>Подарок не найден</b>\n\n<code>{slug}</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    mp4_data: Optional[bytes] = None
    if tgs_ok and tgs_data:
        # Проверяем кэш
        mp4_data = _video_cache_get(slug)
        if mp4_data:
            logger.info("🎯 VIDEO CACHE HIT | slug=%s", slug)
        else:
            await safe_delete(wm)
            # Уведомляем если семафор занят
            if _convert_semaphore._value == 0:
                wm = await message.answer("⏳ Сервер занят, ваш запрос в очереди…")
            else:
                wm = await message.answer("⚙️ Конвертирую в видео…")
            try:
                async with _convert_semaphore:
                    mp4_data = await asyncio.wait_for(
                        asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=180.0)
                if mp4_data:
                    _video_cache_put(slug, mp4_data)
            except asyncio.TimeoutError:
                mp4_data = None

    await safe_delete(wm)
    elapsed = round(time.monotonic() - t0, 2)

    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs, floor_price, ton_rate)
        if ok:
            user_log.info("✅ MP4 (группа) | slug=%s | %s | %.2fс",
                          slug, _u(message.from_user), elapsed)
            return

    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs, floor_price, ton_rate)
            if ok:
                return
        await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> Не удалось создать видео.',
            parse_mode=ParseMode.HTML,
        )


# ── MP4 видео (личка) ─────────────────────────────────────────────────────────
async def _handle_private_video(message: Message, raw: str) -> None:
    uid  = message.from_user.id
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>Plush Pepe 22</code>\n"
            "<code>t.me/nft/PlushPepe-22</code>\n"
            "<code>https://t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    wait = check_antispam(uid)
    if wait > 0:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Слишком быстро!</b> Подожди <code>{wait}</code> сек.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Лимит параллельных генераций в личке (через тот же chat_id = uid)
    if not _chat_acquire(uid):
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            "<b>Уже идёт генерация.</b> Дождись результата предыдущего запроса.",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎬 ЗАПРОС (личка) | slug=%s | %s", slug, _u(message.from_user))

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю данные…")

    name_part, _ = split_slug(slug)
    nice_name = normalize_gift_name(name_part)

    try:
        (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs, (floor_price, ton_rate) = await asyncio.gather(
            fetch_nft_image(slug),
            fetch_nft_tgs(slug),
            fetch_nft_attrs(slug),
            fetch_floor_price(nice_name),
        )
    finally:
        _chat_release(uid)

    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                f"<b>Подарок не найден</b>\n\n<code>{slug}</code>\n\n"
                "<b>Возможные причины:</b>\n"
                "• Такого номера не существует\n"
                "• Подарок сожжён 🔥\n"
                "• Опечатка в названии",
                parse_mode=ParseMode.HTML,
            )
        return

    mp4_data: Optional[bytes] = None
    if tgs_ok and tgs_data:
        mp4_data = _video_cache_get(slug)
        if mp4_data:
            logger.info("🎯 VIDEO CACHE HIT | slug=%s", slug)
        else:
            await safe_delete(wm)
            # Уведомляем если семафор занят
            if _convert_semaphore._value == 0:
                wm = await message.answer("⏳ Сервер занят, ваш запрос в очереди…")
            else:
                wm = await message.answer("⚙️ Конвертирую в видео…")
            try:
                async with _convert_semaphore:
                    mp4_data = await asyncio.wait_for(
                        asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=180.0)
                if mp4_data:
                    _video_cache_put(slug, mp4_data)
            except asyncio.TimeoutError:
                mp4_data = None

    await safe_delete(wm)
    elapsed = round(time.monotonic() - t0, 2)

    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs, floor_price, ton_rate)
        if ok:
            user_log.info("✅ MP4 ОТПРАВЛЕНО | slug=%s | %s | %.2fс",
                          slug, _u(message.from_user), elapsed)
            return

    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs, floor_price, ton_rate)
            if ok:
                return
        await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            "Не удалось создать видео и загрузить картинку.",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  INLINE-РЕЖИМ
# ══════════════════════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_handler(query: InlineQuery) -> None:
    raw = (query.query or "").strip()

    if not raw:
        hint = InlineQueryResultArticle(
            id="hint",
            title="🎁 NFT Gift Viewer",
            description="Введите: Plush Pepe 22 / PlushPepe-22",
            thumbnail_url="https://nft.fragment.com/gift/PlushPepe-1.webp",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "<b>NFT Gift Viewer</b>\n\n"
                    "Добавь бота в чат и отправляй названия подарков!\n\n"
                    "<code>t.me/nft/PlushPepe-22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[hint], cache_time=60, is_personal=False)
        return

    slug = extract_nft_slug(raw)

    if not slug:
        err_result = InlineQueryResultArticle(
            id="err_fmt",
            title="❌ Неверный формат",
            description="Пример: Plush Pepe 22 / PlushPepe-22 / t.me/nft/...",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "<b>Неверный формат</b>\n\n"
                    "<code>PlushPepe-22</code>\n"
                    "<code>Plush Pepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[err_result], cache_time=5, is_personal=True)
        return

    user_log.info("🔍 INLINE | slug=%s | %s", slug, _u(query.from_user))

    name, number = split_slug(slug)
    nice  = normalize_gift_name(name)

    (found, webp, err), attrs, (floor_price, ton_rate) = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
        fetch_floor_price(nice),
    )

    title = f"🎁 {nice} #{number}"

    if err or not found:
        nf = InlineQueryResultArticle(
            id=f"nf_{slug}",
            title=title,
            description=f"⚠️ {err}" if err else "❌ Не найден",
            input_message_content=InputTextMessageContent(
                message_text=f"<b>Не найден</b>\n\n<code>{slug}</code>",
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[nf], cache_time=10, is_personal=True)
        return

    caption, ents = make_caption(slug, attrs, floor_price, ton_rate)
    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть в Telegram", url=f"https://t.me/nft/{slug}")
    ]])

    desc_parts = []
    if attrs.model    != "—": desc_parts.append(f"🪄 {attrs.model}{' · ' + attrs.model_rarity if attrs.model_rarity else ''}")
    if attrs.backdrop != "—": desc_parts.append(f"🎨 {attrs.backdrop}{' · ' + attrs.backdrop_rarity if attrs.backdrop_rarity else ''}")
    if attrs.symbol   != "—": desc_parts.append(f"✨ {attrs.symbol}{' · ' + attrs.symbol_rarity if attrs.symbol_rarity else ''}")

    result = InlineQueryResultPhoto(
        id=str(uuid.uuid4()),
        photo_url=FRAGMENT_IMAGE_URL.format(slug=slug),
        thumbnail_url=FRAGMENT_IMAGE_URL.format(slug=slug),
        title=title,
        description="  ·  ".join(desc_parts) if desc_parts else "NFT Подарок",
        caption=caption,
        caption_entities=ents,
        parse_mode=None,
        reply_markup=kbd,
    )
    await query.answer(results=[result], cache_time=30, is_personal=True)


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

async def on_startup() -> None:
    global BOT_USERNAME
    get_session()
    me = await bot.get_me()
    BOT_USERNAME = me.username or ""

    # Запускаем фоновую очистку
    asyncio.create_task(_background_cleanup())

    logger.info("━" * 60)
    logger.info("✅ БОТ ЗАПУЩЕН: @%s (id=%s)", me.username, me.id)
    logger.info("   Лог-файл  : %s", os.path.abspath(LOG_FILE))
    logger.info("   Admin ID  : %s", ADMIN_ID)
    logger.info("━" * 60)

    if _check_ffmpeg():
        logger.info("   ✅ ffmpeg найден")
    else:
        logger.warning("   ❌ ffmpeg НЕ найден! apt install ffmpeg")

    try:
        from rlottie_python import LottieAnimation  # noqa
        logger.info("   ✅ rlottie-python установлен")
    except ImportError:
        logger.warning("   ❌ rlottie-python НЕ установлен! pip install rlottie-python")

    try:
        from PIL import Image  # noqa
        logger.info("   ✅ Pillow установлен")
    except ImportError:
        logger.warning("   ❌ Pillow НЕ установлен! pip install pillow")

    try:
        from bs4 import BeautifulSoup  # noqa
        logger.info("   ✅ BeautifulSoup4 установлен")
    except ImportError:
        logger.warning("   ❌ BeautifulSoup4 НЕ установлен! pip install beautifulsoup4 lxml")

    logger.info("━" * 60)


async def on_shutdown() -> None:
    logger.info("🛑 БОТ ОСТАНОВЛЕН")
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()
    await bot.session.close()


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "callback_query",
            "inline_query",
            "my_chat_member",
            "pre_checkout_query",
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())
