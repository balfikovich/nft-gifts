"""
NFT Gift Viewer Bot — с поддержкой стикерпаков
=================================================
Зависимости:
    pip install aiogram aiohttp python-dotenv pillow beautifulsoup4 lxml
    apt install ffmpeg

Переменные окружения (.env):
    BOT_TOKEN=xxx
    LOG_FILE=bot.log   (опционально)

─────────────────────────────────────────────────────────────
НОВЫЕ ФИЧИ: СТИКЕРПАКИ
─────────────────────────────────────────────────────────────

1. ПРОФИЛЬНЫЙ СТИКЕРПАК ("Профиль @user | @bot")
   • Кнопка в /start → «🎴 Создать профиль-стикерпак»
   • Бот вызывает getUserGifts (список подарков должен быть публичным!)
   • Берёт до 100 TGS-стикеров и создаёт стикерпак через createNewStickerSet
   • Прогресс-бар в реальном времени (редактируемое сообщение)
   • Имя сета: profile_{user_id}_nftbot (технически уникальное)
   • Заголовок: Профиль @username | @balfikovich_png_bot
   • Стикерпак можно обновить раз в день (addStickerToSet, deleteStickerFromSet)
   • Повторный вызов — предлагает обновить (если прошли сутки) или показывает ссылку

2. ЛИЧНЫЙ СТИКЕРПАК ("Личный @user | @bot")
   • Кнопка «➕ В мой стикерпак» под каждым превью (анимированным)
   • Работает в личке И в группах
   • У каждого юзера свой стикерпак до 100 стикеров
   • Кнопка НЕ пропадает из сообщения (в отличие от других кнопок)
   • Повторное нажатие → всплывающее alert «Стикер уже добавлен!»
   • Имя сета: personal_{user_id}_nftbot
   • Заголовок: Личный @username | @balfikovich_png_bot
   • Отслеживание добавленных стикеров per-user в памяти (slug → True)

3. /start → кнопка «📦 Мои стикерпаки»
   • Красивое оформление, разделение: Профиль / Личный
   • Ссылки на стикерпаки (t.me/addstickers/...)
   • Статус: сколько стикеров, когда последнее обновление

ТЕХНИЧЕСКИЕ ОГРАНИЧЕНИЯ TELEGRAM API:
   • createNewStickerSet — стикерпак с таким именем можно создать только ОДИН раз
   • addStickerToSet — максимум 120 стикеров в сете, лимит: нельзя часто
   • Имя стикерпака (name) заканчивается на _by_<botusername>
   • TGS стикеры: format="animated", type="regular"
   • getUserGifts — только публичный список подарков пользователя
"""

import asyncio
import io
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta
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
    InputSticker,
    PreCheckoutQuery,
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv

# ── Конфиг ───────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s")
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

# callback_data префиксы
CB_NO_COMPRESS       = "nc:"
CB_NO_ANIM           = "na:"
CB_SEND_STICKER      = "sk:"
CB_NO_COMPRESS_VIDEO = "ncv:"
CB_SEND_GIF          = "gif:"
CB_DONATE            = "donate"

# Новые callback для стикерпаков
CB_CREATE_PROFILE_PACK = "cpp:"    # cpp: → создать профильный стикерпак
CB_ADD_TO_PERSONAL     = "atp:"    # atp:slug → добавить в личный стикерпак
CB_MY_STICKER_PACKS    = "msp"     # msp → показать мои стикерпаки
CB_UPDATE_PROFILE_PACK = "upp:"    # upp: → обновить профильный стикерпак

ANTISPAM_SECONDS  = 1.5
ANTISPAM_SLUG_SEC = 120

# Custom Emoji IDs
E_GIFT   = "5408829285685291820"
E_MODEL  = "5408894951440279259"
E_BACK   = "5411585799990830248"
E_SYMBOL = "5409189019261103031"
E_LINK   = "5409143419593321597"
E_WARN   = "5409124594751660992"
E_ERR    = "5408930028438188841"
E_START  = "6028495398941759268"
E_DONATE = "5309759985192832914"

# ══════════════════════════════════════════════════════════════════════════════
#  ХРАНИЛИЩЕ СТИКЕРПАКОВ (in-memory, можно заменить на SQLite)
# ══════════════════════════════════════════════════════════════════════════════

# Структура:
# _sticker_packs[user_id] = {
#   "profile": {
#       "name": "profile_12345_nftbot",   # техническое имя
#       "title": "Профиль @user | @bot",
#       "slugs": ["PlushPepe-1", ...],    # порядок стикеров
#       "created_at": timestamp,
#       "updated_at": timestamp,
#       "count": 42,
#   },
#   "personal": {
#       "name": "personal_12345_nftbot",
#       "title": "Личный @user | @bot",
#       "slugs": ["PlushPepe-1", ...],    # set для быстрой проверки
#       "slugs_set": set(),
#       "created_at": timestamp,
#       "updated_at": timestamp,
#       "count": 7,
#   }
# }

_sticker_packs: dict[int, dict] = {}

# Блокировка создания (чтобы не создавали параллельно)
_pack_creating: set[int] = set()

# ── Работа с хранилищем ───────────────────────────────────────────────────────

def get_user_pack(user_id: int, pack_type: str) -> Optional[dict]:
    return _sticker_packs.get(user_id, {}).get(pack_type)


def set_user_pack(user_id: int, pack_type: str, data: dict) -> None:
    if user_id not in _sticker_packs:
        _sticker_packs[user_id] = {}
    _sticker_packs[user_id][pack_type] = data


def is_personal_pack_has_slug(user_id: int, slug: str) -> bool:
    pack = get_user_pack(user_id, "personal")
    if not pack:
        return False
    return slug.lower() in pack.get("slugs_set", set())


def add_slug_to_personal(user_id: int, slug: str) -> None:
    pack = get_user_pack(user_id, "personal")
    if pack:
        pack["slugs"].append(slug)
        pack.setdefault("slugs_set", set()).add(slug.lower())
        pack["count"] = len(pack["slugs"])
        pack["updated_at"] = time.time()


def can_update_profile_pack(user_id: int) -> tuple[bool, Optional[float]]:
    """Возвращает (можно_ли, секунд_до_разрешения)"""
    pack = get_user_pack(user_id, "profile")
    if not pack:
        return True, None
    updated_at = pack.get("updated_at", 0)
    diff = time.time() - updated_at
    if diff >= 86400:  # 24 часа
        return True, None
    return False, 86400 - diff


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
_GIFT_NAME_MAP: dict[str, str] = {n.lower(): n for n in _GIFT_NAMES}


def normalize_gift_name(raw_name: str) -> str:
    return _GIFT_NAME_MAP.get(raw_name.lower().strip(), readable_name(raw_name))


# ══════════════════════════════════════════════════════════════════════════════
#  АНТИСПАМ И СОСТОЯНИЯ
# ══════════════════════════════════════════════════════════════════════════════

_last_request:           dict[int, float] = {}
_last_slug:              dict[str, float] = {}
_cb_lock:                dict[int, bool]  = {}
_used_no_compress:       set[str]         = set()
_used_no_anim:           set[str]         = set()
_used_sticker:           set[str]         = set()
_used_no_compress_video: set[str]         = set()
_used_gif:               set[str]         = set()
_awaiting_donate:        set[int]         = set()
_last_instr:             dict[int, float] = {}
_last_button:            dict[str, float] = {}
ANTISPAM_INSTR_SEC  = 300
ANTISPAM_BUTTON_SEC = 90.0

BOT_USERNAME: str = ""


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

_RE_LINK  = re.compile(
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
        return f"{m.group(1)}-{m.group(2)}"
    m = _RE_WORDS.match(text)
    if m:
        return f"{m.group(1).replace(' ', '')}-{m.group(2)}"
    return None


def split_slug(slug: str) -> tuple[str, str]:
    parts = slug.rsplit("-", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (slug, "")


def readable_name(raw: str) -> str:
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)


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
    if not value or value == "—":
        return
    if "model" in label and a.model == "—":
        a.model, a.model_rarity = value, rarity
    elif ("backdrop" in label or "background" in label) and a.backdrop == "—":
        a.backdrop, a.backdrop_rarity = value, rarity
    elif "symbol" in label and a.symbol == "—":
        a.symbol, a.symbol_rarity = value, rarity


async def fetch_nft_attrs(slug: str) -> NftAttrs:
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
                return attrs
            html = await resp.text()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        for row in soup.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            vc    = cells[1]
            rs    = vc.find("span")
            r     = rs.get_text(strip=True) if rs else ""
            if rs:
                rs.decompose()
            _set_attr(attrs, label, vc.get_text(strip=True), r)

        if attrs.model == "—":
            for el in soup.find_all(attrs={"data-trait": True}):
                _set_attr(attrs,
                          str(el.get("data-trait", "")),
                          str(el.get("data-value", el.get_text(strip=True))),
                          str(el.get("data-rarity", "")))

        if attrs.model == "—":
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd:
                    rs = dd.find("span")
                    r  = rs.get_text(strip=True) if rs else ""
                    if rs:
                        rs.decompose()
                    _set_attr(attrs, dt.get_text(strip=True), dd.get_text(strip=True), r)

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

    except Exception as e:
        logger.warning("fetch_attrs error | slug=%s | %s", slug, e)
    return attrs


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
#  TGS → MP4
# ══════════════════════════════════════════════════════════════════════════════

def _check_ffmpeg() -> bool:
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def tgs_to_mp4(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image
    except ImportError as e:
        logger.error("tgs_to_mp4: не хватает библиотеки: %s", e)
        return None

    render_size = size
    tmp_dir = tempfile.mkdtemp(prefix="nft_mp4_")
    tgs_path = os.path.join(tmp_dir, "anim.tgs")
    mp4_path = os.path.join(tmp_dir, "out.mp4")

    try:
        with open(tgs_path, "wb") as f:
            f.write(tgs_bytes)

        anim = LottieAnimation.from_tgs(tgs_path)
        frame_count = anim.lottie_animation_get_totalframe()
        fps         = anim.lottie_animation_get_framerate()

        if frame_count == 0:
            return None

        fps = max(fps, 1)
        frames_dir = os.path.join(tmp_dir, "frames")
        os.makedirs(frames_dir, exist_ok=True)

        for i in range(frame_count):
            frame_img = anim.render_pillow_frame(frame_num=i)
            if frame_img is None:
                continue
            if frame_img.size != (render_size, render_size):
                frame_img = frame_img.resize((render_size, render_size), Image.LANCZOS)
            bg = Image.new("RGB", (render_size, render_size), (255, 255, 255))
            if frame_img.mode == "RGBA":
                bg.paste(frame_img, mask=frame_img.split()[3])
            else:
                bg.paste(frame_img)
            bg.save(os.path.join(frames_dir, f"frame_{i:05d}.png"),
                    format="PNG", compress_level=0)

        import multiprocessing
        cpu_count = multiprocessing.cpu_count()

        cmd = [
            "ffmpeg", "-y",
            "-threads", str(cpu_count),
            "-framerate", str(fps),
            "-i", os.path.join(frames_dir, "frame_%05d.png"),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-crf", "15",
            "-preset", "fast",
            "-tune", "animation",
            "-profile:v", "baseline",
            "-level", "3.1",
            "-movflags", "+faststart",
            mp4_path,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0:
            logger.error("ffmpeg error: %s", result.stderr.decode(errors="replace")[-500:])
            return None

        with open(mp4_path, "rb") as f:
            mp4_data = f.read()

        return mp4_data

    except subprocess.TimeoutExpired:
        logger.error("tgs_to_mp4: ffmpeg timeout")
        return None
    except Exception as e:
        logger.error("tgs_to_mp4 error: %s", e, exc_info=True)
        return None
    finally:
        import shutil
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


def tgs_to_gif(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image
    except ImportError as e:
        logger.error("tgs_to_gif: не хватает библиотеки: %s", e)
        return None

    tmp_dir  = tempfile.mkdtemp(prefix="nft_gif_")
    tgs_path = os.path.join(tmp_dir, "anim.tgs")

    try:
        with open(tgs_path, "wb") as f:
            f.write(tgs_bytes)

        anim        = LottieAnimation.from_tgs(tgs_path)
        frame_count = anim.lottie_animation_get_totalframe()
        fps         = anim.lottie_animation_get_framerate()

        if frame_count == 0:
            return None

        fps          = max(fps, 1)
        duration_ms  = max(int(1000 / fps), 20)
        frames       = []

        for i in range(frame_count):
            frame_img = anim.render_pillow_frame(frame_num=i)
            if frame_img is None:
                continue
            if frame_img.size != (size, size):
                frame_img = frame_img.resize((size, size), Image.LANCZOS)
            bg = Image.new("RGB", (size, size), (0, 0, 0))
            bg.paste(frame_img, mask=frame_img.split()[3] if frame_img.mode == "RGBA" else None)
            frames.append(bg)

        if not frames:
            return None

        buf = io.BytesIO()
        frames[0].save(
            buf, format="GIF", save_all=True, append_images=frames[1:],
            loop=0, duration=duration_ms, optimize=False,
        )
        return buf.getvalue()

    except Exception as e:
        logger.error("tgs_to_gif error: %s", e, exc_info=True)
        return None
    finally:
        import shutil
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  СТИКЕРПАК: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def make_pack_name(user_id: int, pack_type: str, bot_username: str) -> str:
    """
    Формирует техническое имя стикерпака.
    Telegram требует: только латиница/цифры/_  + обязательно заканчивается на _by_<botname>
    Например: profile_123456789_by_balfikovich_png_bot
    """
    clean_bot = bot_username.lower().replace("@", "")
    return f"{pack_type}_{user_id}_by_{clean_bot}"


def make_pack_title(username: Optional[str], pack_type: str, bot_username: str) -> str:
    """
    Формирует заголовок стикерпака (видимое название).
    profile → "Профиль @user | @bot"
    personal → "Личный @user | @bot"
    """
    clean_bot = bot_username if bot_username.startswith("@") else f"@{bot_username}"
    uname = f"@{username}" if username else "Аноним"
    if pack_type == "profile":
        return f"Профиль {uname} | {clean_bot}"
    return f"Личный {uname} | {clean_bot}"


def format_progress_bar(current: int, total: int, width: int = 20) -> str:
    """Красивый прогресс-бар с процентом."""
    pct  = current / total if total > 0 else 0
    done = int(width * pct)
    bar  = "█" * done + "░" * (width - done)
    return f"[{bar}] {current}/{total} ({int(pct * 100)}%)"


async def fetch_user_gifts(bot: Bot, user_id: int) -> list[str]:
    """
    Получает список NFT-подарков пользователя через getUserGifts.
    Возвращает список slug-ов (до 100 штук).
    ВНИМАНИЕ: список подарков пользователя должен быть публичным!
    """
    slugs = []
    try:
        # getUserGifts — метод Bot API (доступен с TG Bot API 8.x)
        # Возвращает UserGifts с полем gifts (список SavedGift)
        result = await bot.get_user_gifts(user_id=user_id, limit=100)
        gifts = getattr(result, "gifts", []) or []
        for gift in gifts:
            # SavedGift.gift.sticker.file_id — это file_id TGS стикера
            # Нам нужно получить slug — он хранится в gift.gift.id или через sticker emoji_id
            # В реальном API: result.gifts[i] → SavedGift
            # SavedGift.gift → Gift (у него есть поля: id, sticker, star_count, ...)
            # Используем gift.gift.id как slug (это и есть название подарка)
            gift_obj = getattr(gift, "gift", None)
            if gift_obj is None:
                continue
            gift_id = getattr(gift_obj, "id", None)
            if gift_id:
                slugs.append(str(gift_id))
            if len(slugs) >= 100:
                break
    except TelegramBadRequest as e:
        # Если список подарков скрыт — Telegram вернёт ошибку
        if "GIFT_PRIVACY_RESTRICTED" in str(e) or "user privacy" in str(e).lower():
            raise ValueError("PRIVATE")  # специальный маркер
        logger.warning("fetch_user_gifts error: %s", e)
    except Exception as e:
        logger.warning("fetch_user_gifts error: %s", e)
    return slugs


async def create_sticker_pack(
    bot: Bot,
    user_id: int,
    username: Optional[str],
    pack_type: str,           # "profile" или "personal"
    slugs: list[str],         # список slug-ов для добавления
    progress_msg: Message,    # сообщение для обновления прогресса
) -> Optional[str]:
    """
    Создаёт новый стикерпак из TGS-файлов.
    Возвращает имя стикерпака или None при ошибке.

    Логика:
    1. Скачиваем первый TGS (он нужен при createNewStickerSet)
    2. Создаём стикерпак с первым стикером
    3. В цикле добавляем остальные через addStickerToSet
    4. Обновляем прогресс-бар каждые 5 стикеров
    """
    pack_name  = make_pack_name(user_id, pack_type, BOT_USERNAME)
    pack_title = make_pack_title(username, pack_type, BOT_USERNAME)
    total      = len(slugs)

    if total == 0:
        return None

    # ── Скачиваем первый TGS ──────────────────────────────────────────────────
    first_tgs = None
    first_slug = None
    for slug in slugs:
        ok, tgs_data, err = await fetch_nft_tgs(slug)
        if ok and tgs_data:
            first_tgs  = tgs_data
            first_slug = slug
            break

    if first_tgs is None:
        await progress_msg.edit_text(
            "❌ <b>Не удалось загрузить ни одного стикера.</b>",
            parse_mode=ParseMode.HTML,
        )
        return None

    # ── Создаём стикерпак с первым стикером ──────────────────────────────────
    await _edit_progress(progress_msg, pack_type, 0, total,
                         "🎨 Создаю стикерпак…")
    try:
        first_file = BufferedInputFile(first_tgs, filename=f"{first_slug}.tgs")
        sticker_obj = InputSticker(
            sticker=first_file,
            emoji_list=["🎁"],
            format="animated",
        )
        await bot.create_new_sticker_set(
            user_id=user_id,
            name=pack_name,
            title=pack_title,
            stickers=[sticker_obj],
        )
        logger.info("Стикерпак создан: %s", pack_name)
    except TelegramBadRequest as e:
        if "STICKERSET_INVALID" in str(e) or "already occupied" in str(e).lower():
            # Имя уже занято — это нормально, если пак уже существовал
            logger.warning("create_sticker_pack: пак уже существует (%s)", e)
        else:
            logger.error("create_sticker_pack error: %s", e)
            await progress_msg.edit_text(
                f"❌ <b>Ошибка создания стикерпака:</b>\n<code>{e}</code>",
                parse_mode=ParseMode.HTML,
            )
            return None
    except Exception as e:
        logger.error("create_sticker_pack unexpected: %s", e)
        await progress_msg.edit_text(
            f"❌ <b>Неожиданная ошибка:</b>\n<code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )
        return None

    added = 1  # первый уже добавлен при создании
    await _edit_progress(progress_msg, pack_type, added, total,
                         "➕ Добавляю стикеры…")

    # ── Добавляем остальные стикеры ───────────────────────────────────────────
    remaining = [s for s in slugs if s != first_slug]

    for slug in remaining:
        if added >= 100:
            break  # лимит пака

        ok, tgs_data, err = await fetch_nft_tgs(slug)
        if not ok or not tgs_data:
            logger.warning("Пропускаю стикер %s: %s", slug, err)
            continue

        try:
            sticker_file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
            sticker_obj  = InputSticker(
                sticker=sticker_file,
                emoji_list=["🎁"],
                format="animated",
            )
            await bot.add_sticker_to_set(
                user_id=user_id,
                name=pack_name,
                sticker=sticker_obj,
            )
            added += 1

            # Обновляем прогресс каждые 5 стикеров
            if added % 5 == 0 or added == total:
                await _edit_progress(progress_msg, pack_type, added, total,
                                     "➕ Добавляю стикеры…")

            # Небольшая пауза чтобы не нарваться на флуд-лимит
            await asyncio.sleep(0.3)

        except TelegramRetryAfter as e:
            logger.warning("FloodWait при добавлении стикера: %s сек", e.retry_after)
            await asyncio.sleep(e.retry_after)
            # Повторяем
            try:
                sticker_file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
                sticker_obj  = InputSticker(
                    sticker=sticker_file,
                    emoji_list=["🎁"],
                    format="animated",
                )
                await bot.add_sticker_to_set(
                    user_id=user_id,
                    name=pack_name,
                    sticker=sticker_obj,
                )
                added += 1
            except Exception as e2:
                logger.error("Retry add sticker failed: %s", e2)
                continue

        except TelegramBadRequest as e:
            if "STICKERSET_NOT_MODIFIED" in str(e):
                # Стикер уже есть
                added += 1
                continue
            logger.warning("add_sticker_to_set bad request: %s | slug=%s", e, slug)
            continue
        except Exception as e:
            logger.warning("add_sticker_to_set error: %s | slug=%s", e, slug)
            continue

    return pack_name


async def _edit_progress(
    msg: Message, pack_type: str, current: int, total: int, status_line: str
) -> None:
    """Обновляет сообщение с прогресс-баром. Не падает при ошибке редактирования."""
    icon = "🎴" if pack_type == "profile" else "📦"
    label = "Профильный" if pack_type == "profile" else "Личный"
    bar  = format_progress_bar(current, total)
    pct  = int((current / total * 100)) if total > 0 else 0

    text = (
        f"{icon} <b>Создаю {label} стикерпак…</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"<b>Статус:</b> {status_line}\n\n"
        f"<b>Прогресс:</b>\n"
        f"<code>{bar}</code>\n\n"
        f"{'✅' if current == total else '⏳'} "
        f"<b>{current} из {total}</b> стикеров добавлено\n\n"
        f"<i>Это может занять {'несколько' if total > 20 else ''} минут…</i>"
    )
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def add_single_sticker_to_pack(
    bot: Bot,
    user_id: int,
    username: Optional[str],
    slug: str,
) -> tuple[bool, str]:
    """
    Добавляет один стикер в личный стикерпак пользователя.
    Создаёт пак если его нет.
    Возвращает (успех, имя_пака_или_ошибка).
    """
    pack = get_user_pack(user_id, "personal")
    pack_name = make_pack_name(user_id, "personal", BOT_USERNAME)

    # Проверяем — стикер уже добавлен?
    if is_personal_pack_has_slug(user_id, slug):
        return False, "already_added"

    # Лимит 100 стикеров
    if pack and pack.get("count", 0) >= 100:
        return False, "limit_reached"

    ok, tgs_data, err = await fetch_nft_tgs(slug)
    if not ok or not tgs_data:
        return False, f"fetch_error: {err}"

    sticker_file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
    sticker_obj  = InputSticker(
        sticker=sticker_file,
        emoji_list=["🎁"],
        format="animated",
    )

    try:
        if pack is None:
            # Создаём новый личный стикерпак
            pack_title = make_pack_title(username, "personal", BOT_USERNAME)
            await bot.create_new_sticker_set(
                user_id=user_id,
                name=pack_name,
                title=pack_title,
                stickers=[sticker_obj],
            )
            set_user_pack(user_id, "personal", {
                "name": pack_name,
                "title": pack_title,
                "slugs": [slug],
                "slugs_set": {slug.lower()},
                "created_at": time.time(),
                "updated_at": time.time(),
                "count": 1,
            })
        else:
            # Добавляем в существующий
            await bot.add_sticker_to_set(
                user_id=user_id,
                name=pack_name,
                sticker=sticker_obj,
            )
            add_slug_to_personal(user_id, slug)

        logger.info("Добавлен стикер %s в личный пак user_id=%s", slug, user_id)
        return True, pack_name

    except TelegramBadRequest as e:
        if "STICKERSET_NOT_MODIFIED" in str(e):
            # Уже есть, просто запомним
            add_slug_to_personal(user_id, slug)
            return True, pack_name
        logger.error("add_single_sticker_to_pack error: %s", e)
        return False, str(e)
    except Exception as e:
        logger.error("add_single_sticker_to_pack unexpected: %s", e)
        return False, str(e)


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION (MessageEntity)
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list[MessageEntity]]:
    name, number = split_slug(slug)
    nice = normalize_gift_name(name)

    r_model = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_back  = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    SEP = "━━━━━━━━━━━━━━━━━━━━"
    entities: list[MessageEntity] = []
    t = ""

    def ce(ch: str, eid: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="custom_emoji",
                                      offset=_utf16_len(t),
                                      length=_utf16_len(ch),
                                      custom_emoji_id=eid))
        t += ch

    def bold(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="bold", offset=_utf16_len(t), length=_utf16_len(s)))
        t += s

    def code(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="code", offset=_utf16_len(t), length=_utf16_len(s)))
        t += s

    def lnk(s: str, url: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="text_link", offset=_utf16_len(t),
                                      length=_utf16_len(s), url=url))
        t += s

    def p(s: str) -> None:
        nonlocal t
        t += s

    ce("🎁", E_GIFT);  p(" "); bold(f"{nice} #{number}"); p("\n")
    code(SEP);          p("\n")
    ce("🪄", E_MODEL); p(" "); bold("Модель:");  p(f" {attrs.model}{r_model}\n")
    ce("🎨", E_BACK);  p(" "); bold("Фон:");     p(f" {attrs.backdrop}{r_back}\n")
    ce("✨", E_SYMBOL); p(" "); bold("Символ:");  p(f" {attrs.symbol}{r_sym}\n")
    code(SEP);          p("\n")
    ce("🔗", E_LINK);  p(" "); lnk("Открыть в Telegram", f"https://t.me/nft/{slug}")

    return t, entities


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def make_keyboard_static(slug: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📤 Отправить без сжатия",
                             callback_data=f"{CB_NO_COMPRESS}{slug}")
    ]])


def make_keyboard_video(slug: str) -> InlineKeyboardMarkup:
    """
    Клавиатура под анимированным превью.
    Кнопка «➕ В мой стикерпак» — ПЕРСОНАЛЬНАЯ, она НЕ пропадает из сообщения.
    Остальные кнопки — одноразовые (пропадают после нажатия).
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎞 Отправить как GIF",
                              callback_data=f"{CB_SEND_GIF}{slug}")],
        [InlineKeyboardButton(text="🖼 Без анимации (PNG)",
                              callback_data=f"{CB_NO_ANIM}{slug}")],
        [InlineKeyboardButton(text="🎭 Отправить стикер (TGS)",
                              callback_data=f"{CB_SEND_STICKER}{slug}")],
        # ↓ Эта кнопка ОСТАЁТСЯ в сообщении — у каждого юзера свой стикерпак
        [InlineKeyboardButton(text="➕ В мой стикерпак",
                              callback_data=f"{CB_ADD_TO_PERSONAL}{slug}")],
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  ТЕКСТЫ
# ══════════════════════════════════════════════════════════════════════════════

def get_group_instruction() -> str:
    return (
        "📖 <b>Инструкция NFT Gift Viewer</b>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>Форматы запросов:</b>\n\n"
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимированная (MP4):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n"
        "<code>+а превью t.me/nft/PlushPepe-22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Кнопки под превью — только 1 раз каждая\n"
        "• Кнопка «➕ В мой стикерпак» — личная, у каждого своя\n\n"
        "<b>❓ Нужна помощь?</b>\n"
        "Пиши автору: <a href='https://t.me/balfikovich'>@balfikovich</a>"
    )


def get_group_welcome(chat_title: str) -> str:
    return (
        f"👋 <b>Привет, {chat_title}!</b>\n\n"
        "Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📌 Как пользоваться:</b>\n\n"
        "🖼 <b>Статичная:</b> <code>превью PlushPepe 22</code>\n"
        "🎬 <b>Анимация:</b>  <code>+а превью PlushPepe 22</code>\n\n"
        "🎴 Нажми <b>«➕ В мой стикерпак»</b> под любым превью — стикер сохранится лично тебе!\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Картинка ~1–2 сек | Видео ~3–6 сек\n\n"
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
        "<code>PlushPepe-22</code>\n"
        "<code>Plush Pepe 22</code>\n\n"
        "Под видео — кнопки <b>«Без анимации»</b>, <b>«Стикер»</b> и <b>«В мой стикерпак»</b>.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>🎴 Стикерпаки:</b>\n"
        "• <b>Профильный</b> — все твои NFT-подарки одним паком\n"
        "• <b>Личный</b> — добавляй любые стикеры кнопкой «➕ В мой стикерпак»\n\n"
        "<b>👥 В группе:</b>\n"
        "🖼 <code>превью PlushPepe 22</code>\n"
        "🎬 <code>+а превью PlushPepe 22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


def get_my_packs_text(user_id: int) -> str:
    """Текст «Мои стикерпаки» с красивым оформлением."""
    profile_pack  = get_user_pack(user_id, "profile")
    personal_pack = get_user_pack(user_id, "personal")

    lines = [
        "🗂 <b>Мои стикерпаки</b>",
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n",
    ]

    # ── Профильный ──────────────────────────────────────────────────────────
    lines.append("🎴 <b>Профиль</b> <i>(из ваших NFT-подарков)</i>")
    if profile_pack:
        pack_name   = profile_pack["name"]
        count       = profile_pack.get("count", 0)
        created_ts  = profile_pack.get("created_at", 0)
        updated_ts  = profile_pack.get("updated_at", 0)
        created_str = datetime.fromtimestamp(created_ts).strftime("%d.%m.%Y") if created_ts else "—"
        updated_str = datetime.fromtimestamp(updated_ts).strftime("%d.%m.%Y %H:%M") if updated_ts else "—"

        can_upd, wait_sec = can_update_profile_pack(user_id)
        if can_upd:
            upd_str = "✅ Можно обновить"
        else:
            hours = int(wait_sec // 3600)
            mins  = int((wait_sec % 3600) // 60)
            upd_str = f"⏳ Следующее обновление через {hours}ч {mins}м"

        lines += [
            f"  📦 <b>Стикеров:</b> {count}/100",
            f"  📅 <b>Создан:</b> {created_str}",
            f"  🔄 <b>Обновлён:</b> {updated_str}",
            f"  {upd_str}",
            f"  🔗 <a href='https://t.me/addstickers/{pack_name}'>Открыть стикерпак</a>",
        ]
    else:
        lines += [
            "  <i>Ещё не создан.</i>",
            "  Нажми кнопку ниже, чтобы создать! 👇",
        ]

    lines.append("")

    # ── Личный ──────────────────────────────────────────────────────────────
    lines.append("📦 <b>Личный</b> <i>(добавляешь сам кнопкой «➕ В мой стикерпак»)</i>")
    if personal_pack:
        pack_name   = personal_pack["name"]
        count       = personal_pack.get("count", 0)
        created_ts  = personal_pack.get("created_at", 0)
        updated_str = datetime.fromtimestamp(personal_pack.get("updated_at", 0)).strftime("%d.%m.%Y %H:%M") if personal_pack.get("updated_at") else "—"

        lines += [
            f"  📦 <b>Стикеров:</b> {count}/100",
            f"  🕐 <b>Последнее добавление:</b> {updated_str}",
            f"  🔗 <a href='https://t.me/addstickers/{pack_name}'>Открыть стикерпак</a>",
        ]
        if count >= 100:
            lines.append("  ⚠️ <b>Пак заполнен (100/100)!</b>")
    else:
        lines += [
            "  <i>Ещё пустой.</i>",
            "  Нажми «➕ В мой стикерпак» под любым анимированным превью!",
        ]

    lines += [
        "",
        "<code>━━━━━━━━━━━━━━━━━━━━</code>",
        "<i>Стикерпаки привязаны к твоему аккаунту и остаются в Telegram навсегда.</i>",
    ]

    return "\n".join(lines)


def make_my_packs_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Кнопки под сообщением «Мои стикерпаки»."""
    profile_pack = get_user_pack(user_id, "profile")
    buttons = []

    if profile_pack:
        pack_name = profile_pack["name"]
        can_upd, _ = can_update_profile_pack(user_id)
        buttons.append([InlineKeyboardButton(
            text="🔗 Профиль-стикерпак",
            url=f"https://t.me/addstickers/{pack_name}",
        )])
        if can_upd:
            buttons.append([InlineKeyboardButton(
                text="🔄 Обновить профиль-стикерпак",
                callback_data=f"{CB_UPDATE_PROFILE_PACK}{user_id}",
            )])
    else:
        buttons.append([InlineKeyboardButton(
            text="🎴 Создать профиль-стикерпак",
            callback_data=f"{CB_CREATE_PROFILE_PACK}{user_id}",
        )])

    personal_pack = get_user_pack(user_id, "personal")
    if personal_pack:
        pack_name = personal_pack["name"]
        buttons.append([InlineKeyboardButton(
            text="🔗 Личный стикерпак",
            url=f"https://t.me/addstickers/{pack_name}",
        )])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ══════════════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ОТПРАВКИ
# ══════════════════════════════════════════════════════════════════════════════

async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


async def remove_keyboard_button(msg: Message, remove_prefix: str) -> None:
    """Убирает кнопки с указанным префиксом из клавиатуры (НЕ трогает CB_ADD_TO_PERSONAL!)."""
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
                            slug: str, attrs: NftAttrs) -> bool:
    caption, ents = make_caption(slug, attrs)
    kbd  = make_keyboard_static(slug)
    file = BufferedInputFile(png, filename=f"{slug}.png")
    try:
        await message.answer_photo(
            photo=file, caption=caption, caption_entities=ents,
            parse_mode=None, reply_markup=kbd,
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
    except Exception as e:
        logger.error("send_static_photo: %s", e)
        return False


async def send_video(message: Message, mp4: bytes,
                     slug: str, attrs: NftAttrs) -> bool:
    caption, ents = make_caption(slug, attrs)
    kbd  = make_keyboard_video(slug)
    file = BufferedInputFile(mp4, filename=f"{slug}.mp4")
    try:
        await message.answer_video(
            video=file, caption=caption, caption_entities=ents,
            parse_mode=None, reply_markup=kbd, supports_streaming=True,
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
            pass
        except Exception as e:
            logger.error("on_bot_added welcome: %s", e)
    elif now_out:
        act = "ВЫГНАН" if new in (ChatMemberStatus.KICKED, "kicked") else "УДАЛЁН"
        user_log.info("➖ БОТ %s | кто=%s | чат=%s", act, _u(event.from_user), _chat(event.chat))


# ══════════════════════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════════════════════

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

    # Кнопки стикерпаков
    buttons.append([InlineKeyboardButton(
        text="🎴 Создать профиль-стикерпак",
        callback_data=f"{CB_CREATE_PROFILE_PACK}{message.from_user.id}",
    )])
    buttons.append([InlineKeyboardButton(
        text="📦 Мои стикерпаки",
        callback_data=CB_MY_STICKER_PACKS,
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


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACKS: СТИКЕРПАКИ
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == CB_MY_STICKER_PACKS)
async def callback_my_packs(callback: CallbackQuery) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    text    = get_my_packs_text(user_id)
    kbd     = make_my_packs_keyboard(user_id)
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kbd)
    except Exception:
        await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kbd)

    user_log.info("📦 МОИ СТИКЕРПАКИ | %s", _u(callback.from_user))


@dp.callback_query(F.data.startswith(CB_CREATE_PROFILE_PACK))
async def callback_create_profile_pack(callback: CallbackQuery) -> None:
    """Создание профильного стикерпака из подарков пользователя."""
    user_id  = callback.from_user.id
    username = callback.from_user.username

    # Проверяем — уже создаётся?
    if user_id in _pack_creating:
        await callback.answer("⏳ Стикерпак уже создаётся!", show_alert=True)
        return

    await callback.answer()

    # Проверяем — уже существует? Предлагаем обновить
    existing = get_user_pack(user_id, "profile")
    if existing:
        can_upd, wait_sec = can_update_profile_pack(user_id)
        if not can_upd:
            hours = int(wait_sec // 3600)
            mins  = int((wait_sec % 3600) // 60)
            pack_name = existing["name"]
            await callback.answer(
                f"⏳ Обновить можно через {hours}ч {mins}м",
                show_alert=True,
            )
            return
        # Можно обновить — идём дальше (пересоздаём)

    _pack_creating.add(user_id)

    progress_msg = await callback.message.answer(
        "🔍 <b>Загружаю список ваших подарков…</b>\n\n"
        "<i>Список подарков должен быть публичным в настройках приватности!</i>",
        parse_mode=ParseMode.HTML,
    )

    try:
        # Получаем подарки
        try:
            slugs = await fetch_user_gifts(bot, user_id)
        except ValueError as e:
            if str(e) == "PRIVATE":
                await progress_msg.edit_text(
                    "🔒 <b>Список подарков скрыт!</b>\n\n"
                    "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
                    "Чтобы создать стикерпак из своих подарков, нужно сделать список <b>публичным</b>:\n\n"
                    "1. Открой <b>Telegram → Настройки</b>\n"
                    "2. <b>Конфиденциальность → Подарки</b>\n"
                    "3. Выбери <b>«Все»</b> или <b>«Мои контакты»</b>\n\n"
                    "После этого нажми кнопку снова! 👇",
                    parse_mode=ParseMode.HTML,
                )
                return
            raise

        if not slugs:
            await progress_msg.edit_text(
                "😔 <b>У вас нет NFT-подарков</b> или список пуст.\n\n"
                "Получи NFT-подарок от друга и попробуй снова!",
                parse_mode=ParseMode.HTML,
            )
            return

        user_log.info("🎴 СОЗДАНИЕ ПРОФИЛЬ-ПАКА | %s | подарков=%d", _u(callback.from_user), len(slugs))

        # Создаём пак
        pack_name = await create_sticker_pack(
            bot=bot,
            user_id=user_id,
            username=username,
            pack_type="profile",
            slugs=slugs,
            progress_msg=progress_msg,
        )

        if pack_name:
            # Сохраняем данные
            set_user_pack(user_id, "profile", {
                "name": pack_name,
                "title": make_pack_title(username, "profile", BOT_USERNAME),
                "slugs": slugs,
                "created_at": existing.get("created_at", time.time()) if existing else time.time(),
                "updated_at": time.time(),
                "count": len(slugs),
            })

            await progress_msg.edit_text(
                f"✅ <b>Профиль-стикерпак создан!</b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
                f"🎴 <b>Название:</b> {make_pack_title(username, 'profile', BOT_USERNAME)}\n"
                f"📦 <b>Стикеров:</b> {len(slugs)}\n\n"
                f"👇 Нажми кнопку ниже, чтобы открыть стикерпак!",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="🔗 Открыть стикерпак",
                        url=f"https://t.me/addstickers/{pack_name}",
                    )],
                    [InlineKeyboardButton(
                        text="📦 Мои стикерпаки",
                        callback_data=CB_MY_STICKER_PACKS,
                    )],
                ]),
            )
            user_log.info("✅ ПРОФИЛЬ-ПАК СОЗДАН | %s | name=%s | count=%d",
                          _u(callback.from_user), pack_name, len(slugs))
        else:
            await progress_msg.edit_text(
                "❌ <b>Не удалось создать стикерпак.</b>\n\n"
                "Попробуй позже или напиши автору: "
                "<a href='https://t.me/balfikovich'>@balfikovich</a>",
                parse_mode=ParseMode.HTML,
            )

    except Exception as e:
        logger.error("callback_create_profile_pack: %s", e, exc_info=True)
        await progress_msg.edit_text(
            f"❌ <b>Ошибка:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        _pack_creating.discard(user_id)


@dp.callback_query(F.data.startswith(CB_UPDATE_PROFILE_PACK))
async def callback_update_profile_pack(callback: CallbackQuery) -> None:
    """Обновить профильный стикерпак (раз в сутки)."""
    user_id = callback.from_user.id

    can_upd, wait_sec = can_update_profile_pack(user_id)
    if not can_upd:
        hours = int(wait_sec // 3600)
        mins  = int((wait_sec % 3600) // 60)
        await callback.answer(
            f"⏳ Обновить можно через {hours}ч {mins}м\n"
            "Стикерпак обновляется раз в сутки!",
            show_alert=True,
        )
        return

    # Запускаем как создание (пересоздание)
    await callback_create_profile_pack(callback)


@dp.callback_query(F.data.startswith(CB_ADD_TO_PERSONAL))
async def callback_add_to_personal(callback: CallbackQuery) -> None:
    """
    Добавить стикер в личный стикерпак.
    
    Кнопка НЕ пропадает из сообщения — она персональная!
    Каждый пользователь может добавить стикер только один раз.
    Повторное нажатие — маленькое alert-окошко «Уже добавлен!»
    """
    user_id  = callback.from_user.id
    username = callback.from_user.username
    slug     = callback.data[len(CB_ADD_TO_PERSONAL):]

    # Проверяем — уже добавлен этот slug в личный пак?
    if is_personal_pack_has_slug(user_id, slug):
        name, number = split_slug(slug)
        nice = normalize_gift_name(name)
        pack = get_user_pack(user_id, "personal")
        count = pack.get("count", 0) if pack else 0
        pack_name = pack.get("name", "") if pack else ""
        link_line = f"\nt.me/addstickers/{pack_name}" if pack_name else ""
        await callback.answer(
            f"📦 Стикер уже есть в твоём стикерпаке!\n\n"
            f"🎁 {nice} #{number}\n\n"
            f"Всего стикеров: {count}/100"
            f"{link_line}",
            show_alert=True,  # модальное окошко
        )
        return

    # Лимит?
    pack = get_user_pack(user_id, "personal")
    if pack and pack.get("count", 0) >= 100:
        await callback.answer(
            "📦 Личный стикерпак заполнен (100/100)!\n"
            "Telegram не позволяет добавить больше 100 стикеров.",
            show_alert=True,
        )
        return

    # Блокировка от двойного нажатия
    lock_key = f"personal_{user_id}"
    if _cb_lock.get(lock_key):
        await callback.answer("⏳ Добавляю…", show_alert=False)
        return

    _cb_lock[lock_key] = True
    await callback.answer("⏳ Добавляю в твой стикерпак…", show_alert=False)

    try:
        success, result = await add_single_sticker_to_pack(
            bot=bot,
            user_id=user_id,
            username=username,
            slug=slug,
        )

        if success:
            name, number = split_slug(slug)
            nice = normalize_gift_name(name)
            pack_name = result
            count = get_user_pack(user_id, "personal").get("count", 1)

            # Отвечаем через личку (в группах нельзя ответить напрямую — шлём в личку боту)
            try:
                await bot.send_message(
                    user_id,
                    f"✅ <b>Стикер добавлен в личный стикерпак!</b>\n\n"
                    f"🎁 <b>{nice} #{number}</b>\n"
                    f"📦 Всего стикеров: <b>{count}/100</b>\n\n"
                    f"<a href='https://t.me/addstickers/{pack_name}'>🔗 Открыть стикерпак</a>",
                    parse_mode=ParseMode.HTML,
                )
            except TelegramForbiddenError:
                # Пользователь не начал диалог с ботом — нельзя написать в личку
                # В группе кнопка сработала, просто не уведомляем
                pass
            except Exception:
                pass

            user_log.info("➕ В ЛИЧНЫЙ ПАК | slug=%s | %s | count=%d",
                          slug, _u(callback.from_user), count)

        elif result == "already_added":
            # Race condition — добавили пока обрабатывали
            await callback.answer("✅ Уже в твоём стикерпаке!", show_alert=False)
        elif result == "limit_reached":
            await callback.answer(
                "📦 Личный стикерпак заполнен (100/100)!",
                show_alert=True,
            )
        else:
            await callback.answer(f"❌ Ошибка: {result}", show_alert=True)

    finally:
        _cb_lock[lock_key] = False


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACKS: КНОПКИ ПОД ПРЕВЬЮ (оригинальные)
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith(CB_NO_COMPRESS_VIDEO))
async def callback_no_compress_video(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_COMPRESS_VIDEO):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_no_compress_video:
        await callback.answer("❌ Видео без сжатия уже было отправлено!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_NO_COMPRESS_VIDEO)
    if wait > 0:
        mins = int(wait) // 60; secs = int(wait) % 60
        await callback.answer(f"⏳ Подожди ещё {f'{mins} мин {secs} сек' if mins else f'{secs} сек'}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю оригинал…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(f"❌ Не удалось загрузить: {err or 'файл не найден'}")
            return

        wm = await callback.message.answer("⚙️ Конвертирую в видео без сжатия…")
        try:
            mp4_data = await asyncio.wait_for(asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=120.0)
        except asyncio.TimeoutError:
            mp4_data = None
        await safe_delete(wm)

        if not mp4_data:
            await callback.message.answer("❌ Не удалось конвертировать видео.")
            return

        _used_no_compress_video.add(key)
        await remove_keyboard_button(callback.message, CB_NO_COMPRESS_VIDEO)
        await send_document(callback.message.answer_document, mp4_data, f"{slug}.mp4")
        user_log.info("📤 БЕЗ СЖАТИЯ ВИДЕО | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


@dp.callback_query(F.data.startswith(CB_SEND_GIF))
async def callback_send_gif(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_SEND_GIF):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_gif:
        await callback.answer("❌ GIF уже был отправлен!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_SEND_GIF)
    if wait > 0:
        mins = int(wait) // 60; secs = int(wait) % 60
        await callback.answer(f"⏳ Подожди ещё {f'{mins} мин {secs} сек' if mins else f'{secs} сек'}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(f"❌ Не удалось загрузить: {err or 'файл не найден'}")
            return

        wm = await callback.message.answer("⚙️ Конвертирую…")
        try:
            mp4_data = await asyncio.wait_for(asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=120.0)
        except asyncio.TimeoutError:
            mp4_data = None
        await safe_delete(wm)

        if not mp4_data:
            await callback.message.answer("❌ Не удалось конвертировать.")
            return

        _used_gif.add(key)
        await remove_keyboard_button(callback.message, CB_SEND_GIF)

        file = BufferedInputFile(mp4_data, filename=f"{slug}.mp4")
        await callback.message.answer_animation(animation=file)
        user_log.info("🎞 GIF | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_COMPRESS):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    wait = check_button_antispam(uid, CB_NO_COMPRESS)
    if wait > 0:
        mins = int(wait) // 60; secs = int(wait) % 60
        await callback.answer(f"⏳ Подожди ещё {f'{mins} мин {secs} сек' if mins else f'{secs} сек'}", show_alert=False)
        return

    if key in _used_no_compress:
        await callback.answer("❌ Оригинал уже был отправлен!", show_alert=True)
        return

    _cb_lock[uid] = True
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
    finally:
        _cb_lock[uid] = False


@dp.callback_query(F.data.startswith(CB_NO_ANIM))
async def callback_no_anim(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_ANIM):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_no_anim:
        await callback.answer("❌ Картинка уже была отправлена!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_NO_ANIM)
    if wait > 0:
        mins = int(wait) // 60; secs = int(wait) % 60
        await callback.answer(f"⏳ Подожди ещё {f'{mins} мин {secs} сек' if mins else f'{secs} сек'}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю картинку…")

    try:
        (found, webp, err), attrs = await asyncio.gather(
            fetch_nft_image(slug), fetch_nft_attrs(slug),
        )
        if err or not found:
            await callback.message.answer(f"❌ Не удалось загрузить: {err or 'подарок не найден'}")
            return

        png = webp_to_png(webp)
        if not png:
            await send_document(callback.message.answer_document, webp, f"{slug}.webp")
            return

        _used_no_anim.add(key)
        await remove_keyboard_button(callback.message, CB_NO_ANIM)

        ok = await send_static_photo(callback.message, png, slug, attrs)
        if not ok:
            await send_document(callback.message.answer_document, png, f"{slug}.png")
        user_log.info("🖼 БЕЗ АНИМАЦИИ | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


@dp.callback_query(F.data.startswith(CB_SEND_STICKER))
async def callback_send_sticker(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_SEND_STICKER):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_sticker:
        await callback.answer("❌ Стикер уже был отправлен!", show_alert=True)
        return

    wait = check_button_antispam(uid, CB_SEND_STICKER)
    if wait > 0:
        mins = int(wait) // 60; secs = int(wait) % 60
        await callback.answer(f"⏳ Подожди ещё {f'{mins} мин {secs} сек' if mins else f'{secs} сек'}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю стикер…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(f"❌ Не удалось загрузить стикер: {err or 'не найден'}")
            return

        _used_sticker.add(key)
        await remove_keyboard_button(callback.message, CB_SEND_STICKER)

        file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
        await callback.message.answer_document(document=file)
        user_log.info("🎭 СТИКЕР | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ══════════════════════════════════════════════════════════════════════════════
#  /cancel_donate
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("cancel_donate"))
async def cmd_cancel_donate(message: Message) -> None:
    if message.chat.type != "private":
        return
    uid = message.from_user.id
    if uid in _awaiting_donate:
        _awaiting_donate.discard(uid)
        await message.answer("✅ Донат отменён. Возвращайся когда захочешь! 😊")
    else:
        await message.answer("Нет активного ожидания оплаты. Всё в порядке! 😊")


# ── Callback: «Поддержать автора» ────────────────────────────────────────────
@dp.callback_query(F.data == CB_DONATE)
async def callback_donate(callback: CallbackQuery) -> None:
    await callback.answer()
    uid = callback.from_user.id
    _awaiting_donate.add(uid)
    await callback.message.answer(
        f'<tg-emoji emoji-id="{E_DONATE}">⭐</tg-emoji> <b>Поддержка проекта</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Привет! 👋 Этот бот полностью <b>бесплатен</b>.\n\n"
        "Если хочешь поддержать автора — буду очень благодарен! 🙏\n\n"
        "<b>Напиши число</b> — сколько ⭐ звёзд хочешь отправить.\n"
        "Минимум — <code>1</code>, максимум — <code>2500</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "💡 Передумал — напиши <code>/cancel_donate</code>",
        parse_mode=ParseMode.HTML,
    )


@dp.pre_checkout_query()
async def pre_checkout_handler(query: PreCheckoutQuery) -> None:
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def payment_handler(message: Message) -> None:
    payment = message.successful_payment
    stars   = payment.total_amount
    user    = message.from_user
    uname   = f"@{user.username}" if user.username else f"без username (id={user.id})"

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
            f"🔔 <b>Новый донат!</b>\n<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"👤 <b>Имя:</b> {user.full_name or 'NoName'}\n"
            f"📎 <b>Username:</b> {uname}\n"
            f"⭐ <b>Сумма:</b> {stars} звёзд\n"
            f"🆔 <b>User ID:</b> <code>{user.id}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error("уведомление о донате: %s", e)


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

    # ── ДОНАТ ────────────────────────────────────────────────────────────────
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
                "⚠️ Введи <b>число</b> — количество звёзд.\n\nНапример: <code>10</code>\n\nПередумал? — <code>/cancel_donate</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── ГРУППА ───────────────────────────────────────────────────────────────
    if not is_private:
        lower = raw.lower()

        if lower.strip() in ("превью инструкция", "preview инструкция",
                             "превью instruction", "preview instruction"):
            wait = check_instr_antispam(message.chat.id)
            if wait > 0:
                await safe_delete(message)
                return
            await message.answer(get_group_instruction(), parse_mode=ParseMode.HTML)
            return

        if lower.startswith("+а превью") or lower.startswith("+а preview"):
            for prefix in ("+а превью", "+а preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_group_video(message, raw)
            return

        if lower.startswith("превью") or lower.startswith("preview"):
            for prefix in ("превью", "preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_group_static(message, raw)
            return

        return

    # ── ЛИЧКА ────────────────────────────────────────────────────────────────
    await _handle_private_video(message, raw)


# ── Статичная PNG (группа) ────────────────────────────────────────────────────
async def _handle_group_static(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n<code>превью PlushPepe 22</code>\n<code>превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

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

    t0 = time.monotonic()
    wm = await message.answer(f"🔍 Загружаю <b>{slug}</b>…", parse_mode=ParseMode.HTML)

    (found, webp, err), attrs = await asyncio.gather(
        fetch_nft_image(slug), fetch_nft_attrs(slug),
    )
    await safe_delete(wm)

    if err:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> <b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>',
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Подарок не найден</b>\n\n<code>{slug}</code>\n\n'
            "<b>Возможные причины:</b>\n• Такого номера не существует\n• Подарок сожжён 🔥\n• Опечатка в названии",
            parse_mode=ParseMode.HTML,
        )
        return

    png = webp_to_png(webp)
    if png:
        ok = await send_static_photo(message, png, slug, attrs)
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
            "<b>Примеры:</b>\n<code>+а превью PlushPepe 22</code>\n<code>+а превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

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

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю данные…")

    (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs = await asyncio.gather(
        fetch_nft_image(slug), fetch_nft_tgs(slug), fetch_nft_attrs(slug),
    )

    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> <b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>',
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Подарок не найден</b>\n\n<code>{slug}</code>',
                parse_mode=ParseMode.HTML,
            )
        return

    mp4_data: Optional[bytes] = None
    if tgs_ok and tgs_data:
        await safe_delete(wm)
        wm = await message.answer("⚙️ Конвертирую в видео…")
        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None

    await safe_delete(wm)

    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs)
        if ok:
            return

    # Откат на PNG
    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs)
            if ok:
                return
        await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> Не удалось создать видео и загрузить картинку.',
            parse_mode=ParseMode.HTML,
        )


# ── MP4 (личка) ───────────────────────────────────────────────────────────────
async def _handle_private_video(message: Message, raw: str) -> None:
    uid  = message.from_user.id
    slug = extract_nft_slug(raw)

    if not slug:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n<code>PlushPepe 22</code>\n<code>t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    wait = check_antispam(uid)
    if wait > 0:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> <b>Слишком быстро!</b> Подожди <code>{wait}</code> сек.',
            parse_mode=ParseMode.HTML,
        )
        return

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю данные…")

    (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs = await asyncio.gather(
        fetch_nft_image(slug), fetch_nft_tgs(slug), fetch_nft_attrs(slug),
    )

    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> <b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>',
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Подарок не найден</b>\n\n<code>{slug}</code>\n\n'
                "<b>Возможные причины:</b>\n• Такого номера не существует\n• Подарок сожжён 🔥\n• Опечатка в названии",
                parse_mode=ParseMode.HTML,
            )
        return

    mp4_data: Optional[bytes] = None
    if tgs_ok and tgs_data:
        await safe_delete(wm)
        wm = await message.answer("⚙️ Конвертирую в видео…")
        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data), timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None

    await safe_delete(wm)

    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs)
        if ok:
            return

    # Откат на PNG
    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs)
            if ok:
                return
        await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> Не удалось создать видео и загрузить картинку.',
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
            description="Введите: PlushPepe-22 / Plush Pepe 22",
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
            description="Пример: PlushPepe-22 / Plush Pepe 22 / t.me/nft/...",
            input_message_content=InputTextMessageContent(
                message_text="<b>Неверный формат</b>\n\n<code>PlushPepe-22</code>",
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[err_result], cache_time=5, is_personal=True)
        return

    (found, webp, err), attrs = await asyncio.gather(
        fetch_nft_image(slug), fetch_nft_attrs(slug),
    )

    name, number = split_slug(slug)
    nice  = normalize_gift_name(name)
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

    caption, ents = make_caption(slug, attrs)
    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть в Telegram", url=f"https://t.me/nft/{slug}")
    ]])

    desc_parts = []
    if attrs.model    != "—": desc_parts.append(f"🪄 {attrs.model}")
    if attrs.backdrop != "—": desc_parts.append(f"🎨 {attrs.backdrop}")
    if attrs.symbol   != "—": desc_parts.append(f"✨ {attrs.symbol}")

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
    await query.answer(results=[result], cache_time=60, is_personal=False)


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

async def on_startup() -> None:
    global BOT_USERNAME
    get_session()
    me = await bot.get_me()
    BOT_USERNAME = me.username or ""

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
        logger.warning("   ❌ rlottie-python НЕ установлен!")

    try:
        from PIL import Image  # noqa
        logger.info("   ✅ Pillow установлен")
    except ImportError:
        logger.warning("   ❌ Pillow НЕ установлен!")

    try:
        from bs4 import BeautifulSoup  # noqa
        logger.info("   ✅ BeautifulSoup4 установлен")
    except ImportError:
        logger.warning("   ❌ BeautifulSoup4 НЕ установлен!")

    logger.info("━" * 60)
    logger.info("🎴 СТИКЕРПАКИ ВКЛЮЧЕНЫ")
    logger.info("   Профиль: profile_<uid>_by_%s", BOT_USERNAME.lower())
    logger.info("   Личный:  personal_<uid>_by_%s", BOT_USERNAME.lower())
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
