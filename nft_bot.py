"""
NFT Gift Viewer Bot
===================
Зависимости:
    pip install aiogram aiohttp python-dotenv pillow rlottie-python beautifulsoup4 lxml

Переменные окружения (.env):
    BOT_TOKEN=xxx
    LOG_FILE=bot.log   (опционально)
"""

import asyncio
import io
import logging
import os
import re
import tempfile
import time
import uuid
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

FRAGMENT_IMAGE_URL  = "https://nft.fragment.com/gift/{slug}.webp"
FRAGMENT_TGS_URL    = "https://nft.fragment.com/gift/{slug}.tgs"
REQUEST_TIMEOUT     = aiohttp.ClientTimeout(total=30)

# Префиксы callback_data — важно: CB_NO_ANIM и CB_SEND_STICKER не должны
# быть префиксом друг друга и не должны начинаться одинаково с CB_NO_COMPRESS
CB_NO_COMPRESS  = "nc:"      # nc:slug
CB_DONATE       = "donate"
CB_NO_ANIM      = "na:"      # na:slug
CB_SEND_STICKER = "sk:"      # sk:slug

ANTISPAM_SECONDS   = 1.5
ANTISPAM_SLUG_SEC  = 300    # 5 мин — повтор одного подарка в группе
ANTISPAM_ANIM_SEC  = 120    # 2 мин — кулдаун кнопки «без анимации»
ANTISPAM_INSTR_SEC = 300    # 5 мин — команда «превью инструкция»

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
#  СЛОВАРЬ ПРАВИЛЬНЫХ НАЗВАНИЙ
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
    """Возвращает правильное написание названия подарка из словаря."""
    return _GIFT_NAME_MAP.get(raw_name.lower().strip(), readable_name(raw_name))


# ══════════════════════════════════════════════════════════════════════════════
#  АНТИСПАМ И СОСТОЯНИЯ
# ══════════════════════════════════════════════════════════════════════════════

_last_request:     dict[int, float] = {}   # user_id → timestamp
_last_slug:        dict[str, float] = {}   # "user_id:slug" → timestamp
_last_anim_sent:   dict[str, float] = {}   # "user_id:slug" → timestamp отправки анимации
_last_instr:       dict[int, float] = {}   # chat_id → timestamp
_cb_lock:          dict[int, bool]  = {}   # user_id → bool (занят загрузкой)
_used_no_compress: set[str]         = set()  # "msg_id:slug"
_used_no_anim:     set[str]         = set()  # "msg_id:slug"
_used_sticker:     set[str]         = set()  # "msg_id:slug"
_awaiting_donate:  set[int]         = set()  # user_id ждёт ввода суммы

BOT_USERNAME: str = ""


def check_antispam(user_id: int) -> float:
    """Общий антиспам для личных сообщений. 0 = можно, >0 = сколько ждать."""
    now  = time.monotonic()
    last = _last_request.get(user_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_SECONDS:
        return round(ANTISPAM_SECONDS - diff, 1)
    _last_request[user_id] = now
    return 0.0


def check_slug_antispam(user_id: int, slug: str) -> float:
    """Антиспам по slug в группе. 0 = можно."""
    key  = f"{user_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_slug.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_SLUG_SEC:
        return int(ANTISPAM_SLUG_SEC - diff)
    _last_slug[key] = now
    return 0.0


def check_anim_cooldown(user_id: int, slug: str) -> float:
    """Проверяет кулдаун 2 мин после отправки анимации (для кнопки «без анимации»)."""
    key  = f"{user_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_anim_sent.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_ANIM_SEC:
        return int(ANTISPAM_ANIM_SEC - diff)
    return 0.0


def mark_anim_sent(user_id: int, slug: str) -> None:
    _last_anim_sent[f"{user_id}:{slug.lower()}"] = time.monotonic()


def check_instr_antispam(chat_id: int) -> float:
    """Антиспам для команды «превью инструкция». 0 = можно."""
    now  = time.monotonic()
    last = _last_instr.get(chat_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_INSTR_SEC:
        return int(ANTISPAM_INSTR_SEC - diff)
    _last_instr[chat_id] = now
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
                logger.warning("fetch_attrs HTTP %s | slug=%s", resp.status, slug)
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
    """Возвращает (found, data, error)."""
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                if not data:
                    return False, None, "Пустой ответ"
                return True, data, None
            elif resp.status == 404:
                return False, None, None  # не найден, не ошибка
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
#  TGS → GIF  (правильный API rlottie-python)
#  БАГ БЫЛ: неправильный вызов API — from_data + lottie_animation_render(i, size, size)
#  ПРАВИЛЬНО: from_tgs() и save_animation() или render_pillow_frame()
#  Также: формат буфера BGRA, не RGBA!
# ══════════════════════════════════════════════════════════════════════════════

def tgs_to_gif_bytes(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    """
    Конвертирует TGS → GIF используя rlottie-python.
    Использует временный файл т.к. from_tgs() принимает путь к файлу.
    """
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image

        # Записываем TGS во временный файл (rlottie-python требует файл)
        with tempfile.NamedTemporaryFile(suffix=".tgs", delete=False) as tf:
            tf.write(tgs_bytes)
            tgs_path = tf.name

        try:
            anim = LottieAnimation.from_tgs(tgs_path)

            frame_count = anim.lottie_animation_get_totalframe()
            fps         = anim.lottie_animation_get_framerate()
            w, h        = anim.lottie_animation_get_size()

            if frame_count == 0:
                logger.error("tgs_to_gif: 0 кадров в анимации")
                return None

            # Длительность одного кадра в миллисекундах
            duration_ms = max(int(1000 / fps) if fps > 0 else 60, 20)

            frames: list[Image.Image] = []

            for i in range(frame_count):
                # ПРАВИЛЬНЫЙ вызов: render_pillow_frame возвращает Pillow Image
                # Внутри он использует BGRA формат правильно
                frame_img = anim.render_pillow_frame(frame_num=i)
                if frame_img is None:
                    continue
                # Если размер не совпадает — масштабируем
                if frame_img.size != (size, size):
                    frame_img = frame_img.resize((size, size), Image.LANCZOS)
                frames.append(frame_img.convert("RGBA"))

            if not frames:
                logger.error("tgs_to_gif: нет кадров после рендера")
                return None

            # Конвертируем в GIF
            # Для GIF нужен режим P (palette) или RGB
            gif_frames = []
            for f in frames:
                # Конвертируем RGBA → RGB с белым фоном (GIF не поддерживает полную прозрачность)
                bg = Image.new("RGB", f.size, (0, 0, 0))  # чёрный фон
                bg.paste(f, mask=f.split()[3])  # применяем альфа-канал
                gif_frames.append(bg)

            buf = io.BytesIO()
            gif_frames[0].save(
                buf,
                format="GIF",
                save_all=True,
                append_images=gif_frames[1:],
                loop=0,
                duration=duration_ms,
                optimize=False,
            )
            result = buf.getvalue()
            logger.info("tgs_to_gif OK: %d кадров, %d байт", frame_count, len(result))
            return result

        finally:
            # Удаляем временный файл
            try:
                os.unlink(tgs_path)
            except Exception:
                pass

    except ImportError:
        logger.error("rlottie-python не установлен! pip install rlottie-python")
        return None
    except Exception as e:
        logger.error("tgs_to_gif error: %s", e, exc_info=True)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  ПОДПИСЬ НА GIF
#  БАГ БЫЛ: эмодзи 🪄🎨✨🎁🔗 не рендерятся стандартными шрифтами →
#  заменяем на текстовые метки без эмодзи в подписи на GIF
# ══════════════════════════════════════════════════════════════════════════════

def add_caption_to_gif(gif_bytes: bytes, slug: str, attrs: NftAttrs) -> Optional[bytes]:
    """Добавляет тёмный блок с подписью снизу каждого кадра GIF."""
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageSequence

        name, number = split_slug(slug)
        nice = normalize_gift_name(name)

        def load_font(size: int) -> ImageFont.FreeTypeFont:
            paths = [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
                "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
            ]
            for fp in paths:
                if os.path.exists(fp):
                    try:
                        return ImageFont.truetype(fp, size)
                    except Exception:
                        pass
            # Fallback — дефолтный шрифт (маленький, но хоть что-то)
            return ImageFont.load_default()

        font_title = load_font(22)
        font_attr  = load_font(17)
        font_small = load_font(14)

        PADDING = 14
        TITLE_H = 36
        LINE_H  = 28
        LINK_H  = 30

        # Атрибуты без эмодзи (обычные шрифты их не рендерят)
        attrs_lines: list[tuple[str, str]] = []
        if attrs.model != "—":
            r = f"  {attrs.model_rarity}" if attrs.model_rarity else ""
            attrs_lines.append(("Модель:", f"{attrs.model}{r}"))
        if attrs.backdrop != "—":
            r = f"  {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
            attrs_lines.append(("Фон:", f"{attrs.backdrop}{r}"))
        if attrs.symbol != "—":
            r = f"  {attrs.symbol_rarity}" if attrs.symbol_rarity else ""
            attrs_lines.append(("Символ:", f"{attrs.symbol}{r}"))

        cap_h = (PADDING + TITLE_H + 2
                 + len(attrs_lines) * LINE_H + 4
                 + LINK_H + PADDING)

        # Цвета
        BG_COLOR    = (25, 25, 25)
        WHITE       = (255, 255, 255)
        GREY        = (160, 160, 160)
        LABEL_CLR   = (120, 120, 220)
        VALUE_CLR   = (210, 210, 210)
        DIVIDER_CLR = (55, 55, 55)
        LINK_CLR    = (90, 140, 230)
        NUM_CLR     = (80, 80, 180)

        source = Image.open(io.BytesIO(gif_bytes))
        frames_out: list[Image.Image] = []
        durations:  list[int]         = []

        for frame in ImageSequence.Iterator(source):
            dur = frame.info.get("duration", 60)
            img = frame.convert("RGB")
            W, H = img.size

            canvas = Image.new("RGB", (W, H + cap_h), BG_COLOR)
            canvas.paste(img, (0, 0))

            draw = ImageDraw.Draw(canvas)
            y = H + PADDING

            # ── Заголовок ──────────────────────────────────────────────────
            title_text = f"[NFT]  {nice}  #{number}"
            draw.text((PADDING, y), title_text, font=font_title, fill=WHITE)
            y += TITLE_H

            # Разделитель
            draw.line([(PADDING, y), (W - PADDING, y)], fill=DIVIDER_CLR, width=1)
            y += 6

            # ── Атрибуты ───────────────────────────────────────────────────
            for idx, (label, value) in enumerate(attrs_lines, 1):
                # Номер
                draw.text((PADDING, y + 3), f"{idx}", font=font_small, fill=NUM_CLR)
                # Метка
                draw.text((PADDING + 16, y + 3), label, font=font_attr, fill=LABEL_CLR)
                # Ширина метки для сдвига значения
                try:
                    lw = int(draw.textlength(label, font=font_attr))
                except AttributeError:
                    lw = len(label) * 9
                # Значение
                draw.text((PADDING + 16 + lw + 8, y + 3), value, font=font_attr, fill=VALUE_CLR)
                y += LINE_H

            # Разделитель
            draw.line([(PADDING, y), (W - PADDING, y)], fill=DIVIDER_CLR, width=1)
            y += 6

            # ── Ссылка ─────────────────────────────────────────────────────
            draw.text((PADDING, y + 3), "Открыть в Telegram ->", font=font_attr, fill=LINK_CLR)

            frames_out.append(canvas)
            durations.append(dur)

        if not frames_out:
            return None

        buf = io.BytesIO()
        frames_out[0].save(
            buf,
            format="GIF",
            save_all=True,
            append_images=frames_out[1:],
            loop=0,
            duration=durations,
            optimize=False,
        )
        result = buf.getvalue()
        logger.info("add_caption_to_gif OK: %d кадров, %d байт", len(frames_out), len(result))
        return result

    except Exception as e:
        logger.error("add_caption_to_gif error: %s", e, exc_info=True)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION (MessageEntity) — для статичной картинки
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


def make_keyboard_static(slug: str) -> InlineKeyboardMarkup:
    """Кнопка под статичной картинкой."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📤 Отправить без сжатия",
                             callback_data=f"{CB_NO_COMPRESS}{slug}")
    ]])


def make_keyboard_anim(slug: str) -> InlineKeyboardMarkup:
    """Кнопки под анимированным GIF."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖼 Отправить без анимации",
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
        "<b>Форматы запросов в группе:</b>\n\n"
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимированная (GIF):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n"
        "<code>+а превью t.me/nft/PlushPepe-22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Статичную после анимации — через <b>2 минуты</b>\n"
        "• Кнопки «Без анимации» и «Стикер» — по 1 разу\n"
        "• <code>превью инструкция</code> — раз в 5 минут\n\n"
        "<b>❓ Нужна помощь?</b>\n"
        "Пиши автору: <a href='https://t.me/balfikovich'>@balfikovich</a>"
    )


def get_group_welcome(chat_title: str) -> str:
    return (
        f"👋 <b>Привет, {chat_title}!</b>\n\n"
        "Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📌 Как пользоваться:</b>\n\n"
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n\n"
        "🎬 <b>Анимированная (GIF):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Написать <code>превью инструкция</code> — полная справка\n\n"
        "⚡ Результат за ~2–4 сек\n\n"
        "<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


def get_start_text() -> str:
    return (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Показываю <b>анимированную</b> карточку любого Telegram NFT-подарка.\n\n"
        "<b>📨 Как пользоваться в личке:</b>\n"
        "Отправь ссылку или название — получишь анимированный GIF с подписью.\n\n"
        "<b>✅ Форматы:</b>\n"
        "<code>https://t.me/nft/PlushPepe-22</code>\n"
        "<code>t.me/nft/PlushPepe-22</code>\n"
        "<code>PlushPepe-22</code>\n"
        "<code>PlushPepe 22</code>\n"
        "<code>Plush Pepe 22</code>\n\n"
        "Под GIF — кнопки <b>«Без анимации»</b> и <b>«Стикер»</b>.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>👥 В группе:</b>\n"
        "<code>превью PlushPepe 22</code> — статичная\n"
        "<code>+а превью PlushPepe 22</code> — анимированная\n"
        "<code>превью инструкция</code> — справка\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Анимация ~2–5 сек\n\n"
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


async def remove_button_from_keyboard(msg: Message, remove_prefix: str) -> None:
    """
    Убирает из клавиатуры все кнопки, callback_data которых начинается
    с remove_prefix. Если кнопок не осталось — убирает клавиатуру целиком.
    """
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
    """Отправляет PNG с caption (entities) и кнопкой «без сжатия»."""
    caption, ents = make_caption(slug, attrs)
    kbd  = make_keyboard_static(slug)
    file = BufferedInputFile(png, filename=f"{slug}.png")
    try:
        await message.answer_photo(
            photo=file,
            caption=caption,
            caption_entities=ents,
            parse_mode=None,   # используем entities, не parse_mode
            reply_markup=kbd,
        )
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(png, filename=f"{slug}.png")
            await message.answer_photo(
                photo=file, caption=caption,
                caption_entities=ents, parse_mode=None, reply_markup=kbd,
            )
            return True
        except Exception as ex:
            logger.error("send_static_photo retry: %s", ex)
            return False
    except Exception as e:
        logger.error("send_static_photo: %s", e)
        return False


async def send_anim_gif(message: Message, gif: bytes, slug: str) -> bool:
    """
    Отправляет GIF как анимацию (answer_animation).
    Telegram принимает GIF через sendAnimation и показывает его как анимацию.
    """
    kbd  = make_keyboard_anim(slug)
    file = BufferedInputFile(gif, filename=f"{slug}.gif")
    try:
        await message.answer_animation(animation=file, reply_markup=kbd)
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(gif, filename=f"{slug}.gif")
            await message.answer_animation(animation=file, reply_markup=kbd)
            return True
        except Exception as ex:
            logger.error("send_anim_gif retry: %s", ex)
            return False
    except Exception as e:
        logger.error("send_anim_gif: %s", e)
        return False


async def send_document(send_fn, data: bytes, filename: str) -> None:
    """Отправляет файл как документ (без сжатия/конвертации Telegram)."""
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

    privacy = "приватный" if not getattr(event.chat, "username", None) else "публичный"

    if was_out and now_in:
        role = ("администратором"
                if new in (ChatMemberStatus.ADMINISTRATOR, "administrator")
                else "участником")
        user_log.info("➕ БОТ ДОБАВЛЕН | кто=%s | чат=%s | роль=%s | тип=%s",
                      _u(event.from_user), _chat(event.chat), role, privacy)
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
#  CALLBACK: «Отправить без анимации»
#  Шлёт статичную PNG с caption и кнопкой «без сжатия»
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith(CB_NO_ANIM))
async def callback_no_anim(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_ANIM):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_no_anim:
        await callback.answer("❌ Картинка уже была отправлена!", show_alert=True)
        return

    # Кулдаун 2 минуты
    wait = check_anim_cooldown(uid, slug)
    if wait > 0:
        mins = wait // 60
        secs = wait % 60
        ts   = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=True)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю картинку…")

    try:
        (found, webp, err), attrs = await asyncio.gather(
            fetch_nft_image(slug),
            fetch_nft_attrs(slug),
        )
        if err or not found:
            reason = err or "подарок не найден"
            await callback.message.answer(f"❌ Не удалось загрузить: {reason}")
            return

        png = webp_to_png(webp)
        if not png:
            # Откат — шлём webp как документ
            await send_document(callback.message.answer_document, webp, f"{slug}.webp")
        else:
            _used_no_anim.add(key)
            # Убираем кнопку «без анимации» из клавиатуры GIF-сообщения
            await remove_button_from_keyboard(callback.message, CB_NO_ANIM)
            # Отправляем статичную картинку
            await send_static_photo(callback.message, png, slug, attrs)

        user_log.info("🖼 БЕЗ АНИМАЦИИ | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK: «Отправить стикер (TGS)»
#  Шлёт оригинальный .tgs файл как стикер
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith(CB_SEND_STICKER))
async def callback_send_sticker(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_SEND_STICKER):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_sticker:
        await callback.answer("❌ Стикер уже был отправлен!", show_alert=True)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю стикер…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            reason = err or "стикер не найден"
            await callback.message.answer(f"❌ Не удалось загрузить стикер: {reason}")
            return

        _used_sticker.add(key)
        # Убираем кнопку «стикер» из клавиатуры GIF-сообщения
        await remove_button_from_keyboard(callback.message, CB_SEND_STICKER)

        # Отправляем TGS как стикер
        file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
        await callback.message.answer_sticker(sticker=file)

        user_log.info("🎭 СТИКЕР | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK: «Отправить без сжатия» — шлёт WebP документом
#  БАГ БЫЛ: отправлял TGS вместо WebP
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_COMPRESS):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    wait = check_antispam(uid)
    if wait > 0:
        await callback.answer(f"⏳ Подожди {wait} сек.", show_alert=True)
        return

    if key in _used_no_compress:
        await callback.answer("❌ Оригинал уже был отправлен!", show_alert=True)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю оригинал…")

    try:
        # Скачиваем WebP (НЕ TGS!)
        found, webp, err = await fetch_nft_image(slug)
        if err or not found:
            reason = err or "подарок не найден"
            await callback.message.answer(f"❌ Не удалось загрузить: {reason}")
            return

        _used_no_compress.add(key)
        # Убираем кнопку
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Отправляем WebP как документ (без сжатия Telegram)
        await send_document(callback.message.answer_document, webp, f"{slug}.webp")

        user_log.info("📤 БЕЗ СЖАТИЯ | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


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

    # ── ДОНАТ: перехватываем ввод суммы ──────────────────────────────────────
    if is_private and uid in _awaiting_donate:
        s = raw.strip()
        if s.isdigit():
            amount = int(s)
            if amount < 1:
                await message.answer(
                    "⚠️ Минимум — <b>1 звезда ⭐</b>.", parse_mode=ParseMode.HTML)
                return
            if amount > 2500:
                await message.answer(
                    "⚠️ Максимум — <b>2500 звёзд</b> за один раз.", parse_mode=ParseMode.HTML)
                return
            _awaiting_donate.discard(uid)
            user_log.info("💛 ЧЕК | %s | %s ⭐", _u(message.from_user), amount)
            try:
                await bot.send_invoice(
                    chat_id=message.chat.id,
                    title="⭐ Поддержка автора",
                    description=(
                        f"Донат автору бота NFT Gift Viewer — {amount} звёзд. Спасибо! 🙏"
                    ),
                    payload=f"donate_{uid}_{amount}",
                    currency="XTR",      # Telegram Stars
                    prices=[LabeledPrice(label="Звёзды", amount=amount)],
                    provider_token="",   # для Stars не нужен
                )
            except Exception as e:
                logger.error("send_invoice: %s", e)
                _awaiting_donate.add(uid)
                await message.answer(
                    "❌ Не удалось создать счёт. Попробуй ещё раз.",
                    parse_mode=ParseMode.HTML,
                )
        else:
            await message.answer(
                "⚠️ Введи <b>число</b> — количество звёзд.\n\n"
                "Например: <code>10</code>\n\n"
                "Передумал? — <code>/cancel_donate</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── ГРУППА ────────────────────────────────────────────────────────────────
    if not is_private:
        lower = raw.lower()

        # «превью инструкция»
        if lower.strip() in (
            "превью инструкция", "preview инструкция",
            "превью instruction", "preview instruction",
        ):
            wait = check_instr_antispam(message.chat.id)
            if wait > 0:
                user_log.info("🚫 ИНСТРУКЦИЯ УДАЛЕНА (спам) | %s | %s",
                              _u(message.from_user), _chat(message.chat))
                await safe_delete(message)
                return
            user_log.info("📖 ИНСТРУКЦИЯ | %s | %s",
                          _u(message.from_user), _chat(message.chat))
            await message.answer(get_group_instruction(), parse_mode=ParseMode.HTML)
            return

        # «+а превью ...» — анимированная
        if lower.startswith("+а превью") or lower.startswith("+а preview"):
            for prefix in ("+а превью", "+а preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_anim(message, raw, is_private=False)
            return

        # «превью ...» — статичная
        if lower.startswith("превью") or lower.startswith("preview"):
            for prefix in ("превью", "preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_static(message, raw)
            return

        # Не наше сообщение — игнорируем
        return

    # ── ЛИЧКА: всегда анимированная ──────────────────────────────────────────
    await _handle_anim(message, raw, is_private=True)


# ── Статичная картинка ────────────────────────────────────────────────────────
async def _handle_static(message: Message, raw: str) -> None:
    slug = extract_nft_slug(raw)
    uid  = message.from_user.id

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (статик) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>превью PlushPepe 22</code>\n"
            "<code>превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🖼 ЗАПРОС (статик) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    wait = check_slug_antispam(uid, slug)
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
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )
    elapsed = round(time.monotonic() - t0, 2)
    await safe_delete(wm)

    if err:
        user_log.warning("⚠️ ОШИБКА (статик) | slug=%s | %s | %.2fс", slug, err, elapsed)
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        user_log.info("❌ НЕ НАЙДЕН (статик) | slug=%s | %s", slug, _u(message.from_user))
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

    user_log.info("✅ СТАТИК | slug=%s | модель=%s | %s | %.2fс",
                  slug, attrs.model, _u(message.from_user), elapsed)

    png = webp_to_png(webp)
    if png:
        ok = await send_static_photo(message, png, slug, attrs)
        if not ok:
            logger.warning("send_static_photo упал → документ | slug=%s", slug)
            await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await send_document(message.answer_document, webp, f"{slug}.webp")


# ── Анимированная картинка ────────────────────────────────────────────────────
async def _handle_anim(message: Message, raw: str, is_private: bool) -> None:
    slug = extract_nft_slug(raw)
    uid  = message.from_user.id

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (аним) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        if is_private:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
                "<b>Примеры:</b>\n"
                "<code>PlushPepe 22</code>\n"
                "<code>t.me/nft/PlushPepe-22</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
                "<b>Примеры:</b>\n"
                "<code>+а превью PlushPepe 22</code>\n"
                "<code>+а превью t.me/nft/PlushPepe-22</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    user_log.info("🎬 ЗАПРОС (аним) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    # Антиспам
    if not is_private:
        wait = check_slug_antispam(uid, slug)
        if wait > 0:
            mins, secs = wait // 60, wait % 60
            ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Этот подарок уже был показан.</b>\nПовтор через <code>{ts}</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
    else:
        wait = check_antispam(uid)
        if wait > 0:
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Слишком быстро!</b> Подожди <code>{wait}</code> сек.",
                parse_mode=ParseMode.HTML,
            )
            return

    t0 = time.monotonic()
    wm = await message.answer("🎬 Загружаю анимацию…", parse_mode=ParseMode.HTML)

    # Параллельно скачиваем TGS, WebP и атрибуты
    (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_tgs(slug),
        fetch_nft_attrs(slug),
    )

    # Ничего не нашли
    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            user_log.warning("⚠️ ОШИБКА (аним) | slug=%s | %s", slug, err)
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            user_log.info("❌ НЕ НАЙДЕН (аним) | slug=%s | %s", slug, _u(message.from_user))
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

    # Пробуем конвертировать TGS → GIF
    gif_final: Optional[bytes] = None

    if tgs_ok and tgs_data:
        await safe_delete(wm)
        wm = await message.answer("⚙️ Конвертирую в GIF…")

        try:
            # asyncio.to_thread чтобы не блокировать event loop
            gif_raw = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_gif_bytes, tgs_data),
                timeout=90.0,
            )
        except asyncio.TimeoutError:
            gif_raw = None
            logger.error("tgs_to_gif timeout | slug=%s", slug)

        if gif_raw:
            try:
                gif_captioned = await asyncio.wait_for(
                    asyncio.to_thread(add_caption_to_gif, gif_raw, slug, attrs),
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                gif_captioned = None
                logger.error("add_caption timeout | slug=%s", slug)

            gif_final = gif_captioned if gif_captioned else gif_raw
        else:
            logger.warning("tgs_to_gif вернул None | slug=%s", slug)

    await safe_delete(wm)

    elapsed = round(time.monotonic() - t0, 2)

    if gif_final:
        ok = await send_anim_gif(message, gif_final, slug)
        if ok:
            mark_anim_sent(uid, slug)
            user_log.info("✅ АНИМ ОТПРАВЛЕНО | slug=%s | %s | %.2fс",
                          slug, _u(message.from_user), elapsed)
            return
        logger.warning("send_anim_gif упал → откат на статик | slug=%s", slug)

    # ── Откат на статичную картинку ───────────────────────────────────────────
    user_log.info("🖼 ОТКАТ НА СТАТИК | slug=%s | %s", slug, _u(message.from_user))
    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs)
            if ok:
                return
        await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            "Не удалось создать анимацию и загрузить картинку.",
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

    (found, webp, err), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
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
        InlineKeyboardButton(
            text="🔗 Открыть в Telegram",
            url=f"https://t.me/nft/{slug}",
        )
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

    # Проверяем rlottie-python
    try:
        from rlottie_python import LottieAnimation  # noqa: F401
        logger.info("   ✅ rlottie-python установлен — анимация работает")
    except ImportError:
        logger.warning("   ❌ rlottie-python НЕ установлен!")
        logger.warning("      Установи: pip install rlottie-python")
        logger.warning("      Анимация будет откатываться на статичную картинку!")

    # Проверяем Pillow
    try:
        from PIL import Image  # noqa: F401
        logger.info("   ✅ Pillow установлен")
    except ImportError:
        logger.warning("   ❌ Pillow НЕ установлен! pip install pillow")

    # Проверяем BeautifulSoup
    try:
        from bs4 import BeautifulSoup  # noqa: F401
        logger.info("   ✅ BeautifulSoup4 установлен")
    except ImportError:
        logger.warning("   ❌ BeautifulSoup4 НЕ установлен! pip install beautifulsoup4 lxml")

    logger.info("━" * 60)
    logger.info("ЧЕКЛИСТ @BotFather:")
    logger.info("  /setprivacy  → @%s → Disable  (обязательно для групп!)", me.username)
    logger.info("  /setjoingroups → @%s → Enable", me.username)
    logger.info("  /setinline   → @%s → задай placeholder", me.username)
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
