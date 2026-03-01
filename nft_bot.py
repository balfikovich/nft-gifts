"""
NFT Gift Viewer Bot
===================
Зависимости:
    pip install aiogram aiohttp python-dotenv pillow rlottie-python

Переменные окружения (.env):
    BOT_TOKEN=xxx
    LOG_FILE=bot.log   (опционально)
"""

import asyncio
import gzip
import io
import json
import logging
import os
import re
import textwrap
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
    FSInputFile,
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

ADMIN_ID = 5479063264   # уведомления о донатах

# ══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════

LOG_FILE = os.environ.get("LOG_FILE", "bot.log")
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
_ch = logging.StreamHandler(); _ch.setFormatter(_fmt)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8"); _fh.setFormatter(_fmt)
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

CB_NO_COMPRESS    = "nocompress:"
CB_DONATE         = "donate_start"
CB_NO_ANIM        = "noanim:"      # отправить статичную картинку (из анима-сообщения)
CB_SEND_STICKER   = "sticker:"     # отправить tgs как стикер

ANTISPAM_SECONDS  = 1.5
ANTISPAM_SLUG_SEC = 300   # 5 мин — повтор одного подарка в группе
ANTISPAM_ANIM_SEC = 120   # 2 мин — нельзя запросить картинку после анимации
ANTISPAM_INSTR_SEC = 300  # 5 мин — команда "превью инструкция" в группе

# ── Custom Emoji IDs ──────────────────────────────────────────────────────────
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
#  ПРАВИЛЬНЫЕ НАЗВАНИЯ ПОДАРКОВ (автокапитализация)
# ══════════════════════════════════════════════════════════════════════════════

_GIFT_NAMES: set[str] = {
    "Santa Hat","Signet Ring","Precious Peach","Plush Pepe","Spiced Wine",
    "Jelly Bunny","Durov's Cap","Perfume Bottle","Eternal Rose","Berry Box",
    "Vintage Cigar","Magic Potion","Kissed Frog","Hex Pot","Evil Eye",
    "Sharp Tongue","Trapped Heart","Skull Flower","Scared Cat","Spy Agaric",
    "Homemade Cake","Genie Lamp","Lunar Snake","Party Sparkler","Jester Hat",
    "Witch Hat","Hanging Star","Love Candle","Cookie Heart","Desk Calendar",
    "Jingle Bells","Snow Mittens","Voodoo Doll","Mad Pumpkin","Hypno Lollipop",
    "B-Day Candle","Bunny Muffin","Astral Shard","Flying Broom","Crystal Ball",
    "Eternal Candle","Swiss Watch","Ginger Cookie","Mini Oscar","Lol Pop",
    "Ion Gem","Star Notepad","Loot Bag","Love Potion","Toy Bear","Diamond Ring",
    "Sakura Flower","Sleigh Bell","Top Hat","Record Player","Winter Wreath",
    "Snow Globe","Electric Skull","Tama Gadget","Candy Cane","Neko Helmet",
    "Jack-in-the-Box","Easter Egg","Bonded Ring","Pet Snake","Snake Box",
    "Xmas Stocking","Big Year","Holiday Drink","Gem Signet","Light Sword",
    "Restless Jar","Nail Bracelet","Heroic Helmet","Bow Tie","Heart Locket",
    "Lush Bouquet","Whip Cupcake","Joyful Bundle","Cupid Charm","Valentine Box",
    "Snoop Dogg","Swag Bag","Snoop Cigar","Low Rider","Westside Sign",
    "Stellar Rocket","Jolly Chimp","Moon Pendant","Ionic Dryer","Input Key",
    "Mighty Arm","Artisan Brick","Clover Pin","Sky Stilettos","Fresh Socks",
    "Happy Brownie","Ice Cream","Spring Basket","Instant Ramen","Faith Amulet",
    "Mousse Cake","Bling Binky","Money Pot","Pretty Posy","Khabib's Papakha",
    "UFC Strike","Victory Medal","Rare Bird",
}

# Карта: lowercase → правильное написание
_GIFT_NAME_MAP: dict[str, str] = {n.lower(): n for n in _GIFT_NAMES}


def normalize_gift_name(raw_name: str) -> str:
    """Если имя есть в словаре — вернуть правильное написание, иначе — readable_name."""
    key = raw_name.lower().strip()
    return _GIFT_NAME_MAP.get(key, readable_name(raw_name))


# ══════════════════════════════════════════════════════════════════════════════
#  АНТИСПАМ
# ══════════════════════════════════════════════════════════════════════════════

_last_request:       dict[int, float]  = {}
_last_slug:          dict[str, float]  = {}
_last_anim_sent:     dict[str, float]  = {}   # "user_id:slug" → когда отправили анимацию
_last_instr:         dict[int, float]  = {}   # chat_id → когда последний раз инструкция
_cb_lock:            dict[int, bool]   = {}
_used_no_compress:   set[str]          = set()   # "msg_id:slug"
_used_no_anim:       set[str]          = set()   # "msg_id:slug"
_used_sticker:       set[str]          = set()   # "msg_id:slug"
_awaiting_donate:    set[int]          = set()

BOT_USERNAME: str = ""


def check_antispam(user_id: int) -> float:
    now  = time.monotonic()
    last = _last_request.get(user_id, 0.0)
    if now - last < ANTISPAM_SECONDS:
        return round(ANTISPAM_SECONDS - (now - last), 1)
    _last_request[user_id] = now
    return 0.0


def check_slug_antispam(user_id: int, slug: str) -> float:
    key  = f"{user_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_slug.get(key, 0.0)
    if now - last < ANTISPAM_SLUG_SEC:
        return int(ANTISPAM_SLUG_SEC - (now - last))
    _last_slug[key] = now
    return 0.0


def check_anim_cooldown(user_id: int, slug: str) -> float:
    """Проверяет можно ли уже запросить статичную картинку после анимации."""
    key  = f"{user_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_anim_sent.get(key, 0.0)
    if now - last < ANTISPAM_ANIM_SEC:
        return int(ANTISPAM_ANIM_SEC - (now - last))
    return 0.0


def mark_anim_sent(user_id: int, slug: str) -> None:
    _last_anim_sent[f"{user_id}:{slug.lower()}"] = time.monotonic()


def check_instr_antispam(chat_id: int) -> float:
    now  = time.monotonic()
    last = _last_instr.get(chat_id, 0.0)
    if now - last < ANTISPAM_INSTR_SEC:
        return int(ANTISPAM_INSTR_SEC - (now - last))
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
    if "model"    in label and a.model    == "—": a.model,    a.model_rarity    = value, rarity
    elif ("backdrop" in label or "background" in label) and a.backdrop == "—":
        a.backdrop, a.backdrop_rarity = value, rarity
    elif "symbol"  in label and a.symbol   == "—": a.symbol,   a.symbol_rarity   = value, rarity


async def fetch_nft_attrs(slug: str) -> NftAttrs:
    attrs = NftAttrs()
    url   = f"https://t.me/nft/{slug}"
    hdrs  = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
            if rs: rs.decompose()
            _set_attr(attrs, label, vc.get_text(strip=True), r)

        if attrs.model == "—":
            for el in soup.find_all(attrs={"data-trait": True}):
                _set_attr(attrs, str(el.get("data-trait","")),
                          str(el.get("data-value", el.get_text(strip=True))),
                          str(el.get("data-rarity","")))

        if attrs.model == "—":
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd:
                    rs = dd.find("span"); r = rs.get_text(strip=True) if rs else ""
                    if rs: rs.decompose()
                    _set_attr(attrs, dt.get_text(strip=True), dd.get_text(strip=True), r)

        if attrs.model == "—":
            meta = soup.find("meta", attrs={"property": "og:description"})
            if meta:
                content = str(meta.get("content",""))
                for sep in ("·","\n",","):
                    if sep in content:
                        parts = content.split(sep); break
                else:
                    parts = [content]
                for part in parts:
                    if ":" in part:
                        k,_,v = part.strip().partition(":")
                        _set_attr(attrs, k.strip(), v.strip(), "")

    except Exception as e:
        logger.warning("fetch_attrs | slug=%s | %s", slug, e)
    return attrs


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗКА ИЗОБРАЖЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_nft_image(slug: str) -> tuple:
    """Скачать WebP."""
    url = FRAGMENT_IMAGE_URL.format(slug=slug)
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                return (False, None, "Пустой ответ") if not data else (True, data, None)
            elif resp.status == 404:
                return False, None, None
            return False, None, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Таймаут"
    except Exception as e:
        return False, None, f"Ошибка: {e}"


async def fetch_nft_tgs(slug: str) -> tuple:
    """Скачать TGS (анимированный стикер)."""
    url = FRAGMENT_TGS_URL.format(slug=slug)
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                return (False, None, "Пустой ответ") if not data else (True, data, None)
            elif resp.status == 404:
                return False, None, None
            return False, None, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Таймаут"
    except Exception as e:
        return False, None, f"Ошибка: {e}"


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO(); img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.error("WebP→PNG: %s", e)
        return None


def tgs_to_gif(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    """
    Конвертирует TGS → GIF.
    TGS = gzip(Lottie JSON).
    Использует rlottie-python для рендера кадров.
    """
    try:
        import rlottie_python as rl
        from PIL import Image

        # Распаковываем gzip
        json_data = gzip.decompress(tgs_bytes)

        # Создаём анимацию из JSON
        anim = rl.LottieAnimation.from_data(json_data.decode("utf-8"))

        frame_count = anim.lottie_animation_get_totalframe()
        fps         = anim.lottie_animation_get_framerate()
        duration_ms = int(1000 / fps)

        frames = []
        for i in range(frame_count):
            frame_bytes = anim.lottie_animation_render(i, size, size)
            img = Image.frombytes("RGBA", (size, size), frame_bytes).convert("RGBA")
            frames.append(img)

        if not frames:
            return None

        buf = io.BytesIO()
        frames[0].save(
            buf,
            format="GIF",
            save_all=True,
            append_images=frames[1:],
            loop=0,
            duration=duration_ms,
            disposal=2,
        )
        return buf.getvalue()

    except ImportError:
        logger.error("rlottie-python не установлен: pip install rlottie-python")
        return None
    except Exception as e:
        logger.error("tgs_to_gif: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  ПОДПИСЬ НА GIF
# ══════════════════════════════════════════════════════════════════════════════

def add_caption_to_gif(
    gif_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> Optional[bytes]:
    """
    Добавляет подпись снизу GIF в стиле как на скриншоте:
    тёмный блок с названием, моделью, фоном, символом.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageSequence

        name, number = split_slug(slug)
        nice = normalize_gift_name(name)

        # Пытаемся загрузить шрифт, если нет — используем дефолтный
        def load_font(size: int):
            font_paths = [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
            ]
            for fp in font_paths:
                if os.path.exists(fp):
                    try:
                        return ImageFont.truetype(fp, size)
                    except Exception:
                        pass
            return ImageFont.load_default()

        font_title  = load_font(22)
        font_attr   = load_font(17)
        font_number = load_font(14)

        # Размеры блока подписи
        PADDING    = 14
        LINE_H     = 26
        TITLE_H    = 34
        DIVIDER_H  = 1
        LINK_H     = 28

        attrs_lines = []
        if attrs.model    != "—":
            r = f"  {attrs.model_rarity}" if attrs.model_rarity else ""
            attrs_lines.append(("🪄 Модель:", f"{attrs.model}{r}"))
        if attrs.backdrop != "—":
            r = f"  {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
            attrs_lines.append(("🎨 Фон:", f"{attrs.backdrop}{r}"))
        if attrs.symbol   != "—":
            r = f"  {attrs.symbol_rarity}" if attrs.symbol_rarity else ""
            attrs_lines.append(("✨ Символ:", f"{attrs.symbol}{r}"))

        caption_h = (PADDING + TITLE_H + DIVIDER_H +
                     len(attrs_lines) * LINE_H + DIVIDER_H + LINK_H + PADDING)

        # Цвета
        BG_COLOR     = (30, 30, 30, 230)
        TITLE_COLOR  = (255, 255, 255)
        LABEL_COLOR  = (140, 140, 255)
        VALUE_COLOR  = (220, 220, 220)
        DIVIDER_CLR  = (60, 60, 60)
        LINK_COLOR   = (100, 149, 237)

        source = Image.open(io.BytesIO(gif_bytes))
        frames_out = []
        durations  = []

        for frame in ImageSequence.Iterator(source):
            dur = frame.info.get("duration", 60)
            img = frame.convert("RGBA")
            W, H = img.size

            # Новый холст = оригинал + блок подписи
            new_h = H + caption_h
            canvas = Image.new("RGBA", (W, new_h), (20, 20, 20, 255))
            canvas.paste(img, (0, 0))

            # Фон подписи
            overlay = Image.new("RGBA", (W, caption_h), BG_COLOR)
            canvas.paste(overlay, (0, H), overlay)

            draw = ImageDraw.Draw(canvas)
            y    = H + PADDING

            # Заголовок: "🎁 Name #number"
            draw.text((PADDING, y), f"🎁 {nice}", font=font_title, fill=TITLE_COLOR)
            num_text = f"#{number}"
            try:
                tw = draw.textlength(num_text, font=font_number)
            except AttributeError:
                tw = font_number.getlength(num_text)
            draw.text((W - PADDING - tw, y + 8), num_text, font=font_number, fill=(170, 170, 170))
            y += TITLE_H

            # Разделитель
            draw.line([(PADDING, y), (W - PADDING, y)], fill=DIVIDER_CLR, width=1)
            y += DIVIDER_H + 4

            # Атрибуты
            for idx, (label, value) in enumerate(attrs_lines, 1):
                num_str = f"{idx}"
                draw.text((PADDING, y + 4), num_str, font=font_attr,
                          fill=(100, 100, 200))
                draw.text((PADDING + 18, y + 4), label, font=font_attr,
                          fill=LABEL_COLOR)
                try:
                    lw = draw.textlength(label, font=font_attr)
                except AttributeError:
                    lw = font_attr.getlength(label)
                draw.text((PADDING + 18 + lw + 6, y + 4), value, font=font_attr,
                          fill=VALUE_COLOR)
                y += LINE_H

            # Разделитель
            draw.line([(PADDING, y), (W - PADDING, y)], fill=DIVIDER_CLR, width=1)
            y += DIVIDER_H + 4

            # Ссылка
            link_text = "🔗 Открыть в Telegram"
            draw.text((PADDING, y + 4), link_text, font=font_attr, fill=LINK_COLOR)

            frames_out.append(canvas.convert("P", palette=Image.ADAPTIVE, colors=256))
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
            disposal=2,
            optimize=False,
        )
        return buf.getvalue()

    except Exception as e:
        logger.error("add_caption_to_gif: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION (entities) — для статичной картинки
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def _utf16_offset(t: str) -> int:
    return _utf16_len(t)


def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list[MessageEntity]]:
    name, number = split_slug(slug)
    nice = normalize_gift_name(name)

    r_model = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_back  = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    SEP = "━━━━━━━━━━━━━━━━━━━━"
    entities: list[MessageEntity] = []
    t = ""

    def ce(ch, eid):
        nonlocal t
        entities.append(MessageEntity(type="custom_emoji", offset=_utf16_offset(t),
                                      length=_utf16_len(ch), custom_emoji_id=eid))
        t += ch

    def bold(s):
        nonlocal t
        entities.append(MessageEntity(type="bold", offset=_utf16_offset(t), length=_utf16_len(s)))
        t += s

    def code(s):
        nonlocal t
        entities.append(MessageEntity(type="code", offset=_utf16_offset(t), length=_utf16_len(s)))
        t += s

    def link(s, url):
        nonlocal t
        entities.append(MessageEntity(type="text_link", offset=_utf16_offset(t),
                                      length=_utf16_len(s), url=url))
        t += s

    def p(s): nonlocal t; t += s

    ce("🎁", E_GIFT);  p(" "); bold(f"{nice} #{number}"); p("\n")
    code(SEP);          p("\n")
    ce("🪄", E_MODEL); p(" "); bold("Модель:");  p(f" {attrs.model}{r_model}\n")
    ce("🎨", E_BACK);  p(" "); bold("Фон:");     p(f" {attrs.backdrop}{r_back}\n")
    ce("✨", E_SYMBOL); p(" "); bold("Символ:");  p(f" {attrs.symbol}{r_sym}\n")
    code(SEP);          p("\n")
    ce("🔗", E_LINK);  p(" "); link("Открыть в Telegram", f"https://t.me/nft/{slug}")

    return t, entities


def make_keyboard_static(slug: str) -> InlineKeyboardMarkup:
    """Кнопка под статичной картинкой."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📤 Отправить без сжатия",
                             callback_data=f"{CB_NO_COMPRESS}{slug}")
    ]])


def make_keyboard_anim(slug: str) -> InlineKeyboardMarkup:
    """Кнопки под анимированной картинкой."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖼 Отправить без анимации",
                              callback_data=f"{CB_NO_ANIM}{slug}")],
        [InlineKeyboardButton(text="🎭 Отправить стикер",
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
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимированная картинка (GIF):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n"
        "<code>+а превью t.me/nft/PlushPepe-22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Статичную картинку после анимации — через <b>2 минуты</b>\n"
        "• Кнопки «Без анимации» и «Стикер» — только по 1 разу\n"
        "• Команда «превью инструкция» — раз в 5 минут\n\n"
        "<b>❓ Нужна помощь?</b>\n"
        f"Обращайся к автору: <a href='https://t.me/balfikovich'>@balfikovich</a>"
    )


def get_group_welcome(chat_title: str) -> str:
    return (
        f"👋 <b>Привет, {chat_title}!</b>\n\n"
        "Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📌 Как пользоваться:</b>\n\n"
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимированная (GIF):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Статичную после анимации — через <b>2 минуты</b>\n"
        "• Написать <code>превью инструкция</code> — полная справка\n\n"
        "⚡ Результат приходит за ~2–4 сек\n\n"
        f"<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


def get_start_text() -> str:
    return (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Показываю <b>анимированную</b> карточку любого Telegram NFT-подарка "
        "с моделью, фоном и символом.\n\n"
        "<b>📨 Как пользоваться в личке:</b>\n"
        "Просто отправь ссылку или название — получишь анимированный GIF.\n\n"
        "<b>✅ Форматы:</b>\n"
        "<code>https://t.me/nft/PlushPepe-22</code>\n"
        "<code>t.me/nft/PlushPepe-22</code>\n"
        "<code>PlushPepe-22</code>\n"
        "<code>PlushPepe 22</code>\n"
        "<code>Plush Pepe 22</code>\n\n"
        "Под GIF будет кнопка <b>«Отправить без анимации»</b> — нажми, "
        "чтобы получить статичную картинку с кнопкой без сжатия.\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>👥 В группе:</b>\n"
        "<code>превью PlushPepe 22</code> — статичная\n"
        "<code>+а превью PlushPepe 22</code> — анимированная\n"
        "<code>превью инструкция</code> — справка\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Анимация ~2–4 сек\n\n"
        f"<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ОТПРАВКА
# ══════════════════════════════════════════════════════════════════════════════

async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


async def process_slug(slug: str) -> tuple:
    (found, webp, err), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )
    return found, webp, err, attrs


async def process_slug_anim(slug: str) -> tuple:
    (img_ok, webp, img_err), (tgs_ok, tgs, tgs_err), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_tgs(slug),
        fetch_nft_attrs(slug),
    )
    return img_ok, webp, img_err, tgs_ok, tgs, tgs_err, attrs


async def send_static_photo(message: Message, png: bytes, slug: str,
                            attrs: NftAttrs) -> bool:
    caption, ents = make_caption(slug, attrs)
    kbd = make_keyboard_static(slug)
    file = BufferedInputFile(png, filename=f"{slug}.png")
    try:
        await message.answer_photo(photo=file, caption=caption,
                                   caption_entities=ents, parse_mode=None,
                                   reply_markup=kbd)
        return True
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(png, filename=f"{slug}.png")
            await message.answer_photo(photo=file, caption=caption,
                                       caption_entities=ents, parse_mode=None,
                                       reply_markup=kbd)
            return True
        except Exception: return False
    except Exception: return False


async def send_anim_gif(message: Message, gif: bytes, slug: str,
                        attrs: NftAttrs) -> bool:
    """Отправляет GIF с подписью как анимацию (document, чтобы не сжимался)."""
    kbd = make_keyboard_anim(slug)
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
        except Exception: return False
    except Exception as e:
        logger.error("send_anim_gif: %s", e)
        return False


async def send_document_only(send_fn, data: bytes, filename: str) -> None:
    file = BufferedInputFile(data, filename=filename)
    try:
        await send_fn(document=file)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(data, filename=filename)
        await send_fn(document=file)
    except Exception as e:
        logger.error("send_document_only: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  BOT & DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── Бот добавлен / удалён ────────────────────────────────────────────────────
@dp.my_chat_member()
async def on_bot_chat_member(event: ChatMemberUpdated) -> None:
    if event.chat.type not in ("group", "supergroup"):
        return
    old = event.old_chat_member.status
    new = event.new_chat_member.status
    was_out = old in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")
    now_in  = new in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, "member", "administrator")
    now_out = new in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")
    privacy = "приватный" if not getattr(event.chat, "username", None) else "публичный"

    if was_out and now_in:
        role = "администратором" if new in (ChatMemberStatus.ADMINISTRATOR, "administrator") else "участником"
        user_log.info("➕ БОТ ДОБАВЛЕН | кто=%s | чат=%s | роль=%s | тип=%s",
                      _u(event.from_user), _chat(event.chat), role, privacy)
        try:
            await bot.send_message(event.chat.id,
                                   get_group_welcome(event.chat.title or "чат"),
                                   parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error("welcome error: %s", e)
    elif now_out:
        act = "ВЫГНАН" if new in (ChatMemberStatus.KICKED, "kicked") else "УДАЛЁН"
        user_log.info("➖ БОТ %s | кто=%s | чат=%s", act, _u(event.from_user), _chat(event.chat))


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if message.chat.type != "private":
        return
    user_log.info("▶  /start | пользователь=%s", _u(message.from_user))
    buttons = []
    if BOT_USERNAME:
        buttons.append([InlineKeyboardButton(text="➕ Добавить в группу",
                                              url=f"https://t.me/{BOT_USERNAME}?startgroup")])
    buttons.append([InlineKeyboardButton(text="⭐ Поддержать автора",
                                          callback_data=CB_DONATE)])
    await message.answer(get_start_text(), parse_mode=ParseMode.HTML,
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


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


# ── Донат callback ────────────────────────────────────────────────────────────
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
        "Если бот оказался полезным — буду очень благодарен за любую поддержку! 🙏\n\n"
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
    stars = message.successful_payment.total_amount
    user  = message.from_user
    uname = f"@{user.username}" if user.username else f"без username (id={user.id})"
    user_log.info("✅ ДОНАТ | %s | %s ⭐", _u(user), stars)
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
        logger.error("Уведомление о донате админу: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK: Отправить без анимации (из анима-сообщения)
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith(CB_NO_ANIM))
async def callback_no_anim(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    slug = callback.data[len(CB_NO_ANIM):]
    mid  = callback.message.message_id
    key  = f"{mid}:{slug.lower()}"

    if key in _used_no_anim:
        await callback.answer("❌ Статичная картинка уже была отправлена!", show_alert=True)
        return

    # Проверка кулдауна 2 минуты
    wait = check_anim_cooldown(uid, slug)
    if wait > 0:
        await callback.answer(f"⏳ Подожди ещё {wait} сек.", show_alert=True)
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
            await callback.message.answer("❌ Не удалось загрузить картинку.")
            return

        png = webp_to_png(webp)
        if not png:
            await callback.message.answer("❌ Ошибка конвертации изображения.")
            return

        _used_no_anim.add(key)

        # Убираем кнопку "без анимации" — она использована
        try:
            # Оставляем только кнопку стикера если она ещё не использована
            sticker_key = f"{mid}:{slug.lower()}"
            if sticker_key not in _used_sticker:
                new_kbd = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🎭 Отправить стикер",
                                         callback_data=f"{CB_SEND_STICKER}{slug}")
                ]])
            else:
                new_kbd = None
            await callback.message.edit_reply_markup(reply_markup=new_kbd)
        except Exception:
            pass

        caption, ents = make_caption(slug, attrs)
        kbd = make_keyboard_static(slug)
        file = BufferedInputFile(png, filename=f"{slug}.png")
        await callback.message.answer_photo(photo=file, caption=caption,
                                            caption_entities=ents, parse_mode=None,
                                            reply_markup=kbd)
        user_log.info("🖼 БЕЗ АНИМАЦИИ — ОТПРАВЛЕНО | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK: Отправить стикер (TGS)
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
            await callback.message.answer("❌ Не удалось загрузить стикер.")
            return

        _used_sticker.add(key)

        # Убираем кнопку стикера
        try:
            noanim_key = f"{mid}:{slug.lower()}"
            if noanim_key not in _used_no_anim:
                new_kbd = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🖼 Отправить без анимации",
                                         callback_data=f"{CB_NO_ANIM}{slug}")
                ]])
            else:
                new_kbd = None
            await callback.message.edit_reply_markup(reply_markup=new_kbd)
        except Exception:
            pass

        file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
        await callback.message.answer_sticker(sticker=file)
        user_log.info("🎭 СТИКЕР ОТПРАВЛЕН | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK: Отправить без сжатия (для статичной картинки)
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
        await callback.answer("❌ Оригинал уже был отправлен для этого превью!", show_alert=True)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю оригинал…")

    try:
        found, webp, err = await fetch_nft_image(slug)
        if err or not found:
            await callback.message.answer("❌ Не удалось загрузить." if err else "❌ Не найден.")
            return
        _used_no_compress.add(key)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await send_document_only(callback.message.answer_document, webp, f"{slug}.webp")
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
                await message.answer("⚠️ Минимум — <b>1 звезда</b>.", parse_mode=ParseMode.HTML)
                return
            if amount > 2500:
                await message.answer("⚠️ Максимум — <b>2500 звёзд</b> за раз.", parse_mode=ParseMode.HTML)
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
                await message.answer("❌ Не удалось создать счёт. Попробуй ещё раз.")
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

        # Команда "превью инструкция"
        if lower.strip() in ("превью инструкция", "preview инструкция",
                              "превью instruction", "preview instruction"):
            wait = check_instr_antispam(message.chat.id)
            if wait > 0:
                user_log.info("🚫 ИНСТРУКЦИЯ — УДАЛЕНО (спам) | %s | чат=%s",
                              _u(message.from_user), _chat(message.chat))
                await safe_delete(message)
                return
            user_log.info("📖 ИНСТРУКЦИЯ | %s | чат=%s",
                          _u(message.from_user), _chat(message.chat))
            await message.answer(get_group_instruction(), parse_mode=ParseMode.HTML)
            return

        # Анимированная: "+а превью ..."
        if lower.startswith("+а превью") or lower.startswith("+а preview"):
            for prefix in ("+а превью", "+а preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_anim(message, raw, is_private=False)
            return

        # Статичная: "превью ..." или "preview ..."
        if lower.startswith("превью") or lower.startswith("preview"):
            for prefix in ("превью", "preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_static(message, raw, is_private=False)
            return

        # Не наше сообщение
        return

    # ── ЛИЧКА: всегда анимированная ──────────────────────────────────────────
    await _handle_anim(message, raw, is_private=True)


# ── Обработка статичной картинки ─────────────────────────────────────────────
async def _handle_static(message: Message, raw: str, is_private: bool) -> None:
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
    wm = await message.answer(f"🔍 Загружаю <b>{slug}</b>…", parse_mode=ParseMode.HTML)
    found, webp, err, attrs = await process_slug(slug)
    await safe_delete(wm)

    if err:
        user_log.warning("⚠️ ОШИБКА (статик) | slug=%s | %s", slug, err)
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n<code>{slug}</code>\n<i>{err}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        user_log.info("❌ НЕ НАЙДЕН (статик) | slug=%s | %s", slug, _u(message.from_user))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            f"<b>Подарок не найден</b>\n<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"<code>{slug}</code>\n\n<b>Причины:</b>\n"
            "• Не существует\n• Сожжён 🔥\n• Опечатка",
            parse_mode=ParseMode.HTML,
        )
        return

    png = webp_to_png(webp)
    elapsed = round(time.monotonic() - t0, 2)
    user_log.info("✅ СТАТИК ОТПРАВЛЕНО | slug=%s | %s | %.2fс", slug, _u(message.from_user), elapsed)

    if png:
        ok = await send_static_photo(message, png, slug, attrs)
        if not ok:
            await send_document_only(message.answer_document, webp, f"{slug}.webp")
    else:
        await send_document_only(message.answer_document, webp, f"{slug}.webp")


# ── Обработка анимированного превью ──────────────────────────────────────────
async def _handle_anim(message: Message, raw: str, is_private: bool) -> None:
    slug = extract_nft_slug(raw)
    uid  = message.from_user.id

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (аним) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        hint = (
            "<code>PlushPepe 22</code>\n"
            "<code>t.me/nft/PlushPepe-22</code>"
        )
        if is_private:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
                f"<b>Примеры:</b>\n{hint}",
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
    wm = await message.answer(f"🎬 Загружаю анимацию <b>{slug}</b>…", parse_mode=ParseMode.HTML)

    img_ok, webp, img_err, tgs_ok, tgs_data, tgs_err, attrs = await process_slug_anim(slug)
    await safe_delete(wm)

    # Нет ни картинки ни TGS
    if (img_err and tgs_err) or (not img_ok and not tgs_ok):
        err = tgs_err or img_err
        user_log.warning("⚠️ ОШИБКА ЗАГРУЗКИ (аним) | slug=%s | %s", slug, err)
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n<code>{slug}</code>\n<i>{err or 'не найден'}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not img_ok and not tgs_ok:
        user_log.info("❌ НЕ НАЙДЕН (аним) | slug=%s | %s", slug, _u(message.from_user))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            "<b>Подарок не найден</b>\n\n"
            f"<code>{slug}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    elapsed = round(time.monotonic() - t0, 2)

    # Есть TGS — конвертируем в GIF и добавляем подпись
    if tgs_ok and tgs_data:
        wm2 = await message.answer("⚙️ Конвертирую в GIF…")
        gif_raw = await asyncio.to_thread(tgs_to_gif, tgs_data)
        if gif_raw:
            gif_captioned = await asyncio.to_thread(add_caption_to_gif, gif_raw, slug, attrs)
            await safe_delete(wm2)
            gif_final = gif_captioned or gif_raw
            ok = await send_anim_gif(message, gif_final, slug, attrs)
            if ok:
                mark_anim_sent(uid, slug)
                user_log.info("✅ АНИМ ОТПРАВЛЕНО | slug=%s | %s | %.2fс",
                              slug, _u(message.from_user), elapsed)
                return
        else:
            await safe_delete(wm2)
            user_log.warning("⚠️ tgs_to_gif упал, откат на картинку | slug=%s", slug)

    # Откат на статичную картинку если GIF не получился
    if img_ok and webp:
        png = webp_to_png(webp)
        if png:
            ok = await send_static_photo(message, png, slug, attrs)
            if ok:
                user_log.info("🖼 ОТКАТ НА СТАТИК | slug=%s | %s", slug, _u(message.from_user))
                return
        await send_document_only(message.answer_document, webp, f"{slug}.webp")
    else:
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            "Не удалось создать анимацию и нет статичной картинки.",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  INLINE
# ══════════════════════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_handler(query: InlineQuery) -> None:
    raw = (query.query or "").strip()
    if not raw:
        hint = InlineQueryResultArticle(
            id="hint", title="🎁 NFT Gift Viewer",
            description="Введите: PlushPepe-22 / Plush Pepe 22",
            thumbnail_url="https://nft.fragment.com/gift/PlushPepe-1.webp",
            input_message_content=InputTextMessageContent(
                message_text="<b>NFT Gift Viewer</b>\n\n<code>t.me/nft/PlushPepe-22</code>",
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[hint], cache_time=60, is_personal=False)
        return

    slug = extract_nft_slug(raw)
    if not slug:
        err = InlineQueryResultArticle(
            id="err", title="❌ Неверный формат",
            description="Пример: PlushPepe-22 / Plush Pepe 22",
            input_message_content=InputTextMessageContent(
                message_text="<b>Неверный формат</b>\n\n<code>PlushPepe-22</code>",
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[err], cache_time=5, is_personal=True)
        return

    user_log.info("🔍 INLINE | slug=%s | %s", slug, _u(query.from_user))
    found, webp, err, attrs = await process_slug(slug)
    name, number = split_slug(slug)
    nice  = normalize_gift_name(name)
    title = f"🎁 {nice} #{number}"

    if err or not found:
        nf = InlineQueryResultArticle(
            id=f"nf_{slug}", title=title,
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
        InlineKeyboardButton(text="🔗 Открыть в Telegram",
                             url=f"https://t.me/nft/{slug}")
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
        caption=caption, caption_entities=ents, parse_mode=None,
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
    logger.info("   Лог-файл : %s", os.path.abspath(LOG_FILE))
    logger.info("   Донаты   → admin_id=%s", ADMIN_ID)
    logger.info("━" * 60)
    logger.info("ЧЕКЛИСТ:")
    logger.info("  1. pip install aiogram aiohttp python-dotenv pillow rlottie-python")
    logger.info("  2. @BotFather → /setprivacy → @%s → Disable", me.username)
    logger.info("  3. @BotFather → /setjoingroups → @%s → Enable", me.username)
    logger.info("  4. @BotFather → /setinline → @%s → placeholder", me.username)
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
            "message", "callback_query", "inline_query",
            "my_chat_member", "pre_checkout_query",
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())
