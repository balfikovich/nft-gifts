"""
NFT Gift Viewer Bot
===================
Зависимости:
    pip install aiogram aiohttp python-dotenv pillow beautifulsoup4 lxml
    apt install ffmpeg   (или: pip install imageio[ffmpeg])

Переменные окружения (.env):
    BOT_TOKEN=xxx
    LOG_FILE=bot.log   (опционально)

Логика:
    ЛИЧКА        → TGS → MP4 видео с подписью (caption entities) + кнопки «Без анимации» / «Стикер»
    ГРУППА       → «превью ...»    → PNG фото с caption + кнопка «Без сжатия» (PNG документ)
    ГРУППА       → «+а превью ...» → MP4 видео с подписью + кнопки «Без анимации» / «Стикер»
"""

import asyncio
import io
import logging
import os
import re
import subprocess
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

FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
FRAGMENT_TGS_URL   = "https://nft.fragment.com/gift/{slug}.tgs"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=30)

# callback_data префиксы — важно: не должны быть префиксом друг друга
CB_NO_COMPRESS       = "nc:"   # nc:slug  → PNG документ (без сжатия) под статичной фоткой
CB_NO_ANIM           = "na:"   # na:slug  → PNG документ под MP4
CB_SEND_STICKER      = "sk:"   # sk:slug  → TGS стикер
CB_NO_COMPRESS_VIDEO = "ncv:"  # ncv:slug → MP4 документом (без сжатия Telegram)
CB_SEND_GIF          = "gif:"  # gif:slug → GIF анимация
CB_DONATE            = "donate"

ANTISPAM_SECONDS  = 1.5
ANTISPAM_SLUG_SEC = 120   # 2 мин — повтор одного подарка в группе

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

_last_request:     dict[int, float] = {}
_last_slug:        dict[str, float] = {}
_cb_lock:          dict[int, bool]  = {}
_used_no_compress: set[str]         = set()  # "msg_id:slug" — под статичной фоткой
_used_no_anim:     set[str]         = set()  # "msg_id:slug" — под MP4
_used_sticker:           set[str]   = set()  # "msg_id:slug" — под MP4
_used_no_compress_video:  set[str]   = set()  # "msg_id:slug" — под MP4
_used_gif:                set[str]   = set()  # "msg_id:slug" — под MP4
_awaiting_donate:  set[int]         = set()
_last_instr:       dict[int, float] = {}   # chat_id → timestamp
_last_button:      dict[str, float]  = {}   # "user_id:cb_prefix" → timestamp
ANTISPAM_INSTR_SEC  = 300  # 5 мин
ANTISPAM_BUTTON_SEC = 90.0  # 90 сек между нажатиями одной кнопки

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
    """Антиспам по slug привязан к чату — в разных чатах независимо."""
    key  = f"{chat_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_slug.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_SLUG_SEC:
        return int(ANTISPAM_SLUG_SEC - diff)
    _last_slug[key] = now
    return 0.0


def check_instr_antispam(chat_id: int) -> float:
    """Антиспам для «превью инструкция». 0 = можно."""
    now  = time.monotonic()
    last = _last_instr.get(chat_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_INSTR_SEC:
        return int(ANTISPAM_INSTR_SEC - diff)
    _last_instr[chat_id] = now
    return 0.0


def check_button_antispam(user_id: int, prefix: str) -> float:
    """Антиспам 1.5 сек на каждую кнопку под превью. 0 = можно."""
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

        if attrs.model == "—":
            for line in soup.get_text(separator="\n").splitlines():
                if ":" in line:
                    k, _, v = line.strip().partition(":")
                    if k.strip().lower() in ("model", "backdrop", "background", "symbol") and v.strip():
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
#  TGS → MP4 через ffmpeg
#  Конвертируем: TGS (gzip-сжатый Lottie JSON) → PNG-кадры через rlottie →
#  MP4 через ffmpeg (libx264, без звука, совместимо с Telegram)
# ══════════════════════════════════════════════════════════════════════════════

def _check_ffmpeg() -> bool:
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def tgs_to_mp4(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    """
    TGS → MP4 максимальное качество + максимальная скорость:
    - Рендер 512×512 (нативный размер TGS, апскейл не даёт реального качества)
    - Белый фон (нативный для Telegram NFT подарков)
    - libx264 CRF=0 (lossless), preset=fast, threads=0 (все ядра)
    - yuv420p для совместимости с Telegram
    Требует: rlottie-python + ffmpeg в PATH.
    """
    try:
        from rlottie_python import LottieAnimation
        from PIL import Image
    except ImportError as e:
        logger.error("tgs_to_mp4: не хватает библиотеки: %s", e)
        return None

    render_size = size  # 512×512 — нативный размер TGS

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
            logger.error("tgs_to_mp4: 0 кадров")
            return None

        fps = max(fps, 1)

        frames_dir = os.path.join(tmp_dir, "frames")
        os.makedirs(frames_dir, exist_ok=True)

        for i in range(frame_count):
            frame_img = anim.render_pillow_frame(frame_num=i)
            if frame_img is None:
                continue

            # Апскейл до 1024×1024 с LANCZOS
            if frame_img.size != (render_size, render_size):
                frame_img = frame_img.resize((render_size, render_size), Image.LANCZOS)  # масштаб только если нужно

            # Белый фон — нативный для Telegram NFT подарков
            bg = Image.new("RGB", (render_size, render_size), (255, 255, 255))
            if frame_img.mode == "RGBA":
                bg.paste(frame_img, mask=frame_img.split()[3])
            else:
                bg.paste(frame_img)

            # Сохраняем PNG без сжатия для максимального качества передачи в ffmpeg
            bg.save(os.path.join(frames_dir, f"frame_{i:05d}.png"),
                    format="PNG", compress_level=0)

        # ffmpeg: PNG → MP4, максимальное качество + скорость
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()

        cmd = [
            "ffmpeg", "-y",
            "-threads", str(cpu_count),   # все ядра сервера
            "-framerate", str(fps),
            "-i", os.path.join(frames_dir, "frame_%05d.png"),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",        # совместимость с Telegram
            "-crf", "0",                  # lossless — нулевые потери качества
            "-preset", "fast",            # быстро + lossless = идеально
            "-tune", "animation",         # оптимизация под анимацию
            "-x264-params", "ref=1:me=dia:subme=1:trellis=0:weightp=0",  # ускорение энкодера
            "-movflags", "+faststart",    # быстрый старт воспроизведения
            mp4_path,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0:
            logger.error("ffmpeg error: %s", result.stderr.decode(errors="replace")[-500:])
            return None

        with open(mp4_path, "rb") as f:
            mp4_data = f.read()

        logger.info("tgs_to_mp4 OK: %d кадров, %.1f fps, %d байт",
                    frame_count, fps, len(mp4_data))
        return mp4_data

    except subprocess.TimeoutExpired:
        logger.error("tgs_to_mp4: ffmpeg timeout")
        return None
    except Exception as e:
        logger.error("tgs_to_mp4 error: %s", e, exc_info=True)
        return None
    finally:
        # Чистим временные файлы
        import shutil
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass



def tgs_to_gif(tgs_bytes: bytes, size: int = 512) -> Optional[bytes]:
    """
    TGS → GIF.
    Требует: rlottie-python + Pillow.
    """
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
            logger.error("tgs_to_gif: 0 кадров")
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
            # RGBA → RGB с чёрным фоном
            bg = Image.new("RGB", (size, size), (0, 0, 0))
            bg.paste(frame_img, mask=frame_img.split()[3] if frame_img.mode == "RGBA" else None)
            frames.append(bg)

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
            optimize=False,
        )
        result = buf.getvalue()
        logger.info("tgs_to_gif OK: %d кадров, %d байт", len(frames), len(result))
        return result

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
#  CAPTION (MessageEntity) — для фото и подписи под видео
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
    """Под статичной PNG — только кнопка «Без сжатия» (шлёт PNG документом)."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📤 Отправить без сжатия",
                             callback_data=f"{CB_NO_COMPRESS}{slug}")
    ]])


def make_keyboard_video(slug: str) -> InlineKeyboardMarkup:
    """Под MP4 видео — 3 кнопки."""
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
        "🖼 <b>Статичная картинка:</b>\n"
        "<code>превью PlushPepe 22</code>\n"
        "<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        "🎬 <b>Анимированная (MP4):</b>\n"
        "<code>+а превью PlushPepe 22</code>\n"
        "<code>+а превью t.me/nft/PlushPepe-22</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "<b>📋 Правила:</b>\n"
        "• Один подарок — не чаще <b>1 раза в 5 минут</b>\n"
        "• Кнопки под превью — только 1 раз каждая\n\n"
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
        "<code>t.me/nft/PlushPepe-22</code>\n"
        "<code>PlushPepe-22</code>\n"
        "<code>PlushPepe 22</code>\n"
        "<code>Plush Pepe 22</code>\n\n"
        "Под видео — кнопки <b>«Без анимации»</b> (PNG) и <b>«Стикер»</b> (TGS).\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "<b>👥 В группе:</b>\n"
        "🖼 <b>Статичная:</b> <code>превью PlushPepe 22</code>\n"
        "🎬 <b>Анимация:</b>  <code>+а превью PlushPepe 22</code>\n\n"
        "<b>📋 Правила в группе:</b>\n"
        "• Повтор одного подарка — не чаще <b>1 раза в 5 минут</b>\n"
        "• Кнопки под превью — только 1 раз каждая\n\n"
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
    """Убирает кнопки с указанным префиксом из клавиатуры сообщения."""
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
    """Отправляет PNG фото с caption + кнопка «Без сжатия»."""
    caption, ents = make_caption(slug, attrs)
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
                     slug: str, attrs: NftAttrs) -> bool:
    """
    Отправляет MP4 как video (answer_video) с caption + кнопки «Без анимации» и «Стикер».
    Telegram показывает видео прямо в чате с возможностью воспроизведения.
    """
    caption, ents = make_caption(slug, attrs)
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

# ── «Без сжатия (видео)» под MP4 — шлёт MP4 документом ──────────────────────
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
        mins = int(wait) // 60
        secs = int(wait) % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю оригинал…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(
                f"❌ Не удалось загрузить: {err or 'файл не найден'}"
            )
            return

        # Конвертируем TGS → MP4 с максимальным качеством (CRF=18)
        wm = await callback.message.answer("⚙️ Конвертирую в видео без сжатия…")
        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None
        await safe_delete(wm)

        if not mp4_data:
            await callback.message.answer("❌ Не удалось конвертировать видео.")
            return

        _used_no_compress_video.add(key)
        await remove_keyboard_button(callback.message, CB_NO_COMPRESS_VIDEO)

        # Отправляем MP4 как документ — Telegram не будет его перекодировать
        await send_document(callback.message.answer_document, mp4_data, f"{slug}.mp4")

        user_log.info("📤 БЕЗ СЖАТИЯ ВИДЕО | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ── «Отправить как GIF» под MP4 ─────────────────────────────────────────────
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
        mins = int(wait) // 60
        secs = int(wait) % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(
                f"❌ Не удалось загрузить: {err or 'файл не найден'}"
            )
            return

        wm = await callback.message.answer("⚙️ Конвертирую…")
        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None
        await safe_delete(wm)

        if not mp4_data:
            await callback.message.answer("❌ Не удалось конвертировать.")
            return

        _used_gif.add(key)
        await remove_keyboard_button(callback.message, CB_SEND_GIF)

        # Шлём MP4 через answer_animation — Telegram сам конвертирует в GIF
        # на своей стороне с максимальным качеством
        file = BufferedInputFile(mp4_data, filename=f"{slug}.mp4")
        await callback.message.answer_animation(animation=file)

        user_log.info("🎞 GIF (MP4→Telegram) | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False

# ── «Без сжатия» под статичной фоткой (группа) — шлёт PNG документом ─────────
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
        mins = int(wait) // 60
        secs = int(wait) % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=False)
        return

    if key in _used_no_compress:
        await callback.answer("❌ Оригинал уже был отправлен!", show_alert=True)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю оригинал…")

    try:
        found, webp, err = await fetch_nft_image(slug)
        if err or not found:
            await callback.message.answer(
                f"❌ Не удалось загрузить: {err or 'подарок не найден'}"
            )
            return

        png = webp_to_png(webp)
        if not png:
            # Откат: шлём WebP документом
            await send_document(callback.message.answer_document, webp, f"{slug}.webp")
            return

        _used_no_compress.add(key)
        # Убираем кнопку
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Отправляем PNG как документ (без сжатия Telegram)
        await send_document(callback.message.answer_document, png, f"{slug}.png")

        user_log.info("📤 БЕЗ СЖАТИЯ (PNG) | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ── «Без анимации» под MP4 — шлёт PNG фото со стандартной подписью ───────────
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
        mins = int(wait) // 60
        secs = int(wait) % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=False)
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
            await callback.message.answer(
                f"❌ Не удалось загрузить: {err or 'подарок не найден'}"
            )
            return

        png = webp_to_png(webp)
        if not png:
            await send_document(callback.message.answer_document, webp, f"{slug}.webp")
            return

        _used_no_anim.add(key)
        await remove_keyboard_button(callback.message, CB_NO_ANIM)

        # Отправляем PNG как фото с подписью и кнопкой «Без сжатия»
        ok = await send_static_photo(callback.message, png, slug, attrs)
        if not ok:
            await send_document(callback.message.answer_document, png, f"{slug}.png")

        user_log.info("🖼 БЕЗ АНИМАЦИИ | slug=%s | %s", slug, _u(callback.from_user))
    finally:
        _cb_lock[uid] = False


# ── «Стикер» под MP4 — шлёт оригинальный TGS ────────────────────────────────
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
        mins = int(wait) // 60
        secs = int(wait) % 60
        ts = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.answer(f"⏳ Подожди ещё {ts}", show_alert=False)
        return

    if _cb_lock.get(uid):
        await callback.answer("⏳ Идёт загрузка…", show_alert=False)
        return

    _cb_lock[uid] = True
    await callback.answer("⏳ Загружаю стикер…")

    try:
        found, tgs_data, err = await fetch_nft_tgs(slug)
        if err or not found:
            await callback.message.answer(
                f"❌ Не удалось загрузить стикер: {err or 'не найден'}"
            )
            return

        _used_sticker.add(key)
        await remove_keyboard_button(callback.message, CB_SEND_STICKER)

        # TGS нельзя отправить через answer_sticker с BufferedInputFile —
        # Telegram принимает только file_id или URL для стикеров.
        # Отправляем как документ с расширением .tgs — Telegram сам откроет его как анимированный стикер.
        file = BufferedInputFile(tgs_data, filename=f"{slug}.tgs")
        await callback.message.answer_document(document=file)

        user_log.info("🎭 СТИКЕР (документ) | slug=%s | %s", slug, _u(callback.from_user))
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
                    currency="XTR",
                    prices=[LabeledPrice(label="Звёзды", amount=amount)],
                    provider_token="",
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

        # «превью инструкция» — показать инструкцию
        if lower.strip() in ("превью инструкция", "preview инструкция",
                             "превью instruction", "preview instruction"):
            wait = check_instr_antispam(message.chat.id)
            if wait > 0:
                user_log.info("🚫 ИНСТРУКЦИЯ СПАМ | %s | %s",
                              _u(message.from_user), _chat(message.chat))
                await safe_delete(message)
                return
            user_log.info("📖 ИНСТРУКЦИЯ | %s | %s",
                          _u(message.from_user), _chat(message.chat))
            await message.answer(get_group_instruction(), parse_mode=ParseMode.HTML)
            return

        # «+а превью ...» — анимированная (MP4)
        if lower.startswith("+а превью") or lower.startswith("+а preview"):
            for prefix in ("+а превью", "+а preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_group_video(message, raw)
            return

        # «превью ...» — статичная PNG
        if lower.startswith("превью") or lower.startswith("preview"):
            for prefix in ("превью", "preview"):
                if lower.startswith(prefix):
                    raw = raw[len(prefix):].strip()
                    break
            await _handle_group_static(message, raw)
            return

        # Не наше сообщение — игнорируем
        return

    # ── ЛИЧКА: всегда MP4 видео → при ошибке откат на PNG ────────────────────
    await _handle_private_video(message, raw)


# ── Статичная PNG (группа) ────────────────────────────────────────────────────
async def _handle_group_static(message: Message, raw: str) -> None:
    uid  = message.from_user.id
    slug = extract_nft_slug(raw)

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (группа) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>превью PlushPepe 22</code>\n"
            "<code>превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🖼 ЗАПРОС (группа) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    # Антиспам по slug привязан к чату (не к пользователю)
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
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )
    elapsed = round(time.monotonic() - t0, 2)
    await safe_delete(wm)

    if err:
        user_log.warning("⚠️ ОШИБКА (группа) | slug=%s | %s | %.2fс", slug, err, elapsed)
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        user_log.info("❌ НЕ НАЙДЕН (группа) | slug=%s | %s", slug, _u(message.from_user))
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
        ok = await send_static_photo(message, png, slug, attrs)
        if not ok:
            logger.warning("send_static_photo упал → документ | slug=%s", slug)
            await send_document(message.answer_document, webp, f"{slug}.webp")
    else:
        await send_document(message.answer_document, webp, f"{slug}.webp")


# ── Анимированная MP4 (группа) ───────────────────────────────────────────────
async def _handle_group_video(message: Message, raw: str) -> None:
    uid  = message.from_user.id
    slug = extract_nft_slug(raw)

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (группа аним) | %s | %s",
                      _u(message.from_user), _chat(message.chat))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>+а превью PlushPepe 22</code>\n"
            "<code>+а превью t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎬 ЗАПРОС (группа аним) | slug=%s | %s | %s",
                  slug, _u(message.from_user), _chat(message.chat))

    # Антиспам по slug привязан к чату (не к пользователю)
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
        fetch_nft_image(slug),
        fetch_nft_tgs(slug),
        fetch_nft_attrs(slug),
    )

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
        await safe_delete(wm)
        wm = await message.answer("⚙️ Конвертирую в видео…")
        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None
            logger.error("tgs_to_mp4 timeout (группа) | slug=%s", slug)

    await safe_delete(wm)
    elapsed = round(time.monotonic() - t0, 2)

    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs)
        if ok:
            user_log.info("✅ MP4 (группа) | slug=%s | %s | %.2fс",
                          slug, _u(message.from_user), elapsed)
            return
        logger.warning("send_video упал → откат на PNG (группа) | slug=%s", slug)

    # Откат на статичную PNG
    user_log.info("🖼 ОТКАТ НА PNG (группа) | slug=%s | %s", slug, _u(message.from_user))
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
            "Не удалось создать видео и загрузить картинку.",
            parse_mode=ParseMode.HTML,
        )


# ── MP4 видео (личка) ─────────────────────────────────────────────────────────
async def _handle_private_video(message: Message, raw: str) -> None:
    uid  = message.from_user.id
    slug = extract_nft_slug(raw)

    if not slug:
        user_log.info("❓ НЕВЕРНЫЙ ФОРМАТ (личка) | %s", _u(message.from_user))
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> <b>Неверный формат.</b>\n\n'
            "<b>Примеры:</b>\n"
            "<code>PlushPepe 22</code>\n"
            "<code>t.me/nft/PlushPepe-22</code>\n"
            "<code>https://t.me/nft/PlushPepe-22</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Лёгкий антиспам для лички
    wait = check_antispam(uid)
    if wait > 0:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Слишком быстро!</b> Подожди <code>{wait}</code> сек.",
            parse_mode=ParseMode.HTML,
        )
        return

    user_log.info("🎬 ЗАПРОС (личка) | slug=%s | %s", slug, _u(message.from_user))

    t0 = time.monotonic()
    wm = await message.answer("🔍 Загружаю данные…")

    # Параллельно скачиваем WebP, TGS и атрибуты
    (img_ok, webp, img_err), (tgs_ok, tgs_data, tgs_err), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_tgs(slug),
        fetch_nft_attrs(slug),
    )

    # Ничего не нашли — подарок не существует
    if not img_ok and not tgs_ok:
        await safe_delete(wm)
        err = tgs_err or img_err
        if err:
            user_log.warning("⚠️ ОШИБКА (личка) | slug=%s | %s", slug, err)
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Ошибка загрузки</b>\n<code>{slug}</code>\n<i>{err}</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            user_log.info("❌ НЕ НАЙДЕН (личка) | slug=%s | %s", slug, _u(message.from_user))
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

    # Есть TGS — конвертируем в MP4
    mp4_data: Optional[bytes] = None
    if tgs_ok and tgs_data:
        await safe_delete(wm)
        wm = await message.answer("⚙️ Конвертирую в видео…")

        try:
            mp4_data = await asyncio.wait_for(
                asyncio.to_thread(tgs_to_mp4, tgs_data),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            mp4_data = None
            logger.error("tgs_to_mp4 timeout | slug=%s", slug)

    await safe_delete(wm)
    elapsed = round(time.monotonic() - t0, 2)

    # Отправляем MP4 с подписью и кнопками
    if mp4_data:
        ok = await send_video(message, mp4_data, slug, attrs)
        if ok:
            user_log.info("✅ MP4 ОТПРАВЛЕНО | slug=%s | %s | %.2fс",
                          slug, _u(message.from_user), elapsed)
            return
        logger.warning("send_video упал → откат на PNG | slug=%s", slug)

    # ── Откат на статичную PNG ─────────────────────────────────────────────────
    user_log.info("🖼 ОТКАТ НА PNG | slug=%s | %s", slug, _u(message.from_user))
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

    # Проверяем ffmpeg
    if _check_ffmpeg():
        logger.info("   ✅ ffmpeg найден — конвертация TGS→MP4 работает")
    else:
        logger.warning("   ❌ ffmpeg НЕ найден!")
        logger.warning("      Установи: apt install ffmpeg  или  brew install ffmpeg")
        logger.warning("      Без ffmpeg будет откат на статичную PNG!")

    # Проверяем rlottie-python
    try:
        from rlottie_python import LottieAnimation  # noqa: F401
        logger.info("   ✅ rlottie-python установлен")
    except ImportError:
        logger.warning("   ❌ rlottie-python НЕ установлен!")
        logger.warning("      Установи: pip install rlottie-python")

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
