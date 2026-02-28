import asyncio
import io
import logging
import os
import re
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

# ── ID администратора — сюда приходят уведомления о донатах ──────────────────
ADMIN_ID = 5479063264

# ══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════

LOG_FILE = os.environ.get("LOG_FILE", "bot.log")

_fmt = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)
_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_console_handler, _file_handler])

logger   = logging.getLogger(__name__)       # технические события
user_log = logging.getLogger("user_events")  # действия пользователей


def _u(user) -> str:
    """Имя + @username + id пользователя."""
    if user is None:
        return "unknown"
    name  = (user.full_name or "").strip() or "NoName"
    uname = f" @{user.username}" if user.username else " (без username)"
    return f"{name}{uname} (id={user.id})"


def _chat(chat) -> str:
    """Название + тип + id чата."""
    if chat is None:
        return "unknown"
    if chat.type == "private":
        return f"личка [{chat.type}] (id={chat.id})"
    title = (getattr(chat, "title", None) or "NoTitle").strip()
    return f'"{title}" [{chat.type}] (id={chat.id})'


# ── Константы ─────────────────────────────────────────────────────────────────
FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)
CB_NO_COMPRESS     = "nocompress:"
CB_DONATE          = "donate_start"
AUTHOR             = "@balfikovich"
ANTISPAM_SECONDS   = 1.5
ANTISPAM_SLUG_SEC  = 300

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

# ── Антиспам ──────────────────────────────────────────────────────────────────
_last_request: dict[int, float] = {}
_last_slug: dict[str, float]    = {}
_cb_lock: dict[int, bool]       = {}
_used_no_compress: set[str]     = set()   # "message_id:slug"

# ── Состояние ожидания суммы доната ──────────────────────────────────────────
_awaiting_donate_amount: set[int] = set()

# ── Имя бота ─────────────────────────────────────────────────────────────────
BOT_USERNAME: str = ""


def check_antispam(user_id: int) -> float:
    """Возвращает 0 если запрос пропускается, иначе — сколько секунд ждать."""
    now  = time.monotonic()
    last = _last_request.get(user_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_SECONDS:
        return round(ANTISPAM_SECONDS - diff, 1)
    _last_request[user_id] = now
    return 0.0


def check_slug_antispam(user_id: int, slug: str) -> float:
    key  = f"{user_id}:{slug.lower()}"
    now  = time.monotonic()
    last = _last_slug.get(key, 0.0)
    diff = now - last
    if diff < ANTISPAM_SLUG_SEC:
        return int(ANTISPAM_SLUG_SEC - diff)
    _last_slug[key] = now
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
        self.model           = "—"
        self.model_rarity    = ""
        self.backdrop        = "—"
        self.backdrop_rarity = ""
        self.symbol          = "—"
        self.symbol_rarity   = ""


def _set_attr(attrs: NftAttrs, label: str, value: str, rarity: str) -> None:
    label = label.lower().strip()
    if not value or value == "—":
        return
    if "model" in label and attrs.model == "—":
        attrs.model, attrs.model_rarity = value, rarity
    elif ("backdrop" in label or "background" in label) and attrs.backdrop == "—":
        attrs.backdrop, attrs.backdrop_rarity = value, rarity
    elif "symbol" in label and attrs.symbol == "—":
        attrs.symbol, attrs.symbol_rarity = value, rarity


async def fetch_nft_attrs(slug: str) -> NftAttrs:
    attrs   = NftAttrs()
    url     = f"https://t.me/nft/{slug}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        async with get_session().get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.warning("fetch_attrs | slug=%s | HTTP %s", slug, resp.status)
                return attrs
            html = await resp.text()

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 не установлен — pip install beautifulsoup4 lxml")
            return attrs

        soup = BeautifulSoup(html, "lxml")

        for row in soup.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label       = cells[0].get_text(strip=True).lower()
            value_cell  = cells[1]
            rarity_span = value_cell.find("span")
            rarity = rarity_span.get_text(strip=True) if rarity_span else ""
            if rarity_span:
                rarity_span.decompose()
            value = value_cell.get_text(strip=True)
            _set_attr(attrs, label, value, rarity)

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
        logger.warning("fetch_attrs | slug=%s | error=%s", slug, e)
    return attrs


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗКА ИЗОБРАЖЕНИЯ
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_nft_image(slug: str) -> tuple:
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
        return False, None, "Таймаут (20 сек)"
    except aiohttp.ClientConnectionError:
        return False, None, "Ошибка соединения"
    except Exception as e:
        return False, None, f"Ошибка: {e}"


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.error("WebP→PNG | error=%s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def _utf16_offset(text_so_far: str) -> int:
    return _utf16_len(text_so_far)


def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list[MessageEntity]]:
    name, number = split_slug(slug)
    nice = readable_name(name)

    r_model = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_back  = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    SEP = "━━━━━━━━━━━━━━━━━━━━"
    entities: list[MessageEntity] = []
    t = ""

    def ce(emoji_char: str, emoji_id: str) -> None:
        nonlocal t
        entities.append(MessageEntity(
            type="custom_emoji",
            offset=_utf16_offset(t),
            length=_utf16_len(emoji_char),
            custom_emoji_id=emoji_id,
        ))
        t += emoji_char

    def bold(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="bold", offset=_utf16_offset(t), length=_utf16_len(s)))
        t += s

    def code(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="code", offset=_utf16_offset(t), length=_utf16_len(s)))
        t += s

    def link(s: str, url: str) -> None:
        nonlocal t
        entities.append(MessageEntity(type="text_link", offset=_utf16_offset(t), length=_utf16_len(s), url=url))
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
    ce("🔗", E_LINK);  p(" "); link("Открыть в Telegram", f"https://t.me/nft/{slug}")

    return t, entities


def make_keyboard(slug: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📤 Отправить без сжатия",
            callback_data=f"{CB_NO_COMPRESS}{slug}",
        )
    ]])


# ══════════════════════════════════════════════════════════════════════════════
#  ТЕКСТЫ
# ══════════════════════════════════════════════════════════════════════════════

def get_group_welcome(chat_title: str) -> str:
    return (
        f"👋 <b>Привет, {chat_title}!</b>\n\n"
        f"Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка: "
        f"картинку, модель, фон, символ и редкость.\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"<b>📌 Как пользоваться:</b>\n\n"
        f"Напиши слово <b>превью</b> и через пробел ссылку или название подарка.\n\n"
        f"<b>✅ Примеры:</b>\n"
        f"<code>превью https://t.me/nft/PlushPepe-22</code>\n"
        f"<code>превью t.me/nft/PlushPepe-22</code>\n"
        f"<code>превью PlushPepe-22</code>\n"
        f"<code>превью PlushPepe 22</code>\n"
        f"<code>превью Plush Pepe 22</code>\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"<b>📋 Правила:</b>\n"
        f"• Сообщение должно начинаться со слова <b>превью</b>\n"
        f"• Повтор одного подарка — не чаще <b>1 раза в 2 минуты</b>\n"
        f"• Кнопка <b>«Отправить без сжатия»</b> — только 1 раз на превью\n\n"
        f"⚡ Результат приходит за ~1–2 сек\n\n"
        f"<i>Автор бота: @balfikovich</a></i>"
    )


def get_start_text() -> str:
    return (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Показываю картинку, модель, фон, символ и редкость любого Telegram NFT-подарка.\n\n"
        f"<b>📨 Как пользоваться в личке:</b>\n"
        f"Просто отправь ссылку или название подарка — получишь карточку.\n\n"
        f"<b>✅ Форматы (личка — без префикса):</b>\n"
        f"<code>https://t.me/nft/PlushPepe-22</code>\n"
        f"<code>t.me/nft/PlushPepe-22</code>\n"
        f"<code>PlushPepe-22</code>\n"
        f"<code>PlushPepe 22</code>\n"
        f"<code>Plush Pepe 22</code>\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"<b>👥 Использование в группе / чате:</b>\n"
        f"В группе нужно писать слово <b>превью</b> перед названием:\n"
        f"<code>превью PlushPepe 22</code>\n"
        f"<code>превью t.me/nft/PlushPepe-22</code>\n\n"
        f"<b>📋 Правила в группе:</b>\n"
        f"• Повтор одного подарка — не чаще <b>1 раза в 2 минуты</b>\n"
        f"• <b>«Без сжатия»</b> — только 1 раз на одно превью\n\n"
        f"<b>🚀 Добавить бота в группу:</b>\n"
        f"1. Нажми кнопку <b>«Добавить в группу»</b> ниже\n"
        f"2. Выбери свой чат\n"
        f"3. Дай боту права <b>администратора</b>\n"
        f"4. Бот напишет приветствие с правилами\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"⚡ Проверка ~1–2 сек\n\n"
        f"<i>Автор: @balfikovich</a></i>"
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
    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )
    return found, webp_data, error, attrs


async def send_photo_with_keyboard(
    message: Message,
    png_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> bool:
    caption, ents = make_caption(slug, attrs)
    kbd           = make_keyboard(slug)
    file          = BufferedInputFile(png_bytes, filename=f"{slug}.png")
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
            file = BufferedInputFile(png_bytes, filename=f"{slug}.png")
            await message.answer_photo(
                photo=file,
                caption=caption,
                caption_entities=ents,
                parse_mode=None,
                reply_markup=kbd,
            )
            return True
        except Exception as ex:
            logger.error("Retry send_photo failed: %s", ex)
            return False
    except TelegramBadRequest as e:
        logger.error("BadRequest send_photo: %s", e)
        return False
    except Exception:
        logger.exception("send_photo error")
        return False


async def send_document_only(send_fn, webp_bytes: bytes, slug: str) -> None:
    file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
    try:
        await send_fn(document=file)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
        await send_fn(document=file)
    except Exception as e:
        logger.error("send_document error: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  BOT & DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── Бот добавлен / удалён из группы ─────────────────────────────────────────
@dp.my_chat_member()
async def on_bot_added_to_group(event: ChatMemberUpdated) -> None:
    if event.chat.type not in ("group", "supergroup"):
        return

    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    was_outside = old_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")
    now_inside  = new_status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, "member", "administrator")
    now_outside = new_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")

    who  = _u(event.from_user)
    chat = _chat(event.chat)

    if was_outside and now_inside:
        role    = "администратором" if new_status in (ChatMemberStatus.ADMINISTRATOR, "administrator") else "участником"
        privacy = "приватный" if getattr(event.chat, "username", None) is None else "публичный"
        user_log.info(
            "➕ БОТ ДОБАВЛЕН В ГРУППУ | кто=%s | чат=%s | роль=%s | тип_чата=%s",
            who, chat, role, privacy,
        )
        chat_title = event.chat.title or "этот чат"
        try:
            await bot.send_message(
                chat_id=event.chat.id,
                text=get_group_welcome(chat_title),
                parse_mode=ParseMode.HTML,
            )
            user_log.info("   └─ приветствие отправлено | chat_id=%s", event.chat.id)
        except TelegramForbiddenError:
            logger.warning("on_add | нет прав писать | chat_id=%s", event.chat.id)
        except Exception as e:
            logger.error("on_add | ошибка приветствия | error=%s", e)

    elif now_outside:
        action = "ВЫГНАН/ЗАБАНЕН" if new_status in (ChatMemberStatus.KICKED, "kicked") else "УДАЛЁН"
        user_log.info("➖ БОТ %s ИЗ ГРУППЫ | кто=%s | чат=%s", action, who, chat)


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    if message.chat.type != "private":
        return

    user_log.info("▶  /start | пользователь=%s", _u(message.from_user))

    buttons = []
    if BOT_USERNAME:
        buttons.append([
            InlineKeyboardButton(
                text="➕ Добавить в группу",
                url=f"https://t.me/{BOT_USERNAME}?startgroup",
            )
        ])
    buttons.append([
        InlineKeyboardButton(
            text="⭐ Поддержать автора",
            callback_data=CB_DONATE,
        )
    ])

    await message.answer(
        get_start_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


# ── /cancel_donate — отмена ожидания суммы ───────────────────────────────────
@dp.message(Command("cancel_donate"))
async def cmd_cancel_donate(message: Message) -> None:
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    if user_id in _awaiting_donate_amount:
        _awaiting_donate_amount.discard(user_id)
        user_log.info("❌ ДОНАТ ОТМЕНЁН | пользователь=%s", _u(message.from_user))
        await message.answer(
            "✅ Окей, донат отменён. Если передумаешь — всегда возвращайся! 😊",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.answer("Нет активного ожидания оплаты. Всё в порядке! 😊")


# ── Callback: кнопка «Поддержать автора» ─────────────────────────────────────
@dp.callback_query(F.data == CB_DONATE)
async def callback_donate(callback: CallbackQuery) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    user_log.info("💛 ДОНАТ — ОТКРЫТ ДИАЛОГ | пользователь=%s", _u(callback.from_user))

    _awaiting_donate_amount.add(user_id)

    await callback.message.answer(
        f'<tg-emoji emoji-id="{E_DONATE}">⭐</tg-emoji> <b>Поддержка проекта</b>\n'
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Привет! 👋 Этот бот полностью <b>бесплатен</b> — я не беру с тебя "
        f"ни копейки за его использование.\n\n"
        f"Если бот оказался полезным и ты хочешь поддержать автора — "
        f"я буду безмерно благодарен! Любая сумма важна и мотивирует "
        f"развивать проект дальше 🚀\n\n"
        f"<b>Как задонатить:</b>\n"
        f"Просто напиши <b>число</b> — сколько ⭐ звёзд тебе не жалко.\n"
        f"Минимум — <code>1</code>, максимум — <code>2500</code> за один раз.\n\n"
        f"<b>Например:</b> <code>10</code> или <code>50</code> или <code>100</code>\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"💡 Если передумал — напиши <code>/cancel_donate</code>",
        parse_mode=ParseMode.HTML,
    )


# ── PreCheckout — подтверждаем платёж ────────────────────────────────────────
@dp.pre_checkout_query()
async def pre_checkout_handler(query: PreCheckoutQuery) -> None:
    user_log.info(
        "💳 PRE_CHECKOUT | пользователь=%s | сумма=%s ⭐",
        _u(query.from_user), query.total_amount,
    )
    await query.answer(ok=True)


# ── Успешный платёж ───────────────────────────────────────────────────────────
@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message) -> None:
    payment  = message.successful_payment
    stars    = payment.total_amount
    user     = message.from_user
    username = f"@{user.username}" if user.username else f"без username (id={user.id})"
    name     = user.full_name or "NoName"

    user_log.info("✅ ДОНАТ ПОЛУЧЕН | пользователь=%s | сумма=%s ⭐", _u(user), stars)

    # Благодарность пользователю
    await message.answer(
        f'<tg-emoji emoji-id="{E_DONATE}">⭐</tg-emoji> <b>Огромное спасибо!</b>\n'
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Ты отправил <b>{stars} ⭐</b> — это очень приятно и мотивирует "
        f"продолжать развивать бота! 🙏\n\n"
        f"Я обязательно напишу тебе лично, чтобы поблагодарить!\n\n"
        f"<i>С уважением, <a href='https://t.me/balfikovich'>@balfikovich</a></i>",
        parse_mode=ParseMode.HTML,
    )

    # Уведомление админу
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🔔 <b>Новый донат!</b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
                f"👤 <b>Имя:</b> {name}\n"
                f"📎 <b>Username:</b> {username}\n"
                f"⭐ <b>Сумма:</b> {stars} звёзд\n"
                f"🆔 <b>User ID:</b> <code>{user.id}</code>"
            ),
            parse_mode=ParseMode.HTML,
        )
        user_log.info("   └─ уведомление отправлено админу (id=%s)", ADMIN_ID)
    except Exception as e:
        logger.error("Не удалось уведомить админа о донате: %s", e)


# ── Обработка текста (личка + группа) ────────────────────────────────────────
@dp.message(F.text)
async def handle_text(message: Message) -> None:
    if not message.from_user:
        return

    raw_text   = (message.text or "").strip()
    is_private = message.chat.type == "private"
    user_id    = message.from_user.id

    # ─────────────────────────────────────────────────────────────────────────
    # ДОНАТ: перехватываем ввод суммы
    # ─────────────────────────────────────────────────────────────────────────
    if is_private and user_id in _awaiting_donate_amount:
        amount_str = raw_text.strip()

        if amount_str.isdigit():
            amount = int(amount_str)
            if amount < 1:
                await message.answer(
                    "⚠️ Минимальная сумма — <b>1 звезда ⭐</b>. Введи число от 1 и выше.",
                    parse_mode=ParseMode.HTML,
                )
                return
            if amount > 2500:
                await message.answer(
                    "⚠️ Максимум за один платёж — <b>2500 звёзд</b>.\nВведи число от 1 до 2500.",
                    parse_mode=ParseMode.HTML,
                )
                return

            _awaiting_donate_amount.discard(user_id)
            user_log.info(
                "💛 ДОНАТ — ВЫСТАВЛЯЕМ ЧЕК | пользователь=%s | сумма=%s ⭐",
                _u(message.from_user), amount,
            )

            try:
                await bot.send_invoice(
                    chat_id=message.chat.id,
                    title="⭐ Поддержка автора",
                    description=(
                        f"Донат автору бота NFT Gift Viewer — {amount} звёзд.\n"
                        "Спасибо за поддержку! 🙏"
                    ),
                    payload=f"donate_{user_id}_{amount}",
                    currency="XTR",          # Telegram Stars
                    prices=[LabeledPrice(label="Звёзды", amount=amount)],
                    provider_token="",       # для Stars не нужен
                )
            except Exception as e:
                logger.error("send_invoice error: %s", e)
                _awaiting_donate_amount.add(user_id)  # возвращаем в режим ввода
                await message.answer(
                    "❌ Не удалось создать счёт. Попробуй ещё раз или напиши другую сумму.",
                    parse_mode=ParseMode.HTML,
                )
        else:
            await message.answer(
                "⚠️ Пожалуйста, введи <b>число</b> — количество звёзд.\n\n"
                "Например: <code>10</code>\n\n"
                "Если передумал — напиши <code>/cancel_donate</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    # ─────────────────────────────────────────────────────────────────────────
    # Обычная логика: поиск превью
    # ─────────────────────────────────────────────────────────────────────────
    if not is_private:
        lower = raw_text.lower()
        if not (lower.startswith("превью") or lower.startswith("preview")):
            return
        for prefix in ("превью", "preview"):
            if lower.startswith(prefix):
                raw_text = raw_text[len(prefix):].strip()
                break

    slug = extract_nft_slug(raw_text)

    if not slug:
        user_log.info(
            "❓ НЕВЕРНЫЙ ФОРМАТ | пользователь=%s | чат=%s | текст=%r",
            _u(message.from_user), _chat(message.chat), (message.text or "")[:80],
        )
        if is_private:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                f"<b>Неверный формат.</b>\n\n"
                f"<b>Примеры:</b>\n"
                f"<code>t.me/nft/PlushPepe-22</code>\n"
                f"<code>PlushPepe 22</code>\n"
                f"<code>Plush Pepe 22</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                f"<b>Неверный формат подарка.</b>\n\n"
                f"<b>Примеры:</b>\n"
                f"<code>превью t.me/nft/PlushPepe-22</code>\n"
                f"<code>превью PlushPepe 22</code>\n"
                f"<code>превью Plush Pepe 22</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    where = "личка" if is_private else "группа"
    user_log.info(
        "🎁 ЗАПРОС ПРЕВЬЮ | slug=%s | пользователь=%s | чат=%s | тип=%s",
        slug, _u(message.from_user), _chat(message.chat), where,
    )

    if not is_private:
        slug_wait = check_slug_antispam(user_id, slug)
        if slug_wait > 0:
            mins = slug_wait // 60
            secs = slug_wait % 60
            time_str = f"{mins} мин {secs} сек" if mins > 0 else f"{secs} сек"
            user_log.info(
                "🚫 АНТИСПАМ (повтор) | slug=%s | пользователь=%s | ждать=%s",
                slug, _u(message.from_user), time_str,
            )
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Этот подарок уже был показан.</b>\n"
                f"Повтор доступен через <code>{time_str}</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
    else:
        wait_sec = check_antispam(user_id)
        if wait_sec > 0:
            user_log.info(
                "🚫 АНТИСПАМ (слишком быстро) | пользователь=%s | ждать=%.1f сек",
                _u(message.from_user), wait_sec,
            )
            await message.answer(
                f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
                f"<b>Слишком быстро!</b> Подожди ещё <code>{wait_sec}</code> сек.",
                parse_mode=ParseMode.HTML,
            )
            return

    t_start  = time.monotonic()
    wait_msg = await message.answer(
        f"🔍 Загружаю <b>{slug}</b>…",
        parse_mode=ParseMode.HTML,
    )

    found, webp_data, error, attrs = await process_slug(slug)
    elapsed = round(time.monotonic() - t_start, 2)

    await safe_delete(wait_msg)

    if error:
        user_log.warning(
            "⚠️ ОШИБКА ЗАГРУЗКИ | slug=%s | пользователь=%s | ошибка=%s | время=%.2fс",
            slug, _u(message.from_user), error, elapsed,
        )
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n\n"
            f"<code>{slug}</code>\n<i>{error}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        user_log.info(
            "❌ НЕ НАЙДЕН | slug=%s | пользователь=%s | время=%.2fс",
            slug, _u(message.from_user), elapsed,
        )
        await message.answer(
            f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
            f"<b>Подарок не найден</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"<code>{slug}</code>\n\n"
            f"<b>Возможные причины:</b>\n"
            f"• Такого номера ещё не существует\n"
            f"• Подарок был сожжён 🔥\n"
            f"• Опечатка в ссылке / названии",
            parse_mode=ParseMode.HTML,
        )
        return

    model_info = attrs.model + (f" ({attrs.model_rarity})" if attrs.model_rarity else "")
    back_info  = attrs.backdrop + (f" ({attrs.backdrop_rarity})" if attrs.backdrop_rarity else "")
    sym_info   = attrs.symbol + (f" ({attrs.symbol_rarity})" if attrs.symbol_rarity else "")

    user_log.info(
        "✅ ПРЕВЬЮ ОТПРАВЛЕНО | slug=%s | модель=%s | фон=%s | символ=%s | пользователь=%s | время=%.2fс",
        slug, model_info, back_info, sym_info, _u(message.from_user), elapsed,
    )

    png_data = webp_to_png(webp_data)
    if png_data:
        success = await send_photo_with_keyboard(message, png_data, slug, attrs)
        if not success:
            user_log.warning("send_photo упал → шлём документом | slug=%s", slug)
            await send_document_only(message.answer_document, webp_data, slug)
    else:
        user_log.warning("WebP→PNG упал → шлём документом | slug=%s", slug)
        await send_document_only(message.answer_document, webp_data, slug)


# ── Кнопка «Отправить без сжатия» ────────────────────────────────────────────
@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    user_id    = callback.from_user.id
    slug       = callback.data[len(CB_NO_COMPRESS):]
    message_id = callback.message.message_id

    if _cb_lock.get(user_id):
        user_log.info(
            "🔒 КНОПКА ЗАБЛОКИРОВАНА | slug=%s | пользователь=%s",
            slug, _u(callback.from_user),
        )
        await callback.answer("⏳ Подожди, идёт загрузка…", show_alert=False)
        return

    wait_sec = check_antispam(user_id)
    if wait_sec > 0:
        user_log.info(
            "🚫 АНТИСПАМ (кнопка) | slug=%s | пользователь=%s | ждать=%.1f сек",
            slug, _u(callback.from_user), wait_sec,
        )
        await callback.answer(f"⏳ Подожди {wait_sec} сек.", show_alert=True)
        return

    no_compress_key = f"{message_id}:{slug.lower()}"

    if no_compress_key in _used_no_compress:
        user_log.info(
            "🚫 КНОПКА УЖЕ ИСПОЛЬЗОВАНА | slug=%s | msg_id=%s | пользователь=%s",
            slug, message_id, _u(callback.from_user),
        )
        await callback.answer(
            "❌ Оригинал уже был отправлен для этого превью!",
            show_alert=True,
        )
        return

    user_log.info(
        "📤 БЕЗ СЖАТИЯ — ЗАПРОС | slug=%s | пользователь=%s | чат=%s",
        slug, _u(callback.from_user), _chat(callback.message.chat),
    )

    _cb_lock[user_id] = True
    await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

    try:
        t_start = time.monotonic()
        found, webp_data, error = await fetch_nft_image(slug)
        elapsed = round(time.monotonic() - t_start, 2)

        if error or not found:
            user_log.warning(
                "⚠️ БЕЗ СЖАТИЯ — ОШИБКА | slug=%s | причина=%s | время=%.2fс",
                slug, error or "не найден", elapsed,
            )
            await callback.message.answer(
                "❌ Не удалось загрузить" if error else "❌ Подарок не найден"
            )
            return

        _used_no_compress.add(no_compress_key)

        # Убираем кнопку после нажатия
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        await send_document_only(callback.message.answer_document, webp_data, slug)

        user_log.info(
            "✅ БЕЗ СЖАТИЯ — ОТПРАВЛЕНО | slug=%s | пользователь=%s | время=%.2fс",
            slug, _u(callback.from_user), elapsed,
        )
    finally:
        _cb_lock[user_id] = False


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
            description="Введите ссылку или название → PlushPepe-22 / Plush Pepe 22",
            thumbnail_url="https://nft.fragment.com/gift/PlushPepe-1.webp",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "<b>NFT Gift Viewer</b>\n\n"
                    "Добавь бота в чат и отправляй ссылки или названия подарков!\n\n"
                    "<code>t.me/nft/PlushPepe-22</code>\n"
                    "<code>PlushPepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[hint], cache_time=60, is_personal=False)
        return

    slug = extract_nft_slug(raw)

    if not slug:
        user_log.info(
            "🔍 INLINE — НЕВЕРНЫЙ ФОРМАТ | пользователь=%s | запрос=%r",
            _u(query.from_user), raw[:60],
        )
        err = InlineQueryResultArticle(
            id="err_format",
            title="❌ Неверный формат",
            description="Пример: PlushPepe-22 / Plush Pepe 22 / t.me/nft/...",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "<b>Неверный формат</b>\n\n"
                    "<code>t.me/nft/PlushPepe-22</code>\n"
                    "<code>PlushPepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[err], cache_time=5, is_personal=True)
        return

    user_log.info(
        "🔍 INLINE — ЗАПРОС | slug=%s | пользователь=%s",
        slug, _u(query.from_user),
    )

    t_start = time.monotonic()
    found, webp_data, error, attrs = await process_slug(slug)
    elapsed = round(time.monotonic() - t_start, 2)

    name, number = split_slug(slug)
    nice         = readable_name(name)
    title        = f"🎁 {nice} #{number}"

    if error or not found:
        reason = error or "не найден"
        user_log.info(
            "🔍 INLINE — НЕ НАЙДЕН | slug=%s | причина=%s | время=%.2fс",
            slug, reason, elapsed,
        )
        description = f"⚠️ {error}" if error else "❌ Подарок не найден"
        not_found = InlineQueryResultArticle(
            id=f"nf_{slug}",
            title=title,
            description=description,
            input_message_content=InputTextMessageContent(
                message_text=f"<b>Подарок не найден</b>\n\n<code>{slug}</code>",
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[not_found], cache_time=10, is_personal=True)
        return

    user_log.info(
        "🔍 INLINE — УСПЕХ | slug=%s | модель=%s | пользователь=%s | время=%.2fс",
        slug, attrs.model, _u(query.from_user), elapsed,
    )

    caption, ents = make_caption(slug, attrs)
    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть в Telegram", url=f"https://t.me/nft/{slug}")
    ]])

    desc_parts = []
    if attrs.model    != "—": desc_parts.append(f"🪄 {attrs.model}")
    if attrs.backdrop != "—": desc_parts.append(f"🎨 {attrs.backdrop}")
    if attrs.symbol   != "—": desc_parts.append(f"✨ {attrs.symbol}")
    description = "  ·  ".join(desc_parts) if desc_parts else "NFT Подарок"

    photo_url = FRAGMENT_IMAGE_URL.format(slug=slug)

    result = InlineQueryResultPhoto(
        id=str(uuid.uuid4()),
        photo_url=photo_url,
        thumbnail_url=photo_url,
        title=title,
        description=description,
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
    logger.info("   Лог-файл: %s", os.path.abspath(LOG_FILE))
    logger.info("   Донаты → admin_id=%s", ADMIN_ID)
    logger.info("━" * 60)
    logger.info("ЧЕКЛИСТ:")
    logger.info("  1. @BotFather → /setprivacy → @%s → Disable", me.username)
    logger.info("     БЕЗ ЭТОГО бот не видит сообщения в группах!")
    logger.info("  2. @BotFather → /setjoingroups → @%s → Enable", me.username)
    logger.info("  3. @BotFather → /setinline → @%s → placeholder", me.username)
    logger.info("  4. В группе дать боту права администратора")
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
