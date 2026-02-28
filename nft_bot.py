"""
Telegram NFT Gift Checker Bot
Автор: @balfikovich

ВАЖНО — ДО ЗАПУСКА сделай в @BotFather:
  1. /setinline  → выбери бота → введи placeholder, например:
        gift link / @username / model name
     БЕЗ ЭТОГО inline-режим (@бот запрос) НЕ БУДЕТ РАБОТАТЬ!
  2. /setjoingroups → Enable  (чтобы бота можно было добавлять в группы)
  3. /setprivacy → Disable  (чтобы бот видел все сообщения в группе)

Форматы запроса (личка И группа):
  • https://t.me/nft/PlushPepe-22
  • t.me/nft/PlushPepe-22
  • PlushPepe-22
  • PlushPepe 22
  • Plush Pepe 22
"""

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
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InputTextMessageContent,
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from dotenv import load_dotenv

# ── Конфиг ───────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан! Создай .env: BOT_TOKEN=xxx")

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────
FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)
CB_NO_COMPRESS     = "nocompress:"
AUTHOR             = "@balfikovich"
ANTISPAM_SECONDS   = 1.5

# ── Custom Emoji IDs ──────────────────────────────────────────────────────────
E_GIFT   = "5408829285685291820"
E_MODEL  = "5408894951440279259"
E_BACK   = "5411585799990830248"
E_SYMBOL = "5409189019261103031"
E_LINK   = "5409143419593321597"
E_WARN   = "5409124594751660992"
E_ERR    = "5408930028438188841"
E_START  = "6028495398941759268"

# ── Антиспам ──────────────────────────────────────────────────────────────────
_last_request: dict[int, float] = {}
_cb_lock: dict[int, bool] = {}

# ── Имя бота (заполняется при старте) ────────────────────────────────────────
BOT_USERNAME: str = ""


def check_antispam(user_id: int) -> float:
    now  = time.monotonic()
    last = _last_request.get(user_id, 0.0)
    diff = now - last
    if diff < ANTISPAM_SECONDS:
        return round(ANTISPAM_SECONDS - diff, 1)
    _last_request[user_id] = now
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

# Формат 1: полная ссылка  t.me/nft/PlushPepe-22
_RE_LINK = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)

# Формат 2: slug с дефисом  PlushPepe-22
_RE_SLUG = re.compile(
    r"^([A-Za-z][A-Za-z0-9]*)[-](\d+)$",
    re.IGNORECASE,
)

# Формат 3: слово(а) + пробел + число  "PlushPepe 22"  "Plush Pepe 22"
_RE_WORDS = re.compile(
    r"^([A-Za-z][A-Za-z0-9]*(?:\s+[A-Za-z][A-Za-z0-9]*)*)\s+(\d+)$",
    re.IGNORECASE,
)


def extract_nft_slug(raw: str) -> Optional[str]:
    """
    Поддерживаемые форматы:
      https://t.me/nft/PlushPepe-22
      t.me/nft/PlushPepe-22
      PlushPepe-22
      PlushPepe 22
      Plush Pepe 22
    """
    text = raw.strip()

    m = _RE_LINK.search(text)
    if m:
        return m.group(1)

    m = _RE_SLUG.match(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = _RE_WORDS.match(text)
    if m:
        name   = m.group(1).replace(" ", "")
        number = m.group(2)
        return f"{name}-{number}"

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
                logger.warning("t.me/nft/%s -> HTTP %s", slug, resp.status)
                return attrs
            html = await resp.text()

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 не установлен")
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
        logger.warning("fetch_nft_attrs(%s): %s", slug, e)
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
        logger.error("WebP→PNG: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  CAPTION через entities
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
#  ОТПРАВКА
# ══════════════════════════════════════════════════════════════════════════════

async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


async def send_photo_with_keyboard(message: Message, png_bytes: bytes, slug: str, attrs: NftAttrs) -> bool:
    caption, ents = make_caption(slug, attrs)
    kbd           = make_keyboard(slug)
    file          = BufferedInputFile(png_bytes, filename=f"{slug}.png")
    try:
        # parse_mode=None ОБЯЗАТЕЛЕН — иначе aiogram берёт дефолтный parse_mode
        # из конфига бота и перезаписывает caption_entities, ломая custom emoji
        await message.answer_photo(
            photo=file,
            caption=caption,
            caption_entities=ents,
            parse_mode=None,          # ← ключевой фикс
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
            logger.error("Retry failed: %s", ex)
            return False
    except TelegramBadRequest as e:
        logger.error("BadRequest: %s", e)
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
#  ОБЩАЯ ЛОГИКА ОБРАБОТКИ ЗАПРОСА (используется и в личке/группе, и в inline)
# ══════════════════════════════════════════════════════════════════════════════

async def process_slug(slug: str) -> tuple:
    """
    Загружает фото + атрибуты параллельно.
    Возвращает (found, webp_data, error, attrs).
    """
    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )
    return found, webp_data, error, attrs


# ══════════════════════════════════════════════════════════════════════════════
#  BOT & DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    is_private = message.chat.type == "private"

    text = (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Показываю картинку, модель, фон, символ и редкость любого Telegram NFT-подарка.\n\n"
        f"<b>📨 Как пользоваться:</b>\n"
        f"Отправь ссылку или название подарка — и я пришлю карточку.\n\n"
        f"<b>✅ Поддерживаемые форматы:</b>\n"
        f"<code>https://t.me/nft/PlushPepe-22</code>\n"
        f"<code>t.me/nft/PlushPepe-22</code>\n"
        f"<code>PlushPepe-22</code>\n"
        f"<code>PlushPepe 22</code>\n"
        f"<code>Plush Pepe 22</code>\n\n"
        f"<b>👥 В группе / чате:</b>\n"
        f"Напечатай <code>@{BOT_USERNAME or 'бот'} PlushPepe 22</code> — "
        f"появится карточка с фото. Нажми на неё — результат отправится в чат!\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"⚡ Проверка ~1–2 сек\n\n"
        f"<i>Автор: <a href='https://t.me/balfikovich'>{AUTHOR}</a></i>"
    )

    buttons = []

    if is_private:
        # Кнопка добавления в группу через startgroup deep link
        # При нажатии открывается стандартный диалог выбора группы Telegram,
        # где показываются только чаты где у пользователя есть права администратора.
        add_url = f"https://t.me/{BOT_USERNAME}?startgroup=start" if BOT_USERNAME else None
        if add_url:
            buttons.append([
                InlineKeyboardButton(text="➕ Добавить бота в группу", url=add_url)
            ])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


# ── Обработка текста (личка + группа) ────────────────────────────────────────
@dp.message(F.text)
async def handle_text(message: Message) -> None:
    raw_text = (message.text or "").strip()
    slug = extract_nft_slug(raw_text)

    if not slug:
        if message.chat.type == "private":
            await message.answer(
                f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                f"<b>Неверный формат.</b>\n\n"
                f"<b>Примеры:</b>\n"
                f"<code>t.me/nft/PlushPepe-22</code>\n"
                f"<code>PlushPepe 22</code>\n"
                f"<code>Plush Pepe 22</code>",
                parse_mode=ParseMode.HTML,
            )
        return

    user_id = message.from_user.id

    wait_sec = check_antispam(user_id)
    if wait_sec > 0:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Слишком быстро!</b> Подожди ещё <code>{wait_sec}</code> сек.",
            parse_mode=ParseMode.HTML,
        )
        return

    wait_msg = await message.answer(f"🔍 Загружаю <b>{slug}</b>…", parse_mode=ParseMode.HTML)
    found, webp_data, error, attrs = await process_slug(slug)
    await safe_delete(wait_msg)

    if error:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Не удалось загрузить</b>\n\n"
            f"<code>{slug}</code>\n<i>{error}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
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

    png_data = webp_to_png(webp_data)
    if png_data:
        success = await send_photo_with_keyboard(message, png_data, slug, attrs)
        if not success:
            await send_document_only(message.answer_document, webp_data, slug)
    else:
        await send_document_only(message.answer_document, webp_data, slug)


# ── Кнопка «Отправить без сжатия» ────────────────────────────────────────────
@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    slug    = callback.data[len(CB_NO_COMPRESS):]

    if _cb_lock.get(user_id):
        await callback.answer("⏳ Подожди, идёт загрузка…", show_alert=False)
        return

    wait_sec = check_antispam(user_id)
    if wait_sec > 0:
        await callback.answer(f"⏳ Подожди {wait_sec} сек.", show_alert=True)
        return

    _cb_lock[user_id] = True
    await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

    try:
        found, webp_data, error = await fetch_nft_image(slug)
        if error or not found:
            await callback.message.answer("❌ Не удалось загрузить" if error else "❌ Подарок не найден")
            return
        await send_document_only(callback.message.answer_document, webp_data, slug)
    finally:
        _cb_lock[user_id] = False


# ══════════════════════════════════════════════════════════════════════════════
#  INLINE-РЕЖИМ
#
#  КАК ЭТО РАБОТАЕТ:
#  Пользователь в любом чате набирает @botusername <запрос>
#  Telegram отправляет боту InlineQuery с этим запросом.
#  Бот отвечает списком результатов (фото / статья).
#  Пользователь нажимает — результат отправляется в чат.
#
#  ТРЕБОВАНИЕ: в @BotFather обязательно /setinline для этого бота!
# ══════════════════════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_handler(query: InlineQuery) -> None:
    raw = (query.query or "").strip()

    # ── Пустой запрос: показываем подсказку ──────────────────────────────
    if not raw:
        hint = InlineQueryResultArticle(
            id="hint",
            title="🎁 NFT Gift Viewer",
            description="Введите ссылку или название → PlushPepe-22 / Plush Pepe 22",
            thumbnail_url="https://nft.fragment.com/gift/PlushPepe-1.webp",
            input_message_content=InputTextMessageContent(
                message_text=(
                    f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> '
                    f"<b>NFT Gift Viewer</b>\n\n"
                    f"Отправь ссылку или название подарка:\n"
                    f"<code>t.me/nft/PlushPepe-22</code>\n"
                    f"<code>PlushPepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[hint], cache_time=60, is_personal=False)
        return

    slug = extract_nft_slug(raw)

    # ── Неверный формат ───────────────────────────────────────────────────
    if not slug:
        err = InlineQueryResultArticle(
            id="err_format",
            title="❌ Неверный формат",
            description="Пример: PlushPepe-22 / Plush Pepe 22 / t.me/nft/...",
            input_message_content=InputTextMessageContent(
                message_text=(
                    f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                    f"<b>Неверный формат запроса</b>\n\n"
                    f"<b>Примеры:</b>\n"
                    f"<code>t.me/nft/PlushPepe-22</code>\n"
                    f"<code>PlushPepe 22</code>\n"
                    f"<code>Plush Pepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[err], cache_time=5, is_personal=True)
        return

    # ── Загружаем фото и атрибуты параллельно ────────────────────────────
    found, webp_data, error, attrs = await process_slug(slug)

    name, number = split_slug(slug)
    nice         = readable_name(name)
    title        = f"🎁 {nice} #{number}"

    # ── Подарок не найден / ошибка ────────────────────────────────────────
    if error or not found:
        description = f"⚠️ {error}" if error else "❌ Подарок не найден или сожжён"
        not_found = InlineQueryResultArticle(
            id=f"nf_{slug}",
            title=title,
            description=description,
            input_message_content=InputTextMessageContent(
                message_text=(
                    f'<tg-emoji emoji-id="{E_ERR}">❌</tg-emoji> '
                    f"<b>Подарок не найден</b>\n\n"
                    f"<code>{slug}</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[not_found], cache_time=10, is_personal=True)
        return

    # ── Собираем результат ────────────────────────────────────────────────
    caption, ents = make_caption(slug, attrs)

    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔗 Открыть в Telegram", url=f"https://t.me/nft/{slug}")
    ]])

    desc_parts = []
    if attrs.model    != "—": desc_parts.append(f"🪄 {attrs.model}")
    if attrs.backdrop != "—": desc_parts.append(f"🎨 {attrs.backdrop}")
    if attrs.symbol   != "—": desc_parts.append(f"✨ {attrs.symbol}")
    description = "  ·  ".join(desc_parts) if desc_parts else "NFT Подарок"

    # fragment.com отдаёт webp — Telegram принимает его как photo_url в inline
    photo_url = FRAGMENT_IMAGE_URL.format(slug=slug)

    result = InlineQueryResultPhoto(
        id=str(uuid.uuid4()),
        photo_url=photo_url,
        thumbnail_url=photo_url,
        title=title,
        description=description,
        caption=caption,
        caption_entities=ents,
        parse_mode=None,          # ← не передаём parse_mode, только entities
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
    logger.info("✅ Bot started: @%s (id=%s)", me.username, me.id)
    logger.info("━" * 60)
    logger.info("ЧЕКЛИСТ (если что-то не работает):")
    logger.info("  1. @BotFather → /setinline → @%s → введи placeholder", me.username)
    logger.info("     например: gift link / @username / model name")
    logger.info("     БЕЗ ЭТОГО inline (@бот запрос) НЕ РАБОТАЕТ!")
    logger.info("  2. @BotFather → /setjoingroups → @%s → Enable", me.username)
    logger.info("  3. @BotFather → /setprivacy → @%s → Disable", me.username)
    logger.info("     (чтобы бот видел сообщения в группе, не только команды)")
    logger.info("━" * 60)


async def on_shutdown() -> None:
    logger.info("🛑 Shutting down…")
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()
    await bot.session.close()


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(
        bot,
        allowed_updates=["message", "callback_query", "inline_query"],
    )


if __name__ == "__main__":
    asyncio.run(main())
