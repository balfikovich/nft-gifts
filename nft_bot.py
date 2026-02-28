"""
Telegram NFT Gift Checker Bot
Автор: @balfikovich

Возможности:
  • Личка: /start — инструкция + кнопка «Добавить в чат»
  • Личка / группа: отправь ссылку или название подарка
  • Inline-режим: @бот <запрос> — мини-карточка с фото

Форматы запроса:
  1. https://t.me/nft/PlushPepe-22
  2. t.me/nft/PlushPepe-22
  3. PlushPepe-22
  4. PlushPepe 22
  5. Plush Pepe 22
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
    SwitchInlineQueryChosenChat,
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
E_GIFT   = "5408829285685291820"   # 🎁
E_MODEL  = "5408894951440279259"   # 🪄
E_BACK   = "5411585799990830248"   # 🎨
E_SYMBOL = "5409189019261103031"   # ✨
E_LINK   = "5409143419593321597"   # 🔗
E_WARN   = "5409124594751660992"   # ⚠️
E_ERR    = "5408930028438188841"   # ❌
E_START  = "6028495398941759268"   # ✨

# ── Антиспам ──────────────────────────────────────────────────────────────────
_last_request: dict[int, float] = {}
_cb_lock: dict[int, bool] = {}


def check_antispam(user_id: int) -> float:
    """Возвращает 0 если можно, иначе — сколько секунд ждать."""
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
    Пробует извлечь slug из произвольного текста.

    Поддерживаемые форматы:
      https://t.me/nft/PlushPepe-22
      t.me/nft/PlushPepe-22
      PlushPepe-22
      PlushPepe 22
      Plush Pepe 22
    """
    text = raw.strip()

    # Формат 1: ссылка
    m = _RE_LINK.search(text)
    if m:
        return m.group(1)

    # Формат 2: slug с дефисом
    m = _RE_SLUG.match(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # Формат 3: слова + число (убираем пробелы внутри имени)
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
    """CamelCase → «Camel Case»."""
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
    """Парсит атрибуты со страницы t.me/nft/{slug}."""
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

        # Метод 1: таблица <tr><td>
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

        # Метод 2: data-trait
        if attrs.model == "—":
            for el in soup.find_all(attrs={"data-trait": True}):
                _set_attr(attrs,
                          str(el.get("data-trait", "")),
                          str(el.get("data-value", el.get_text(strip=True))),
                          str(el.get("data-rarity", "")))

        # Метод 3: dl/dt/dd
        if attrs.model == "—":
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd:
                    rs = dd.find("span")
                    r  = rs.get_text(strip=True) if rs else ""
                    if rs:
                        rs.decompose()
                    _set_attr(attrs, dt.get_text(strip=True), dd.get_text(strip=True), r)

        # Метод 4: og:description
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

        # Метод 5: построчный текст
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
    """Возвращает (found: bool, data: bytes | None, error: str | None)."""
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
#  CAPTION через entities (без parse_mode)
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def _utf16_offset(text_so_far: str) -> int:
    return _utf16_len(text_so_far)


def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list[MessageEntity]]:
    """Возвращает (plain_text, caption_entities)."""
    name, number = split_slug(slug)
    nice = readable_name(name)

    r_model = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_back  = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    SEP = "━━━━━━━━━━━━━━━━━━━━"
    entities: list[MessageEntity] = []
    t = ""

    def add_custom_emoji(emoji_char: str, emoji_id: str) -> None:
        nonlocal t
        entities.append(MessageEntity(
            type="custom_emoji",
            offset=_utf16_offset(t),
            length=_utf16_len(emoji_char),
            custom_emoji_id=emoji_id,
        ))
        t += emoji_char

    def add_bold(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(
            type="bold",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
        ))
        t += s

    def add_code(s: str) -> None:
        nonlocal t
        entities.append(MessageEntity(
            type="code",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
        ))
        t += s

    def add_text_link(s: str, url: str) -> None:
        nonlocal t
        entities.append(MessageEntity(
            type="text_link",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
            url=url,
        ))
        t += s

    def plain(s: str) -> None:
        nonlocal t
        t += s

    # 🎁 Neko Helmet #2279
    add_custom_emoji("🎁", E_GIFT)
    plain(" ")
    add_bold(f"{nice} #{number}")
    plain("\n")

    # ━━━━━━━━━━━━━━━━━━━━
    add_code(SEP)
    plain("\n")

    # 🪄 Модель
    add_custom_emoji("🪄", E_MODEL)
    plain(" ")
    add_bold("Модель:")
    plain(f" {attrs.model}{r_model}\n")

    # 🎨 Фон
    add_custom_emoji("🎨", E_BACK)
    plain(" ")
    add_bold("Фон:")
    plain(f" {attrs.backdrop}{r_back}\n")

    # ✨ Символ
    add_custom_emoji("✨", E_SYMBOL)
    plain(" ")
    add_bold("Символ:")
    plain(f" {attrs.symbol}{r_sym}\n")

    # ━━━━━━━━━━━━━━━━━━━━
    add_code(SEP)
    plain("\n")

    # 🔗 Открыть в Telegram
    add_custom_emoji("🔗", E_LINK)
    plain(" ")
    add_text_link("Открыть в Telegram", f"https://t.me/nft/{slug}")

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


async def send_photo_with_keyboard(
    message: Message,
    png_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> bool:
    file          = BufferedInputFile(png_bytes, filename=f"{slug}.png")
    caption, ents = make_caption(slug, attrs)
    kbd           = make_keyboard(slug)
    try:
        await message.answer_photo(
            photo=file,
            caption=caption,
            caption_entities=ents,
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
                reply_markup=kbd,
            )
            return True
        except Exception as e2:
            logger.error("Retry failed: %s", e2)
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
        f"<b>📨 Как использовать в личке:</b>\n"
        f"Просто отправь ссылку или название подарка.\n\n"
        f"<b>✅ Поддерживаемые форматы:</b>\n"
        f"<code>https://t.me/nft/PlushPepe-22</code>\n"
        f"<code>t.me/nft/PlushPepe-22</code>\n"
        f"<code>PlushPepe-22</code>\n"
        f"<code>PlushPepe 22</code>\n"
        f"<code>Plush Pepe 22</code>\n\n"
        f"<b>👥 Использование в группе / чате:</b>\n"
        f"Напечатай <code>@бот</code> и через пробел ссылку или название — "
        f"появится карточка с фото подарка. Нажми на неё — "
        f"и результат будет отправлен прямо в чат!\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"⚡ Проверка занимает ~1–2 сек\n\n"
        f"<i>Автор: <a href='https://t.me/balfikovich'>{AUTHOR}</a></i>"
    )

    # Кнопка «Добавить бота в чат» — только в личке
    reply_markup = None
    if is_private:
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="➕ Добавить бота в чат",
                # Открывает список чатов; после выбора вставляет @botname в поле ввода
                switch_inline_query_chosen_chat=SwitchInlineQueryChosenChat(
                    query="",
                    allow_group_chats=True,
                    allow_channel_chats=False,
                    allow_bot_chats=False,
                    allow_user_chats=False,
                ),
            )
        ]])

    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


# ── Обработка текстового сообщения ────────────────────────────────────────────
@dp.message(F.text)
async def handle_text(message: Message) -> None:
    raw_text = (message.text or "").strip()
    slug = extract_nft_slug(raw_text)

    if not slug:
        # В группах бот молчит на сообщения без slug, чтобы не мусорить
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

    wait_msg = await message.answer(
        f"🔍 Загружаю <b>{slug}</b>…",
        parse_mode=ParseMode.HTML,
    )

    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )

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
            msg = "❌ Не удалось загрузить" if error else "❌ Подарок не найден"
            await callback.message.answer(msg)
            return

        await send_document_only(callback.message.answer_document, webp_data, slug)
    finally:
        _cb_lock[user_id] = False


# ══════════════════════════════════════════════════════════════════════════════
#  INLINE-РЕЖИМ
# ══════════════════════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_handler(query: InlineQuery) -> None:
    """
    Обрабатывает запросы вида @бот <ссылка_или_название>.

    • Пустой запрос → карточка-подсказка с инструкцией
    • Нераспознанный формат → карточка с примерами
    • Подарок найден → фото-карточка, при нажатии отправляет в чат
    """
    raw = (query.query or "").strip()

    # ── Пустой запрос: подсказка ──────────────────────────────────────────
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
                    f"<code>PlushPepe 22</code>\n"
                    f"<code>Plush Pepe 22</code>"
                ),
                parse_mode=ParseMode.HTML,
            ),
        )
        await query.answer(results=[hint], cache_time=300, is_personal=False)
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
    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )

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

    # ── Формируем caption и кнопку ────────────────────────────────────────
    caption, ents = make_caption(slug, attrs)

    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🔗 Открыть в Telegram",
            url=f"https://t.me/nft/{slug}",
        )
    ]])

    # Описание для карточки предпросмотра
    desc_parts = []
    if attrs.model    != "—": desc_parts.append(f"🪄 {attrs.model}")
    if attrs.backdrop != "—": desc_parts.append(f"🎨 {attrs.backdrop}")
    if attrs.symbol   != "—": desc_parts.append(f"✨ {attrs.symbol}")
    description = "  ·  ".join(desc_parts) if desc_parts else "NFT Подарок"

    # fragment.com отдаёт webp — Telegram принимает его в inline-режиме
    photo_url = FRAGMENT_IMAGE_URL.format(slug=slug)

    result = InlineQueryResultPhoto(
        id=str(uuid.uuid4()),
        photo_url=photo_url,
        thumbnail_url=photo_url,
        title=title,
        description=description,
        caption=caption,
        caption_entities=ents,
        reply_markup=kbd,
    )

    await query.answer(results=[result], cache_time=60, is_personal=False)


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

async def on_startup() -> None:
    get_session()
    me = await bot.get_me()
    logger.info("✅ Bot started: @%s (id=%s)", me.username, me.id)
    logger.info(
        "   ⚠️  Убедись что в @BotFather включён Inline Mode: "
        "Bot Settings → Inline Mode → Enable"
    )


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
