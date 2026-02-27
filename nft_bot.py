"""
Telegram NFT Gift Checker Bot
Автор: @balfikovich
"""

import asyncio
import io
import logging
import os
import re
import time
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
ANTISPAM_SECONDS   = 1.5   # минимум секунд между запросами одного юзера

# ── Custom Emoji IDs ──────────────────────────────────────────────────────────
E_GIFT    = "5408829285685291820"   # 🎁  заголовок caption
E_MODEL   = "5408894951440279259"   # 🪄  Модель
E_BACK    = "5411585799990830248"   # 🎨  Фон
E_SYMBOL  = "5409189019261103031"   # ✨  Символ
E_LINK    = "5409143419593321597"   # 🔗  Открыть в Telegram
E_BTN     = "5359785904535774578"   # 📤  кнопка
E_WARN    = "5409124594751660992"   # ⚠️  ошибка загрузки
E_ERR     = "5408930028438188841"   # ❌  не найден
E_START   = "6028495398941759268"   # ✨  заголовок /start

# ── Антиспам: user_id → timestamp последнего запроса ─────────────────────────
_last_request: dict[int, float] = {}
_cb_lock: dict[int, bool] = {}   # блокировка пока обрабатывается callback


def check_antispam(user_id: int) -> float:
    """Возвращает 0 если можно обрабатывать, иначе сколько секунд ждать."""
    now = time.monotonic()
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


# ── Regex ─────────────────────────────────────────────────────────────────────
NFT_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)


def extract_nft_slug(text: str) -> Optional[str]:
    m = NFT_LINK_RE.search(text)
    return m.group(1) if m else None


def split_slug(slug: str):
    parts = slug.rsplit("-", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (slug, "")


def readable_name(raw: str) -> str:
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)


# ── Атрибуты NFT ──────────────────────────────────────────────────────────────
class NftAttrs:
    __slots__ = ("model", "model_rarity", "backdrop", "backdrop_rarity",
                 "symbol", "symbol_rarity")

    def __init__(self):
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

        # Метод 2: data-trait атрибуты
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
                logger.info("og:description: %r", content)
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

        # Метод 5: полный текст страницы построчно
        if attrs.model == "—":
            for line in soup.get_text(separator="\n").splitlines():
                if ":" in line:
                    k, _, v = line.strip().partition(":")
                    if k.strip().lower() in ("model","backdrop","background","symbol") and v.strip():
                        _set_attr(attrs, k.strip(), v.strip(), "")

    except Exception as e:
        logger.warning("fetch_nft_attrs(%s): %s", slug, e)
    return attrs


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
        logger.error("WebP->PNG: %s", e)
        return None


async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


# ── Caption с custom emoji через entities ────────────────────────────────────
# В caption Telegram НЕ поддерживает <tg-emoji> HTML-тег.
# Единственный способ — передавать caption_entities отдельно.
# Строим plain-текст + список MessageEntity (custom_emoji, bold, text_link, code).

def _utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2

def _utf16_offset(text_so_far: str) -> int:
    return _utf16_len(text_so_far)

def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list]:
    """
    Возвращает (plain_text, entities[]).
    parse_mode НЕ передаётся — форматирование только через entities.
    """
    name, number = split_slug(slug)
    nice = readable_name(name)

    r_model = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_back  = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_sym   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    SEP = "━━━━━━━━━━━━━━━━━━━━\n"
    entities = []
    t = ""  # накапливаем plain-text

    def add_custom_emoji(emoji_char: str, emoji_id: str):
        nonlocal t
        entities.append(MessageEntity(
            type="custom_emoji",
            offset=_utf16_offset(t),
            length=_utf16_len(emoji_char),
            custom_emoji_id=emoji_id,
        ))
        t += emoji_char

    def add_bold(s: str):
        nonlocal t
        entities.append(MessageEntity(
            type="bold",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
        ))
        t += s

    def add_code(s: str):
        nonlocal t
        entities.append(MessageEntity(
            type="code",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
        ))
        t += s

    def add_text_link(s: str, url: str):
        nonlocal t
        entities.append(MessageEntity(
            type="text_link",
            offset=_utf16_offset(t),
            length=_utf16_len(s),
            url=url,
        ))
        t += s

    def plain(s: str):
        nonlocal t
        t += s

    # ── Строка 1: 🎁 Neko Helmet #2279 ──
    add_custom_emoji("🎁", E_GIFT)
    plain(" ")
    add_bold(f"{nice} #{number}")
    plain("\n")

    # ── Разделитель ──
    add_code(SEP.rstrip("\n"))
    plain("\n")

    # ── 🪄 Модель ──
    add_custom_emoji("🪄", E_MODEL)
    plain(" ")
    add_bold("Модель:")
    plain(f" {attrs.model}{r_model}\n")

    # ── 🎨 Фон ──
    add_custom_emoji("🎨", E_BACK)
    plain(" ")
    add_bold("Фон:")
    plain(f" {attrs.backdrop}{r_back}\n")

    # ── ✨ Символ ──
    add_custom_emoji("✨", E_SYMBOL)
    plain(" ")
    add_bold("Символ:")
    plain(f" {attrs.symbol}{r_sym}\n")

    # ── Разделитель ──
    add_code(SEP.rstrip("\n"))
    plain("\n")

    # ── 🔗 Ссылка ──
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


# ── Отправка ──────────────────────────────────────────────────────────────────
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
    """Просто файл — без caption."""
    file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
    try:
        await send_fn(document=file)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
        await send_fn(document=file)
    except Exception as e:
        logger.error("send_document error: %s", e)


# ── Bot & Dispatcher ──────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    # custom emoji в тексте сообщения — здесь работает через HTML entities
    text = (
        f'<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n'
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"Скинь ссылку на Telegram NFT-подарок.\n"
        f"Покажу картинку, модель, фон, символ и редкость.\n\n"
        f"<b>Примеры:</b>\n"
        f"<code>t.me/nft/IceCream-133675</code>\n"
        f"<code>https://t.me/nft/DeskCalendar-152473</code>\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"⚡ Проверка ~1–2 сек\n\n"
        f"<i>Автор: <a href='https://t.me/balfikovich'>{AUTHOR}</a> · все вопросы туда</i>"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


# ── Обработка ссылки ──────────────────────────────────────────────────────────
@dp.message(F.text)
async def handle_text(message: Message) -> None:
    slug = extract_nft_slug(message.text or "")
    if not slug:
        return

    user_id = message.from_user.id

    # ── Антиспам ──────────────────────────────────────────────────────────
    wait_sec = check_antispam(user_id)
    if wait_sec > 0:
        await message.answer(
            f'<tg-emoji emoji-id="{E_WARN}">⚠️</tg-emoji> '
            f"<b>Слишком быстро!</b>\n\n"
            f"Подожди ещё <code>{wait_sec}</code> сек.",
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
            f"• Номер ещё не существует\n"
            f"• Подарок был сожжён 🔥\n"
            f"• Опечатка в ссылке",
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

    # ── Антиспам по кнопке ────────────────────────────────────────────────
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


# ── Startup / Shutdown ────────────────────────────────────────────────────────
async def on_startup() -> None:
    get_session()
    me = await bot.get_me()
    logger.info("✅ Bot started: @%s (id=%s)", me.username, me.id)


async def on_shutdown() -> None:
    logger.info("🛑 Shutting down…")
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()
    await bot.session.close()


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
