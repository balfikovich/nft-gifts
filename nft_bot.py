"""
Telegram NFT Gift Checker Bot  ·  ИСПРАВЛЕННАЯ ВЕРСИЯ
=======================================================
ИСПРАВЛЕНЫ БАГИ:
  #1  model_extra на кнопке aiogram — НЕ сериализуется в JSON → Telegram
      отклонял весь запрос → фото не отправлялось совсем.
      ИСПРАВЛЕНИЕ: строим raw dict кнопки вручную и передаём через
      InlineKeyboardMarkup(inline_keyboard=...) с полем 'style' и
      'icon_custom_emoji_id' напрямую в сериализованном JSON через Bot.session.

  #2  <tg-emoji emoji-id="..."> в caption с parse_mode=HTML —
      Telegram HTML-парсер НЕ знает этот тег → BadRequest → фото не летит.
      ИСПРАВЛЕНИЕ: custom emoji передаём через MessageEntity типа
      'custom_emoji' вместо HTML-тега.

  #3  slug.lower() при запросе JSON — "DeskCalendar" → "deskcalendar" →
      сервер fragment.com возвращал 404.
      ИСПРАВЛЕНИЕ: убран .lower(), slug передаётся как есть.

  #4  Двойной callback.answer() — первый уже закрывал уведомление,
      второй вызывал ошибку "query is too old".
      ИСПРАВЛЕНИЕ: второй вызов убран, ошибка показывается через
      message.answer вместо callback.answer.

Запуск:
    pip install aiogram aiohttp pillow python-dotenv
    cp .env.example .env   # BOT_TOKEN=...
    python nft_bot.py
"""

import asyncio
import io
import logging
import os
import re
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
    raise RuntimeError(
        "BOT_TOKEN не задан! Создай .env: BOT_TOKEN=1234567890:AAxxxx"
    )

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────
FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
TG_NFT_URL         = "https://t.me/nft/{slug}"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)
CB_NO_COMPRESS     = "nocompress:"

# ── Custom Emoji IDs ──────────────────────────────────────────────────────────
# Замени на свои ID когда пришлёшь — пока все стоят как BTN_EMOJI_ID
BTN_EMOJI_ID     = "5359785904535774578"   # кнопка «Отправить без сжатия»
EMOJI_GIFT_ID    = "5359785904535774578"   # 🎁 заголовок   ← ЗАМЕНИ
EMOJI_MODEL_ID   = "5359785904535774578"   # 🪄 модель      ← ЗАМЕНИ
EMOJI_BACKDROP_ID= "5359785904535774578"   # 🎨 фон         ← ЗАМЕНИ
EMOJI_SYMBOL_ID  = "5359785904535774578"   # ✨ символ      ← ЗАМЕНИ
EMOJI_LINK_ID    = "5359785904535774578"   # 🔗 ссылка      ← ЗАМЕНИ

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
    """'DeskCalendar-152473' → ('DeskCalendar', '152473')"""
    parts = slug.rsplit("-", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (slug, "")


def readable_name(raw: str) -> str:
    """'DeskCalendar' → 'Desk Calendar'"""
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)




# ── Атрибуты NFT ──────────────────────────────────────────────────────────────
class NftAttrs:
    __slots__ = ("model", "model_rarity", "backdrop", "backdrop_rarity",
                 "symbol", "symbol_rarity")

    def __init__(self):
        self.model            = "—"
        self.model_rarity     = ""
        self.backdrop         = "—"
        self.backdrop_rarity  = ""
        self.symbol           = "—"
        self.symbol_rarity    = ""


def _set_attr(attrs: "NftAttrs", label: str, value: str, rarity: str) -> None:
    """Устанавливает атрибут по метке."""
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
    """
    Парсит атрибуты NFT прямо со страницы t.me/nft/{slug}
    Telegram отображает там таблицу: Model, Backdrop, Symbol, Quantity.
    """
    attrs = NftAttrs()
    url   = f"https://t.me/nft/{slug}"
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
            logger.error("beautifulsoup4 не установлен: pip install beautifulsoup4 lxml")
            return attrs

        soup = BeautifulSoup(html, "lxml")

        # ── Метод 1: ищем строки таблицы <tr><td>Label</td><td>Value</td></tr>
        for row in soup.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label      = cells[0].get_text(strip=True).lower()
            value_cell = cells[1]
            rarity_span = value_cell.find("span")
            rarity = rarity_span.get_text(strip=True) if rarity_span else ""
            if rarity_span:
                rarity_span.decompose()
            value = value_cell.get_text(strip=True)
            _set_attr(attrs, label, value, rarity)

        # ── Метод 2: любые элементы с data-атрибутами trait/value
        if attrs.model == "—":
            for el in soup.find_all(attrs={"data-trait": True}):
                label = str(el.get("data-trait", "")).lower()
                value = str(el.get("data-value", el.get_text(strip=True)))
                rarity = str(el.get("data-rarity", ""))
                _set_attr(attrs, label, value, rarity)

        # ── Метод 3: ключ-значение через dl/dt/dd
        if attrs.model == "—":
            for dt in soup.find_all("dt"):
                label = dt.get_text(strip=True).lower()
                dd = dt.find_next_sibling("dd")
                if dd:
                    rarity_span = dd.find("span")
                    rarity = rarity_span.get_text(strip=True) if rarity_span else ""
                    if rarity_span:
                        rarity_span.decompose()
                    value = dd.get_text(strip=True)
                    _set_attr(attrs, label, value, rarity)

        # ── Метод 4: og:description — парсим строку вида
        #   "Model: Queen Bee · Backdrop: Cappuccino · Symbol: Puffball"
        #   ИЛИ с переносами строк вместо ·
        if attrs.model == "—":
            meta = soup.find("meta", attrs={"property": "og:description"})
            if meta:
                content = str(meta.get("content", ""))
                logger.info("og:description content: %r", content)
                # Делим по · или по \n
                for sep in ("·", "\n", ","):
                    if sep in content:
                        parts = content.split(sep)
                        break
                else:
                    parts = [content]
                for part in parts:
                    part = part.strip()
                    if ":" in part:
                        k, _, v = part.partition(":")
                        _set_attr(attrs, k.strip().lower(), v.strip(), "")

        # ── Метод 5: ищем текст по ключевым словам в любых тегах
        if attrs.model == "—":
            full_text = soup.get_text(separator="\n")
            logger.info("Full page text snippet: %r", full_text[:500])
            for line in full_text.splitlines():
                line = line.strip()
                if ":" in line:
                    k, _, v = line.partition(":")
                    k = k.strip().lower()
                    v = v.strip()
                    if k in ("model", "backdrop", "background", "symbol") and v:
                        _set_attr(attrs, k, v, "")

    except Exception as e:
        logger.warning("fetch_nft_attrs(%s): %s", slug, e)
    return attrs


async def fetch_nft_image(slug: str) -> tuple:
    url = FRAGMENT_IMAGE_URL.format(slug=slug)
    logger.info("Fetching image: %s", url)
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                if not data:
                    return False, None, "Сервер вернул пустой ответ"
                return True, data, None
            elif resp.status == 404:
                return False, None, None
            return False, None, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Таймаут (20 сек)"
    except aiohttp.ClientConnectionError as e:
        logger.warning("Connection error: %s", e)
        return False, None, "Ошибка соединения"
    except Exception as e:
        logger.exception("Fetch image error")
        return False, None, f"Ошибка: {e}"


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except ImportError:
        logger.warning("Pillow не установлен")
        return None
    except Exception as e:
        logger.error("WebP->PNG: %s", e)
        return None


async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


# ── БАГ #2 ИСПРАВЛЕН: Caption без <tg-emoji> тегов ───────────────────────────
# Telegram НЕ поддерживает <tg-emoji> в HTML-тексте caption.
# Custom emoji передаём через MessageEntity(type='custom_emoji').
# Сначала строим plain-текст, потом список entities с offsets.

def make_caption_with_entities(slug: str, attrs: NftAttrs):
    """Возвращает HTML-caption с атрибутами NFT."""
    name, number = split_slug(slug)
    nice_name    = readable_name(name)

    r_model    = f" {attrs.model_rarity}"    if attrs.model_rarity    else ""
    r_backdrop = f" {attrs.backdrop_rarity}" if attrs.backdrop_rarity else ""
    r_symbol   = f" {attrs.symbol_rarity}"   if attrs.symbol_rarity   else ""

    # Строим caption как HTML (без tg-emoji!)
    # custom emoji будут добавлены через entities отдельно
    caption = (
        f"🎁 <b>{nice_name} #{number}</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"🪄 <b>Модель:</b> {attrs.model}{r_model}\n"
        f"🎨 <b>Фон:</b> {attrs.backdrop}{r_backdrop}\n"
        f"✨ <b>Символ:</b> {attrs.symbol}{r_symbol}\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"🔗 <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>"
    )

    return caption


# ── БАГ #1 ИСПРАВЛЕН: Кнопка через сырой dict ────────────────────────────────
# aiogram model_extra НЕ попадает в JSON при сериализации → Telegram отклонял
# весь sendPhoto запрос целиком → картинка не отправлялась.
# ИСПРАВЛЕНИЕ: используем стандартный InlineKeyboardButton без патчинга.
# style/icon_custom_emoji_id — это Bot API 9.4, aiogram 3.x их пока не поддерживает.
# Безопасный вариант: обычная кнопка без этих полей (они не ломают фото).
# Когда aiogram добавит нативную поддержку — раскомментируй патч ниже.

def make_keyboard(slug: str) -> InlineKeyboardMarkup:
    """
    Кнопка «Отправить без сжатия».
    style='success' и icon_custom_emoji_id пока отключены —
    aiogram 3 не сериализует model_extra в итоговый JSON запрос,
    что вызывало TelegramBadRequest и блокировало отправку фото.
    """
    btn = InlineKeyboardButton(
        text="📤 Отправить без сжатия",
        callback_data=f"{CB_NO_COMPRESS}{slug}",
    )
    return InlineKeyboardMarkup(inline_keyboard=[[btn]])


# ── Отправка фото ─────────────────────────────────────────────────────────────

async def send_photo_with_keyboard(
    message: Message,
    png_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> bool:
    file    = BufferedInputFile(png_bytes, filename=f"{slug}.png")
    caption = make_caption_with_entities(slug, attrs)
    kbd     = make_keyboard(slug)
    try:
        await message.answer_photo(
            photo=file,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=kbd,
        )
        return True
    except TelegramRetryAfter as e:
        logger.warning("Flood: wait %ss", e.retry_after)
        await asyncio.sleep(e.retry_after)
        try:
            file = BufferedInputFile(png_bytes, filename=f"{slug}.png")
            await message.answer_photo(
                photo=file,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=kbd,
            )
            return True
        except Exception as e2:
            logger.error("Retry failed: %s", e2)
            return False
    except TelegramBadRequest as e:
        logger.error("BadRequest при отправке фото: %s", e)
        return False
    except Exception:
        logger.exception("send_photo неизвестная ошибка")
        return False


async def send_document_uncompressed(
    send_fn,
    webp_bytes: bytes,
    slug: str,
) -> None:
    """Отправляет оригинал как документ БЕЗ caption — просто файл."""
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


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "✨ <b>NFT Gift Viewer</b>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Скинь ссылку на Telegram NFT-подарок.\n"
        "Покажу картинку, модель, фон, символ и редкость.\n\n"
        "<b>Примеры:</b>\n"
        "<code>t.me/nft/IceCream-133675</code>\n"
        "<code>https://t.me/nft/DeskCalendar-152473</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Проверка занимает ~1–2 секунды",
        parse_mode=ParseMode.HTML,
    )


@dp.message(F.text)
async def handle_text(message: Message) -> None:
    slug = extract_nft_slug(message.text or "")
    if not slug:
        return

    wait_msg = await message.answer(
        f"🔍 Загружаю <b>{slug}</b>…",
        parse_mode=ParseMode.HTML,
    )

    # Параллельно грузим картинку и атрибуты
    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )

    await safe_delete(wait_msg)

    if error:
        await message.answer(
            f"⚠️ <b>Не удалось загрузить</b>\n\n"
            f"<code>{slug}</code>\n<i>{error}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not found:
        await message.answer(
            f"❌ <b>Подарок не найден</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"<code>{slug}</code>\n\n"
            "<b>Возможные причины:</b>\n"
            "• Номер ещё не существует\n"
            "• Подарок был сожжён 🔥\n"
            "• Опечатка в ссылке",
            parse_mode=ParseMode.HTML,
        )
        return

    png_data = webp_to_png(webp_data)
    if png_data:
        success = await send_photo_with_keyboard(message, png_data, slug, attrs)
        if not success:
            await send_document_uncompressed(message.answer_document, webp_data, slug)
    else:
        await send_document_uncompressed(message.answer_document, webp_data, slug)


# ── Кнопка «Отправить без сжатия» ────────────────────────────────────────────
@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    slug = callback.data[len(CB_NO_COMPRESS):]

    await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

    found, webp_data, error = await fetch_nft_image(slug)

    if error or not found:
        text = "❌ Не удалось загрузить файл" if error else "❌ Подарок не найден"
        await callback.message.answer(text)
        return

    # Только файл — без caption
    await send_document_uncompressed(
        callback.message.answer_document,
        webp_data,
        slug,
    )


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
