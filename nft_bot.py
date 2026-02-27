"""
Telegram NFT Gift Checker Bot
==============================
- Парсит атрибуты (Модель, Фон, Символ + редкость) с Fragment JSON API
- Bot API 9.4: зелёная кнопка style="success" + icon_custom_emoji_id
- Анимированные custom emoji в тексте (Premium бот)
- Кнопка «Отправить без сжатия»

Запуск:
    pip install aiogram aiohttp pillow python-dotenv
    cp .env.example .env
    python nft_bot.py
"""

import asyncio
import io
import json
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
    Message,
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# ── Конфиг ───────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s")
if not BOT_TOKEN:
    raise RuntimeError(
        "Переменная окружения BOT_TOKEN не задана!\n"
        "Создай файл .env и добавь: BOT_TOKEN=1234567890:AAxxxx"
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
FRAGMENT_JSON_URL  = "https://nft.fragment.com/gift/{slug}.json"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)
CB_NO_COMPRESS     = "nocompress:"

# ──────────────────────────────────────────────────────────────────────────────
#  EMOJI IDs — вставь свои ID сюда
#  Формат в тексте: <tg-emoji emoji-id="ID">🎁</tg-emoji>
# ──────────────────────────────────────────────────────────────────────────────
# Кнопка «Отправить без сжатия»
BTN_EMOJI_ID    = "5359785904535774578"   # эмодзи на кнопке (задан тобой)

# Эмодзи в тексте капшена — ЗАМЕНИ на свои ID когда пришлёшь
EMOJI_GIFT      = "5359785904535774578"   # 🎁  заголовок
EMOJI_MODEL     = "5359785904535774578"   # 🪄  модель    — ЗАМЕНИ
EMOJI_BACKDROP  = "5359785904535774578"   # 🎨  фон       — ЗАМЕНИ
EMOJI_SYMBOL    = "5359785904535774578"   # ✨  символ    — ЗАМЕНИ
EMOJI_LINK      = "5359785904535774578"   # 🔗  ссылка    — ЗАМЕНИ

# helper: вставить анимированный эмодзи в HTML-текст
def ae(emoji_id: str, fallback: str = "●") -> str:
    """Animated/custom emoji для Premium-бота."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


# ── Regex ────────────────────────────────────────────────────────────────────
NFT_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)

# ── HTTP-сессия ───────────────────────────────────────────────────────────────
http_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
    return http_session


# ── Датакласс атрибутов NFT ───────────────────────────────────────────────────
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


# ── Вспомогательные функции ───────────────────────────────────────────────────

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
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    return s


def fmt_rarity(rarity_val) -> str:
    """Форматирует редкость: 2.4 → '2.4%'"""
    try:
        r = float(rarity_val)
        return f"{r:g}%"
    except (TypeError, ValueError):
        return ""


async def fetch_nft_image(slug: str) -> tuple:
    """
    Скачивает .webp с fragment.com.
    → (found: bool, data: bytes|None, error: str|None)
    """
    url = FRAGMENT_IMAGE_URL.format(slug=slug)
    logger.info("Fetching image: %s", url)
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                return (False, None, "Сервер вернул пустой ответ") if not data else (True, data, None)
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


async def fetch_nft_attrs(slug: str) -> NftAttrs:
    """
    Загружает JSON-метаданные с fragment.com и извлекает атрибуты.
    https://nft.fragment.com/gift/{slug}.json
    Возвращает NftAttrs (поля могут быть '—' если не найдены).
    """
    attrs = NftAttrs()
    url   = FRAGMENT_JSON_URL.format(slug=slug.lower())
    try:
        async with get_session().get(url) as resp:
            if resp.status != 200:
                logger.warning("Attrs JSON returned %s for %s", resp.status, url)
                return attrs
            raw = await resp.json(content_type=None)

        # Структура JSON:
        # { "attributes": [ {"trait_type": "Model", "value": "Pumpkin"},
        #                    {"trait_type": "Backdrop", "value": "Onyx Black"},
        #                    {"trait_type": "Symbol", "value": "Illuminati"} ],
        #   ...  }
        # ИЛИ в некоторых версиях:
        # { "model": "Pumpkin", "backdrop": "Onyx Black", "symbol": "Illuminati" }

        # Пробуем массив attributes (стандартный OpenSea-формат)
        attr_list = raw.get("attributes", [])
        if isinstance(attr_list, list):
            for item in attr_list:
                tt = str(item.get("trait_type", "")).lower()
                val = str(item.get("value", ""))
                rarity = fmt_rarity(item.get("rarity", item.get("readable_rarity", "")))
                if "model" in tt:
                    attrs.model, attrs.model_rarity = val, rarity
                elif "backdrop" in tt or "background" in tt:
                    attrs.backdrop, attrs.backdrop_rarity = val, rarity
                elif "symbol" in tt:
                    attrs.symbol, attrs.symbol_rarity = val, rarity

        # Fallback: прямые ключи верхнего уровня
        if attrs.model == "—" and "model" in raw:
            attrs.model = str(raw["model"])
        if attrs.backdrop == "—" and "backdrop" in raw:
            attrs.backdrop = str(raw["backdrop"])
        if attrs.symbol == "—" and "symbol" in raw:
            attrs.symbol = str(raw["symbol"])

    except Exception as e:
        logger.warning("fetch_nft_attrs error (%s): %s", slug, e)

    return attrs


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    """WebP → PNG (Telegram не принимает webp в answer_photo)."""
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


def make_caption(slug: str, attrs: NftAttrs) -> str:
    """Красивый HTML-caption с анимированными эмодзи и атрибутами."""
    name, number = split_slug(slug)
    nice_name    = readable_name(name)

    # Редкость рядом с названием атрибута
    def attr_line(label: str, value: str, rarity: str) -> str:
        r = f" <code>{rarity}</code>" if rarity else ""
        return f"{label} {value}{r}"

    return (
        f"{ae(EMOJI_GIFT, '🎁')} <b>{nice_name} #{number}</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"{attr_line(ae(EMOJI_MODEL,    '🪄') + ' <b>Модель:</b>',  attrs.model,    attrs.model_rarity)}\n"
        f"{attr_line(ae(EMOJI_BACKDROP, '🎨') + ' <b>Фон:</b>',     attrs.backdrop, attrs.backdrop_rarity)}\n"
        f"{attr_line(ae(EMOJI_SYMBOL,   '✨') + ' <b>Символ:</b>',  attrs.symbol,   attrs.symbol_rarity)}\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"{ae(EMOJI_LINK, '🔗')} <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>"
    )


def make_keyboard(slug: str):
    """
    Bot API 9.4:
      style = "success"  → зелёная кнопка
      icon_custom_emoji_id → кастомный эмодзи слева от текста
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Отправить без сжатия",
        callback_data=f"{CB_NO_COMPRESS}{slug}",
    )
    keyboard = builder.as_markup()

    # Патчим кнопку напрямую — aiogram ещё не поддерживает эти поля нативно
    btn = keyboard.inline_keyboard[0][0]
    btn.model_extra = {
        "style": "success",
        "icon_custom_emoji_id": BTN_EMOJI_ID,
    }
    return keyboard


async def send_photo_with_keyboard(
    message: Message,
    png_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> bool:
    file    = BufferedInputFile(png_bytes, filename=f"{slug}.png")
    caption = make_caption(slug, attrs)
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
        logger.error("BadRequest: %s", e)
        return False
    except Exception:
        logger.exception("send_photo error")
        return False


async def send_document_uncompressed(
    send_fn,
    webp_bytes: bytes,
    slug: str,
    attrs: NftAttrs,
) -> None:
    """Отправляет оригинал .webp как документ (без сжатия)."""
    file    = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
    caption = (
        make_caption(slug, attrs)
        + "\n\n<i>📎 Оригинальное качество — без сжатия</i>"
    )
    try:
        await send_fn(document=file, caption=caption, parse_mode=ParseMode.HTML)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
        await send_fn(document=file, caption=caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error("send_document error: %s", e)


# ── Bot & Dispatcher ──────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        f"{ae(EMOJI_GIFT, '✨')} <b>NFT Gift Viewer</b>\n"
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
        f"<code>🔍</code> Загружаю <b>{slug}</b>…",
        parse_mode=ParseMode.HTML,
    )

    # Параллельно грузим картинку и атрибуты
    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )

    await safe_delete(wait_msg)

    # ── Ошибка сети ───────────────────────────────────────────────────────
    if error:
        await message.answer(
            f"⚠️ <b>Не удалось загрузить</b>\n\n"
            f"<code>{slug}</code>\n"
            f"<i>{error}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Не найден ─────────────────────────────────────────────────────────
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

    # ── Найден → фото + кнопка ────────────────────────────────────────────
    png_data = webp_to_png(webp_data)
    if png_data:
        success = await send_photo_with_keyboard(message, png_data, slug, attrs)
        if not success:
            await send_document_uncompressed(message.answer_document, webp_data, slug, attrs)
    else:
        await send_document_uncompressed(message.answer_document, webp_data, slug, attrs)


@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    slug = callback.data[len(CB_NO_COMPRESS):]
    await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

    (found, webp_data, error), attrs = await asyncio.gather(
        fetch_nft_image(slug),
        fetch_nft_attrs(slug),
    )

    if error or not found:
        await callback.answer(
            "❌ Не удалось загрузить файл" if error else "❌ Подарок не найден",
            show_alert=True,
        )
        return

    await send_document_uncompressed(
        callback.message.answer_document,
        webp_data,
        slug,
        attrs,
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
