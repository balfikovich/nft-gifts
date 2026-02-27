"""
Telegram NFT Gift Checker Bot
==============================
Отправь ссылку вида  t.me/nft/IceCream-133675
Бот проверит, существует ли подарок, пришлёт картинку
и кнопку «Отправить без сжатия».

Запуск:
    pip install aiogram aiohttp pillow python-dotenv
    cp .env.example .env
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
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from dotenv import load_dotenv

# ── Конфиг ──────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s")
if not BOT_TOKEN:
    raise RuntimeError(
        "Переменная окружения BOT_TOKEN не задана!\n"
        "Создай файл .env и добавь: BOT_TOKEN=1234567890:AAxxxx"
    )

# ── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Константы ────────────────────────────────────────────────────────────────
FRAGMENT_IMAGE_URL   = "https://nft.fragment.com/gift/{slug}.webp"
REQUEST_TIMEOUT      = aiohttp.ClientTimeout(total=20)

# Custom emoji ID для кнопки (синяя стрелка отправки)
SEND_EMOJI_ID        = "5359785904535774578"

# Callback-префикс для кнопки «без сжатия»
CB_NO_COMPRESS       = "nocompress:"

# Regex: t.me/nft/<Name>-<number>
NFT_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)

# ── Глобальная HTTP-сессия ────────────────────────────────────────────────────
http_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
    return http_session


# ── Хелперы ───────────────────────────────────────────────────────────────────

def extract_nft_slug(text: str) -> Optional[str]:
    m = NFT_LINK_RE.search(text)
    return m.group(1) if m else None


def split_slug(slug: str):
    """Разбивает 'DeskCalendar-152473' на ('DeskCalendar', '152473')."""
    parts = slug.rsplit("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return slug, ""


def make_caption(slug: str) -> str:
    """Красивый caption под фотографией."""
    name, number = split_slug(slug)
    # Добавляем пробелы между словами в CamelCase: DeskCalendar -> Desk Calendar
    readable = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    readable = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", readable)

    return (
        f"<b>🎁 {readable} #{number}</b>\n"
        f"<code>──────────────────</code>\n"
        f"<b>Коллекция:</b> {readable}\n"
        f"<b>Номер:</b> <code>#{number}</code>\n"
        f"<code>──────────────────</code>\n"
        f"🔗 <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>  "
        f"·  <a href='https://nft.fragment.com/gift/{slug}'>Fragment</a>"
    )


def make_keyboard(slug: str) -> InlineKeyboardMarkup:
    """Инлайн-клавиатура с кнопкой «Отправить без сжатия»."""
    btn = InlineKeyboardButton(
        text=f"📤 Отправить без сжатия",
        callback_data=f"{CB_NO_COMPRESS}{slug}",
    )
    return InlineKeyboardMarkup(inline_keyboard=[[btn]])


async def fetch_nft_image(slug: str) -> tuple:
    """
    Скачивает .webp с fragment.com.
    Возвращает (found: bool, data: bytes|None, error: str|None)
    """
    url = FRAGMENT_IMAGE_URL.format(slug=slug)
    logger.info("Fetching: %s", url)
    try:
        async with get_session().get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                return (False, None, "Сервер вернул пустой ответ") if not data else (True, data, None)
            elif resp.status == 404:
                return False, None, None
            else:
                return False, None, f"Неожиданный HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Сервер не ответил за 20 сек, попробуй позже"
    except aiohttp.ClientConnectionError as e:
        logger.warning("Connection error: %s", e)
        return False, None, "Ошибка соединения с fragment.com"
    except Exception as e:
        logger.exception("Unexpected fetch error")
        return False, None, f"Неизвестная ошибка: {e}"


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    """WebP → PNG. Telegram не принимает .webp в answer_photo."""
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except ImportError:
        logger.warning("Pillow not installed")
        return None
    except Exception as e:
        logger.error("WebP->PNG failed: %s", e)
        return None


async def safe_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


async def send_photo_with_keyboard(
    message: Message,
    png_bytes: bytes,
    slug: str,
) -> bool:
    """Отправляет сжатое фото (превью) + инлайн-кнопку."""
    file    = BufferedInputFile(png_bytes, filename=f"{slug}.png")
    caption = make_caption(slug)
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
        logger.exception("Unexpected error sending photo")
        return False


async def send_document_uncompressed(
    target,           # Message или CallbackQuery
    webp_bytes: bytes,
    slug: str,
    is_callback: bool = False,
) -> None:
    """Отправляет оригинальный .webp как документ (без сжатия)."""
    file    = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
    caption = (
        make_caption(slug) +
        "\n\n<i>📎 Файл без сжатия — оригинальное качество</i>"
    )

    send_fn = target.message.answer_document if is_callback else target.answer_document

    try:
        await send_fn(
            document=file,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        file = BufferedInputFile(webp_bytes, filename=f"{slug}.png")
        await send_fn(document=file, caption=caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error("Failed to send document: %s", e)


# ── Bot + Dispatcher ─────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


# ── /start ───────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "✨ <b>NFT Gift Viewer</b>\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        "Скинь ссылку на Telegram NFT-подарок — покажу картинку "
        "и дам возможность скачать в оригинальном качестве.\n\n"
        "<b>Форматы ссылок:</b>\n"
        "<code>https://t.me/nft/IceCream-133675</code>\n"
        "<code>t.me/nft/DeskCalendar-152473</code>\n\n"
        "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        "⚡ Проверка занимает ~1 секунду",
        parse_mode=ParseMode.HTML,
    )


# ── Обработка текстового сообщения со ссылкой ────────────────────────────────
@dp.message(F.text)
async def handle_text(message: Message) -> None:
    slug = extract_nft_slug(message.text or "")
    if not slug:
        return

    wait_msg = await message.answer(
        f"<code>🔍</code> Загружаю <b>{slug}</b>…",
        parse_mode=ParseMode.HTML,
    )

    found, webp_data, error = await fetch_nft_image(slug)
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
            f"<b>Причины:</b>\n"
            f"• Номер ещё не существует\n"
            f"• Подарок был сожжён 🔥\n"
            f"• Опечатка в ссылке",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Найден — отправляем фото + кнопку ────────────────────────────────
    png_data = webp_to_png(webp_data)

    if png_data:
        success = await send_photo_with_keyboard(message, png_data, slug)
        if not success:
            # Fallback: если фото не ушло — шлём документом
            await send_document_uncompressed(message, webp_data, slug)
    else:
        # Pillow не установлен — сразу документ
        await send_document_uncompressed(message, webp_data, slug)


# ── Callback: кнопка «Отправить без сжатия» ──────────────────────────────────
@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
    slug = callback.data[len(CB_NO_COMPRESS):]

    # Уведомление пользователю (крутилка в кнопке)
    await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

    found, webp_data, error = await fetch_nft_image(slug)

    if error or not found:
        await callback.answer(
            "❌ Не удалось загрузить файл" if error else "❌ Подарок не найден",
            show_alert=True,
        )
        return

    await send_document_uncompressed(callback, webp_data, slug, is_callback=True)


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
