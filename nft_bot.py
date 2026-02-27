"""
Telegram NFT Gift Checker Bot
==============================
Отправь ссылку вида  t.me/nft/IceCream-133675
Бот проверит, существует ли подарок, и пришлёт его картинку.

Запуск:
    pip install aiogram aiohttp pillow python-dotenv
    cp .env.example .env   # вставь токен
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
from aiogram.types import BufferedInputFile, Message
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from dotenv import load_dotenv

# ── Загрузка конфига ────────────────────────────────────────────────────────
load_dotenv()  # читает файл .env если есть

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s")
if not BOT_TOKEN:
    raise RuntimeError(
        "Переменная окружения BOT_TOKEN не задана!\n"
        "Создай файл .env и добавь строку: BOT_TOKEN=1234567890:AAxxxx"
    )

# ── Логирование ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Константы ────────────────────────────────────────────────────────────────
FRAGMENT_IMAGE_URL = "https://nft.fragment.com/gift/{slug}.webp"
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)

# Regex: ловит slug из https://t.me/nft/... или t.me/nft/...
NFT_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/nft/([A-Za-z0-9]+(?:[_-][A-Za-z0-9]+)*-\d+)",
    re.IGNORECASE,
)

# ── Глобальная HTTP-сессия (создаётся один раз при старте) ───────────────────
http_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
    return http_session


# ── Вспомогательные функции ─────────────────────────────────────────────────

def extract_nft_slug(text: str) -> Optional[str]:
    """
    Из любого текста достаёт slug NFT-подарка (например 'IceCream-133675').
    Возвращает None если ссылки нет.
    """
    m = NFT_LINK_RE.search(text)
    return m.group(1) if m else None


async def fetch_nft_image(slug: str) -> tuple:
    """
    Скачивает .webp картинку с fragment.com.

    Возвращает (found, raw_bytes, error_message):
      - (True,  bytes, None)   -> подарок найден
      - (False, None,  None)   -> 404, подарок не существует / сожжён
      - (False, None,  str)    -> сетевая или иная ошибка
    """
    url = FRAGMENT_IMAGE_URL.format(slug=slug)
    logger.info("Fetching: %s", url)
    try:
        session = get_session()
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                if not data:
                    return False, None, "Сервер вернул пустой ответ"
                return True, data, None
            elif resp.status == 404:
                return False, None, None          # подарок не найден — штатная ситуация
            else:
                return False, None, f"Неожиданный HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, None, "Сервер не ответил за 20 секунд, попробуй позже"
    except aiohttp.ClientConnectionError as e:
        logger.warning("Connection error: %s", e)
        return False, None, "Ошибка соединения с fragment.com"
    except Exception as e:
        logger.exception("Unexpected error fetching %s", url)
        return False, None, f"Неизвестная ошибка: {e}"


def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
    """
    Конвертирует WebP -> PNG.
    ИСПРАВЛЕН БАГ: Telegram не принимает .webp в answer_photo, нужен PNG/JPEG.
    Возвращает None если Pillow не установлен или конвертация не удалась.
    """
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(webp_bytes)).convert("RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except ImportError:
        logger.warning("Pillow not installed — cannot convert WebP to PNG")
        return None
    except Exception as e:
        logger.error("WebP->PNG conversion failed: %s", e)
        return None


async def safe_delete(msg: Message) -> None:
    """Тихо удаляет сообщение, игнорируя ошибки."""
    try:
        await msg.delete()
    except Exception:
        pass


async def safe_send_photo(
    message: Message,
    photo_bytes: bytes,
    filename: str,
    caption: str,
) -> bool:
    """
    Отправляет фото с обработкой flood-control и других ошибок Telegram.
    Возвращает True при успехе.
    """
    file = BufferedInputFile(photo_bytes, filename=filename)
    try:
        await message.answer_photo(photo=file, caption=caption, parse_mode=ParseMode.HTML)
        return True
    except TelegramRetryAfter as e:
        logger.warning("Flood control: waiting %s sec", e.retry_after)
        await asyncio.sleep(e.retry_after)
        try:
            await message.answer_photo(photo=file, caption=caption, parse_mode=ParseMode.HTML)
            return True
        except Exception as e2:
            logger.error("Retry after flood also failed: %s", e2)
            return False
    except TelegramBadRequest as e:
        logger.error("Bad request sending photo: %s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error sending photo")
        return False


# ── Хэндлеры ────────────────────────────────────────────────────────────────

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "👋 <b>NFT Gift Checker</b>\n\n"
        "Скинь мне ссылку на Telegram NFT-подарок, и я покажу его картинку.\n\n"
        "<b>Поддерживаемые форматы:</b>\n"
        "• <code>https://t.me/nft/IceCream-133675</code>\n"
        "• <code>t.me/nft/DeskCalendar-152473</code>\n\n"
        "Если подарок существует — пришлю изображение.\n"
        "Если нет — объясню почему.",
        parse_mode=ParseMode.HTML,
    )


@dp.message(F.text)
async def handle_text(message: Message) -> None:
    text = message.text or ""
    slug = extract_nft_slug(text)

    if not slug:
        # Не реагируем на обычные сообщения без ссылок
        # (раскомментируй строки ниже если хочешь отвечать на любое сообщение)
        # await message.answer(
        #     "❓ Не нашёл NFT-ссылки.\nПример: <code>t.me/nft/IceCream-133675</code>",
        #     parse_mode=ParseMode.HTML,
        # )
        return

    wait_msg = await message.answer(
        f"🔍 Проверяю <code>{slug}</code>…",
        parse_mode=ParseMode.HTML,
    )

    found, webp_data, error = await fetch_nft_image(slug)

    await safe_delete(wait_msg)

    # ── Случай 1: сетевая / серверная ошибка ─────────────────────────────
    if error:
        await message.answer(
            f"⚠️ Не удалось проверить <b>{slug}</b>\n\n<i>{error}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Случай 2: подарок не найден (404) ────────────────────────────────
    if not found:
        await message.answer(
            f"❌ Подарок <b>{slug}</b> не найден.\n\n"
            "Возможные причины:\n"
            "• Такого номера ещё не существует\n"
            "• Подарок был сожжён\n"
            "• Опечатка в ссылке",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Случай 3: подарок найден ─────────────────────────────────────────
    # ИСПРАВЛЕН БАГ: .webp не поддерживается в answer_photo -> конвертируем в PNG
    png_data = webp_to_png(webp_data)

    caption = (
        f"🎁 <b>{slug}</b>\n"
        f"🔗 <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>"
    )

    if png_data:
        # Отправляем как фото (красиво, с превью)
        success = await safe_send_photo(message, png_data, f"{slug}.png", caption)
        if not success:
            await message.answer(
                f"✅ Подарок <b>{slug}</b> существует, но произошла ошибка при отправке фото.\n"
                f"🔗 <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>",
                parse_mode=ParseMode.HTML,
            )
    else:
        # Pillow не установлен — шлём оригинальный webp как документ (fallback)
        file = BufferedInputFile(webp_data, filename=f"{slug}.webp")
        fallback_caption = caption + "\n<i>(WebP-файл — открой в браузере)</i>"
        try:
            await message.answer_document(
                document=file,
                caption=fallback_caption,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.error("Failed to send document fallback: %s", e)
            await message.answer(
                f"✅ Подарок <b>{slug}</b> существует.\n"
                f"🔗 <a href='https://t.me/nft/{slug}'>Открыть в Telegram</a>\n\n"
                f"<i>Установи Pillow для отображения картинки: pip install pillow</i>",
                parse_mode=ParseMode.HTML,
            )


# ── Startup / Shutdown ───────────────────────────────────────────────────────

async def on_startup() -> None:
    get_session()  # создаём HTTP-сессию заранее
    me = await bot.get_me()
    logger.info("Bot started: @%s (id=%s)", me.username, me.id)


async def on_shutdown() -> None:
    logger.info("Shutting down…")
    global http_session
    if http_session and not http_session.closed:
        await http_session.close()
    await bot.session.close()


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=["message"])


if __name__ == "__main__":
    asyncio.run(main())
