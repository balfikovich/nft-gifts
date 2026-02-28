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
from aiogram.filters import CommandStart
from aiogram.types import (
BufferedInputFile,
CallbackQuery,
ChatMemberUpdated,
InlineKeyboardButton,
InlineKeyboardMarkup,
Message,
MessageEntity,
InlineQuery,
InlineQueryResultArticle,
InlineQueryResultPhoto,
InputTextMessageContent,
)
from aiogram.filters.chat_member_updated import (
ChatMemberUpdatedFilter,
IS_NOT_MEMBER,
IS_MEMBER,
IS_ADMIN,
)
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv

# ── Конфиг ───────────────────────────────────────────────────────────────────

load_dotenv()

BOT_TOKEN: str = os.environ.get(“BOT_TOKEN”, “8748246335:AAGgirhqiuwgnxVO8jYmdhCO7pbThTFiL0s”)
if not BOT_TOKEN:
raise RuntimeError(“BOT_TOKEN не задан! Создай .env: BOT_TOKEN=xxx”)

# ── Логирование ───────────────────────────────────────────────────────────────

logging.basicConfig(
level=logging.INFO,
format=”%(asctime)s [%(levelname)s] %(name)s: %(message)s”,
datefmt=”%Y-%m-%d %H:%M:%S”,
)
logger = logging.getLogger(**name**)

# ── Константы ─────────────────────────────────────────────────────────────────

FRAGMENT_IMAGE_URL = “https://nft.fragment.com/gift/{slug}.webp”
REQUEST_TIMEOUT    = aiohttp.ClientTimeout(total=20)
CB_NO_COMPRESS     = “nocompress:”
AUTHOR             = “@balfikovich”
ANTISPAM_SECONDS   = 1.5    # личка: минимум между разными запросами
ANTISPAM_SLUG_SEC  = 300   # группа: повтор одного подарка не чаще раз в 2 мин

# ── Custom Emoji IDs ──────────────────────────────────────────────────────────

E_GIFT   = “5408829285685291820”
E_MODEL  = “5408894951440279259”
E_BACK   = “5411585799990830248”
E_SYMBOL = “5409189019261103031”
E_LINK   = “5409143419593321597”
E_WARN   = “5409124594751660992”
E_ERR    = “5408930028438188841”
E_START  = “6028495398941759268”

# ── Антиспам ──────────────────────────────────────────────────────────────────

_last_request: dict[int, float] = {}   # user_id → время последнего запроса (личка)
_last_slug: dict[str, float] = {}       # “user_id:slug” → время последнего запроса
_cb_lock: dict[int, bool] = {}          # user_id → блокировка callback

# ── ИСПРАВЛЕНИЕ #5: Ключ теперь “message_id:slug”, а не “user_id:slug”

# Это позволяет разным пользователям нажать кнопку на одном превью

_used_no_compress: set[str] = set()     # “message_id:slug” → уже нажата кнопка

# ── Имя бота (заполняется при старте) ────────────────────────────────────────

BOT_USERNAME: str = “”

def check_antispam(user_id: int) -> float:
“””
Общий антиспам для лички.

```
ИСПРАВЛЕНИЕ #3: счётчик НЕ обновляется если пользователь в блокировке.
Раньше время записывалось при любом вызове, поэтому пользователь,
который слал запросы быстро, никогда не мог «дождаться» окончания паузы.
Теперь время обновляется только когда запрос пропускается (возвращаем 0).
"""
now  = time.monotonic()
last = _last_request.get(user_id, 0.0)
diff = now - last
if diff < ANTISPAM_SECONDS:
    return round(ANTISPAM_SECONDS - diff, 1)
# Пропускаем — только сейчас обновляем время
_last_request[user_id] = now
return 0.0
```

def check_slug_antispam(user_id: int, slug: str) -> float:
“”“Антиспам для группы: один и тот же подарок не чаще раза в 2 минуты.”””
key = f”{user_id}:{slug.lower()}”
now  = time.monotonic()
last = _last_slug.get(key, 0.0)
diff = now - last
if diff < ANTISPAM_SLUG_SEC:
remaining = int(ANTISPAM_SLUG_SEC - diff)
mins = remaining // 60
secs = remaining % 60
return mins * 60 + secs  # возвращаем секунды
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

# ПАРСИНГ SLUG

# ══════════════════════════════════════════════════════════════════════════════

*RE_LINK = re.compile(
r”(?:https?://)?t.me/nft/([A-Za-z0-9]+(?:[*-][A-Za-z0-9]+)*-\d+)”,
re.IGNORECASE,
)
_RE_SLUG = re.compile(r”^([A-Za-z][A-Za-z0-9]*)[-](\d+)$”, re.IGNORECASE)
_RE_WORDS = re.compile(
r”^([A-Za-z][A-Za-z0-9]*(?:\s+[A-Za-z][A-Za-z0-9]*)*)\s+(\d+)$”,
re.IGNORECASE,
)

def extract_nft_slug(raw: str) -> Optional[str]:
text = raw.strip()
m = _RE_LINK.search(text)
if m:
return m.group(1)
m = _RE_SLUG.match(text)
if m:
return f”{m.group(1)}-{m.group(2)}”
m = _RE_WORDS.match(text)
if m:
return f”{m.group(1).replace(’ ’, ‘’)}-{m.group(2)}”
return None

def split_slug(slug: str) -> tuple[str, str]:
parts = slug.rsplit(”-”, 1)
return (parts[0], parts[1]) if len(parts) == 2 else (slug, “”)

def readable_name(raw: str) -> str:
s = re.sub(r”(?<=[a-z])(?=[A-Z])”, “ “, raw)
return re.sub(r”(?<=[A-Z])(?=[A-Z][a-z])”, “ “, s)

# ══════════════════════════════════════════════════════════════════════════════

# АТРИБУТЫ NFT

# ══════════════════════════════════════════════════════════════════════════════

class NftAttrs:
**slots** = (“model”, “model_rarity”, “backdrop”, “backdrop_rarity”,
“symbol”, “symbol_rarity”)

```
def __init__(self) -> None:
    self.model           = "—"
    self.model_rarity    = ""
    self.backdrop        = "—"
    self.backdrop_rarity = ""
    self.symbol          = "—"
    self.symbol_rarity   = ""
```

def _set_attr(attrs: NftAttrs, label: str, value: str, rarity: str) -> None:
label = label.lower().strip()
if not value or value == “—”:
return
if “model” in label and attrs.model == “—”:
attrs.model, attrs.model_rarity = value, rarity
elif (“backdrop” in label or “background” in label) and attrs.backdrop == “—”:
attrs.backdrop, attrs.backdrop_rarity = value, rarity
elif “symbol” in label and attrs.symbol == “—”:
attrs.symbol, attrs.symbol_rarity = value, rarity

async def fetch_nft_attrs(slug: str) -> NftAttrs:
attrs   = NftAttrs()
url     = f”https://t.me/nft/{slug}”
headers = {
“User-Agent”: (
“Mozilla/5.0 (Windows NT 10.0; Win64; x64) “
“AppleWebKit/537.36 (KHTML, like Gecko) “
“Chrome/124.0.0.0 Safari/537.36”
),
“Accept-Language”: “en-US,en;q=0.9”,
}
try:
async with get_session().get(url, headers=headers) as resp:
if resp.status != 200:
logger.warning(“t.me/nft/%s -> HTTP %s”, slug, resp.status)
return attrs
html = await resp.text()

```
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
```

# ══════════════════════════════════════════════════════════════════════════════

# ЗАГРУЗКА ИЗОБРАЖЕНИЯ

# ══════════════════════════════════════════════════════════════════════════════

async def fetch_nft_image(slug: str) -> tuple:
url = FRAGMENT_IMAGE_URL.format(slug=slug)
try:
async with get_session().get(url) as resp:
if resp.status == 200:
data = await resp.read()
return (False, None, “Пустой ответ”) if not data else (True, data, None)
elif resp.status == 404:
return False, None, None
return False, None, f”HTTP {resp.status}”
except asyncio.TimeoutError:
return False, None, “Таймаут (20 сек)”
except aiohttp.ClientConnectionError:
return False, None, “Ошибка соединения”
except Exception as e:
return False, None, f”Ошибка: {e}”

def webp_to_png(webp_bytes: bytes) -> Optional[bytes]:
try:
from PIL import Image
img = Image.open(io.BytesIO(webp_bytes)).convert(“RGBA”)
buf = io.BytesIO()
img.save(buf, format=“PNG”)
return buf.getvalue()
except Exception as e:
logger.error(“WebP→PNG: %s”, e)
return None

# ══════════════════════════════════════════════════════════════════════════════

# CAPTION через entities

# parse_mode=None ОБЯЗАТЕЛЕН — иначе Telegram игнорирует entities

# ══════════════════════════════════════════════════════════════════════════════

def _utf16_len(s: str) -> int:
return len(s.encode(“utf-16-le”)) // 2

def _utf16_offset(text_so_far: str) -> int:
return _utf16_len(text_so_far)

def make_caption(slug: str, attrs: NftAttrs) -> tuple[str, list[MessageEntity]]:
name, number = split_slug(slug)
nice = readable_name(name)

```
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
```

def make_keyboard(slug: str) -> InlineKeyboardMarkup:
return InlineKeyboardMarkup(inline_keyboard=[[
InlineKeyboardButton(
text=“📤 Отправить без сжатия”,
callback_data=f”{CB_NO_COMPRESS}{slug}”,
)
]])

# ══════════════════════════════════════════════════════════════════════════════

# ТЕКСТЫ — приветствие в группе и /start в личке

# ══════════════════════════════════════════════════════════════════════════════

def get_group_welcome(chat_title: str) -> str:
“”“Приветственное сообщение когда бота добавляют в группу.”””
return (
f”👋 <b>Привет, {chat_title}!</b>\n\n”
f”Я <b>NFT Gift Viewer</b> — показываю карточку любого Telegram NFT-подарка: “
f”картинку, модель, фон, символ и редкость.\n\n”
f”<code>━━━━━━━━━━━━━━━━━━━━</code>\n”
f”<b>📌 Как пользоваться:</b>\n\n”
f”Напиши слово <b>превью</b> и через пробел ссылку или название подарка.\n\n”
f”<b>✅ Примеры:</b>\n”
f”<code>превью https://t.me/nft/PlushPepe-22</code>\n”
f”<code>превью t.me/nft/PlushPepe-22</code>\n”
f”<code>превью PlushPepe-22</code>\n”
f”<code>превью PlushPepe 22</code>\n”
f”<code>превью Plush Pepe 22</code>\n\n”
f”<code>━━━━━━━━━━━━━━━━━━━━</code>\n”
f”<b>📋 Правила:</b>\n”
f”• Сообщение должно начинаться со слова <b>превью</b>\n”
f”• Повтор одного подарка — не чаще <b>1 раза в 2 минуты</b>\n”
f”• Кнопка <b>«Отправить без сжатия»</b> — только 1 раз на превью\n\n”
f”⚡ Результат приходит за ~1–2 сек\n\n”
f”<i>Автор бота: <a href='https://t.me/balfikovich'>@balfikovich</a></i>”
)

def get_start_text() -> str:
“”“Текст /start в личке.”””
bot_name = BOT_USERNAME or “бот”
return (
f’<tg-emoji emoji-id="{E_START}">✨</tg-emoji> <b>NFT Gift Viewer</b>\n’
f”<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n”
f”Показываю картинку, модель, фон, символ и редкость любого Telegram NFT-подарка.\n\n”
f”<b>📨 Как пользоваться в личке:</b>\n”
f”Просто отправь ссылку или название подарка — получишь карточку.\n\n”
f”<b>✅ Форматы (личка — без префикса):</b>\n”
f”<code>https://t.me/nft/PlushPepe-22</code>\n”
f”<code>t.me/nft/PlushPepe-22</code>\n”
f”<code>PlushPepe-22</code>\n”
f”<code>PlushPepe 22</code>\n”
f”<code>Plush Pepe 22</code>\n\n”
f”<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n”
f”<b>👥 Использование в группе / чате:</b>\n”
f”В группе нужно писать слово <b>превью</b> перед названием:\n”
f”<code>превью PlushPepe 22</code>\n”
f”<code>превью t.me/nft/PlushPepe-22</code>\n\n”
f”<b>📋 Правила в группе:</b>\n”
f”• Повтор одного подарка — не чаще <b>1 раза в 2 минуты</b>\n”
f”• <b>«Без сжатия»</b> — только 1 раз на одно превью\n\n”
f”<b>🚀 Добавить бота в группу:</b>\n”
f”1. Нажми кнопку <b>«Добавить в группу»</b> ниже\n”
f”2. Выбери свой чат\n”
f”3. Дай боту права <b>администратора</b>\n”
f”4. Бот напишет приветствие с правилами\n\n”
f”<code>━━━━━━━━━━━━━━━━━━━━</code>\n”
f”⚡ Проверка ~1–2 сек\n\n”
f”<i>Автор: <a href='https://t.me/balfikovich'>@balfikovich</a></i>”
)

# ══════════════════════════════════════════════════════════════════════════════

# ОТПРАВКА

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
file          = BufferedInputFile(png_bytes, filename=f”{slug}.png”)
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
file = BufferedInputFile(png_bytes, filename=f”{slug}.png”)
await message.answer_photo(
photo=file,
caption=caption,
caption_entities=ents,
parse_mode=None,
reply_markup=kbd,
)
return True
except Exception as ex:
logger.error(“Retry failed: %s”, ex)
return False
except TelegramBadRequest as e:
logger.error(“BadRequest: %s”, e)
return False
except Exception:
logger.exception(“send_photo error”)
return False

async def send_document_only(send_fn, webp_bytes: bytes, slug: str) -> None:
file = BufferedInputFile(webp_bytes, filename=f”{slug}.png”)
try:
await send_fn(document=file)
except TelegramRetryAfter as e:
await asyncio.sleep(e.retry_after)
file = BufferedInputFile(webp_bytes, filename=f”{slug}.png”)
await send_fn(document=file)
except Exception as e:
logger.error(“send_document error: %s”, e)

# ══════════════════════════════════════════════════════════════════════════════

# BOT & DISPATCHER

# ══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# ── Бот добавлен в группу — приветствие ──────────────────────────────────────

@dp.my_chat_member()
async def on_bot_added_to_group(event: ChatMemberUpdated) -> None:
if event.chat.type not in (“group”, “supergroup”):
return

```
old_status = event.old_chat_member.status
new_status = event.new_chat_member.status

was_outside = old_status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED, "left", "kicked")
now_inside  = new_status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, "member", "administrator")

if was_outside and now_inside:
    chat_title = event.chat.title or "этот чат"
    try:
        await bot.send_message(
            chat_id=event.chat.id,
            text=get_group_welcome(chat_title),
            parse_mode=ParseMode.HTML,
        )
        logger.info("✅ Приветствие отправлено в чат: %s (%s)", chat_title, event.chat.id)
    except TelegramForbiddenError:
        logger.warning("Нет прав писать в чат %s", event.chat.id)
    except Exception as e:
        logger.error("Ошибка отправки приветствия: %s", e)
```

# ── /start ────────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
if message.chat.type != “private”:
return

```
buttons = []
if BOT_USERNAME:
    buttons.append([
        InlineKeyboardButton(
            text="➕ Добавить в группу",
            url=f"https://t.me/{BOT_USERNAME}?startgroup",
        )
    ])

reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
await message.answer(
    get_start_text(),
    parse_mode=ParseMode.HTML,
    reply_markup=reply_markup,
)
```

# ── Обработка текста (личка + группа) ────────────────────────────────────────

@dp.message(F.text)
async def handle_text(message: Message) -> None:
# Защита от сообщений без отправителя (каналы, системные)
if not message.from_user:
return

```
raw_text = (message.text or "").strip()
is_private = message.chat.type == "private"

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

user_id = message.from_user.id

if not is_private:
    slug_wait = check_slug_antispam(user_id, slug)
    if slug_wait > 0:
        mins = slug_wait // 60
        secs = slug_wait % 60
        time_str = f"{mins} мин {secs} сек" if mins > 0 else f"{secs} сек"
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
```

# ── Кнопка «Отправить без сжатия» ────────────────────────────────────────────

@dp.callback_query(F.data.startswith(CB_NO_COMPRESS))
async def callback_no_compress(callback: CallbackQuery) -> None:
user_id = callback.from_user.id
slug    = callback.data[len(CB_NO_COMPRESS):]

```
if _cb_lock.get(user_id):
    await callback.answer("⏳ Подожди, идёт загрузка…", show_alert=False)
    return

wait_sec = check_antispam(user_id)
if wait_sec > 0:
    await callback.answer(f"⏳ Подожди {wait_sec} сек.", show_alert=True)
    return

# ИСПРАВЛЕНИЕ #5: ключ теперь по message_id, а не по user_id
# Разные пользователи могут нажать кнопку на одном и том же превью
message_id = callback.message.message_id
no_compress_key = f"{message_id}:{slug.lower()}"

if no_compress_key in _used_no_compress:
    await callback.answer(
        "❌ Оригинал уже был отправлен для этого превью!",
        show_alert=True,
    )
    return

_cb_lock[user_id] = True
await callback.answer("⏳ Загружаю оригинал…", show_alert=False)

try:
    found, webp_data, error = await fetch_nft_image(slug)
    if error or not found:
        await callback.message.answer(
            "❌ Не удалось загрузить" if error else "❌ Подарок не найден"
        )
        return

    # Помечаем кнопку как использованную (по сообщению, не по пользователю)
    _used_no_compress.add(no_compress_key)

    # ИСПРАВЛЕНИЕ #4: убираем кнопку с сообщения после нажатия
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass  # если не удалось убрать кнопку — не критично

    await send_document_only(callback.message.answer_document, webp_data, slug)
finally:
    _cb_lock[user_id] = False
```

# ══════════════════════════════════════════════════════════════════════════════

# INLINE-РЕЖИМ

# ══════════════════════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_handler(query: InlineQuery) -> None:
raw = (query.query or “”).strip()

```
if not raw:
    hint = InlineQueryResultArticle(
        id="hint",
        title="🎁 NFT Gift Viewer",
        description="Введите ссылку или название → PlushPepe-22 / Plush Pepe 22",
        thumbnail_url="https://nft.fragment.com/gift/PlushPepe-1.webp",
        input_message_content=InputTextMessageContent(
            message_text=(
                f"<b>NFT Gift Viewer</b>\n\n"
                f"Добавь бота в чат и отправляй ссылки или названия подарков прямо в чат!\n\n"
                f"<code>t.me/nft/PlushPepe-22</code>\n"
                f"<code>PlushPepe 22</code>"
            ),
            parse_mode=ParseMode.HTML,
        ),
    )
    await query.answer(results=[hint], cache_time=60, is_personal=False)
    return

slug = extract_nft_slug(raw)

if not slug:
    err = InlineQueryResultArticle(
        id="err_format",
        title="❌ Неверный формат",
        description="Пример: PlushPepe-22 / Plush Pepe 22 / t.me/nft/...",
        input_message_content=InputTextMessageContent(
            message_text=(
                f"<b>Неверный формат</b>\n\n"
                f"<code>t.me/nft/PlushPepe-22</code>\n"
                f"<code>PlushPepe 22</code>"
            ),
            parse_mode=ParseMode.HTML,
        ),
    )
    await query.answer(results=[err], cache_time=5, is_personal=True)
    return

found, webp_data, error, attrs = await process_slug(slug)
name, number = split_slug(slug)
nice         = readable_name(name)
title        = f"🎁 {nice} #{number}"

if error or not found:
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
```

# ══════════════════════════════════════════════════════════════════════════════

# STARTUP / SHUTDOWN

# ══════════════════════════════════════════════════════════════════════════════

async def on_startup() -> None:
global BOT_USERNAME
get_session()
me = await bot.get_me()
BOT_USERNAME = me.username or “”
logger.info(“✅ Bot started: @%s (id=%s)”, me.username, me.id)
logger.info(“━” * 60)
logger.info(“ЧЕКЛИСТ:”)
logger.info(”  1. @BotFather → /setprivacy → @%s → Disable”, me.username)
logger.info(”     БЕЗ ЭТОГО бот не видит сообщения в группах!”)
logger.info(”  2. @BotFather → /setjoingroups → @%s → Enable”, me.username)
logger.info(”  3. @BotFather → /setinline → @%s → placeholder”, me.username)
logger.info(”  4. В группе дать боту права администратора”)
logger.info(“━” * 60)

async def on_shutdown() -> None:
logger.info(“🛑 Shutting down…”)
global http_session
if http_session and not http_session.closed:
await http_session.close()
await bot.session.close()

async def main() -> None:
dp.startup.register(on_startup)
dp.shutdown.register(on_shutdown)
await dp.start_polling(
bot,
allowed_updates=[“message”, “callback_query”, “inline_query”, “my_chat_member”],
)

if **name** == “**main**”:
asyncio.run(main())