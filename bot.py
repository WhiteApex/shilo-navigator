import os
import json
import shutil
import asyncio
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Callable, Awaitable, Dict, Any, List, Tuple, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties

from datetime import date as date_type
from datetime import datetime, timedelta


from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramBadRequest,
    TelegramNetworkError,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    FSInputFile,
    InputMediaPhoto,
    ReplyKeyboardRemove,
)

from aiogram import BaseMiddleware

from keyboards import (
    events_keyboard,
    event_card_keyboard,
    phone_share_keyboard,
    broadcast_scope_keyboard,
    event_card_keyboard_registered,
    confirmation_keyboard,
    confirmations_events_keyboard,
    send_confirmations_events_keyboard,
    send_confirmations_mode_keyboard,
    adults_count_keyboard,
    children_count_keyboard,
    confirm_change_details_keyboard,
    event_cta_keyboard,
    cancel_event_broadcast_keyboard,
    cancel_event_user_keyboard,
    sendconf_preview_keyboard
)




from db import Database
from db import DB_PATH as DB_FILE

from make_tree import write_tree

# ---------- Настройки ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_WELCOME_IMAGE = os.getenv("WELCOME_IMAGE_PATH", "assets\welcome 12-01.jpg")
EVENTS_JSON_PATH = os.getenv("EVENTS_JSON_PATH", "events.json")
ADMIN_ID = int(os.getenv("ADMIN_ID", "353090716"))  # 0 = выключено
START_TEXT_PATH = os.getenv("START_TEXT_PATH", "assets/start_text.html")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

BASE_DIR = Path(__file__).resolve().parent
DB_ABS_PATH = Path(DB_FILE).resolve()


dp = Dispatcher()
db = Database()

# ---------- Состояния ----------
class BroadcastStates(StatesGroup):
    waiting_text = State()

class RegisterFlow(StatesGroup):
    waiting_phone = State()

class CreateEventStates(StatesGroup):
    waiting_data = State()

class EditEventStates(StatesGroup):
    waiting_new_desc = State()

class EditEventPhotoStates(StatesGroup):
    waiting_new_photo = State()

class CancelEventStates(StatesGroup):
    waiting_text = State()
    waiting_event_choice = State()

class ConfirmCountsStates(StatesGroup):
    waiting_adults = State()
    waiting_children = State()

class EditWelcomePhotoStates(StatesGroup):
    waiting_new_photo = State()

class SendConfirmationsStates(StatesGroup):
    waiting_program = State()
    waiting_preview_confirm = State()


# ---------- Утилиты ----------
SUCCESS_TEXT = "✅ Вы зарегистрированы"
PROMPT_PHONE_TEXT = (
    "Чтобы записать вас на мероприятие, нужен номер телефона. "
    "Нажмите кнопку ниже — Telegram отправит ваш номер автоматически."
)

CANCEL_EVENT_TEMPLATE = (
    "Здравствуйте!\n\n"
    "К сожалению, мероприятие «{title}»{date_part} не состоится. "
    "Приносим извинения за доставленные неудобства.\n\n"
    "Вы можете выбрать другое мероприятие в общем меню."
)

def _format_user_name_row(user_row) -> str:
    """
    Принимает строку из таблицы users (SELECT * FROM users ...).
    Ожидаемый порядок полей смотри в CREATE_USERS_SQL.
    """
    # users: user_id, username, first_name, last_name, language, created_at, last_seen, phone, last_event_id...
    _uid, username, first_name, last_name, *_rest = user_row

    if username:
        return f"@{username}"
    full = " ".join(p for p in [first_name, last_name] if p)
    return full or "друг"


async def delete_after_delay(bot: Bot, chat_id: int, message_id: int, delay: int = 0):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass



@dp.callback_query(F.data.startswith("confirm_yes_"))
async def on_confirm_yes(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    event_id = int(callback.data.split("_")[2])

    existing = await db.get_confirmation_details(event_id, user.id)

    # Если уже подтвердил "да" и данные заполнены — не перетираем, а предлагаем выбор
    if existing and existing[0] == "yes" and existing[1] is not None and existing[2] is not None:
        adults_count, children_count = existing[1], existing[2]

        try:
            await callback.message.edit_reply_markup()
        except Exception:
            pass

        await callback.message.answer(
            "Вы уже подтверждали участие.\n"
            f"Взрослых: {adults_count}\n"
            f"Детей: {children_count}\n\n"
            "Оставляем как есть или хотите изменить?",
            reply_markup=confirm_change_details_keyboard(event_id),
        )
        return await callback.answer()

    # Если подтверждения не было или было "no" — фиксируем "yes" (counts спросим дальше)
    if (not existing) or (existing[0] != "yes"):
        await db.set_confirmation(event_id, user.id, "yes", None, None)

    try:
        await callback.message.edit_reply_markup()  # убираем кнопки «Да/Нет»
    except Exception:
        pass

    await state.set_state(ConfirmCountsStates.waiting_adults)
    await state.update_data(event_id=event_id)

    await callback.message.answer(
        "Сколько будет взрослых?",
        reply_markup=adults_count_keyboard(event_id),
    )
    await callback.answer("Принято ✅", show_alert=False)


@dp.callback_query(F.data.startswith("confirm_no_"))
async def on_confirm_no(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    event_id = int(callback.data.split("_")[2])

    await db.set_confirmation(event_id, user.id, "no", None, None)
    await state.clear()
    await callback.answer("Понял, планы поменялись ❌", show_alert=False)

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass


@dp.callback_query(F.data.startswith("conf_adults_"))
async def cb_conf_adults(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "event_id" not in data:
        return await callback.answer("Сценарий подтверждения не активен.", show_alert=True)

    _prefix, _adults, event_id_s, count_s = callback.data.split("_", 3)
    event_id = int(event_id_s)
    adults_count = int(count_s)

    if event_id != int(data["event_id"]):
        return await callback.answer("Это подтверждение относится к другому мероприятию.", show_alert=True)

    await state.update_data(adults_count=adults_count)
    await state.set_state(ConfirmCountsStates.waiting_children)

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.message.answer(
        "Сколько детей?",
        reply_markup=children_count_keyboard(event_id),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("conf_children_"))
async def cb_conf_children(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "event_id" not in data or "adults_count" not in data:
        return await callback.answer("Сценарий подтверждения не активен.", show_alert=True)

    _prefix, _children, event_id_s, count_s = callback.data.split("_", 3)
    event_id = int(event_id_s)
    children_count = int(count_s)

    if event_id != int(data["event_id"]):
        return await callback.answer("Это подтверждение относится к другому мероприятию.", show_alert=True)

    adults_count = int(data["adults_count"])

    await db.set_confirmation(event_id, callback.from_user.id, "yes", adults_count, children_count)
    await state.clear()

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.message.answer("Спасибо! Записал детали.")
    await callback.answer()

def _event_caption(title: str, desc: str | None) -> str:
    return f"{title}\n\n{desc or ''}"

def load_start_text() -> str:
    try:
        with open(START_TEXT_PATH, "r", encoding="utf-8") as f:
            text = f.read().strip()
            if text:
                return text
    except Exception:
        pass
    return "Привет! 👋 Выберите мероприятие из списка ниже:"

def _clean_phone(raw: str) -> str:
    # минимальная очистка под E.164: оставляем + и цифры
    return "".join(ch for ch in raw if ch.isdigit() or ch == "+")

async def sync_events_from_json() -> int:
    """Читает events.json и делает upsert в таблицу events. Возвращает число синхронизированных записей."""
    if not os.path.exists(EVENTS_JSON_PATH):
        logger.warning("events.json не найден: %s", EVENTS_JSON_PATH)
        return 0
    try:
        with open(EVENTS_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        count = 0
        for item in data:
            await db.upsert_event(
                id=int(item["id"]),
                title=item["title"],
                description=item.get("description"),
                photo_path=item.get("photo_path"),
                starts_at=item.get("starts_at"),
                ends_at=item.get("ends_at"),
                is_public=item.get("is_public", True),
            )

            count += 1
        return count
    except Exception as e:
        logger.exception("Ошибка чтения events.json: %s", e)
        return 0

async def render_events_media() -> tuple[Optional[str], bool, str]:
    """
    returns: (media, is_file_id, caption)
    media = Telegram file_id OR filesystem path
    """
    caption = load_start_text()

    # 1) приоритет: file_id из БД
    fid = await db.get_setting("welcome_photo_file_id")
    if fid:
        return fid, True, caption

    # 2) fallback: файл на диске
    image_path = DEFAULT_WELCOME_IMAGE if os.path.exists(DEFAULT_WELCOME_IMAGE) else None
    return image_path, False, caption


def extract_start_payload(text: Optional[str]) -> Optional[str]:
    """
    Возвращает payload из команды '/start <payload>', если он есть.
    Пример: '/start sber' -> 'sber'
    """
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    return payload or None


async def safe_send_photo(bot: Bot, chat_id: int, path: str, **kwargs):
    try:
        return await bot.send_photo(chat_id, FSInputFile(path), **kwargs)
    except TelegramNetworkError:
        await asyncio.sleep(0.8)
        return await bot.send_photo(chat_id, FSInputFile(path), **kwargs)


async def show_event_card(message: Message, bot: Bot, event_id: int, user_id: int):
    event = await db.get_event(event_id)
    if not event:
        await bot.send_message(message.chat.id, "Мероприятие не найдено.")
        return

    eid, title, desc, photo_path, starts_at, ends_at, photo_file_id = event
    registered = await db.is_registered(eid, user_id)
    phone = await db.get_user_phone(user_id)

    # UI по правилам:
    if registered and phone:
        caption = _event_caption(title, desc)
        kb = event_card_keyboard_registered(eid)
    else:
        caption = _event_caption(title, desc)
        kb = event_card_keyboard(eid)  # с кнопкой "Записаться"

    # отрисовка
    if photo_file_id:
        media = InputMediaPhoto(media=photo_file_id, caption=caption)
        await message.edit_media(media=media, reply_markup=kb)
        return

    path = photo_path if (photo_path and os.path.exists(photo_path)) else (
        DEFAULT_WELCOME_IMAGE if os.path.exists(DEFAULT_WELCOME_IMAGE) else None
    )
    if path:
        tmp = await bot.send_photo(message.chat.id, FSInputFile(path))
        try:
            fid = tmp.photo[-1].file_id
            await db.set_event_photo_file_id(eid, fid)
        finally:
            try:
                await bot.delete_message(message.chat.id, tmp.message_id)
            except Exception:
                pass

        media = InputMediaPhoto(media=fid, caption=caption)
        await message.edit_media(media=media, reply_markup=kb)
    else:
        await message.edit_caption(caption, reply_markup=kb)

# ---------- Middleware ----------
class AutoRegisterMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event, data: Dict[str, Any]):
        user = data.get("event_from_user")
        if user:
            await db.upsert_user(
                user_id=user.id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name,
                language=user.language_code,
            )
        return await handler(event, data)

dp.message.middleware(AutoRegisterMiddleware())
dp.callback_query.middleware(AutoRegisterMiddleware())

# ---------- Хэндлеры ----------
@dp.message(CommandStart())
async def handle_start(message: Message, bot: Bot):
    user = message.from_user

    await db.upsert_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language=user.language_code,
    )

    payload = extract_start_payload(message.text)

    if payload:
        # записываем реф-код к пользователю (для аналитики, один раз)
        await db.save_referral(user.id, payload)

        # ищем соответствующее событие
        event_id = await db.get_event_for_referral(payload)

        if event_id:
            # показываем карточку СРАЗУ, даже если событие приватное (is_public = 0)
            await send_event_card_message(
                chat_id=message.chat.id,
                bot=bot,
                event_id=event_id,
                user_id=user.id,
                force_registered=False,  # не считаем записанным по факту перехода
            )
            return

    # если payload пустой или код неизвестен — обычный список ПУБЛИЧНЫХ событий
    await show_events_list(
        message.chat.id,
        bot,
        user_id=message.from_user.id,
    )

@dp.callback_query(F.data == "back_to_events")
async def back_to_events(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    try:
        await show_events_list(
            callback.message.chat.id,
            bot,
            user_id=callback.from_user.id,
            edit_message=callback.message,
        )
    except Exception:
        await show_events_list(
            callback.message.chat.id,
            bot,
            user_id=callback.from_user.id,
        )

@dp.callback_query(F.data.startswith("sendconf_choose_"))
async def cb_sendconf_choose(callback: CallbackQuery):
    event_id = int(callback.data.split("_")[-1])
    try:
        await callback.message.edit_text(
            "Кому отправить запросы на подтверждение?",
            reply_markup=send_confirmations_mode_keyboard(event_id),
        )
    except Exception:
        pass
    await callback.answer()

@dp.callback_query(F.data.startswith("event_"))
async def open_event(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    event_id = int(callback.data.split("_", 1)[1])

    # фиксируем контекст — на каком событии пользователь «остановился»
    await db.set_last_event(callback.from_user.id, event_id)

    try:
        # нормальный путь — редактируем текущее сообщение (список → карточка)
        await show_event_card(callback.message, bot, event_id, callback.from_user.id)
    except Exception:
        # fallback: удаляем старое и шлём НОВУЮ карточку того же события (не меню!)
        try:
            await bot.delete_message(callback.message.chat.id, callback.message.message_id)
        except Exception:
            pass
        await send_event_card_message(
            chat_id=callback.message.chat.id,
            bot=bot,
            event_id=event_id,
            user_id=callback.from_user.id,
            force_registered=False
        )

@dp.callback_query(F.data.startswith("register_"))
async def register_user(callback: CallbackQuery, bot: Bot, state: FSMContext):
    user = callback.from_user
    event_id = int(callback.data.split("_", 1)[1])

    # фиксируем последнее событие сразу при клике "Записаться"
    await db.set_last_event(user.id, event_id)

    await db.upsert_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language=user.language_code
    )

    phone = await db.get_user_phone(user.id)
    registered = await db.is_registered(event_id, user.id)

    if registered and phone:
        try:
            await callback.message.edit_reply_markup(reply_markup=event_card_keyboard_registered(event_id))
        except Exception:
            # если редактировать нельзя — удаляем старую карточку и шлём новую того же события
            try:
                await bot.delete_message(callback.message.chat.id, callback.message.message_id)
            except Exception:
                pass
            await send_event_card_message(callback.message.chat.id, bot, event_id, user.id, force_registered=True)
        return await callback.answer("Вы уже записаны ✅", show_alert=False)

    if not phone:
        await state.set_state(RegisterFlow.waiting_phone)
        await state.update_data(
            event_id=event_id,
            card_msg_id=callback.message.message_id,
            chat_id=callback.message.chat.id,
        )
        req_msg = await callback.message.answer(
            PROMPT_PHONE_TEXT,
            reply_markup=phone_share_keyboard()
        )
        await state.update_data(
            prompt_msg_id=req_msg.message_id,
            prompt_chat_id=req_msg.chat.id,
        )
        asyncio.create_task(delete_after_delay(bot, req_msg.chat.id, req_msg.message_id, 30))
        await callback.answer()
        return

    if not registered and phone:
        await db.register(event_id, user.id)
        # оставляем на карточке; если редактировать нельзя — удалим и пошлём новую карточку того же события
        try:
            await callback.message.edit_reply_markup(reply_markup=event_card_keyboard_registered(event_id))
        except Exception:
            try:
                await bot.delete_message(callback.message.chat.id, callback.message.message_id)
            except Exception:
                pass
            await send_event_card_message(callback.message.chat.id, bot, event_id, user.id, force_registered=True)
        return await callback.answer("Вы успешно записаны! 🎉", show_alert=False)


async def send_event_card_message(chat_id: int, bot: Bot, event_id: int, user_id: int, *, force_registered: bool = False):
    event = await db.get_event(event_id)
    if not event:
        await bot.send_message(chat_id, "Мероприятие не найдено.")
        return

    eid, title, desc, photo_path, starts_at, ends_at, photo_file_id = event
    registered = await db.is_registered(eid, user_id)
    phone = await db.get_user_phone(user_id)

    if force_registered or (registered and phone):
        caption = f"{title}\n\n{desc or ''}"
        kb = event_card_keyboard_registered(eid)
    else:
        caption = f"{title}\n\n{desc or ''}"
        kb = event_card_keyboard(eid)

    if photo_file_id:
        await bot.send_photo(
            chat_id,
            photo_file_id,
            caption=caption,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )
        return

    path = photo_path if (photo_path and os.path.exists(photo_path)) else (
        DEFAULT_WELCOME_IMAGE if os.path.exists(DEFAULT_WELCOME_IMAGE) else None
    )
    if path:
        msg = await bot.send_photo(
            chat_id,
            FSInputFile(path),
            caption=caption,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )
        try:
            fid = msg.photo[-1].file_id
            await db.set_event_photo_file_id(eid, fid)
        except Exception:
            pass
    else:
        await bot.send_message(
            chat_id,
            caption,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )

# ---- ДОБАВЛЕНО: обработка контакта и фолбэк ----
@dp.message(RegisterFlow.waiting_phone, F.contact)
async def on_contact_shared(message: Message, state: FSMContext, bot: Bot):
    contact = message.contact
    if not contact or not contact.phone_number or contact.user_id != message.from_user.id:
        return await message.answer("Нажмите кнопку «📱 Поделиться номером» — Telegram отправит ваш номер автоматически.")

    cleaned = _clean_phone(contact.phone_number)
    await db.set_user_phone(message.from_user.id, cleaned)

    data = await state.get_data()
    event_id     = int(data["event_id"])
    await db.set_last_event(message.from_user.id, event_id)
    card_msg_id  = data.get("card_msg_id")    # старая карточка бота
    chat_id      = data.get("chat_id")
    prompt_id    = data.get("prompt_msg_id")  # сообщение «нужен номер»
    prompt_chat  = data.get("prompt_chat_id")

    if not await db.is_registered(event_id, message.from_user.id):
        await db.register(event_id, message.from_user.id)

    # Удаляем наши старые сообщения
    if prompt_id and prompt_chat:
        asyncio.create_task(delete_after_delay(bot, prompt_chat, prompt_id, 0))
    if card_msg_id and chat_id:
        asyncio.create_task(delete_after_delay(bot, chat_id, card_msg_id, 0))

    # Отправляем НОВУЮ карточку выбранного события внизу
    await send_event_card_message(
        chat_id=message.chat.id,
        bot=bot,
        event_id=event_id,
        user_id=message.from_user.id,
        force_registered=True
    )

    # Короткое спасибо — и убрать через 5 сек
    ok = await message.answer("Спасибо! Номер сохранён ✅", reply_markup=ReplyKeyboardRemove())
    asyncio.create_task(delete_after_delay(bot, ok.chat.id, ok.message_id, 5))

    await state.clear()

@dp.message(RegisterFlow.waiting_phone)
async def on_waiting_phone_wrong_input(message: Message):
    await message.answer(
        "Чтобы завершить запись, отправьте номер через кнопку ниже. "
        "Иначе мы не сможем подтвердить участие.",
    )

# ---------- Админ команды ----------
@dp.message(Command("info"))
async def admin_info(message: Message):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    users_total, events_total, regs_total = await db.stats_totals()
    per_event_total = await db.stats_per_event()

    since_dt = datetime.utcnow() - timedelta(hours=24)
    since_iso = since_dt.replace(microsecond=0).isoformat() + "Z"

    users_24h, regs_24h = await db.stats_last_24h(since_iso)
    per_event_24h = await db.stats_per_event_last_24h(since_iso)

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

    lines = [
        "<b>Статистика бота</b>",
        f"🕒 Обновлено: <code>{ts}</code>",
        "",
        "— <b>ИТОГО</b> —",
        f"👥 Пользователи: <b>{users_total}</b>",
        f"🗓️ Мероприятия: <b>{events_total}</b>",
        f"✅ Записей всего: <b>{regs_total}</b>",
        "",
        "— <b>ЗА 24 ЧАСА</b> —",
        f"🆕 Новые пользователи: <b>{users_24h}</b>",
        f"🆕 Новых записей: <b>{regs_24h}</b>",
        "",
        "<b>По мероприятиям (всё время):</b>",
    ]

    if not per_event_total:
        lines.append("— нет событий —")
    else:
        for eid, title, cnt in per_event_total:
            lines.append(f"• <code>[{eid}]</code> {title} — <b>{cnt}</b>")

    lines += ["", "<b>По мероприятиям (за 24 часа):</b>"]
    if not per_event_24h:
        lines.append("— нет событий —")
    else:
        for eid, title, cnt24 in per_event_24h:
            lines.append(f"• <code>[{eid}]</code> {title} — <b>{cnt24}</b>")

    await message.reply("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


@dp.message(Command("addevent"))
async def cmd_add_event(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    await message.answer(
        "Отправьте данные мероприятия в одном сообщении в формате:\n"
        "<b>ДД.ММ Название</b>\n"
        "потом пустая строка и текст описания.\n\n"
        "Пример:\n"
        "02.12 Мастер-класс по дому\n"
        "\n"
        "Подробное описание тут...",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(CreateEventStates.waiting_data)

@dp.message(Command("get_db"))
async def get_db_dump(message: Message, bot: Bot):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    if not DB_ABS_PATH.exists():
        return await message.reply(f"Файл БД не найден: {DB_ABS_PATH}")
    a = os.getenv("DB_PATH")
    await message.reply(f"Путь env: {a}")
    await message.reply(f"Файл БД путь: {DB_ABS_PATH}")

    db_tmp_path = BASE_DIR / "db_backup.sqlite3"
    tree_tmp_path = BASE_DIR / "project_tree.txt"

    try:
        shutil.copyfile(DB_ABS_PATH, db_tmp_path)
        size = db_tmp_path.stat().st_size
        await bot.send_document(
            chat_id=message.chat.id,
            document=FSInputFile(str(db_tmp_path)),
            caption=f"Бэкап БД ({size} байт)",
        )

        write_tree(
            root=str(BASE_DIR),
            out_file=str(tree_tmp_path),
            ignore={".git", "node_modules", ".venv", "__pycache__"},
            max_depth=None,
        )
        await bot.send_document(
            chat_id=message.chat.id,
            document=FSInputFile(str(tree_tmp_path)),
            caption="Структура проекта",
        )
    finally:
        for p in (db_tmp_path, tree_tmp_path):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass

@dp.message(Command("broadcast"))
async def broadcast_start(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    await state.set_state(BroadcastStates.waiting_text)
    await message.reply(
        "Пришлите текст рассылки (HTML разрешён). "
        "После этого я предложу выбрать аудиторию."
    )

@dp.message(Command("cancel_event"))
async def cancel_event_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    await state.set_state(CancelEventStates.waiting_text)

    await message.reply(
        "Отправьте текст рассылки.\n\n"
        "Если хотите использовать базовый шаблон — отправьте «БАЗОВЫЙ»."
    )


@dp.message(CancelEventStates.waiting_text)
async def cancel_event_receive_text(message: Message, state: FSMContext):
    admin_text = message.text.strip()

    if admin_text.upper() == "БАЗОВЫЙ":
        await state.update_data(custom_text=None)
    else:
        await state.update_data(custom_text=admin_text)

    # используем глобальный db
    events = await db.list_events_raw_admin()
    events = _filter_upcoming_events(events)

    if not events:
        await message.reply("Нет мероприятий для отмены.")
        return await state.clear()

    await state.set_state(CancelEventStates.waiting_event_choice)

    await message.reply(
        "Теперь выберите мероприятие, которое нужно отменить:",
        reply_markup=cancel_event_broadcast_keyboard(events)
    )


@dp.callback_query(F.data.startswith("cancel_event_bc_"))
async def cancel_event_bc(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    # достаём event_id из callback_data вида: cancel_event_bc_<id>
    try:
        event_id = int(callback.data.split("_")[-1])
    except (ValueError, IndexError):
        return await callback.answer("Ошибка ID события.", show_alert=True)

    # грузим событие из БД
    event = await db.get_event(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    eid, title, desc, photo_path, starts_at, ends_at, photo_file_id = event
    dt = _get_event_datetime(event)

    date_part = f" (дата: {dt.strftime('%d.%m.%Y %H:%M')})" if dt else ""

    # ---------- кастомный или базовый текст ----------
    data = await state.get_data()
    custom_text = data.get("custom_text")

    if custom_text:
        final_text = custom_text
    else:
        final_text = CANCEL_EVENT_TEMPLATE.format(
            title=title,
            date_part=date_part,
        )
    # -------------------------------------------------

    # только пользователи, записанные на это мероприятие
    user_ids = await db.list_registered_user_ids(event_id)
    if not user_ids:
        await callback.answer("На мероприятие никто не записан.", show_alert=True)
        await state.clear()
        return

    markup = cancel_event_user_keyboard()

    sent = failed = 0
    for uid in user_ids:
        try:
            await bot.send_message(
                uid,
                final_text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
            )
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await callback.answer(
        f"Рассылка завершена. Отправлено: {sent}, ошибок: {failed}",
        show_alert=True,
    )
    await callback.message.edit_reply_markup()
    await state.clear()

@dp.message(BroadcastStates.waiting_text)
async def broadcast_choose_scope(message: Message, state: FSMContext, bot: Bot):
    # Пытаемся вытащить текст (из text или caption)
    text_html: Optional[str] = None
    if message.text:
        text_html = message.text.strip()
    elif message.caption:
        text_html = message.caption.strip()

    # Пытаемся вытащить фото (file_id самого большого варианта)
    photo_file_id: Optional[str] = None
    if message.photo:
        photo_file_id = message.photo[-1].file_id

    # Если нет ни текста, ни фото — ругаемся
    if not text_html and not photo_file_id:
        return await message.reply(
            "Нужно прислать либо текст, либо фото (с подписью по желанию)."
        )

    # Сохраняем в FSM оба параметра
    await state.update_data(
        bc_text=text_html,
        bc_photo_id=photo_file_id,
    )

    events = await db.list_events()
    if not events:
        await message.reply("Нет событий. Разошлю всем.")
        user_ids = await db.list_all_user_ids()

        sent, failed = await _broadcast_send(
            bot,
            user_ids,
            text=text_html,
            photo_file_id=photo_file_id,
        )

        await state.clear()
        return await message.reply(f"Готово: ✅ {sent}, ❌ {failed}")

    await message.reply(
        "Кому отправляем?",
        reply_markup=broadcast_scope_keyboard(events),
    )



@dp.callback_query(F.data == "bc_cancel")
async def bc_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено.", show_alert=False)
    await callback.message.edit_reply_markup()

@dp.callback_query(F.data == "bc_all")
async def bc_all(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    text = data.get("bc_text")
    photo_file_id = data.get("bc_photo_id")

    if not text and not photo_file_id:
        await callback.answer("Контент не найден, начните с /broadcast", show_alert=True)
        return

    user_ids = await db.list_all_user_ids()
    sent, failed = await _broadcast_send(
        bot,
        user_ids,
        text=text,
        photo_file_id=photo_file_id,
    )

    await state.clear()
    await callback.answer("Готово.", show_alert=False)
    await callback.message.edit_text(
        f"Рассылка всем завершена: ✅ {sent}, ❌ {failed}"
    )
@dp.callback_query(F.data == "cancel_event_bc_cancel")
async def cancel_event_bc_cancel(callback: CallbackQuery):
    await callback.answer("Отменено.", show_alert=False)
    await callback.message.edit_reply_markup()


@dp.callback_query(F.data.startswith("bc_event_"))
async def bc_event(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    text = data.get("bc_text")
    photo_file_id = data.get("bc_photo_id")

    if not text and not photo_file_id:
        await callback.answer("Контент не найден, начните с /broadcast", show_alert=True)
        return

    event_id = int(callback.data.split("_", 2)[2])
    user_ids = await db.list_registered_user_ids(event_id)
    if not user_ids:
        await callback.answer("Никто не записан на это мероприятие.", show_alert=True)
        return

    sent, failed = await _broadcast_send(
        bot,
        user_ids,
        text=text,
        photo_file_id=photo_file_id,
    )

    await state.clear()
    await callback.answer("Готово.", show_alert=False)
    await callback.message.edit_text(
        f"Рассылка по событию {event_id} завершена: ✅ {sent}, ❌ {failed}"
    )

@dp.callback_query(F.data.startswith("cancel_event_bc_"))
async def cancel_event_bc(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    try:
        event_id = int(callback.data.split("_")[-1])
    except:
        return await callback.answer("Ошибка ID события.", show_alert=True)

    event = await db.get_event(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    eid, title, desc, photo_path, starts_at, ends_at, photo_file_id = event
    dt = _get_event_datetime(event)

    date_part = f" (дата: {dt.strftime('%d.%m.%Y %H:%M')})" if dt else ""

    # ---------- ВАЖНО: кастомный или базовый текст ----------
    data = await state.get_data()
    custom_text = data.get("custom_text")

    if custom_text:
        final_text = custom_text
    else:
        final_text = CANCEL_EVENT_TEMPLATE.format(
            title=title,
            date_part=date_part
        )
    # ---------------------------------------------------------

    user_ids = await db.list_registered_user_ids(event_id)
    if not user_ids:
        await callback.answer("На мероприятие никто не записан.", show_alert=True)
        return await state.clear()

    markup = cancel_event_user_keyboard()

    sent = failed = 0
    for uid in user_ids:
        try:
            await bot.send_message(
                uid,
                final_text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup
            )
            sent += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)

    await callback.answer(
        f"Рассылка завершена. Отправлено: {sent}, ошибок: {failed}",
        show_alert=True
    )

    await callback.message.edit_reply_markup()
    await state.clear()


@dp.callback_query(F.data.startswith("bc_notreg_"))
async def bc_notreg(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    data = await state.get_data()
    base_text = data.get("bc_text")  # текст, который ты написал в /broadcast

    if not base_text:
        await callback.answer("Нужно прислать текст в /broadcast.", show_alert=True)
        return

    # bc_notreg_{event_id} -> ["bc", "notreg", "{id}"]
    event_id = int(callback.data.split("_", 2)[2])

    # грузим событие, чтобы взять ЕГО фотку
    event = await db.get_event(event_id)
    if not event:
        await callback.answer("Мероприятие не найдено.", show_alert=True)
        return

    eid, title, desc, photo_path, starts_at, ends_at, event_photo_file_id = event

    # 1) все пользователи
    all_user_ids = await db.list_all_user_ids()
    # 2) те, кто уже записан
    registered_ids = set(await db.list_registered_user_ids(event_id))
    # 3) целевая аудитория — только незаписанные
    target_user_ids = [uid for uid in all_user_ids if uid not in registered_ids]

    if not target_user_ids:
        await callback.answer(
            "Нет пользователей, которые ещё не записаны на это мероприятие.",
            show_alert=True,
        )
        await state.clear()
        try:
            await callback.message.edit_reply_markup()
        except Exception:
            pass
        return

    sent = failed = 0

    for uid in target_user_ids:
        personal_text = base_text

        # персонализация по имени
        user_row = await db.get_user(uid)
        if user_row:
            personal_text = _personalize_text_for_user(base_text, user_row)

        try:
            if event_photo_file_id:
                # ТОЛЬКО фото события
                await bot.send_photo(
                    uid,
                    event_photo_file_id,
                    caption=personal_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=event_cta_keyboard(eid),
                )
            else:
                # У события нет своей картинки → только текст
                await bot.send_message(
                    uid,
                    personal_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=event_cta_keyboard(eid),
                )
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNetworkError):
            failed += 1

        await asyncio.sleep(0.05)

    await state.clear()
    await callback.answer("Готово.", show_alert=False)
    await callback.message.edit_text(
        f"Рассылка НЕзаписавшимся на событие {event_id} завершена: ✅ {sent}, ❌ {failed}"
    )


@dp.callback_query(F.data.startswith("bc_event_notreg_"))
async def bc_event_notreg(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    data = await state.get_data()
    text = data.get("bc_text")
    photo_file_id = data.get("bc_photo_id")

    if not text and not photo_file_id:
        await callback.answer("Контент не найден, начните с /broadcast", show_alert=True)
        return

    # bc_event_notreg_{event_id} -> ["bc","event","notreg","{id}"]
    parts = callback.data.split("_", 3)
    event_id = int(parts[3])

    # 1) все пользователи бота
    all_user_ids = await db.list_all_user_ids()
    # 2) те, кто уже записан на это мероприятие
    registered_ids = set(await db.list_registered_user_ids(event_id))

    # 3) фильтруем: только те, кого НЕТ в registrations
    target_user_ids = [uid for uid in all_user_ids if uid not in registered_ids]

    if not target_user_ids:
        await callback.answer("Нет пользователей, которые ещё не записаны на это мероприятие.", show_alert=True)
        await state.clear()
        try:
            await callback.message.edit_reply_markup()
        except Exception:
            pass
        return

    sent, failed = await _broadcast_send(
        bot,
        target_user_ids,
        text=text,
        photo_file_id=photo_file_id,
    )

    await state.clear()
    await callback.answer("Готово.", show_alert=False)
    await callback.message.edit_text(
        f"Рассылка НЕзаписавшимся на событие {event_id} завершена: ✅ {sent}, ❌ {failed}"
    )


from datetime import datetime  # у тебя это уже импортировано выше

def _get_event_datetime(event_row, now: datetime | None = None) -> datetime | None:
    """
    event_row:
        (id, title, description, photo_path, starts_at, ends_at[, photo_file_id])
    Пытается вернуть datetime начала события.
    1) сначала смотрим на starts_at (ISO-строка);
    2) если пусто — берём дату из заголовка в формате 'дд.мм ...'.
    """
    if now is None:
        now = datetime.now()

    # допускаем как 6, так и 7+ полей в кортеже события
    try:
        _, title, _desc, _photo_path, starts_at, _ends_at, *_rest = event_row
    except ValueError:
        # fallback: старый формат без лишних полей
        if len(event_row) >= 6:
            _, title, _desc, _photo_path, starts_at, _ends_at = event_row[:6]
        else:
            # если структура вообще неизвестна — лучше вернуть None
            return None

    # 1) пытаемся распарсить starts_at как ISO
    if starts_at:
        try:
            value = starts_at.strip()
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except Exception:
            pass

    # 2) fallback: дата из начала title вида "2.11 ..." или "02.11 ..."
    try:
        first_token = title.split()[0]
        first_token = first_token.strip(".")
        day_str, month_str = first_token.split(".")
        year = now.year
        return datetime(year=year, month=int(month_str), day=int(day_str))
    except Exception:
        return None


def _filter_upcoming_events(events: list[tuple]) -> list[tuple]:
    """
    Фильтрует список событий, оставляя только те, у которых дата >= сегодня.
    Если дату не удалось распарсить — событие оставляем (лучше показать, чем
    потерять из-за бага разметки).
    Сортировка: сначала по дате, затем по id.
    """
    now = datetime.now()
    upcoming: list[tuple] = []

    for row in events:
        dt = _get_event_datetime(row, now=now)
        if dt is None:
            # непарсибельная дата — оставляем
            upcoming.append(row)
            continue

        if dt.date() >= now.date():
            upcoming.append(row)

    def sort_key(row):
        dt = _get_event_datetime(row, now=now)
        if dt is None:
            dt = datetime.max
        return (dt, row[0])

    upcoming.sort(key=sort_key)
    return upcoming


async def show_events_list(
    chat_id: int,
    bot: Bot,
    *,
    user_id: Optional[int] = None,
    edit_message: Optional[Message] = None,
):
    # 1. Публичные мероприятия
    events = await db.list_events()  # возвращает только is_public = 1

    # 2. Пытаемся добавить ОДНО «секретное» мероприятие по реф-коду пользователя
    if user_id is not None:
        ref_code = await db.get_user_referral(user_id)
        if ref_code:
            event_id = await db.get_event_for_referral(ref_code)
            if event_id:
                secret = await db.get_event(event_id)
                if secret:
                    # get_event возвращает: (id, title, desc, photo_path, starts_at, ends_at, photo_file_id)
                    secret_row_for_list = secret[:6]  # приводим к формату list_events
                    # не дублируем, если вдруг это и так публичное мероприятие
                    if not any(row[0] == secret_row_for_list[0] for row in events):
                        events.append(secret_row_for_list)

    # 3. Фильтруем и сортируем по дате
    events = _filter_upcoming_events(events)

    # 4. Если после всего список пуст — честно говорим, что ничего нет
    if not events:
        text = "Пока нет активных мероприятий."
        if edit_message:
            await edit_message.edit_caption(text, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id, text)
        return

    # 5. Дальше твой исходный код вывода (фото / текст + клавиатура)
    media_src, is_file_id, caption = await render_events_media()

    if media_src:
        if edit_message:
            media = InputMediaPhoto(
                media=media_src if is_file_id else FSInputFile(media_src),
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            await edit_message.edit_media(media=media, reply_markup=events_keyboard(events))
        else:
            if is_file_id:
                await bot.send_photo(
                    chat_id,
                    media_src,
                    caption=caption,
                    reply_markup=events_keyboard(events),
                    parse_mode=ParseMode.HTML,
                )
            else:
                await safe_send_photo(
                    bot,
                    chat_id,
                    media_src,
                    caption=caption,
                    reply_markup=events_keyboard(events),
                    parse_mode=ParseMode.HTML,
                )
    else:
        if edit_message:
            await edit_message.edit_caption(
                caption,
                reply_markup=events_keyboard(events),
                parse_mode=ParseMode.HTML,
            )
        else:
            await bot.send_message(
                chat_id,
                caption,
                reply_markup=events_keyboard(events),
                parse_mode=ParseMode.HTML,
            )


@dp.message(Command("edit_welcome_photo"))
async def cmd_edit_welcome_photo(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    await state.set_state(EditWelcomePhotoStates.waiting_new_photo)
    await message.answer("Пришлите новую афишу одним сообщением (фото).")


@dp.message(EditWelcomePhotoStates.waiting_new_photo)
async def edit_welcome_photo_receive(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        await state.clear()
        return await message.reply("Недостаточно прав.")

    if not message.photo:
        return await message.reply("Нужно прислать именно фото.")

    new_file_id = message.photo[-1].file_id
    await db.set_setting("welcome_photo_file_id", new_file_id)

    await state.clear()
    await message.reply("Афиша обновлена ✅")



@dp.message(CreateEventStates.waiting_data)
async def add_event_receive(message: Message, state: FSMContext):
    # защита: только админ
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        await state.clear()
        return await message.reply("Недостаточно прав.")

    # 1. Вынимаем текст и фото
    photo_file_id: Optional[str] = None

    if message.photo:
        # админ прислал фото с подписью
        photo_file_id = message.photo[-1].file_id

    # Берём сырой текст (а не html_text, чтобы не экранировать <a> и т.п.)
    raw_text_src = message.text or message.caption or ""
    raw_text = raw_text_src.strip()

    if not raw_text:
        return await message.reply(
            "Нужен текст с заголовком формата:\n"
            "ДД.ММ Название\n\nОписание мероприятия."
        )

    # 2. Парсим шапку и описание
    parts = raw_text.split("\n", 1)
    header = parts[0].strip()
    description = parts[1].strip() if len(parts) > 1 else ""

    tokens = header.split(maxsplit=1)
    if not tokens:
        return await message.reply(
            "Неверный формат. Первая строка должна быть вида 'ДД.ММ Название'."
        )

    date_token = tokens[0]

    # Заголовок, который уйдёт в БД (и в кнопки/карточку)
    title = header

    # 3. Пытаемся вытащить дату и положить в starts_at как ISO
    from datetime import datetime
    now = datetime.now()
    starts_at_iso: Optional[str] = None

    try:
        day_str, month_str = date_token.strip(".").split(".")
        year = now.year
        start_dt = datetime(year=year, month=int(month_str), day=int(day_str))
        starts_at_iso = start_dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
    except Exception:
        # если дата кривая — просто не ставим starts_at.
        # Такое событие всё равно будет показано, см. _filter_upcoming_events
        pass

    # 4. Создаём событие в БД
    event_id = await db.add_event(
        title=title,
        description=description or None,
        photo_path=None,      # мы не используем файловую систему, только file_id
        starts_at=starts_at_iso,
        ends_at=None,
    )

    # 5. Если было фото — сразу сохраняем file_id
    if photo_file_id:
        await db.set_event_photo_file_id(event_id, photo_file_id)

    await state.clear()
    await message.answer(f"Мероприятие добавлено (id={event_id}).")

@dp.message(Command("sync_events"))
async def admin_sync_events(message: Message):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    updated = await sync_events_from_json()
    await message.answer(f"Синхронизировано мероприятий: {updated}")

@dp.message(Command("edit_event_desc"))
async def cmd_edit_event_desc(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    events = await db.list_events()

    if not events:
        return await message.reply("Нет мероприятий для редактирования.")

    from keyboards import admin_events_keyboard

    await message.answer(
        "Выберите мероприятие, которое хотите редактировать:",
        reply_markup=admin_events_keyboard(events)
    )

@dp.message(Command("edit_event_photo"))
async def cmd_edit_event_photo(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    events = await db.list_events()
    if not events:
        return await message.reply("Нет мероприятий для редактирования.")

    # простая клавиатура "по одному событию в строку"
    kb_rows = []
    for (eid, title, *_rest) in events:
        kb_rows.append([
            InlineKeyboardButton(
                text=f"🖼 {title}",
                callback_data=f"edit_event_photo_{eid}",
            )
        ])
    kb_rows.append([
        InlineKeyboardButton(text="✖️ Отмена", callback_data="cancel_edit_photo")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await message.answer(
        "Выберите мероприятие, у которого хотите поменять картинку:",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("edit_event_photo_"))
async def cb_edit_event_photo_select(callback: CallbackQuery, state: FSMContext):
    if not ADMIN_ID or callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    event = await db.get_event(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    _, title, *_ = event

    await state.update_data(event_id=event_id)

    # убираем клавиатуру выбора событий
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.message.answer(
        f"Мероприятие: <b>{title}</b>\n\n"
        "Пришлите новую картинку одним сообщением (фото).",
        parse_mode=ParseMode.HTML,
    )

    await state.set_state(EditEventPhotoStates.waiting_new_photo)
    await callback.answer()


@dp.message(EditEventPhotoStates.waiting_new_photo)
async def edit_event_photo_receive(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        await state.clear()
        return await message.reply("Недостаточно прав.")

    if not message.photo:
        return await message.reply("Нужно прислать именно фото.")

    data = await state.get_data()
    event_id = data.get("event_id")
    if not event_id:
        await state.clear()
        return await message.reply("Не удалось определить мероприятие, начните заново с /edit_event_photo.")

    new_file_id = message.photo[-1].file_id

    # сохраняем новый file_id в БД
    await db.set_event_photo_file_id(int(event_id), new_file_id)

    # опционально можешь очистить photo_path, если он тебе больше не нужен:
    # await db._conn.execute("UPDATE events SET photo_path = NULL WHERE id = ?;", (event_id,))
    # await db._conn.commit()

    await state.clear()
    await message.reply("Картинка мероприятия обновлена ✅")

@dp.callback_query(lambda c: c.data and c.data.startswith("edit_event_"))
async def callback_edit_event_select(callback: CallbackQuery, state: FSMContext):
    if not ADMIN_ID or callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[2])

    event = await db.get_event(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    _, title, desc, *_ = event

    await state.update_data(event_id=event_id)

    await callback.message.edit_text(
        f"<b>{title}</b>\n\n"
        f"Текущее описание:\n{desc or '(пусто)'}\n\n"
        "Отправьте новое описание (HTML поддерживается).",
        parse_mode=ParseMode.HTML
    )

    await state.set_state(EditEventStates.waiting_new_desc)

@dp.message(EditEventStates.waiting_new_desc)
async def edit_event_desc_receive(message: Message, state: FSMContext):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        await state.clear()
        return await message.reply("Недостаточно прав.")

    data = await state.get_data()
    event_id = data.get("event_id")

    # Берём сырой текст, без html_text, чтобы не получить &lt;a&gt;
    new_desc = (message.text or message.caption or "").strip()
    if not new_desc:
        return await message.reply("Описание не может быть пустым.")

    await db.update_event_description(event_id, new_desc)

    await state.clear()
    await message.reply("Описание обновлено.", parse_mode=ParseMode.HTML)


async def _broadcast_send(
    bot: Bot,
    user_ids: List[int],
    text: Optional[str] = None,
    photo_file_id: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Массовая рассылка:
    - если есть first_name/last_name — обращаемся по имени;
    - если имя скрыто — шлём текст без имени;
    - username (@ник) НИГДЕ не используем.
    """
    sent = failed = 0
    seen = set()

    for uid in user_ids:
        if uid in seen:
            continue
        seen.add(uid)

        # Базовый текст может быть None (например, чисто фото)
        personal_text = text

        # Если есть текст и пользователь в базе — персонализируем
        if text:
            user_row = await db.get_user(uid)
            if user_row:
                personal_text = _personalize_text_for_user(text, user_row)

        try:
            if photo_file_id:
                # Отправка фото с опциональной подписью
                kwargs: Dict[str, Any] = {}
                if personal_text:
                    kwargs["caption"] = personal_text
                    kwargs["parse_mode"] = ParseMode.HTML

                await bot.send_photo(uid, photo_file_id, **kwargs)
            else:
                # Классический текстовый broadcast
                if not personal_text:
                    # Нечего отправлять — пропускаем
                    continue

                await bot.send_message(
                    uid,
                    personal_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )

            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNetworkError):
            failed += 1

        await asyncio.sleep(0.05)

    return sent, failed

@dp.callback_query(F.data.startswith("sendconf_choose_"))
async def cb_sendconf_choose(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    try:
        await callback.message.edit_text(
            "Кому отправить запросы на подтверждение?",
            reply_markup=send_confirmations_mode_keyboard(event_id),
        )
    except Exception:
        # fallback если edit_text нельзя
        await callback.message.answer(
            "Кому отправить запросы на подтверждение?",
            reply_markup=send_confirmations_mode_keyboard(event_id),
        )

    await callback.answer()


async def send_confirmations_for_date(bot: Bot, target_date: date):
    """
    Проходит по событиям на target_date и шлёт запрос на подтверждение
    всем зарегистрированным пользователям.

    Обращается по имени (first_name/last_name). Если имени нет — без обращения.
    username (@ник) НЕ используется.
    """
    events = await db.list_events()

    for row in events:
        eid, title, desc, photo_path, starts_at, ends_at = row[:6]

        dt = _get_event_datetime(row)
        if not dt or dt.date() != target_date:
            continue

        user_ids = await db.list_registered_user_ids(eid)
        if not user_ids:
            continue

        # базовый текст БЕЗ имени
        base_text = (
            "Напоминаем про мероприятие:\n\n"
            f"<b>{title}</b>\n\n"
            "Мы сейчас готовимся и уточняем количество участников.\n"
            "Подтвердите, пожалуйста, участие."
        )



        for uid in user_ids:
            user_row = await db.get_user(uid)

            # если нашли юзера в БД — персонализируем по имени
            if user_row:
                text = _personalize_text_for_user(base_text, user_row)
            else:
                text = base_text

            try:
                await bot.send_message(
                    uid,
                    text,
                    reply_markup=confirmation_keyboard(eid),
                    parse_mode=ParseMode.HTML,
                )
            except (TelegramForbiddenError, TelegramBadRequest, TelegramNetworkError):
                # заблокировал бота / ошибка — просто пропускаем
                pass

            await asyncio.sleep(0.05)
@dp.message(Command("send_confirmations_today"))
async def cmd_send_confirmations_today(message: Message, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")
    await send_confirmations_for_date(bot, datetime.utcnow().date())
    await message.reply("Запросы на подтверждение отправлены (на сегодня).")


async def _send_confirmations_report(bot: Bot, chat_id: int, event_id: int):
    rows = await db.get_event_confirmation_report(event_id)

    yes, no, none = [], [], []

    total_adults = 0
    total_children = 0

    for (
        user_id,
        phone,
        status,
        adults_count,
        children_count,
        username,
        first_name,
        last_name,
    ) in rows:
        name_parts = [p for p in (first_name, last_name) if p]
        name = " ".join(name_parts).strip() or username or str(user_id)

        details = ""
        if status == "yes":
            a = adults_count or 0
            c = children_count or 0
            total_adults += a
            total_children += c
            details = f" (взрослых: {a}, детей: {c})"

        label = f"{name} — {phone or 'телефон не указан'}{details}"

        if status == "yes":
            yes.append(f"— {label}")
        elif status == "no":
            no.append(f"— {label}")
        else:
            none.append(f"— {label}")

    text = (
        f"Подтверждения по мероприятию [{event_id}]\n\n"
        f"✅ Придут ({len(yes)}):\n" + ("\n".join(yes) if yes else "— никто") + "\n\n"
        f"❌ Не придут ({len(no)}):\n" + ("\n".join(no) if no else "— никто") + "\n\n"
        f"❓ Не ответили ({len(none)}):\n" + ("\n".join(none) if none else "— никто") + "\n\n"
        f"Итого людей: {total_adults + total_children}\n"
        f"(взрослых: {total_adults}, детей: {total_children})"
    )

    await bot.send_message(chat_id, text)


@dp.message(Command("confirmations"))
async def cmd_confirmations(message: Message, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    parts = message.text.split(maxsplit=1)

    # Вариант: /confirmations 3 — прямой вызов по id (оставляем как опцию)
    if len(parts) > 1:
        try:
            event_id = int(parts[1])
        except ValueError:
            return await message.reply("Некорректный id мероприятия.")

        await _send_confirmations_report(message.chat.id, event_id, bot)
        return

    # Вариант: просто /confirmations — показать список мероприятий
    events = await db.list_events()
    if not events:
        return await message.reply("Нет мероприятий для отчёта по подтверждениям.")

    await message.reply(
        "Выберите мероприятие, чтобы посмотреть, кто подтвердил участие:",
        reply_markup=confirmations_events_keyboard(events),
    )

@dp.callback_query(F.data.startswith("conf_event_"))
async def cb_conf_event(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[2])

    # убираем клавиатуру под сообщением со списком
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await _send_confirmations_report(bot, callback.message.chat.id, event_id)
    await callback.answer()


@dp.callback_query(F.data == "conf_cancel")
async def cb_conf_cancel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.answer("Отменено.", show_alert=False)


# --- УТИЛИТЫ ДЛЯ ИМЁН И ПЕРСОНАЛИЗАЦИИ ---

from typing import Optional

def _get_display_name_from_user_row(user_row) -> Optional[str]:
    """
    Достаём отображаемое имя пользователя так, как оно указано в аккаунте.
    НЕ используем username (@ник).
    Если имени нет — возвращаем None.

    users схема:
    user_id, username, first_name, last_name, language, created_at, last_seen, ...
    """
    _user_id, _username, first_name, last_name, *rest = user_row
    parts = [p for p in (first_name, last_name) if p]
    full_name = " ".join(parts).strip()
    return full_name or None

def _weekday_ru(d: date_type) -> str:

    names = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    return names[d.weekday()]

def _human_when(event_date: date_type, today: date_type | None = None) -> str:
    today = today or date_type.today()
    if event_date == today:
        return f"сегодня ({_weekday_ru(event_date)})"
    if event_date == today + timedelta(days=1):
        return f"уже завтра ({_weekday_ru(event_date)})"
    # иначе просто дата (можно улучшить форматом)
    return f"{event_date.strftime('%d.%m.%Y')} ({_weekday_ru(event_date)})"

def build_confirmation_text(
    *,
    full_name: str | None,
    when_text: str,
    program_block: str,
) -> str:
    # Это и есть ваш "шаблон": верх + вставка программы + низ
    # program_block админ присылает как многострочный текст
    name_part = f"{full_name}, " if full_name else ""
    return (
        f"{name_part}{when_text} ждем вас в Shilo Development! 🏠\n\n"
        "Программа:\n"
        f"{program_block.strip()}\n\n"
        "А пока мы закупаем мясо для плова и разогреваем печи для пирогов, хотим уточнить: "
        "ваши планы не поменялись?"
    )


def _personalize_text_for_user(base_text: str, user_row) -> str:
    """
    Если есть имя (first_name/last_name) — используем его.
    Если имени нет (человек скрыл данные) — возвращаем base_text без упоминания имени.

    Поддерживает плейсхолдер {name} в тексте рассылки.
    Примеры:
      "Привет, {name}! У нас новость" -> "Привет, Максим! У нас новость"
      "Привет, {name}!" (а имени нет) -> "Привет!"
    Если {name} не используется — просто добавляем "Имя, " в начало при его наличии.
    """
    name = _get_display_name_from_user_row(user_row)

    # Вариант, когда админ явно использует плейсхолдер {name} в тексте
    if "{name}" in base_text:
        if not name:
            # имени нет — убираем {name}, чутка чистим пробелы/знаки
            text = base_text.replace("{name}", "").strip()
            # возможная ситуация "Привет, !": убираем лишние запятые/пробелы в начале
            while text and text[0] in ",-:; ":
                text = text[1:].lstrip()
            return text

        # имя есть — просто подставляем
        return base_text.replace("{name}", name)

    # Если плейсхолдера нет и имени нет — возвращаем как есть, без обращения
    if not name:
        return base_text

    # Если имени нет в плейсхолдерах, но оно есть — делаем "Имя, текст"
    return f"{name}, {base_text}"


@dp.message(Command("send_confirmations"))
async def cmd_send_confirmations(message: Message, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    events = await db.list_events_raw_admin()
    if not events:
        return await message.reply("Нет мероприятий.")

    await message.reply(
        "Выберите мероприятие, чтобы отправить запросы на подтверждение участникам:",
        reply_markup=send_confirmations_events_keyboard(events),
    )

@dp.callback_query(F.data.startswith("sendconf_event_"))
async def cb_sendconf_event(callback: CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await send_confirmations_for_event(bot, event_id)
    await callback.message.answer(f"Запросы на подтверждение отправлены (мероприятие id={event_id}).")
    await callback.answer()

@dp.callback_query(F.data == "sendconf_cancel")
async def cb_sendconf_cancel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.answer("Отменено.", show_alert=False)


async def send_confirmations_for_event(
    bot: Bot,
    event_id: int,
    mode: str = "all",
    *,
    program_block: str | None = None,
):
    event = await db.get_event(event_id)
    if not event:
        return

    eid, title, desc, photo_path, starts_at, ends_at = event[:6]

    # получаем дату события (у вас уже есть _get_event_datetime(row))
    dt = _get_event_datetime(event)
    if not dt:
        return

    when_text = _human_when(dt.date())

    if mode == "pending":
        user_ids = await db.list_registered_user_ids_pending_confirmation(event_id)
    else:
        user_ids = await db.list_registered_user_ids(event_id)

    if not user_ids:
        return

    # fallback: если админ не передал program_block, можно взять дефолтный блок
    program = (program_block or "").strip()
    if not program:
        program = "✨ 11:00 — ...\n🥘 13:00 — ...\n🏠 14:00 — ..."

    for uid in user_ids:
        user_row = await db.get_user(uid)

        # имя в формате "Имя Фамилия" (username не используем)
        full_name = None
        if user_row:
            # подстройте под вашу схему: у вас в _personalize_text_for_user уже есть логика имени
            full_name = (user_row.get("full_name") if isinstance(user_row, dict) else None)

        text = build_confirmation_text(
            full_name=full_name,
            when_text=when_text,
            program_block=program,
        )

        try:
            await bot.send_message(
                uid,
                text,
                reply_markup=confirmation_keyboard(event_id),
                parse_mode=None,  # важно: т.к. админ присылает эмодзи/символы, HTML может ломаться
            )
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNetworkError):
            pass

        await asyncio.sleep(0.05)


@dp.callback_query(F.data.startswith("sendconf_all_"))
async def cb_sendconf_all(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    await state.set_state(SendConfirmationsStates.waiting_program)
    await state.update_data(event_id=event_id, mode="all")

    await callback.message.answer(
        "Пришлите блок программы (только этот фрагмент), например:\n\n"
        "✨ 11:00 — ...\n"
        "🥘 13:00 — ...\n"
        "🏠 14:00 — ...\n\n"
        "После этого я покажу предпросмотр и попрошу подтвердить отправку."
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("sendconf_pending_"))
async def cb_sendconf_pending(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    await state.set_state(SendConfirmationsStates.waiting_program)
    await state.update_data(event_id=event_id, mode="pending")

    await callback.message.answer(
        "Пришлите блок программы (только этот фрагмент).\n"
        "Я покажу предпросмотр и попрошу подтвердить отправку ТОЛЬКО тем, кто ещё не ответил."
    )
    await callback.answer()



@dp.message(SendConfirmationsStates.waiting_program)
async def sendconf_receive_program(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return

    program_block = (message.text or "").strip()
    if not program_block:
        return await message.reply("Блок программы пустой. Пришлите текст программы.")

    data = await state.get_data()
    event_id = int(data["event_id"])
    mode = data.get("mode", "all")

    # собираем предпросмотр на админе (с его именем из БД, если есть)
    event = await db.get_event(event_id)
    if not event:
        await state.clear()
        return await message.reply("Событие не найдено.")

    dt = _get_event_datetime(event)
    when_text = _human_when(dt.date()) if dt else "уже завтра"

    admin_row = await db.get_user(ADMIN_ID)
    full_name = None
    if admin_row:
        full_name = (admin_row.get("full_name") if isinstance(admin_row, dict) else None)

    preview_text = build_confirmation_text(
        full_name=full_name,
        when_text=when_text,
        program_block=program_block,
    )

    await state.update_data(program_block=program_block)
    await state.set_state(SendConfirmationsStates.waiting_preview_confirm)

    await message.answer(
        "Предпросмотр (именно так увидят пользователи):\n\n" + preview_text,
        reply_markup=sendconf_preview_keyboard(event_id, mode),
    )


@dp.callback_query(F.data.startswith("sendconf_cancel:"))
async def sendconf_cancel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.message.edit_reply_markup()
    await callback.message.answer("Отменено.")
    await callback.answer()


@dp.callback_query(F.data.startswith("sendconf_edit:"))
async def sendconf_edit(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return

    # остаёмся с тем же event_id/mode, только снова просим блок
    await state.set_state(SendConfirmationsStates.waiting_program)
    await callback.message.answer("Ок. Пришлите новый блок программы.")
    await callback.answer()


@dp.callback_query(F.data.startswith("sendconf_do:"))
async def sendconf_do(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return

    data = await state.get_data()
    program_block = (data.get("program_block") or "").strip()
    if not program_block:
        await state.clear()
        return await callback.message.answer("Не найден блок программы. Начните заново.")

    # event_id и mode можно взять из callback_data
    _, payload = callback.data.split("sendconf_do:", 1)
    event_id_str, mode = payload.split(":", 1)
    event_id = int(event_id_str)

    await callback.message.edit_reply_markup()

    await send_confirmations_for_event(
        bot,
        event_id,
        mode=mode,
        program_block=program_block,
    )

    await state.clear()
    await callback.message.answer(f"Готово. Подтверждения отправлены (event_id={event_id}, mode={mode}).")
    await callback.answer()



@dp.message(Command("confirmations_today"))
async def cmd_confirmations_today(message: Message, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    target_date = datetime.utcnow().date()

    await send_confirmations_for_date(bot, target_date)

    await message.reply(
        f"Запросы на подтверждение отправлены на дату "
        f"{target_date.strftime('%d.%m.%Y')}."
    )


@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    from keyboards import admin_main_keyboard
    await message.answer("Админ панель:", reply_markup=admin_main_keyboard())


@dp.callback_query(F.data == "admin_events")
async def admin_events_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    events = await db.list_events_raw_admin()

    rows = []
    for (eid, title, desc, photo_path, starts_at, ends_at, is_public) in events:
        pub = "🌐" if is_public else "🔒"
        rows.append([InlineKeyboardButton(
            text=f"{pub} {title}",
            callback_data=f"admin_event_{eid}"
        )])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await callback.message.edit_text(
        "Список мероприятий:",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_event_"))
async def admin_event_actions(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])
    event = await db.get_event_full(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    eid, title, desc, photo_path, starts_at, ends_at, photo_file_id, is_public = event

    from keyboards import admin_event_actions_keyboard
    kb = admin_event_actions_keyboard(eid, bool(is_public))

    await callback.message.edit_text(
        f"<b>{title}</b>\n\nВыберите действие:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )
    await callback.answer()

@dp.message(Command("del_ref"))
async def del_ref_handler(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply("Использование: /del_ref <код>")

    code = parts[1].strip()
    if not code:
        return await message.reply("Код не может быть пустым.")

    await db.delete_referral(code)
    await message.reply(f"Реф-код '{code}' удалён (если существовал).")


@dp.callback_query(F.data.startswith("admin_ref_"))
async def admin_ref_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])

    event = await db.get_event_full(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    eid, title, *_rest = event

    codes = await db.list_referrals_for_event(eid)

    if codes:
        codes_text = "\n".join(f"• <code>{c}</code>" for c in codes)
    else:
        codes_text = "(пока нет реф-кодов)"

    text = (
        f"<b>{title}</b>\n\n"
        f"Текущие реф-коды для этого мероприятия:\n"
        f"{codes_text}\n\n"
        "Чтобы добавить код, используйте команду:\n"
        f"<code>/add_ref NEWCODE {eid}</code>\n"
        "Чтобы удалить код, используйте команду:\n"
        f"<code>/del_ref EXISTING_CODE</code>"
    )

    # простая кнопка «Назад»
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_event_{eid}")
    ]])

    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("conf_keep_"))
async def cb_conf_keep(callback: CallbackQuery, state: FSMContext):
    event_id = int(callback.data.split("_")[2])

    await state.clear()
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await callback.message.answer("Ок, оставил как есть.")
    await callback.answer()

@dp.message(Command("imgid"))
async def admin_get_image_ids(message: Message):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    if not message.photo:
        return await message.reply("Пришлите фото вместе с командой /imgid (или фото с подписью /imgid).")

    p = message.photo[-1]  # самый большой размер
    await message.reply(
        f"<b>Telegram image IDs</b>\n"
        f"<b>file_id:</b>\n<code>{p.file_id}</code>\n\n"
        f"<b>file_unique_id:</b>\n<code>{p.file_unique_id}</code>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


@dp.callback_query(F.data.startswith("conf_change_"))
async def cb_conf_change(callback: CallbackQuery, state: FSMContext):
    event_id = int(callback.data.split("_")[2])

    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass

    await state.set_state(ConfirmCountsStates.waiting_adults)
    await state.update_data(event_id=event_id)

    await callback.message.answer(
        "Хорошо, давайте обновим данные.\nСколько будет взрослых?",
        reply_markup=adults_count_keyboard(event_id),
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_event_visibility_"))
async def toggle_event_visibility(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await callback.answer("Недостаточно прав.", show_alert=True)

    event_id = int(callback.data.split("_")[-1])
    event = await db.get_event_full(event_id)
    if not event:
        return await callback.answer("Мероприятие не найдено.", show_alert=True)

    _, title, _, _, _, _, _, is_public = event

    new_value = not bool(is_public)
    await db.set_event_public(event_id, new_value)

    text = "Мероприятие стало Публичным 🌐" if new_value else "Мероприятие стало Приватным 🔒"

    await callback.answer(text, show_alert=False)

    # перерисовать меню
    from keyboards import admin_event_actions_keyboard
    kb = admin_event_actions_keyboard(event_id, new_value)

    await callback.message.edit_reply_markup(kb)

@dp.message(Command("add_ref"))
async def add_ref_handler(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("Недостаточно прав.")

    try:
        _, code, event_id_str = message.text.split()
        event_id = int(event_id_str)
    except:
        return await message.reply("Использование: /add_ref <код> <event_id>\nНапример: /add_ref sber 7")

    await db.add_referral(code, event_id)
    await message.reply(f"Реф-код '{code}' привязан к событию {event_id}")


@dp.message(Command("db_info"))
async def db_info(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    row = await db._conn.execute_fetchone("SELECT COUNT(*) FROM users;")
    await message.reply(f"Пользователей в БД: {row[0]}")


async def send_db_backup_to_chat(bot: Bot, chat_id: int):
    """
    Делает копию БД и отправляет в указанный чат.
    chat_id: ID чата/канала/группы, куда бот имеет право писать.
    """
    if not DB_ABS_PATH.exists():
        logger.warning("DB file not found for backup: %s", DB_ABS_PATH)
        return

    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    db_tmp_path = BASE_DIR / f"db_backup_{ts}.sqlite3"

    try:
        shutil.copyfile(DB_ABS_PATH, db_tmp_path)
        size = db_tmp_path.stat().st_size

        await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(str(db_tmp_path)),
            caption=f"Бэкап БД ({size} байт) — {ts}Z",
        )
    except Exception as e:
        logger.exception("Backup send failed: %s", e)
    finally:
        try:
            db_tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

async def backup_loop(bot: Bot, chat_id: int, interval_seconds: int = 3600):
    """
    Фоновая задача: раз в interval_seconds делает бэкап и отправляет в чат.
    """
    # небольшой сдвиг, чтобы бот успел подняться
    await asyncio.sleep(5)

    while True:
        try:
            await send_db_backup_to_chat(bot, chat_id)
        except Exception:
            logger.exception("Backup loop iteration failed")
        await asyncio.sleep(interval_seconds)

# ---------- Точка входа ----------
async def main():
    await db.connect()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


    # --- BACKUP SETTINGS ---
    backup_chat_id_raw = os.getenv("BACKUP_CHAT_ID", "").strip()
    backup_every_seconds = int(os.getenv("BACKUP_EVERY_SECONDS", "3600"))

    if backup_chat_id_raw:
        backup_chat_id = int(backup_chat_id_raw)
        asyncio.create_task(backup_loop(bot, backup_chat_id, backup_every_seconds))
        logger.info("Hourly backup enabled. Target chat_id=%s, every=%ss", backup_chat_id, backup_every_seconds)
    else:
        logger.info("Hourly backup disabled (BACKUP_CHAT_ID is empty).")

    await dp.start_polling(bot)




if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")



