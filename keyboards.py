from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Tuple

def events_keyboard(events: List[Tuple]) -> InlineKeyboardMarkup:
    rows = []
    for (eid, title, *_rest) in events[:10]:
        rows.append([InlineKeyboardButton(text=title, callback_data=f"event_{eid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def event_card_keyboard(event_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="✅ Записаться", callback_data=f"register_{event_id}")],
        [InlineKeyboardButton(text="← Назад к списку", callback_data="back_to_events")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Tuple

def broadcast_scope_keyboard(events: List[Tuple]) -> InlineKeyboardMarkup:
    """
    Клавиатура: 'Всем' + по каждому мероприятию:
    - записавшимся
    - всем, кто ещё НЕ записан
    """
    rows = []
    rows.append([
        InlineKeyboardButton(text="📣 Разослать ВСЕМ", callback_data="bc_all")
    ])

    for (eid, title, *_rest) in events:
        rows.append([
            InlineKeyboardButton(
                text=f"🎯 Записавшимся: {title}",
                callback_data=f"bc_event_{eid}",
            ),
            InlineKeyboardButton(
                text=f"👋 Не записались: {title}",
                callback_data=f"bc_notreg_{eid}",
            ),
        ])

    rows.append([
        InlineKeyboardButton(text="✖️ Отмена", callback_data="bc_cancel")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def cancel_event_broadcast_keyboard(events: List[Tuple]) -> InlineKeyboardMarkup:
    """
    Список мероприятий для сценария 'отмена':
    по каждому событию — отдельная кнопка.
    """
    rows = []
    for (eid, title, *_rest) in events:
        rows.append([
            InlineKeyboardButton(
                text=f"❌ Отменить: {title}",
                callback_data=f"cancel_event_bc_{eid}",
            )
        ])

    rows.append([
        InlineKeyboardButton(text="✖️ Отмена", callback_data="cancel_event_bc_cancel")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def phone_share_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поделиться номером", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Нажмите, чтобы отправить номер"
    )


def event_card_keyboard_registered(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Уже записаны", callback_data=f"noop_{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_events")],
    ])


def admin_events_keyboard(events: List[Tuple]) -> InlineKeyboardMarkup:
    rows = []
    for (eid, title, *_rest) in events:
        rows.append([InlineKeyboardButton(
            text=f"✏️ {title}",
            callback_data=f"edit_event_{eid}"
        )])
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="cancel_edit")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def confirmation_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Да, буду",
                callback_data=f"confirm_yes_{event_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Нет, планы поменялись",
                callback_data=f"confirm_no_{event_id}",
            )
        ],
    ])

def confirmations_events_keyboard(events: List[Tuple]) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора мероприятия для просмотра подтверждений.
    """
    rows = []
    for (eid, title, *_rest) in events:
        rows.append([
            InlineKeyboardButton(
                text=title,
                callback_data=f"conf_event_{eid}"
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="✖️ Отмена",
            callback_data="conf_cancel"
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def event_cta_keyboard(event_id: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для рассылки: ведёт человека на карточку мероприятия.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Подробнее / Записаться",
                callback_data=f"event_{event_id}",
            )
        ]
    ])

def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 Мероприятия", callback_data="admin_events")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="❌ Отмена мероприятия", callback_data="cancel_event_bc")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
    ])


def admin_event_actions_keyboard(event_id: int, is_public: bool) -> InlineKeyboardMarkup:
    status = "Публичное" if is_public else "Приватное"
    toggle_text = "Сделать приватным" if is_public else "Сделать публичным"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔒 {toggle_text}", callback_data=f"toggle_event_visibility_{event_id}")],
        [InlineKeyboardButton(text="✏️ Изменить описание", callback_data=f"edit_event_{event_id}")],
        [InlineKeyboardButton(text="🖼 Изменить фото", callback_data=f"edit_event_photo_{event_id}")],
        [InlineKeyboardButton(text="🔗 Реф-коды", callback_data=f"admin_ref_{event_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_events")],
    ])

def cancel_event_user_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура под сообщением об отмене:
    ведёт пользователя в общую афишу.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Общее меню",
                callback_data="back_to_events",
            )
        ]
    ])

def send_confirmations_events_keyboard(events):
    rows = [
        [InlineKeyboardButton(text=title, callback_data=f"sendconf_choose_{eid}")]
        for (eid, title, *_rest) in events
    ]
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="sendconf_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def send_confirmations_mode_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Всем записанным", callback_data=f"sendconf_all_{event_id}")],
        [InlineKeyboardButton(text="⏳ Только не ответили", callback_data=f"sendconf_pending_{event_id}")],
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="sendconf_cancel")],
    ])




async def list_registered_user_ids_pending_confirmation(self, event_id: int) -> list[int]:
    sql = """
    SELECT r.user_id
    FROM registrations r
    LEFT JOIN confirmations c
        ON c.event_id = r.event_id AND c.user_id = r.user_id
    WHERE r.event_id = ?
      AND c.user_id IS NULL
    ORDER BY r.user_id;
    """
    cur = await self._conn.execute(sql, (event_id,))
    rows = await cur.fetchall()
    await cur.close()
    return [int(r[0]) for r in rows]


def adults_count_keyboard(event_id: int) -> InlineKeyboardMarkup:
    """Сколько будет взрослых? 1..5"""
    rows = [[
        InlineKeyboardButton(text=str(i), callback_data=f"conf_adults_{event_id}_{i}")
        for i in range(1, 6)
    ]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def children_count_keyboard(event_id: int) -> InlineKeyboardMarkup:
    """Сколько детей? 0..5"""
    rows = [[
        InlineKeyboardButton(text=str(i), callback_data=f"conf_children_{event_id}_{i}")
        for i in range(0, 6)
    ]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_change_details_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Оставить как есть",
                callback_data=f"conf_keep_{event_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Изменить количество",
                callback_data=f"conf_change_{event_id}",
            )
        ],
    ])


def sendconf_preview_keyboard(event_id: int, mode: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Отправить", callback_data=f"sendconf_do:{event_id}:{mode}"),
            InlineKeyboardButton(text="✏️ Изменить", callback_data=f"sendconf_edit:{event_id}:{mode}"),
        ],
        [
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"sendconf_cancel:{event_id}:{mode}"),
        ]
    ])
