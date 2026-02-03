import asyncio
import logging
import os
import datetime
from typing import List, Tuple, Optional, Any

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

# ================= НАСТРОЙКИ =================

API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("ENV API_TOKEN is not set")

MASTER_CHAT_ID_RAW = os.getenv("MASTER_CHAT_ID")
if not MASTER_CHAT_ID_RAW:
    raise RuntimeError("ENV MASTER_CHAT_ID is not set")
MASTER_CHAT_ID = int(MASTER_CHAT_ID_RAW)

DB_PATH = os.getenv("DB_PATH", "appointments.db")

# Ссылку в канал задашь позже — просто поменяй тут
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/your_channel_here")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ================= FSM =================

class BookingStates(StatesGroup):
    choosing_date = State()
    choosing_time = State()
    waiting_phone = State()
    waiting_username = State()

# ================= БАЗА ДАННЫХ =================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS appointments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    contact TEXT NOT NULL,
    username TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(date, time)
);
"""

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)

        # мягкая миграция для старых баз
        try:
            await db.execute("ALTER TABLE appointments ADD COLUMN username TEXT;")
        except Exception:
            pass

        await db.commit()

async def is_slot_free(date_iso: str, time_str: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM appointments WHERE date=? AND time=? LIMIT 1",
            (date_iso, time_str),
        ) as cur:
            row = await cur.fetchone()
            return row is None

async def list_free_times(date_iso: str, times: List[str]) -> List[str]:
    free: List[str] = []
    async with aiosqlite.connect(DB_PATH) as db:
        for t in times:
            async with db.execute(
                "SELECT 1 FROM appointments WHERE date=? AND time=? LIMIT 1",
                (date_iso, t),
            ) as cur:
                row = await cur.fetchone()
                if row is None:
                    free.append(t)
    return free

async def create_appointment(user_id: int, date_iso: str, time_str: str, contact: str, username: str) -> bool:
    """
    True если запись создана.
    False если слот уже занят (защита от гонок/двойных кликов).
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO appointments(user_id, date, time, contact, username) VALUES(?,?,?,?,?)",
                (user_id, date_iso, time_str, contact, username),
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def list_user_appointments(
    user_id: int,
    only_future: bool = True
) -> List[Tuple[int, str, str, str, Optional[str]]]:
    """
    (id, date_iso, time_str, contact, username)
    """
    query = "SELECT id, date, time, contact, username FROM appointments WHERE user_id=?"
    params: List[Any] = [user_id]

    if only_future:
        today_iso = datetime.date.today().isoformat()
        query += " AND date >= ?"
        params.append(today_iso)

    query += " ORDER BY date ASC, time ASC"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
            out: List[Tuple[int, str, str, str, Optional[str]]] = []
            for r in rows:
                out.append((int(r[0]), str(r[1]), str(r[2]), str(r[3]), (str(r[4]) if r[4] is not None else None)))
            return out

async def get_user_appointment_by_id(user_id: int, appointment_id: int) -> Optional[Tuple[int, str, str, str, Optional[str]]]:
    """
    Возвращает (id, date, time, contact, username) если принадлежит user_id
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, date, time, contact, username FROM appointments WHERE id=? AND user_id=? LIMIT 1",
            (appointment_id, user_id),
        ) as cur:
            r = await cur.fetchone()
            if not r:
                return None
            return (int(r[0]), str(r[1]), str(r[2]), str(r[3]), (str(r[4]) if r[4] is not None else None))

async def delete_appointment(user_id: int, appointment_id: int) -> Optional[Tuple[int, str, str, str, Optional[str]]]:
    """
    Удаляет запись, только если принадлежит этому user_id.
    Возвращает данные удалённой записи (id, date, time, contact, username) или None.
    """
    appt = await get_user_appointment_by_id(user_id, appointment_id)
    if appt is None:
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM appointments WHERE id=? AND user_id=?", (appointment_id, user_id))
        await db.commit()

    return appt

# ================= ДАТЫ/МЕСЯЦЫ =================

RU_MONTHS = [
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"
]

def next_months(count: int = 6) -> List[Tuple[int, int, str]]:
    today = datetime.date.today()
    y, m = today.year, today.month
    out: List[Tuple[int, int, str]] = []
    for i in range(count):
        mm = m + i
        yy = y + (mm - 1) // 12
        m2 = ((mm - 1) % 12) + 1
        out.append((yy, m2, RU_MONTHS[m2 - 1]))
    return out

def days_in_month(year: int, month: int) -> int:
    next_m = month + 1
    next_y = year
    if next_m == 13:
        next_m = 1
        next_y += 1
    last_day = (datetime.date(next_y, next_m, 1) - datetime.timedelta(days=1)).day
    return last_day

def format_date_iso(year: int, month: int, day: int) -> str:
    return f"{year:04d}-{month:02d}-{day:02d}"

def human_date(date_iso: str) -> str:
    d = datetime.date.fromisoformat(date_iso)
    return f"{d.day:02d}.{d.month:02d}.{d.year}"

def normalize_username(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    if t.startswith("@"):
        return t
    if " " in t:
        return t
    return "@" + t

# ================= КЛАВИАТУРЫ (UI) =================

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗓 Записаться", callback_data="menu:book")],
            [
                InlineKeyboardButton(text="📋 Мои записи", callback_data="menu:my"),
                InlineKeyboardButton(text="❌ Отменить", callback_data="menu:cancel"),
            ],
            [InlineKeyboardButton(text="📢 Телеграмм канал", url=CHANNEL_URL)],
        ]
    )

def months_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for (yy, mm, name) in next_months(6):
        cb = f"m:{yy}:{mm}"
        row.append(InlineKeyboardButton(text=f"{name} {yy}", callback_data=cb))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def days_kb(year: int, month: int) -> InlineKeyboardMarkup:
    max_day = days_in_month(year, month)
    today = datetime.date.today()

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []

    for day in range(1, max_day + 1):
        d = datetime.date(year, month, day)
        if d < today:
            continue

        cb = f"d:{year}:{month}:{day}"
        row.append(InlineKeyboardButton(text=f"{day:02d}", callback_data=cb))
        if len(row) == 7:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="⬅️ Назад к месяцам", callback_data="back:months"),
        InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def times_kb(date_iso: str, free_times: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for t in free_times:
        cb = f"t:{date_iso}:{t}"
        row.append(InlineKeyboardButton(text=t, callback_data=cb))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="⬅️ Назад к дням", callback_data=f"back:days:{date_iso}"),
        InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📞 Отправить телефон (контакт)", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Нажми кнопку, чтобы отправить телефон",
    )

def username_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Оставить как есть", callback_data="uname:keep")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")],
        ]
    )

def cancel_list_kb(appointments: List[Tuple[int, str, str, str, Optional[str]]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for (app_id, date_iso, time_str, _, _) in appointments:
        rows.append([
            InlineKeyboardButton(
                text=f"❌ {human_date(date_iso)} {time_str}",
                callback_data=f"cancel:{app_id}",
            )
        ])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================= СЛОТЫ =================

DEFAULT_TIMES = [
    "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00",
    "16:00", "17:00", "18:00",
]

# ================= ОБЩЕЕ: ПОКАЗ МЕНЮ =================

async def show_home(message_or_call: Any):
    text = "💅 *Маникюр — запись*\n\nВыбери действие:"
    if isinstance(message_or_call, types.Message):
        await message_or_call.answer(text, reply_markup=main_menu_kb(), parse_mode="Markdown")
    else:
        await message_or_call.message.edit_text(text, reply_markup=main_menu_kb(), parse_mode="Markdown")
        await message_or_call.answer()

# ================= START =================

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await show_home(message)

# ================= МЕНЮ CALLBACKS =================

@dp.callback_query(lambda c: c.data == "menu:home")
async def cb_home(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await show_home(call)

@dp.callback_query(lambda c: c.data == "menu:book")
async def cb_menu_book(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Выбери месяц:", reply_markup=months_kb())
    await state.set_state(BookingStates.choosing_date)
    await call.answer()

@dp.callback_query(lambda c: c.data == "menu:my")
async def cb_menu_my(call: types.CallbackQuery):
    apps = await list_user_appointments(call.from_user.id, only_future=True)
    if not apps:
        await call.message.edit_text("У тебя нет будущих записей 🙂", reply_markup=main_menu_kb())
        await call.answer()
        return

    lines = ["📋 *Твои записи:*"]
    for _, date_iso, time_str, contact, username in apps:
        uname = username or "-"
        lines.append(f"• *{human_date(date_iso)}* в *{time_str}*\n  телефон: `{contact}`\n  username: `{uname}`")

    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_kb(), parse_mode="Markdown")
    await call.answer()

@dp.callback_query(lambda c: c.data == "menu:cancel")
async def cb_menu_cancel(call: types.CallbackQuery):
    apps = await list_user_appointments(call.from_user.id, only_future=True)
    if not apps:
        await call.message.edit_text("Нечего отменять — будущих записей нет 🙂", reply_markup=main_menu_kb())
        await call.answer()
        return

    await call.message.edit_text("Выбери запись для отмены:", reply_markup=cancel_list_kb(apps))
    await call.answer()

# ================= ПРОЦЕСС ЗАПИСИ =================

@dp.callback_query(lambda c: c.data and c.data.startswith("m:"))
async def cb_month(call: types.CallbackQuery, state: FSMContext):
    _, yy, mm = call.data.split(":")
    year = int(yy)
    month = int(mm)

    await state.update_data(year=year, month=month)
    await call.message.edit_text(
        f"Выбери день ({RU_MONTHS[month-1]} {year}):",
        reply_markup=days_kb(year, month),
    )
    await call.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("d:"))
async def cb_day(call: types.CallbackQuery, state: FSMContext):
    _, yy, mm, dd = call.data.split(":")
    date_iso = format_date_iso(int(yy), int(mm), int(dd))

    free_times = await list_free_times(date_iso, DEFAULT_TIMES)
    if not free_times:
        await call.answer("На этот день свободных слотов нет 😔", show_alert=True)
        return

    await state.update_data(date_iso=date_iso)
    await call.message.edit_text(
        f"Дата: {human_date(date_iso)}\nВыбери время:",
        reply_markup=times_kb(date_iso, free_times),
    )
    await state.set_state(BookingStates.choosing_time)
    await call.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("t:"))
async def cb_time(call: types.CallbackQuery, state: FSMContext):
    # FIX: время содержит ":", поэтому split ограничиваем до 3 частей
    # "t:YYYY-MM-DD:HH:MM" -> ["t", "YYYY-MM-DD", "HH:MM"]
    _, date_iso, time_str = call.data.split(":", 2)

    if not await is_slot_free(date_iso, time_str):
        await call.answer("Этот слот уже занят, выбери другое время.", show_alert=True)
        return

    await state.update_data(time_str=time_str)

    await call.message.answer(
        f"Отлично! {human_date(date_iso)} в {time_str}.\n\n"
        f"Теперь отправь *телефон* (кнопкой контакта) 👇",
        reply_markup=contact_kb(),
        parse_mode="Markdown",
    )
    await state.set_state(BookingStates.waiting_phone)
    await call.answer()

@dp.message(BookingStates.waiting_phone)
async def on_phone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    date_iso = data.get("date_iso")
    time_str = data.get("time_str")

    if not date_iso or not time_str:
        await message.answer("Кажется, запись сбилась. Нажми /start и попробуй снова.")
        await state.clear()
        return

    phone: Optional[str] = None
    if message.contact and message.contact.phone_number:
        phone = message.contact.phone_number
    else:
        txt = (message.text or "").strip()
        if txt:
            phone = txt

    if not phone:
        await message.answer("Пожалуйста, отправь телефон (контактом или текстом).")
        return

    await state.update_data(phone=phone)

    tg_username = message.from_user.username
    if tg_username:
        uname = "@" + tg_username
        await state.update_data(username=uname)
        await message.answer(
            f"Теперь нужен *юзернейм*.\n"
            f"Я вижу твой: `{uname}`\n\n"
            f"Если он верный — нажми кнопку ниже. Если другой — просто напиши текстом.",
            reply_markup=username_confirm_kb(),
            parse_mode="Markdown",
        )
    else:
        await message.answer(
            "Теперь отправь *юзернейм* (например `@nickname`).\n"
            "Если юзернейма нет — напиши имя/как к тебе обращаться.",
            parse_mode="Markdown",
        )

    await state.set_state(BookingStates.waiting_username)

@dp.callback_query(lambda c: c.data == "uname:keep")
async def cb_username_keep(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    username = data.get("username")
    if not username:
        await call.answer("Не вижу username — напиши его текстом.", show_alert=True)
        return

    await finalize_booking(call.from_user, state, call.message, via_callback=True)
    await call.answer()

@dp.message(BookingStates.waiting_username)
async def on_username_text(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if not txt:
        await message.answer("Напиши юзернейм текстом (например `@nickname`) или имя.", parse_mode="Markdown")
        return

    username = normalize_username(txt)
    await state.update_data(username=username)

    await finalize_booking(message.from_user, state, message, via_callback=False)

async def finalize_booking(user: types.User, state: FSMContext, msg_obj: Any, via_callback: bool):
    data = await state.get_data()
    date_iso = data.get("date_iso")
    time_str = data.get("time_str")
    phone = data.get("phone")
    username = data.get("username") or "-"

    if not date_iso or not time_str or not phone:
        await msg_obj.answer("Кажется, запись сбилась. Нажми /start и попробуй снова.")
        await state.clear()
        return

    ok = await create_appointment(
        user_id=user.id,
        date_iso=date_iso,
        time_str=time_str,
        contact=phone,
        username=username,
    )

    if not ok:
        await msg_obj.answer("Упс — этот слот только что заняли 😔\nВернись в меню и выбери другое время.")
        await state.clear()
        return

    await bot.send_message(
        MASTER_CHAT_ID,
        "📌 Новая запись!\n"
        f"Дата: {human_date(date_iso)}\n"
        f"Время: {time_str}\n"
        f"Телефон: {phone}\n"
        f"Username: {username}\n"
        f"User ID: {user.id}",
    )

    await bot.send_message(
        user.id,
        "✅ Запись создана!\n"
        f"Дата: {human_date(date_iso)}\n"
        f"Время: {time_str}\n"
        f"Телефон: {phone}\n"
        f"Username: {username}\n\n"
        "Можешь посмотреть/отменить запись в меню 👇",
        reply_markup=types.ReplyKeyboardRemove(),
    )

    await bot.send_message(user.id, "Выбери действие:", reply_markup=main_menu_kb())

    await state.clear()

# ================= ОТМЕНА ЗАПИСИ =================

@dp.callback_query(lambda c: c.data and c.data.startswith("cancel:"))
async def cb_cancel(call: types.CallbackQuery):
    raw = call.data.split(":", 1)[1]
    try:
        app_id = int(raw)
    except ValueError:
        await call.answer("Ошибка.", show_alert=True)
        return

    deleted = await delete_appointment(call.from_user.id, app_id)
    if deleted is None:
        await call.answer("Не удалось отменить (возможно, записи уже нет).", show_alert=True)
        return

    _, date_iso, time_str, phone, username = deleted
    username = username or "-"

    await bot.send_message(
        MASTER_CHAT_ID,
        "❌ Отмена записи!\n"
        f"Дата: {human_date(date_iso)}\n"
        f"Время: {time_str}\n"
        f"Телефон: {phone}\n"
        f"Username: {username}\n"
        f"User ID: {call.from_user.id}\n"
        f"ID записи: {app_id}",
    )

    apps = await list_user_appointments(call.from_user.id, only_future=True)
    if not apps:
        await call.message.edit_text("Запись отменена ✅\nБольше будущих записей нет.", reply_markup=main_menu_kb())
    else:
        await call.message.edit_text("Запись отменена ✅\nМожно отменить ещё:", reply_markup=cancel_list_kb(apps))

    await call.answer("Отменено ✅")

# ================= НАЗАД =================

@dp.callback_query(lambda c: c.data and c.data.startswith("back:months"))
async def cb_back_months(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Выбери месяц:", reply_markup=months_kb())
    await state.set_state(BookingStates.choosing_date)
    await call.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("back:days:"))
async def cb_back_days(call: types.CallbackQuery, state: FSMContext):
    _, _, date_iso = call.data.split(":", 2)
    d = datetime.date.fromisoformat(date_iso)
    await state.update_data(year=d.year, month=d.month)
    await call.message.edit_text(
        f"Выбери день ({RU_MONTHS[d.month-1]} {d.year}):",
        reply_markup=days_kb(d.year, d.month),
    )
    await state.set_state(BookingStates.choosing_date)
    await call.answer()

# ================= (Опционально) Команды как запасной вариант =================

@dp.message(Command("my"))
async def cmd_my(message: types.Message):
    apps = await list_user_appointments(message.from_user.id, only_future=True)
    if not apps:
        await message.answer("У тебя нет будущих записей 🙂", reply_markup=main_menu_kb())
        return

    lines = ["📋 Твои записи:"]
    for _, date_iso, time_str, phone, username in apps:
        lines.append(f"• {human_date(date_iso)} {time_str} — {phone} — {username or '-'}")
    await message.answer("\n".join(lines), reply_markup=main_menu_kb())

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    apps = await list_user_appointments(message.from_user.id, only_future=True)
    if not apps:
        await message.answer("Нечего отменять — будущих записей нет 🙂", reply_markup=main_menu_kb())
        return
    await message.answer("Выбери запись для отмены:", reply_markup=cancel_list_kb(apps))

# ================= MAIN =================

async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())