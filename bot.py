import asyncio
import logging
import os
import sqlite3
import datetime

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---------- LOGS ----------
logging.basicConfig(level=logging.INFO)

# ---------- ENV ----------
API_TOKEN = os.getenv("API_TOKEN")
MASTER_CHAT_ID = int(os.getenv("MASTER_CHAT_ID"))

if not API_TOKEN:
    raise RuntimeError("API_TOKEN not found")

# ---------- BOT ----------
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------- DB ----------
conn = sqlite3.connect("appointments.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS appointments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    date TEXT,
    time TEXT,
    contact TEXT
)
""")
conn.commit()

# ---------- TEMP ----------
user_state = {}

# ---------- START ----------
@dp.message(Command("start"))
async def start(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💅 Записаться", callback_data="book")],
        [InlineKeyboardButton(text="❌ Отменить запись", callback_data="cancel")],
        [InlineKeyboardButton(text="📞 Связь с мастером", callback_data="contact")]
    ])
    await message.answer("Привет 👋\nЯ бот для записи на маникюр 💅", reply_markup=kb)

# ---------- MONTH ----------
@dp.callback_query(lambda c: c.data == "book")
async def choose_month(call: types.CallbackQuery):
    now = datetime.date.today()
    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for i in range(3):
        month = (now.month + i - 1) % 12 + 1
        year = now.year + ((now.month + i - 1) // 12)
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{month}.{year}",
                callback_data=f"month_{year}_{month}"
            )
        ])

    await call.message.answer("Выберите месяц:", reply_markup=kb)

# ---------- DAY ----------
@dp.callback_query(lambda c: c.data.startswith("month_"))
async def choose_day(call: types.CallbackQuery):
    _, year, month = call.data.split("_")
    year, month = int(year), int(month)

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    days_in_month = (datetime.date(year, month % 12 + 1, 1) - datetime.timedelta(days=1)).day

    cursor.execute("SELECT date FROM appointments")
    busy = {row[0] for row in cursor.fetchall()}

    for day in range(1, days_in_month + 1):
        date_str = f"{year}-{month:02}-{day:02}"
        if date_str not in busy:
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=str(day),
                    callback_data=f"day_{date_str}"
                )
            ])

    await call.message.answer("Выберите день:", reply_markup=kb)

# ---------- TIME ----------
@dp.callback_query(lambda c: c.data.startswith("day_"))
async def choose_time(call: types.CallbackQuery):
    date = call.data.replace("day_", "")
    user_state[call.from_user.id] = {"date": date}

    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for hour in range(10, 19):
        time = f"{hour}:00"
        cursor.execute(
            "SELECT 1 FROM appointments WHERE date=? AND time=?",
            (date, time)
        )
        if not cursor.fetchone():
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=time,
                    callback_data=f"time_{time}"
                )
            ])

    await call.message.answer("Выберите время:", reply_markup=kb)

# ---------- CONTACT ----------
@dp.callback_query(lambda c: c.data.startswith("time_"))
async def ask_contact(call: types.CallbackQuery):
    time = call.data.replace("time_", "")
    user_state[call.from_user.id]["time"] = time

    await call.message.answer(
        "Отправьте номер телефона сообщением\nили кнопкой «Поделиться контактом»"
    )

# ---------- SAVE ----------
@dp.message()
async def save_booking(message: types.Message):
    uid = message.from_user.id
    if uid not in user_state:
        return

    contact = message.contact.phone_number if message.contact else message.text
    date = user_state[uid]["date"]
    time = user_state[uid]["time"]

    cursor.execute(
        "INSERT INTO appointments (user_id, date, time, contact) VALUES (?, ?, ?, ?)",
        (uid, date, time, contact)
    )
    conn.commit()

    await message.answer(f"✅ Запись сохранена:\n{date} в {time}")
    await bot.send_message(
        MASTER_CHAT_ID,
        f"📌 Новая запись\n{date} {time}\n📞 {contact}"
    )

    user_state.pop(uid)

# ---------- CANCEL ----------
@dp.callback_query(lambda c: c.data == "cancel")
async def cancel(call: types.CallbackQuery):
    cursor.execute(
        "SELECT id, date, time FROM appointments WHERE user_id=?",
        (call.from_user.id,)
    )
    rows = cursor.fetchall()

    if not rows:
        await call.message.answer("У вас нет записей.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for r in rows:
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{r[1]} {r[2]}",
                callback_data=f"del_{r[0]}"
            )
        ])

    await call.message.answer("Выберите запись для отмены:", reply_markup=kb)

@dp.callback_query(lambda c: c.data.startswith("del_"))
async def delete(call: types.CallbackQuery):
    rid = int(call.data.replace("del_", ""))
    cursor.execute("DELETE FROM appointments WHERE id=?", (rid,))
    conn.commit()
    await call.message.answer("❌ Запись отменена")

# ---------- RUN ----------
async def main():
    logging.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
