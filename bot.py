import os
import logging
import sqlite3
import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils import executor

# --- Логирование ---
logging.basicConfig(level=logging.INFO)

# --- Переменные окружения ---
API_TOKEN = os.environ.get("API_TOKEN")
MASTER_CHAT_ID = int(os.environ.get("MASTER_CHAT_ID"))

# --- Инициализация бота ---
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# --- База данных ---
conn = sqlite3.connect('appointments.db')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS appointments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    contact TEXT,
    date TEXT,
    time TEXT
)
''')
conn.commit()

# --- Временные данные пользователя ---
user_temp_data = {}

# --- /start ---
@dp.message(Command("start"))
async def start(message: types.Message):
    logging.info(f"Получено /start от пользователя {message.from_user.id}")
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        types.InlineKeyboardButton("Записаться", callback_data="book"),
        types.InlineKeyboardButton("Связь с мастером", callback_data="contact_master"),
        types.InlineKeyboardButton("Телеграм канал", url="https://t.me/ТВОЙ_КАНАЛ"),
        types.InlineKeyboardButton("Отменить запись", callback_data="cancel")
    )
    await message.answer("Привет! Я бот для записи на маникюр. Выбери действие:", reply_markup=keyboard)

# --- Создание календаря ---
def get_calendar(month, year):
    keyboard = types.InlineKeyboardMarkup(row_width=7)
    days_in_month = (datetime.date(year, month % 12 + 1, 1) - datetime.timedelta(days=1)).day
    cursor.execute("SELECT date FROM appointments WHERE date LIKE ?", (f"{year}-{month:02}-%",))
    booked_days = [int(d[0].split("-")[2]) for d in cursor.fetchall()]

    for day in range(1, days_in_month + 1):
        if day not in booked_days:
            keyboard.add(types.InlineKeyboardButton(str(day), callback_data=f"day_{year}_{month}_{day}"))
    return keyboard

# --- Callback-кнопки ---
@dp.callback_query(lambda c: True)
async def callback_handler(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data

    if data == "book":
        now = datetime.date.today()
        keyboard = types.InlineKeyboardMarkup(row_width=3)
        for i in range(3):
            month_date = now + datetime.timedelta(days=30*i)
            keyboard.add(types.InlineKeyboardButton(month_date.strftime('%B'), callback_data=f"month_{month_date.month}_{month_date.year}"))
        await callback_query.message.answer("Выберите месяц:", reply_markup=keyboard)

    elif data.startswith("month_"):
        _, month, year = data.split("_")
        month = int(month)
        year = int(year)
        calendar = get_calendar(month, year)
        await callback_query.message.answer(f"Выберите день в {datetime.date(year, month, 1).strftime('%B')}:",
                                            reply_markup=calendar)

    elif data.startswith("day_"):
        _, year, month, day = data.split("_")
        date = f"{year}-{int(month):02}-{int(day):02}"
        user_temp_data[user_id] = {"date": date}

        keyboard = types.InlineKeyboardMarkup(row_width=4)
        for hour in range(10, 19):
            time_slot = f"{hour}:00"
            cursor.execute("SELECT * FROM appointments WHERE date=? AND time=?", (date, time_slot))
            if cursor.fetchone() is None:
                keyboard.add(types.InlineKeyboardButton(time_slot, callback_data=f"time_{time_slot}"))
        await callback_query.message.answer(f"Вы выбрали {date}. Выберите время:", reply_markup=keyboard)

    elif data.startswith("time_"):
        time_slot = data.split("_")[1]
        user_temp_data[user_id]["time"] = time_slot
        await callback_query.message.answer(
            f"Вы выбрали {user_temp_data[user_id]['date']} в {time_slot}. "
            "Пожалуйста, отправьте свой контакт (кнопкой Telegram 'Поделиться контактом' или напишите номер)."
        )

    elif data == "contact_master":
        await callback_query.message.answer("Контакт мастера: +7XXXXXXXXXX")

    elif data == "cancel":
        cursor.execute("SELECT id, date, time FROM appointments WHERE user_id=?", (user_id,))
        records = cursor.fetchall()
        if not records:
            await callback_query.message.answer("У вас нет записей.")
        else:
            keyboard = types.InlineKeyboardMarkup()
            for r in records:
                keyboard.add(types.InlineKeyboardButton(f"{r[1]} {r[2]}", callback_data=f"cancel_{r[0]}"))
            await callback_query.message.answer("Выберите запись для отмены:", reply_markup=keyboard)

    elif data.startswith("cancel_"):
        record_id = int(data.split("_")[1])
        cursor.execute("DELETE FROM appointments WHERE id=?", (record_id,))
        conn.commit()
        await callback_query.message.answer("Запись отменена!")

# --- Контакт через кнопку ---
@dp.message(types.ContentType.CONTACT)
async def save_contact(message: types.Message):
    user_id = message.from_user.id
    contact = message.contact.phone_number
    if user_id in user_temp_data and "date" in user_temp_data[user_id] and "time" in user_temp_data[user_id]:
        date = user_temp_data[user_id]["date"]
        time_slot = user_temp_data[user_id]["time"]
        cursor.execute("INSERT INTO appointments (user_id, contact, date, time) VALUES (?, ?, ?, ?)",
                       (user_id, contact, date, time_slot))
        conn.commit()
        await message.answer(f"Ваша запись на {date} в {time_slot} сохранена! Мастер уведомлён.")
        await bot.send_message(MASTER_CHAT_ID, f"Новая запись: {date} {time_slot}\nКонтакт клиента: {contact}")
        user_temp_data.pop(user_id)
    else:
        await message.answer("Сначала выберите дату и время.")

# --- Контакт вручную ---
@dp.message()
async def save_manual_contact(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_temp_data and "date" in user_temp_data[user_id] and "time" in user_temp_data[user_id]:
        date = user_temp_data[user_id]["date"]
        time_slot = user_temp_data[user_id]["time"]
        contact = message.text
        cursor.execute("INSERT INTO appointments (user_id, contact, date, time) VALUES (?, ?, ?, ?)",
                       (user_id, contact, date, time_slot))
        conn.commit()
        await message.answer(f"Ваша запись на {date} в {time_slot} сохранена! Мастер уведомлён.")
        await bot.send_message(MASTER_CHAT_ID, f"Новая запись: {date} {time_slot}\nКонтакт клиента: {contact}")
        user_temp_data.pop(user_id)
    else:
        await message.answer("Сначала выберите дату и время.")

# --- Запуск ---
if __name__ == '__main__':
    logging.info("Бот запущен!")
    executor.start_polling(dp, skip_updates=True)