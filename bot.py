import os
import logging
import pandas as pd
import io
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    MessageHandler, 
    filters,
    CallbackQueryHandler
)

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from parser import process_excel_file

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

user_data = {}

# --- ИСПРАВЛЕНИЕ: Команда /start больше не удаляет данные ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    welcome_text = (
        "👋 Привет! Я бот для анализа поездок.\n\n"
        "Просто отправьте мне один или несколько Excel-файлов (.xlsx) с отчетами. "
        "Когда закончите, используйте кнопки ниже для получения статистики."
    )
    
    # Сообщаем пользователю, если у него уже есть данные
    if user_id in user_data and not user_data[user_id].empty:
        records_count = len(user_data[user_id])
        welcome_text += f"\n\nℹ️ У вас уже загружено записей: {records_count}. " \
                        "Для сброса используйте кнопку 'Очистить данные'."

    keyboard = [
        [InlineKeyboardButton("📊 Общая статистика", callback_data='stats')],
        [
            InlineKeyboardButton("🏆 Топ-5", callback_data='top'),
            InlineKeyboardButton("📥 Экспорт в Excel", callback_data='export')
        ],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='clear')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
        
    await update.message.reply_text(welcome_text, reply_markup=reply_markup)

# --- ИСПРАВЛЕНИЕ: Теперь все функции вызываются единообразно ---
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    command = query.data
    # Передаем весь объект update, чтобы функции работали универсально
    if command == 'stats':
        await show_stats(update, context, is_callback=True)
    elif command == 'clear':
        await clear(update, context, is_callback=True)
    elif command == 'top':
        await show_top_stats(update, context, is_callback=True)
    elif command == 'export':
        await export_data(update, context)

# --- ИСПРАВЛЕНИЕ: функции теперь правильно обрабатывают вызовы от кнопок ---
async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
    
    df = user_data[user_id]
    # ... остальная логика функции ...
    total_trips = len(df)
    total_earnings = df['Стоимость'].sum()
    unique_cars_count = df['Гос_номер'].nunique()
    unique_drivers_count = df['Водитель'].nunique()
    unique_files_count = df['Источник'].nunique()

    message = (
        f"📊 *Общая статистика*\n\n"
        f"▫️ Обработано файлов: {unique_files_count}\n"
        f"▫️ Всего маршрутов: {total_trips}\n"
        f"▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n"
        f"▫️ Уникальных машин: {unique_cars_count}\n"
        f"▫️ Уникальных водителей: {unique_drivers_count}"
    )

    if is_callback:
        await update.callback_query.edit_message_text(text=message, parse_mode='Markdown')
    else:
        await context.bot.send_message(chat_id, text=message, parse_mode='Markdown')

async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if user_id in user_data:
        del user_data[user_id]
        message = "🗑️ Все загруженные данные удалены. Можете начать сначала."
    else:
        message = "ℹ️ У вас нет загруженных данных для очистки."

    if is_callback:
        # При нажатии на кнопку, редактируем сообщение с меню
        await update.callback_query.edit_message_text(text=message)
    else:
        await context.bot.send_message(chat_id, text=message)

async def show_top_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Данные для анализа отсутствуют.")
        return

    df = user_data[user_id]
    top_drivers = df.groupby('Водитель')['Стоимость'].sum().nlargest(5)
    top_drivers_text = "".join([f"{i}. {driver} - {total:,.0f} руб.\n" for i, (driver, total) in enumerate(top_drivers.items(), 1)])
    top_cars = df.groupby('Гос_номер')['Стоимость'].sum().nlargest(5)
    top_cars_text = "".join([f"{i}. Номер {car} - {total:,.0f} руб.\n" for i, (car, total) in enumerate(top_cars.items(), 1)])

    message = (
        f"🏆 *Топ-5 по заработку*\n\n"
        f"👤 *Лучшие водители:*\n{top_drivers_text}\n"
        f"🚗 *Самые прибыльные машины:*\n{top_cars_text}"
    )
    
    if is_callback:
        await update.callback_query.edit_message_text(text=message, parse_mode='Markdown')
    else:
        await context.bot.send_message(chat_id, text=message, parse_mode='Markdown')

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Нет данных для экспорта.")
        return
        
    # ... остальная логика функции ...
    df = user_data[user_id]
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Сводный отчет')
        worksheet = writer.sheets['Сводный отчет']
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
            worksheet.set_column(idx, idx, max_len)
    output.seek(0)
    
    await context.bot.send_document(
        chat_id=chat_id, 
        document=output, 
        filename='сводный_отчет.xlsx',
        caption='📊 Ваш сводный отчет по всем загруженным файлам.'
    )

# --- Остальные функции (без изменений, но привожу для полноты) ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file = await update.message.document.get_file()
    file_name = update.message.document.file_name
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл в формате .xlsx")
        return
    await update.message.reply_text(f"⏳ Получил файл '{file_name}'. Начинаю обработку...")
    file_content = await file.download_as_bytearray()
    new_df = process_excel_file(bytes(file_content), file_name)
    if new_df is None or new_df.empty:
        await update.message.reply_text(f"⚠️ Не удалось извлечь данные из файла '{file_name}'.")
        return
    if user_id in user_data:
        user_data[user_id] = pd.concat([user_data[user_id], new_df], ignore_index=True)
    else:
        user_data[user_id] = new_df
    total_rows = len(user_data[user_id])
    await update.message.reply_text(
        f"✅ Файл '{file_name}' успешно обработан!\n\n"
        f"Добавлено записей: {len(new_df)}\n"
        f"Всего загружено записей: {total_rows}\n\n"
        "Отправьте еще файл или используйте кнопки для просмотра итогов."
    )

async def car_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
    if not context.args:
        await update.message.reply_text("⚠️ Укажите госномер после команды. Пример: `/car 123`")
        return
    car_number = context.args[0]
    df = user_data[user_id]
    car_df = df[df['Гос_номер'].astype(str).str.contains(car_number, case=False, na=False)]
    if car_df.empty:
        await update.message.reply_text(f"❌ Машина с госномером '{car_number}' не найдена.")
        return
    total_trips = len(car_df)
    total_earnings = car_df['Стоимость'].sum()
    drivers = ", ".join(car_df['Водитель'].unique())
    message = (
        f"🚗 *Статистика по машине {car_number}*\n\n"
        f"▫️ Совершено маршрутов: {total_trips}\n"
        f"▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n"
        f"▫️ Водители на этой машине: {drivers}"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode='Markdown')
    
async def driver_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
    if not context.args:
        await update.message.reply_text("⚠️ Укажите фамилию водителя. Пример: `/driver Иванов`")
        return
    driver_name = context.args[0]
    df = user_data[user_id]
    driver_df = df[df['Водитель'].str.contains(driver_name, case=False, na=False)]
    if driver_df.empty:
        await update.message.reply_text(f"❌ Водитель с фамилией '{driver_name}' не найден.")
        return
    total_trips = len(driver_df)
    total_earnings = driver_df['Стоимость'].sum()
    cars = ", ".join(driver_df['Гос_номер'].unique())
    message = (
        f"👤 *Статистика по водителю {driver_name}*\n\n"
        f"▫️ Совершено маршрутов: {total_trips}\n"
        f"▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n"
        f"▫️ Работал(а) на машинах: {cars}"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode='Markdown')

# --- Код для фонового веб-сервера (без изменений) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is alive")

def run_health_check_server():
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logging.info(f"Health check server running on port {port}")
    httpd.serve_forever()

if __name__ == '__main__':
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")

    application = ApplicationBuilder().token(TOKEN).build()
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('stats', show_stats))
    application.add_handler(CommandHandler('clear', clear))
    application.add_handler(CommandHandler('top', show_top_stats))
    application.add_handler(CommandHandler('export', export_data))
    application.add_handler(CommandHandler('car', car_stats))
    application.add_handler(CommandHandler('driver', driver_stats))
    
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    health_thread = threading.Thread(target=run_health_check_server)
    health_thread.daemon = True
    health_thread.start()
    
    print("Бот запущен...")
    application.run_polling()
