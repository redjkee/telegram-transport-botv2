
import os
import logging
import pandas as pd
import io # Для работы с файлами в памяти
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    MessageHandler, 
    filters,
    CallbackQueryHandler
)

# Импортируем нашу функцию парсинга
from parser import process_excel_file

# Настраиваем логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Словарь для хранения данных пользователей в формате {user_id: DataFrame}
user_data = {}

# --- ОСНОВНЫЕ КОМАНДЫ И ОБРАБОТЧИКИ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # При старте очищаем старые данные пользователя
    if user_id in user_data:
        del user_data[user_id]

    # Создаем интерактивные кнопки
    keyboard = [
        [InlineKeyboardButton("📊 Общая статистика", callback_data='stats')],
        [
            InlineKeyboardButton("🏆 Топ-5", callback_data='top'),
            InlineKeyboardButton("📥 Экспорт в Excel", callback_data='export')
        ],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='clear')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
        
    await update.message.reply_text(
        (
            "👋 Привет! Я бот для анализа поездок.\n\n"
            "Просто отправьте мне один или несколько Excel-файлов (.xlsx) с отчетами. "
            "Когда закончите, используйте кнопки ниже для получения статистики.\n\n"
            "Также доступны команды для поиска:\n"
            "🚗 `/car [номер]` - статистика по машине (например, `/car 123`)\n"
            "👤 `/driver [фамилия]` - статистика по водителю (например, `/driver иванов`)"
        ),
        reply_markup=reply_markup
    )

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

# --- НОВЫЙ ОБРАБОТЧИК ДЛЯ КНОПОК ---
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() # Отвечаем на колбэк, чтобы убрать "часики" с кнопки

    command = query.data
    if command == 'stats':
        await show_stats(query, context, is_callback=True)
    elif command == 'clear':
        await clear(query, context, is_callback=True)
    elif command == 'top':
        await show_top_stats(query, context, is_callback=True)
    elif command == 'export':
        await export_data(query, context, is_callback=True)

# --- ФУНКЦИИ СТАТИСТИКИ (обновлены для работы с кнопками) ---

async def show_stats(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update_or_query.from_user.id
    chat_id = update_or_query.effective_chat.id
    
    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
    
    df = user_data[user_id]
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
    # Если это колбэк от кнопки, то редактируем сообщение, иначе отправляем новое
    if is_callback:
        await update_or_query.edit_message_text(text=message, parse_mode='Markdown')
    else:
        await context.bot.send_message(chat_id, text=message, parse_mode='Markdown')

async def clear(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update_or_query.from_user.id
    chat_id = update_or_query.effective_chat.id

    if user_id in user_data:
        del user_data[user_id]
        message = "🗑️ Все загруженные данные удалены. Можете начать сначала."
    else:
        message = "ℹ️ У вас нет загруженных данных для очистки."

    if is_callback:
        await update_or_query.edit_message_text(text=message)
    else:
        await context.bot.send_message(chat_id, text=message)

# --- НОВЫЕ ФУНКЦИИ ДЛЯ РАСШИРЕННОЙ СТАТИСТИКИ ---

async def show_top_stats(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update_or_query.from_user.id
    chat_id = update_or_query.effective_chat.id

    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Данные для анализа отсутствуют.")
        return

    df = user_data[user_id]
    
    # Топ 5 водителей
    top_drivers = df.groupby('Водитель')['Стоимость'].sum().nlargest(5)
    top_drivers_text = ""
    for i, (driver, total) in enumerate(top_drivers.items(), 1):
        top_drivers_text += f"{i}. {driver} - {total:,.0f} руб.\n"
        
    # Топ 5 машин
    top_cars = df.groupby('Гос_номер')['Стоимость'].sum().nlargest(5)
    top_cars_text = ""
    for i, (car, total) in enumerate(top_cars.items(), 1):
        top_cars_text += f"{i}. Номер {car} - {total:,.0f} руб.\n"

    message = (
        f"🏆 *Топ-5 по заработку*\n\n"
        f"👤 *Лучшие водители:*\n{top_drivers_text}\n"
        f"🚗 *Самые прибыльные машины:*\n{top_cars_text}"
    )
    
    if is_callback:
        await update_or_query.edit_message_text(text=message, parse_mode='Markdown')
    else:
        await context.bot.send_message(chat_id, text=message, parse_mode='Markdown')
        
async def export_data(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    user_id = update_or_query.from_user.id
    chat_id = update_or_query.effective_chat.id

    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id, "ℹ️ Нет данных для экспорта.")
        return
        
    df = user_data[user_id]
    
    # Создаем Excel-файл в памяти
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Сводный отчет')
        # Автоподбор ширины колонок для красоты
        worksheet = writer.sheets['Сводный отчет']
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            )) + 1
            worksheet.set_column(idx, idx, max_len)

    output.seek(0)
    
    await context.bot.send_document(
        chat_id=chat_id, 
        document=output, 
        filename='сводный_отчет.xlsx',
        caption='📊 Ваш сводный отчет по всем загруженным файлам.'
    )

# --- КОМАНДЫ ПОИСКА ---

async def car_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Эта и следующая функции остаются без изменений, так как они требуют ввода от пользователя
    ...

async def driver_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ...
# (Код функций car_stats и driver_stats остается таким же, как в предыдущей версии)
# Просто скопируйте их сюда из предыдущего ответа, они работают хорошо.

if __name__ == '__main__':
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")

    application = ApplicationBuilder().token(TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('stats', show_stats))
    application.add_handler(CommandHandler('clear', clear))
    application.add_handler(CommandHandler('top', show_top_stats))
    application.add_handler(CommandHandler('export', export_data))
    application.add_handler(CommandHandler('car', car_stats))
    application.add_handler(CommandHandler('driver', driver_stats))
    
    # Добавляем обработчик для кнопок
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Добавляем обработчик документов
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    print("Бот запущен...")
    application.run_polling()

