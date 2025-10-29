import os
import logging
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

# Импортируем нашу функцию парсинга
from parser import process_excel_file

# Настраиваем логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Словарь для хранения данных пользователей в формате {user_id: DataFrame}
user_data = {}

# --- КОМАНДЫ БОТА ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    # При старте очищаем старые данные пользователя
    if user_id in user_data:
        del user_data[user_id]
        
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "👋 Привет! Я бот для анализа поездок.\n\n"
            "Просто отправьте мне один или несколько Excel-файлов (.xlsx) с отчетами. "
            "Я соберу из них данные.\n\n"
            "Доступные команды:\n"
            "📊 /stats - показать общую статистику по всем загруженным файлам\n"
            "🚗 /car [госномер] - статистика по машине (например, `/car 123`)\n"
            "👤 /driver [фамилия] - статистика по водителю (например, `/driver Иванов`)\n"
            "🗑️ /clear - удалить все загруженные данные и начать заново"
        )
    )

async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id in user_data:
        del user_data[user_id]
        await context.bot.send_message(chat_id=update.effective_chat.id, text="🗑️ Все загруженные данные удалены. Можете начать сначала.")
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="ℹ️ У вас нет загруженных данных для очистки.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    file = await update.message.document.get_file()
    file_name = update.message.document.file_name

    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл в формате .xlsx")
        return

    await update.message.reply_text(f"⏳ Получил файл '{file_name}'. Начинаю обработку...")
    
    # Скачиваем файл в память
    file_content = await file.download_as_bytearray()
    
    # Обрабатываем файл с помощью нашего парсера
    new_df = process_excel_file(bytes(file_content), file_name)

    if new_df is None or new_df.empty:
        await update.message.reply_text(f"⚠️ Не удалось извлечь данные из файла '{file_name}'. Возможно, структура файла не подходит.")
        return

    # Добавляем или обновляем данные пользователя
    if user_id in user_data:
        user_data[user_id] = pd.concat([user_data[user_id], new_df], ignore_index=True)
    else:
        user_data[user_id] = new_df
    
    total_rows = len(user_data[user_id])
    total_sum = user_data[user_id]['Стоимость'].sum()
    
    await update.message.reply_text(
        f"✅ Файл '{file_name}' успешно обработан!\n\n"
        f"Добавлено записей: {len(new_df)}\n"
        f"Всего загружено записей: {total_rows}\n"
        f"Общая сумма: {total_sum:,.0f} руб.\n\n"
        "Отправьте еще файл или используйте команду /stats для просмотра итогов."
    )

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="ℹ️ Данные для анализа отсутствуют. Пожалуйста, сначала загрузите файлы.")
        return
    
    df = user_data[user_id]
    total_trips = len(df)
    total_earnings = df['Стоимость'].sum()
    unique_cars_count = df['Гос_номер'].nunique()
    unique_drivers_count = df['Водитель'].nunique()

    message = (
        f"📊 *Общая статистика*\n\n"
        f"▫️ Всего маршрутов: {total_trips}\n"
        f"▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n"
        f"▫️ Уникальных машин: {unique_cars_count}\n"
        f"▫️ Уникальных водителей: {unique_drivers_count}"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode='Markdown')

async def car_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
        
    if not context.args:
        await update.message.reply_text("⚠️ Укажите госномер после команды. Пример: `/car 123`")
        return
        
    car_number = context.args[0]
    df = user_data[user_id]
    
    # Ищем без учета регистра и пробелов
    car_df = df[df['Гос_номер'].astype(str).str.contains(car_number, case=False, na=False)]
    
    if car_df.empty:
        await update.message.reply_text(f"❌ Машина с госномером '{car_number}' не найдена в загруженных данных.")
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
    user_id = update.message.from_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют. Загрузите файлы.")
        return
        
    if not context.args:
        await update.message.reply_text("⚠️ Укажите фамилию водителя. Пример: `/driver Иванов`")
        return
        
    driver_name = context.args[0]
    df = user_data[user_id]
    
    # Ищем по фамилии без учета регистра
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


if __name__ == '__main__':
    # Получаем токен из переменных окружения
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")

    application = ApplicationBuilder().token(TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('clear', clear))
    application.add_handler(CommandHandler('stats', show_stats))
    application.add_handler(CommandHandler('car', car_stats))
    application.add_handler(CommandHandler('driver', driver_stats))
    
    # Добавляем обработчик документов
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    print("Бот запущен...")
    application.run_polling()
