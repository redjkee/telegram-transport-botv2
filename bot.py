import os
import logging
import pandas as pd
import io
import traceback # <-- Добавляем для подробных логов ошибок
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

# Настраиваем логирование, чтобы видеть все сообщения
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
# Добавляем логгер для библиотеки telegram, чтобы видеть и ее сообщения
logging.getLogger("httpx").setLevel(logging.WARNING)


user_data = {}

# --- ОБРАБОТЧИК КНОПОК МЕНЮ (С МАКСИМАЛЬНЫМ ЛОГИРОВАНИЕМ) ---

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки с подробным логированием."""
    
    # 1. Записываем в лог, что функция вообще была вызвана
    logging.info("--- Button callback triggered! ---")
    
    query = update.callback_query
    if not query:
        logging.error("!!! CallbackQuery object is MISSING in the update!")
        return

    try:
        # 2. Пытаемся ответить на запрос, чтобы убрать "часики"
        await query.answer()
        logging.info("Successfully answered the callback query.")
        
        user_id = query.from_user.id
        command = query.data
        
        # 3. Записываем в лог, какая кнопка была нажата
        logging.info(f"User '{user_id}' pressed button with data: '{command}'")

        # 4. Выполняем основную логику, обернув ее в try-except
        
        has_data = user_id in user_data and not user_data[user_id].empty

        if command == 'stats':
            if not has_data:
                await query.edit_message_text("ℹ️ Данные для анализа отсутствуют.")
                logging.info("Sent 'no data' message for stats.")
                return
            
            df = user_data[user_id]
            message = f"📊 *Общая статистика*\n\n▫️ Всего маршрутов: {len(df)}" # Упрощено для теста
            await query.edit_message_text(text=message, parse_mode='Markdown')
            logging.info("Successfully edited message for 'stats'.")

        elif command == 'clear':
            if user_id in user_data:
                del user_data[user_id]
                await query.edit_message_text("🗑️ Все загруженные данные удалены.")
                logging.info("Cleared user data.")
            else:
                await query.edit_message_text("ℹ️ У вас нет данных для очистки.")
                logging.info("Sent 'no data to clear' message.")

        # ... (остальные кнопки пока можно проигнорировать, главное - проверить одну) ...
        else:
             logging.warning(f"Received unknown button command: {command}")
             await query.edit_message_text(f"Неизвестная команда: {command}")


    except Exception as e:
        # 5. Если что-то пошло не так, записываем ПОЛНУЮ ошибку в лог
        logging.error("!!! AN ERROR OCCURRED IN BUTTON_CALLBACK !!!")
        logging.error(f"Error Type: {type(e).__name__}")
        logging.error(f"Error Message: {e}")
        # traceback.format_exc() даст нам полный путь ошибки
        logging.error(f"Traceback:\n{traceback.format_exc()}")
        
        # Также сообщаем об ошибке пользователю
        try:
            await query.edit_message_text(f"❌ Произошла внутренняя ошибка при обработке кнопки.")
        except Exception as e2:
            logging.error(f"Could not even send an error message to user: {e2}")

    logging.info("--- Button callback finished. ---")


# --- Остальной код бота (без изменений, просто для полноты) ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    welcome_text = "👋 Привет! Это бот для анализа поездок."
    if user_id in user_data and not user_data[user_id].empty:
        welcome_text += f"\n\nℹ️ У вас уже загружено записей: {len(user_data[user_id])}."
    keyboard = [
        [InlineKeyboardButton("📊 Общая статистика", callback_data='stats')],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='clear')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(welcome_text, reply_markup=reply_markup)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file = await update.message.document.get_file()
    file_name = update.message.document.file_name
    await update.message.reply_text(f"⏳ Получил файл '{file_name}'. Обрабатываю...")
    file_content = await file.download_as_bytearray()
    new_df = process_excel_file(bytes(file_content), file_name)
    if new_df is None or new_df.empty:
        await update.message.reply_text(f"⚠️ Не удалось извлечь данные из файла '{file_name}'.")
        return
    if user_id in user_data:
        user_data[user_id] = pd.concat([user_data[user_id], new_df], ignore_index=True)
    else:
        user_data[user_id] = new_df
    await update.message.reply_text(f"✅ Файл '{file_name}' обработан! Всего записей: {len(user_data[user_id])}")

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers(); self.wfile.write(b"Bot is alive")
def run_health_check_server():
    port = int(os.environ.get("PORT", 8080)); httpd = HTTPServer(('', port), HealthCheckHandler); httpd.serve_forever()

if __name__ == '__main__':
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN: raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")
    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    threading.Thread(target=run_health_check_server, daemon=True).start()
    print("Бот запущен в диагностическом режиме...")
    application.run_polling()
