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
# Импортируем класс ошибки, чтобы ее ловить
from telegram.error import BadRequest
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from parser import process_excel_file

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

user_data = {}

# --- Клавиатуры для удобства ---
back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data='back_to_menu')]])

# Клавиатура после загрузки файла
post_upload_keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton("📊 Отчет по авто", callback_data='summary_car')],
    [InlineKeyboardButton("👤 Отчет по водителям", callback_data='summary_driver')],
    [InlineKeyboardButton("⬅️ В главное меню", callback_data='back_to_menu')]
])

def get_main_menu(user_id):
    """Возвращает текст и клавиатуру для главного меню."""
    welcome_text = (
        "👋 **Главное меню**\n\n"
        "Отправьте мне Excel-файл для анализа или используйте кнопки ниже для просмотра статистики."
    )
    if user_id in user_data and not user_data[user_id].empty:
        welcome_text += f"\n\nℹ️ Загружено записей: {len(user_data[user_id])}."
    
    keyboard = [
        [InlineKeyboardButton("📊 Общая статистика", callback_data='stats')],
        [InlineKeyboardButton("🏆 Топ-5", callback_data='top'), InlineKeyboardButton("📥 Экспорт в Excel", callback_data='export')],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='clear')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    return welcome_text, reply_markup

# --- ОБРАБОТЧИКИ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет главное меню."""
    user_id = update.effective_user.id
    text, markup = get_main_menu(user_id)
    await update.message.reply_text(text, reply_markup=markup, parse_mode='Markdown')

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает все нажатия на инлайн-кнопки."""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    command = query.data

    try:
        if command == 'back_to_menu':
            text, markup = get_main_menu(user_id)
            await query.edit_message_text(text, reply_markup=markup, parse_mode='Markdown')
            return

        has_data = user_id in user_data and not user_data[user_id].empty

        if command == 'summary_car' or command == 'summary_driver':
            if not has_data:
                await query.edit_message_text("ℹ️ Данные для анализа отсутствуют.", reply_markup=back_keyboard)
                return
            
            df = user_data[user_id]
            group_by_col = 'Гос_номер' if command == 'summary_car' else 'Водитель'
            title = "🚗 Сводка по автомобилям" if command == 'summary_car' else "👤 Сводка по водителям"
            
            summary = df.groupby(group_by_col)['Стоимость'].sum().sort_values(ascending=False)
            
            summary_text = f"**{title}**\n\n"
            if summary.empty:
                summary_text += "Нет данных для отображения."
            else:
                for item, total in summary.items():
                    summary_text += f"▫️ {item}: *{total:,.0f} руб.*\n"
            
            await query.edit_message_text(summary_text, parse_mode='Markdown', reply_markup=back_keyboard)

        elif command == 'stats':
            if not has_data:
                await query.edit_message_text("ℹ️ Данные для анализа отсутствуют.", reply_markup=back_keyboard)
                return
            df = user_data[user_id]
            message = (f"📊 *Общая статистика*\n\n"
                       f"▫️ Обработано файлов: {df['Источник'].nunique()}\n"
                       f"▫️ Всего маршрутов: {len(df)}\n"
                       f"▫️ Общий заработок: *{df['Стоимость'].sum():,.2f} руб.*\n"
                       f"▫️ Уникальных машин: {df['Гос_номер'].nunique()}\n"
                       f"▫️ Уникальных водителей: {df['Водитель'].nunique()}")
            await query.edit_message_text(text=message, parse_mode='Markdown', reply_markup=back_keyboard)

        elif command == 'top':
            if not has_data:
                await query.edit_message_text("ℹ️ Данные для анализа отсутствуют.", reply_markup=back_keyboard)
                return
            df = user_data[user_id]
            top_drivers = df.groupby('Водитель')['Стоимость'].sum().nlargest(5)
            top_drivers_text = "".join([f"{i}. {d} - {t:,.0f} руб.\n" for i, (d, t) in enumerate(top_drivers.items(), 1)])
            top_cars = df.groupby('Гос_номер')['Стоимость'].sum().nlargest(5)
            top_cars_text = "".join([f"{i}. Номер {c} - {t:,.0f} руб.\n" for i, (c, t) in enumerate(top_cars.items(), 1)])
            message = (f"🏆 *Топ-5 по заработку*\n\n"
                       f"👤 *Лучшие водители:*\n{top_drivers_text or 'Нет данных'}\n"
                       f"🚗 *Самые прибыльные машины:*\n{top_cars_text or 'Нет данных'}")
            await query.edit_message_text(text=message, parse_mode='Markdown', reply_markup=back_keyboard)

        elif command == 'clear':
            if has_data:
                del user_data[user_id]
                await query.edit_message_text("🗑️ Все загруженные данные удалены.", reply_markup=back_keyboard)
            else:
                await query.edit_message_text("ℹ️ У вас нет данных для очистки.", reply_markup=back_keyboard)

        elif command == 'export':
            chat_id = query.message.chat_id
            if not has_data:
                await context.bot.send_message(chat_id=chat_id, text="ℹ️ Нет данных для экспорта.")
                return
            df = user_data[user_id]
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Сводный отчет')
                worksheet = writer.sheets['Сводный отчет']
                for idx, col in enumerate(df):
                    max_len = max(df[col].astype(str).map(len).max(), len(str(df[col].name))) + 1
                    worksheet.set_column(idx, idx, max_len)
            output.seek(0)
            await context.bot.send_document(chat_id=chat_id, document=output, filename='сводный_отчет.xlsx',
                                            caption='📊 Ваш сводный отчет по всем загруженным файлам.')
            await context.bot.send_message(chat_id=chat_id, text="✅ Файл с отчетом готов.")

    except BadRequest as e:
        if "Message is not modified" in str(e):
            logging.info("Ignoring 'Message is not modified' error due to fast double-click or old process.")
        else:
            logging.error(f"An unexpected BadRequest error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred in button_callback: {e}")
        try:
            await query.edit_message_text("❌ Произошла ошибка при обработке вашего запроса.", reply_markup=back_keyboard)
        except Exception as e2:
            logging.error(f"Could not even send an error message to user: {e2}")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file = await update.message.document.get_file()
    file_name = update.message.document.file_name
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл в формате .xlsx")
        return
    await update.message.reply_text(f"⏳ Получил файл '{file_name}'. Обрабатываю...")
    file_content = await file.download_as_bytearray()
    new_df = process_excel_file(bytes(file_content), file_name)
    if new_df is None or new_df.empty:
        await update.message.reply_text(f"⚠️ Не удалось извлечь данные из файла '{file_name}'.")
        return
    if user_id in user_data: user_data[user_id] = pd.concat([user_data[user_id], new_df], ignore_index=True)
    else: user_data[user_id] = new_df
    
    message_text = (f"✅ Файл '{file_name}' успешно обработан!\n"
                    f"Добавлено записей: {len(new_df)}\n"
                    f"Всего загружено: {len(user_data[user_id])}\n\n"
                    "Что вы хотите сделать дальше?")
    await update.message.reply_text(message_text, reply_markup=post_upload_keyboard)

async def car_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют.")
        return
    if not context.args:
        await update.message.reply_text("⚠️ **Ошибка:** Вы не указали номер.\nПример: `/car 123`", parse_mode='Markdown')
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
    message = (f"🚗 *Статистика по машине {car_number}*\n\n▫️ Совершено маршрутов: {total_trips}\n▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n▫️ Водители на этой машине: {drivers}")
    await update.message.reply_text(message, parse_mode='Markdown')

async def driver_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or user_data[user_id].empty:
        await update.message.reply_text("ℹ️ Данные для анализа отсутствуют.")
        return
    if not context.args:
        await update.message.reply_text("⚠️ **Ошибка:** Вы не указали фамилию.\nПример: `/driver Иванов`", parse_mode='Markdown')
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
    message = (f"👤 *Статистика по водителю {driver_name}*\n\n▫️ Совершено маршрутов: {total_trips}\n▫️ Общий заработок: *{total_earnings:,.2f} руб.*\n▫️ Работал(а) на машинах: {cars}")
    await update.message.reply_text(message, parse_mode='Markdown')

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers(); self.wfile.write(b"Bot is alive")
def run_health_check_server():
    port = int(os.environ.get("PORT", 8080)); httpd = HTTPServer(('', port), HealthCheckHandler); httpd.serve_forever()

if __name__ == '__main__':
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN: raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")
    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('car', car_stats))
    application.add_handler(CommandHandler('driver', driver_stats))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    threading.Thread(target=run_health_check_server, daemon=True).start()
    print("Бот запущен в финальном рабочем режиме...")
    application.run_polling()
