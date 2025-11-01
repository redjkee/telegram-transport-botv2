# bot.py (ВЕРСИЯ 7.1 - ПРОФЕССИОНАЛЬНЫЕ ОТЧЕТЫ, ПОЛНЫЙ КОД)

import os
import logging
import pandas as pd
import io
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ConversationHandler
)
from telegram.error import BadRequest
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from parser import process_excel_file
import db

# --- Настройка ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

(ASK_CAR_STATS, ASK_DRIVER_STATS, ASK_CAR_EXPORT, ASK_DRIVER_EXPORT) = range(4)

# --- КОНСТАНТЫ ДЛЯ ОТЧЕТА ---
EARNINGS_MAP = {
    20000: 4000, 36000: 7000, 140000: 25000,
    24000: 4000, 155000: 25000, 304000: 60000
}
RUSSIAN_MONTHS = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель", 5: "май", 6: "июнь",
    7: "июль", 8: "август", 9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
}

# --- Клавиатуры ---
def get_main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Общая статистика", callback_data='main_stats')],
        [InlineKeyboardButton("🚗 Статистика по гос. номеру", callback_data='main_ask_car_stats')],
        [InlineKeyboardButton("👤 Статистика по фамилии", callback_data='main_ask_driver_stats')],
        [InlineKeyboardButton("📥 Экспорт в Excel", callback_data='main_export_menu')],
        [InlineKeyboardButton("🏆 Топ-5", callback_data='main_top')],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='main_clear')],
    ])
def get_export_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Полный отчет", callback_data='export_full')],
        [InlineKeyboardButton("🚗 По гос. номеру (кастомный)", callback_data='export_ask_car')],
        [InlineKeyboardButton("👤 По фамилии", callback_data='export_ask_driver')],
        [InlineKeyboardButton("⬅️ Назад в главное меню", callback_data='back_to_main_menu')],
    ])
post_upload_keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton("📊 Отчет по авто", callback_data='summary_car')],
    [InlineKeyboardButton("👤 Отчет по водителям", callback_data='summary_driver')],
    [InlineKeyboardButton("⬅️ В главное меню", callback_data='back_to_main_menu')]
])
cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data='cancel_conversation')]])
back_to_main_menu_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в главное меню", callback_data='back_to_main_menu')]])

# --- ИНИЦИАЛИЗАЦИЯ БД ---
async def post_init(application: Application):
    if not await db.init_db():
        logging.critical("CRITICAL: Could not initialize database.")

# --- НОВАЯ, УЛУЧШЕННАЯ ФУНКЦИЯ СОЗДАНИЯ ОТЧЕТА ---
async def create_car_report_excel(df: pd.DataFrame, car_plate: str) -> io.BytesIO:
    output = io.BytesIO()
    
    report_df = df.copy()
    report_df['ЗП Водителя'] = report_df['Стоимость'].map(EARNINGS_MAP)
    final_df = report_df[['Дата', 'Маршрут', 'Стоимость', 'ЗП Водителя']].copy()
    
    total_cost = final_df['Стоимость'].sum()
    total_driver_earnings = final_df['ЗП Водителя'].sum()
    tax = total_cost * 0.11
    profit = total_cost - total_driver_earnings - tax

    month_name = ""
    try:
        first_date_str = final_df['Дата'].dropna().iloc[0]
        month_num = datetime.strptime(first_date_str, '%d.%m.%y').month
        month_name = RUSSIAN_MONTHS.get(month_num, '')
    except (IndexError, ValueError): pass

    sheet_name = f"{car_plate} {month_name}".strip()
    sheet_title = f"Отчет по машине {car_plate} за {month_name}"

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet

        # --- Создаем форматы ---
        title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter'})
        header_format = workbook.add_format({'bold': True, 'border': 1, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter'})
        cell_border_format = workbook.add_format({'border': 1})
        currency_border_format = workbook.add_format({'border': 1, 'num_format': '#,##0'})
        date_border_format = workbook.add_format({'border': 1, 'num_format': 'dd.mm.yy'})
        
        summary_label_format = workbook.add_format({'bold': True, 'align': 'right'})
        summary_value_format = workbook.add_format({'bold': True, 'num_format': '#,##0'})

        # --- Заголовок отчета ---
        worksheet.merge_range('A1:D1', sheet_title, title_format)
        worksheet.set_row(0, 30) # Высота строки для заголовка

        # --- Заголовки таблицы ---
        worksheet.write_row('A2', final_df.columns, header_format)

        # --- Данные таблицы (запись вручную для применения форматов) ---
        for row_num, data in enumerate(final_df.itertuples(index=False), 3):
            worksheet.write(f'A{row_num}', data[0], date_border_format) # Дата
            worksheet.write(f'B{row_num}', data[1], cell_border_format) # Маршрут
            worksheet.write(f'C{row_num}', data[2], currency_border_format) # Стоимость
            if pd.notna(data[3]):
                worksheet.write(f'D{row_num}', data[3], currency_border_format) # ЗП Водителя
            else:
                worksheet.write(f'D{row_num}', '', cell_border_format) # Пустая ячейка с рамкой

        # --- Настройка ширины колонок ---
        worksheet.set_column('A:A', 12)
        worksheet.set_column('B:B', 40)
        worksheet.set_column('C:D', 15)

        # --- Итоги под таблицей ---
        summary_start_row = len(final_df) + 4
        worksheet.write(summary_start_row, 1, "Итого:", summary_label_format)
        worksheet.write(summary_start_row, 2, total_cost, summary_value_format)
        worksheet.write(summary_start_row, 3, total_driver_earnings, summary_value_format)
        
        worksheet.write(summary_start_row + 1, 1, "Налог (11%):", summary_label_format)
        worksheet.write_formula(summary_start_row + 1, 2, f'=C{summary_start_row+1}*0.11', summary_value_format, tax)
        
        worksheet.write(summary_start_row + 2, 1, "Прибыль:", summary_label_format)
        worksheet.write_formula(summary_start_row + 2, 2, f'=C{summary_start_row+1}-D{summary_start_row+1}-C{summary_start_row+2}', summary_value_format, profit)

    output.seek(0)
    return output

# --- Остальной код ---
# (Ниже идет полный код всех остальных функций, чтобы вам не пришлось ничего совмещать)

# ... (start, button_handler, и все остальные функции из v6.0/v5.3) ...
# --- Полный код для bot.py для простоты ---
# (Ниже идет полный код, чтобы вам не пришлось ничего совмещать)
# ... (начало файла: импорты, логирование, состояния)
# ... (Клавиатуры)
def get_main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Общая статистика", callback_data='main_stats')],
        [InlineKeyboardButton("🚗 Статистика по гос. номеру", callback_data='main_ask_car_stats')],
        [InlineKeyboardButton("👤 Статистика по фамилии", callback_data='main_ask_driver_stats')],
        [InlineKeyboardButton("📥 Экспорт в Excel", callback_data='main_export_menu')],
        [InlineKeyboardButton("🏆 Топ-5", callback_data='main_top')],
        [InlineKeyboardButton("🗑️ Очистить данные", callback_data='main_clear')],
    ])
def get_export_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Полный отчет", callback_data='export_full')],
        [InlineKeyboardButton("🚗 По гос. номеру (кастомный)", callback_data='export_ask_car')],
        [InlineKeyboardButton("👤 По фамилии", callback_data='export_ask_driver')],
        [InlineKeyboardButton("⬅️ Назад в главное меню", callback_data='back_to_main_menu')],
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_id = update.effective_user.id
    welcome_text = ( "👋 **Аналитический бот v7.1**\n\nВыберите действие:")
    df = await db.get_all_trips_as_df(user_id)
    if not df.empty:
        processed_files = await db.get_processed_files(user_id)
        welcome_text += (f"\n\n**Текущая сессия:**\n▫️ Загружено файлов: {len(processed_files)}\n▫️ Всего записей: {len(df)}\n▫️ Общий доход: *{df['Стоимость'].sum():,.0f} руб.*")
    if update.callback_query:
        await update.callback_query.edit_message_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode='Markdown')
    else:
        await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode='Markdown')
    return ConversationHandler.END
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    command = query.data
    try:
        df = await db.get_all_trips_as_df(user_id)
        has_data = not df.empty
        if command == 'back_to_main_menu':
            await start(update, context)
            return
        if command == 'main_export_menu':
            await query.edit_message_text("📥 **Экспорт в Excel**\n\nВыберите тип отчета:", reply_markup=get_export_menu_keyboard(), parse_mode='Markdown')
            return
        if command == 'main_clear':
            await db.clear_user_data(user_id)
            await query.edit_message_text("🗑️ Все загруженные данные удалены.", reply_markup=back_to_main_menu_keyboard)
            return
        if not has_data:
            await query.edit_message_text("ℹ️ Данные для анализа отсутствуют. Загрузите файлы.", reply_markup=back_to_main_menu_keyboard)
            return
        if command == 'main_stats':
            processed_files = await db.get_processed_files(user_id)
            message = (f"📊 *Общая статистика*\n\n▫️ Обработано файлов: {len(processed_files)}\n▫️ Всего маршрутов: {len(df)}\n▫️ Общий заработок: *{df['Стоимость'].sum():,.2f} руб.*\n▫️ Уникальных машин: {df['Гос_номер'].nunique()}\n▫️ Уникальных водителей: {df['Водитель'].nunique()}")
            await query.edit_message_text(message, parse_mode='Markdown', reply_markup=back_to_main_menu_keyboard)
        elif command == 'main_top':
            top_drivers = df.groupby('Водитель')['Стоимость'].sum().nlargest(5)
            top_drivers_text = "".join([f"{i}. {d} - {t:,.0f} руб.\n" for i, (d, t) in enumerate(top_drivers.items(), 1)])
            top_cars = df.groupby('Гос_номер')['Стоимость'].sum().nlargest(5)
            top_cars_text = "".join([f"{i}. Номер {c} - {t:,.0f} руб.\n" for i, (c, t) in enumerate(top_cars.items(), 1)])
            message = (f"🏆 *Топ-5 по заработку*\n\n👤 *Лучшие водители:*\n{top_drivers_text or 'Нет данных'}\n🚗 *Самые прибыльные машины:*\n{top_cars_text or 'Нет данных'}")
            await query.edit_message_text(message, parse_mode='Markdown', reply_markup=back_to_main_menu_keyboard)
        elif command == 'export_full':
            await send_excel_report(df, query.message.chat_id, context, "полный_отчет.xlsx")
            await context.bot.send_message(query.message.chat_id, "Выберите следующее действие:", reply_markup=back_to_main_menu_keyboard)
        elif command == 'summary_car' or command == 'summary_driver':
            group_by_col = 'Гос_номер' if command == 'summary_car' else 'Водитель'
            title = "🚗 Сводка по автомобилям" if command == 'summary_car' else "👤 Сводка по водителям"
            summary = df.groupby(group_by_col)['Стоимость'].sum().sort_values(ascending=False)
            summary_text = f"**{title}**\n\n"
            for item, total in summary.items():
                summary_text += f"▫️ {item}: *{total:,.0f} руб.*\n"
            await query.edit_message_text(summary_text, parse_mode='Markdown', reply_markup=back_to_main_menu_keyboard)
    except BadRequest as e:
        if "Message is not modified" in str(e): logging.info("Ignoring 'Message is not modified' error.")
        else: logging.error(f"An unexpected BadRequest error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred in button_callback: {e}")
        try: await query.edit_message_text("❌ Произошла ошибка.", reply_markup=back_to_main_menu_keyboard)
        except Exception as e2: logging.error(f"Could not send error message to user: {e2}")
async def ask_for_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    query = update.callback_query
    await query.answer()
    action = query.data
    if action == 'main_ask_car_stats':
        await query.edit_message_text("🔢 Пожалуйста, введите 3 цифры гос. номера:", reply_markup=cancel_keyboard)
        return ASK_CAR_STATS
    elif action == 'main_ask_driver_stats':
        await query.edit_message_text("👤 Пожалуйста, введите фамилию водителя:", reply_markup=cancel_keyboard)
        return ASK_DRIVER_STATS
    elif action == 'export_ask_car':
        await query.edit_message_text("🔢 Введите гос. номер для создания отчета:", reply_markup=cancel_keyboard)
        return ASK_CAR_EXPORT
    elif action == 'export_ask_driver':
        await query.edit_message_text("👤 Введите фамилию для создания отчета:", reply_markup=cancel_keyboard)
        return ASK_DRIVER_EXPORT
async def handle_car_stats_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_input = update.message.text
    user_id = update.effective_user.id
    df = await db.get_all_trips_as_df(user_id)
    car_df = df[df['Гос_номер'].astype(str).str.contains(user_input, case=False, na=False)]
    if car_df.empty:
        await update.message.reply_text(f"❌ Машина с номером '{user_input}' не найдена. Попробуйте еще раз или нажмите 'Отмена'.", reply_markup=cancel_keyboard)
        return ASK_CAR_STATS
    drivers = ", ".join(car_df['Водитель'].unique())
    message = (f"🚗 *Статистика по машине {user_input}*\n\n▫️ Совершено маршрутов: {len(car_df)}\n▫️ Общий заработок: *{car_df['Стоимость'].sum():,.2f} руб.*\n▫️ Водители: {drivers}")
    await update.message.reply_text(message, parse_mode='Markdown', reply_markup=back_to_main_menu_keyboard)
    return ConversationHandler.END
async def handle_driver_stats_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_input = update.message.text
    user_id = update.effective_user.id
    df = await db.get_all_trips_as_df(user_id)
    driver_df = df[df['Водитель'].str.contains(user_input, case=False, na=False)]
    if driver_df.empty:
        await update.message.reply_text(f"❌ Водитель '{user_input}' не найден. Попробуйте еще раз или нажмите 'Отмена'.", reply_markup=cancel_keyboard)
        return ASK_DRIVER_STATS
    cars = ", ".join(driver_df['Гос_номер'].unique())
    message = (f"👤 *Статистика по водителю {user_input}*\n\n▫️ Совершено маршрутов: {len(driver_df)}\n▫️ Общий заработок: *{driver_df['Стоимость'].sum():,.2f} руб.*\n▫️ Машины: {cars}")
    await update.message.reply_text(message, parse_mode='Markdown', reply_markup=back_to_main_menu_keyboard)
    return ConversationHandler.END
async def handle_car_export_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_input = update.message.text
    user_id = update.effective_user.id
    df = await db.get_all_trips_as_df(user_id)
    car_df = df[df['Гос_номер'].astype(str).str.contains(user_input, case=False, na=False)]
    if car_df.empty:
        await update.message.reply_text(f"❌ Машина '{user_input}' не найдена. Попробуйте еще раз или отмените экспорт.", reply_markup=cancel_keyboard)
        return ASK_CAR_EXPORT
    report_buffer = await create_car_report_excel(car_df, user_input)
    await context.bot.send_document(chat_id=update.message.chat_id, document=report_buffer, filename=f"отчет_{user_input}.xlsx", caption=f"📊 Ваш кастомный отчет по машине {user_input} готов.")
    await update.message.reply_text("Выберите следующее действие:", reply_markup=back_to_main_menu_keyboard)
    return ConversationHandler.END
async def handle_driver_export_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_input = update.message.text
    user_id = update.effective_user.id
    df = await db.get_all_trips_as_df(user_id)
    driver_df = df[df['Водитель'].str.contains(user_input, case=False, na=False)]
    if driver_df.empty:
        await update.message.reply_text(f"❌ Водитель '{user_input}' не найден. Попробуйте еще раз или отмените экспорт.", reply_markup=cancel_keyboard)
        return ASK_DRIVER_EXPORT
    await send_excel_report(driver_df, update.message.chat_id, context, f"отчет_водитель_{user_input}.xlsx")
    await update.message.reply_text("Выберите следующее действие:", reply_markup=back_to_main_menu_keyboard)
    return ConversationHandler.END
async def send_excel_report(df: pd.DataFrame, chat_id: int, context: ContextTypes.DEFAULT_TYPE, filename: str):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Отчет')
        worksheet = writer.sheets['Отчет']
        for idx, col in enumerate(df):
            max_len = max((df[col].astype(str).map(len).max(), len(str(df[col].name)) + 1))
            worksheet.set_column(idx, idx, max_len)
    output.seek(0)
    await context.bot.send_document(chat_id=chat_id, document=output, filename=filename, caption='📊 Ваш отчет готов.')
async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Действие отменено.", reply_markup=back_to_main_menu_keyboard)
    return ConversationHandler.END
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await db.get_or_create_user(update)
    user_id = update.effective_user.id
    file = await update.message.document.get_file()
    file_name = update.message.document.file_name
    processed_files = await db.get_processed_files(user_id)
    if file_name in processed_files:
        await update.message.reply_text(f"⚠️ Файл '{file_name}' уже был обработан ранее. Загрузка пропущена.")
        return
    await update.message.reply_text(f"⏳ Получил файл '{file_name}'. Обрабатываю...")
    file_content = await file.download_as_bytearray()
    new_df = process_excel_file(bytes(file_content), file_name)
    if new_df is None or new_df.empty:
        await update.message.reply_text(f"⚠️ Не удалось извлечь данные из файла '{file_name}'.")
        return
    await db.add_trips_from_df(user_id, new_df)
    full_df = await db.get_all_trips_as_df(user_id)
    message_text = (f"✅ Файл '{file_name}' успешно обработан!\n"
                    f"Добавлено записей: {len(new_df)}\n"
                    f"Всего загружено: {len(full_df)}\n\n"
                    "Что вы хотите сделать дальше?")
    await update.message.reply_text(message_text, reply_markup=post_upload_keyboard)
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers(); self.wfile.write(b"Bot is alive")
    def do_HEAD(self): self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers()
    def log_message(self, format, *args): return
def run_health_check_server():
    port = int(os.environ.get("PORT", 8080)); httpd = HTTPServer(('', port), HealthCheckHandler); httpd.serve_forever()

if __name__ == '__main__':
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN: raise ValueError("Необходимо установить переменную окружения TELEGRAM_TOKEN")
    application = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(ask_for_input, pattern='^main_ask_car_stats$'),
            CallbackQueryHandler(ask_for_input, pattern='^main_ask_driver_stats$'),
            CallbackQueryHandler(ask_for_input, pattern='^export_ask_car$'),
            CallbackQueryHandler(ask_for_input, pattern='^export_ask_driver$'),
        ],
        states={
            ASK_CAR_STATS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_car_stats_input)],
            ASK_DRIVER_STATS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_driver_stats_input)],
            ASK_CAR_EXPORT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_car_export_input)],
            ASK_DRIVER_EXPORT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_driver_export_input)],
        },
        fallbacks=[
            CommandHandler('start', start),
            CallbackQueryHandler(cancel_conversation, pattern='^cancel_conversation$')
        ],
        per_message=False
    )
    application.add_handler(CommandHandler('start', start))
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    threading.Thread(target=run_health_check_server, daemon=True).start()
    print("Бот запущен в финальной версии (v7.0 - Кастомные отчеты)...")
    application.run_polling()
