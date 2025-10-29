import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from openpyxl import load_workbook
import re
import tempfile
import asyncio
from collections import defaultdict

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота из переменных окружения
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Хранилище для данных пользователей
user_data_store = defaultdict(list)

# Функции парсинга (БЕЗ PANDAS)
def find_table_structure(ws):
    headers_positions = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value:
                cell_value = str(cell.value).strip()
                if "Товары (работы, услуги)" in cell_value:
                    headers_positions['description'] = (cell.row, cell.column)
                elif "Сумма" in cell_value and cell_value != "Сумма с НДС":
                    headers_positions['amount'] = (cell.row, cell.column)
    return headers_positions

def extract_data_from_description(description):
    description_str = str(description)
    route = description_str.split(',')[0].strip()
    date_match = re.search(r'от\s+(\d{2}\.\d{2}\.\d{2})', description_str)
    date_str = date_match.group(1) if date_match else "Дата не найдена"
    plate_match = re.search(r'(\d{3})', description_str)
    car_plate = plate_match.group(1) if plate_match else "Неизвестно"
    driver_match = re.search(r',\s*([А-Я][а-я]+)\s+[А-Я]\.[А-Я]\.', description_str)
    if driver_match:
        driver_name = driver_match.group(1)
    else:
        alt_driver_match = re.search(r',\s*([А-Я][а-я]+)', description_str)
        driver_name = alt_driver_match.group(1) if alt_driver_match else "Фамилия не найдена"
    return route, date_str, car_plate, driver_name

def parse_invoice_file(file_path):
    try:
        wb = load_workbook(file_path, data_only=True)
        ws = wb.active
        headers = find_table_structure(ws)
        
        if not headers.get('description') or not headers.get('amount'):
            return []
        
        header_row = max(h[0] for h in headers.values())
        description_col = headers['description'][1]
        amount_col = headers['amount'][1]
        
        parsed_data = []
        row_num = header_row + 1
        current_empty_rows = 0
        max_empty_rows = 5
        
        while current_empty_rows < max_empty_rows:
            description_cell = ws.cell(row=row_num, column=description_col)
            description = description_cell.value
            
            if not description:
                current_empty_rows += 1
                row_num += 1
                continue
                
            current_empty_rows = 0
            description_str = str(description)
            
            if any(word in description_str.lower() for word in ['итого', 'всего', 'итог', 'сумма']):
                row_num += 1
                continue
            
            amount_cell = ws.cell(row=row_num, column=amount_col)
            amount = amount_cell.value
            
            if amount is not None:
                try:
                    if isinstance(amount, str) and any(char.isalpha() for char in amount.replace(' ', '').replace(',', '.')):
                        row_num += 1
                        continue
                    
                    amount_str = str(amount).replace(' ', '').replace(',', '.')
                    amount_value = float(amount_str)
                    
                    route, date_str, car_plate, driver_name = extract_data_from_description(description_str)
                    
                    if car_plate != "Неизвестно" and amount_value > 0:
                        parsed_data.append({
                            'Дата': date_str,
                            'Маршрут': route,
                            'Стоимость': amount_value,
                            'Гос_номер': car_plate,
                            'Водитель': driver_name,
                            'Файл': os.path.basename(file_path)
                        })
                    
                except (ValueError, TypeError):
                    pass
            
            row_num += 1
            
            if row_num > header_row + 1000:
                break
        
        return parsed_data
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла: {e}")
        return []

# Функции для статистики БЕЗ PANDAS
def calculate_statistics(data):
    """Расчет статистики без pandas"""
    if not data:
        return None
    
    # Общая статистика
    total_trips = len(data)
    total_amount = sum(item['Стоимость'] for item in data)
    
    # Уникальные значения
    unique_cars = set(item['Гос_номер'] for item in data)
    unique_drivers = set(item['Водитель'] for item in data)
    unique_files = set(item['Файл'] for item in data)
    
    # Статистика по автомобилям
    car_stats = {}
    for item in data:
        car_plate = item['Гос_номер']
        if car_plate not in car_stats:
            car_stats[car_plate] = {
                'total_amount': 0,
                'trips_count': 0,
                'drivers': set(),
                'files': set()
            }
        
        car_stats[car_plate]['total_amount'] += item['Стоимость']
        car_stats[car_plate]['trips_count'] += 1
        car_stats[car_plate]['drivers'].add(item['Водитель'])
        car_stats[car_plate]['files'].add(item['Файл'])
    
    return {
        'total_trips': total_trips,
        'total_amount': total_amount,
        'unique_cars': len(unique_cars),
        'unique_drivers': len(unique_drivers),
        'unique_files': len(unique_files),
        'car_stats': car_stats
    }

def calculate_file_statistics(file_data):
    """Статистика по одному файлу"""
    if not file_data:
        return None
    
    total_amount = sum(item['Стоимость'] for item in file_data)
    trips_count = len(file_data)
    
    return {
        'total_amount': total_amount,
        'trips_count': trips_count
    }

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    welcome_text = """
🚛 *Transport Analytics Bot*

Отправьте мне Excel файлы с транспортными накладными, и я:
• Соберу данные из ВСЕХ файлов
• Покажу суммарную статистику по автомобилям
• Сгенерирую общий отчет

*Режимы работы:*
1. Отправьте один файл - получите отчет по нему
2. Отправьте несколько файлов - получите ОБЩИЙ отчет по всем
3. /clear - очистить все загруженные файлы
4. /report - получить отчет по текущим данным

*Поддерживаемые форматы:* .xlsx, .xls

Просто отправляйте файлы один за другим!
    """
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def clear_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистка данных пользователя"""
    user_id = update.effective_user.id
    user_data_store[user_id] = []
    await update.message.reply_text("✅ Все данные очищены! Можно загружать новые файлы.")

async def show_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать отчет по текущим данным"""
    user_id = update.effective_user.id
    user_data = user_data_store[user_id]
    
    if not user_data:
        await update.message.reply_text("📭 Нет данных для отчета. Сначала отправьте файлы.")
        return
    
    await generate_report(update, user_data, "ТЕКУЩИЙ ОТЧЕТ")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загруженных файлов"""
    try:
        user_id = update.effective_user.id
        
        # Получаем информацию о файле
        document = update.message.document
        file = await context.bot.get_file(document.file_id)
        
        # Проверяем что это Excel файл
        if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
            await update.message.reply_text("❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)")
            return
        
        await update.message.reply_text(f"🔍 Обрабатываю файл: {document.file_name}")
        
        # Скачиваем файл во временную папку
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            await file.download_to_drive(temp_file.name)
            
            # Парсим файл
            file_data = parse_invoice_file(temp_file.name)
            
            # Удаляем временный файл
            os.unlink(temp_file.name)
        
        if not file_data:
            await update.message.reply_text("❌ Не удалось найти данные в файле. Проверьте формат.")
            return
        
        # Сохраняем данные пользователя
        user_data_store[user_id].extend(file_data)
        
        # Статистика по файлу (БЕЗ PANDAS)
        file_stats = calculate_file_statistics(file_data)
        
        # Общая статистика пользователя (БЕЗ PANDAS)
        user_data = user_data_store[user_id]
        all_stats = calculate_statistics(user_data)
        
        response = f"""
📄 *Файл обработан: {document.file_name}*

*Данные файла:*
• Поездок в файле: {file_stats['trips_count']}
• Сумма в файле: {file_stats['total_amount']:,.0f} руб.

*Общая статистика:*
• Файлов загружено: {all_stats['unique_files']}
• Всего поездок: {all_stats['total_trips']}
• Общая сумма: {all_stats['total_amount']:,.0f} руб.

💡 Отправьте еще файлы или используйте /report для получения отчета
        """
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке файла")

async def generate_report(update: Update, data, title):
    """Генерация отчета (БЕЗ PANDAS)"""
    if not data:
        await update.message.reply_text("❌ Нет данных для отчета")
        return
    
    stats = calculate_statistics(data)
    
    # Формируем отчет по автомобилям
    car_reports = []
    for car_plate, car_data in stats['car_stats'].items():
        drivers = ', '.join(car_data['drivers'])
        files = ', '.join(list(car_data['files'])[:3])  # Показываем первые 3 файла
        if len(car_data['files']) > 3:
            files += f" ... (еще {len(car_data['files']) - 3})"
        
        car_reports.append(f"🚗 *{car_plate}*\n"
                         f"• Поездок: {car_data['trips_count']}\n"
                         f"• Водители: {drivers}\n"
                         f"• Файлы: {files}\n"
                         f"• Общая сумма: {car_data['total_amount']:,.0f} руб.\n")
    
    # Формируем ответ
    response = f"""
📊 *{title}*

*Общая статистика:*
• Файлов обработано: {stats['unique_files']}
• Всего поездок: {stats['total_trips']}
• Автомобилей: {stats['unique_cars']}  
• Водителей: {stats['unique_drivers']}
• Общая сумма: {stats['total_amount']:,.0f} руб.

*По автомобилям:*
{chr(10).join(car_reports)}

✅ Отчет сформирован!
    """
    
    # Разбиваем длинные сообщения
    if len(response) > 4000:
        parts = []
        current_part = ""
        for line in response.split('\n'):
            if len(current_part + line + '\n') > 4000:
                parts.append(current_part)
                current_part = line + '\n'
            else:
                current_part += line + '\n'
        parts.append(current_part)
        
        for part in parts:
            await update.message.reply_text(part, parse_mode='Markdown')
            await asyncio.sleep(0.5)
    else:
        await update.message.reply_text(response, parse_mode='Markdown')

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {context.error}")
    await update.message.reply_text("❌ Произошла ошибка. Попробуйте еще раз.")

def main():
    """Запуск бота"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN не установлен!")
        return
    
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("clear", clear_data))
    application.add_handler(CommandHandler("report", show_report))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_error_handler(error_handler)
    
    # Запускаем бота
    logger.info("🤖 Бот запущен...")
    application.run_polling()

if __name__ == "__main__":
    main()
