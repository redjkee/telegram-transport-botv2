import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
import pandas as pd
import io

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Токен бота из переменных окружения
API_TOKEN = os.getenv('BOT_TOKEN')

if not API_TOKEN:
    raise ValueError("Токен бота не найден! Убедитесь, что переменная BOT_TOKEN установлена.")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Хранилище данных пользователей
user_data_store = {}

def calculate_statistics(data):
    """Расчет статистики из данных"""
    if not data:
        return {}
    
    total_trips = len(data)
    unique_files = len(set(item['Файл'] for item in data))
    unique_cars = len(set(item['Гос_номер'] for item in data))
    unique_drivers = len(set(item['Водитель'] for item in data if item['Водитель'] != "Фамилия не найдена"))
    total_amount = sum(item['Стоимость'] for item in data)
    
    # Статистика по автомобилям
    car_stats = {}
    for item in data:
        car_plate = item['Гос_номер']
        if car_plate not in car_stats:
            car_stats[car_plate] = {
                'trips_count': 0,
                'total_amount': 0,
                'drivers': set(),
                'files': set()
            }
        car_stats[car_plate]['trips_count'] += 1
        car_stats[car_plate]['total_amount'] += item['Стоимость']
        car_stats[car_plate]['drivers'].add(item['Водитель'])
        car_stats[car_plate]['files'].add(item['Файл'])
    
    return {
        'total_trips': total_trips,
        'unique_files': unique_files,
        'unique_cars': unique_cars,
        'unique_drivers': unique_drivers,
        'total_amount': total_amount,
        'car_stats': car_stats
    }

async def generate_report(message: Message, data, title):
    if not data:
        await message.answer("❌ Нет данных для отчета")
        return
    
    stats = calculate_statistics(data)
    
    # Отчет по автомобилям
    car_reports = []
    for car_plate, car_data in stats['car_stats'].items():
        drivers = ', '.join(car_data['drivers'])
        files = ', '.join(list(car_data['files'])[:3])
        if len(car_data['files']) > 3:
            files += f" ... (еще {len(car_data['files']) - 3})"
        
        car_reports.append(f"🚗 {car_plate}\n"
                         f"• Поездок: {car_data['trips_count']}\n"
                         f"• Водители: {drivers}\n"
                         f"• Файлы: {files}\n"
                         f"• Общая сумма: {car_data['total_amount']:,.0f} руб.")
    
    # Отчет по водителям
    driver_stats = {}
    for item in data:
        driver = item['Водитель']
        if driver not in driver_stats:
            driver_stats[driver] = {
                'total_amount': 0,
                'trips_count': 0,
                'cars': set(),
                'files': set()
            }
        driver_stats[driver]['total_amount'] += item['Стоимость']
        driver_stats[driver]['trips_count'] += 1
        driver_stats[driver]['cars'].add(item['Гос_номер'])
        driver_stats[driver]['files'].add(item['Файл'])
    
    driver_reports = []
    for driver, driver_data in driver_stats.items():
        if driver == "Фамилия не найдена":
            continue
        cars = ', '.join(driver_data['cars'])
        files = ', '.join(list(driver_data['files'])[:3])
        if len(driver_data['files']) > 3:
            files += f" ... (еще {len(driver_data['files']) - 3})"
        
        driver_reports.append(f"👤 {driver}\n"
                            f"• Поездок: {driver_data['trips_count']}\n"
                            f"• Автомобили: {cars}\n"
                            f"• Файлы: {files}\n"
                            f"• Общая сумма: {driver_data['total_amount']:,.0f} руб.")
    
    response = f"""
📊 {title}

ОБЩАЯ СТАТИСТИКА:
• Файлов обработано: {stats['unique_files']}
• Всего поездок: {stats['total_trips']}
• Автомобилей: {stats['unique_cars']}  
• Водителей: {stats['unique_drivers']}
• Общая сумма: {stats['total_amount']:,.0f} руб.

ПО АВТОМОБИЛЯМ:
{chr(10).join(car_reports)}

ПО ВОДИТЕЛЯМ:
{chr(10).join(driver_reports)}

✅ Отчет сформирован!
    """
    
    if len(response) > 4000:
        parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
        for part in parts:
            await message.answer(part)
            await asyncio.sleep(0.5)
    else:
        await message.answer(response)

@dp.message(Command("start"))
async def start_handler(message: Message):
    welcome_text = """
🚛 Transport Analytics Bot

Отправьте мне Excel файлы с транспортными накладными, и я:
• Соберу данные из ВСЕХ файлов
• Покажу суммарную статистику по автомобилям и водителям
• Сгенерирую общий отчет

📊 ДОСТУПНЫЕ КОМАНДЫ:

/report - полный отчет (авто + водители)
/cars - отчет только по автомобилям  
/drivers - отчет только по водителям
/clear - очистить все данные

🔍 ПОИСК:
Отправьте номер автомобиля (например: 302) или фамилию водителя для получения детальной статистики

📁 Поддерживаемые форматы: .xlsx, .xls

💡 Просто отправляйте файлы один за другим, а затем используйте команды для получения отчетов!
    """
    await message.answer(welcome_text)

@dp.message(Command("report"))
async def report_handler(message: Message):
    """Полный отчет"""
    user_id = message.from_user.id
    user_data = user_data_store.get(user_id, [])
    
    await generate_report(message, user_data, "ПОЛНЫЙ ОТЧЕТ")

@dp.message(Command("cars"))
async def cars_handler(message: Message):
    """Отчет только по автомобилям"""
    user_id = message.from_user.id
    user_data = user_data_store.get(user_id, [])
    
    if not user_data:
        await message.answer("📭 Нет данных для отчета. Сначала отправьте файлы.")
        return
    
    stats = calculate_statistics(user_data)
    
    car_reports = []
    for car_plate, car_data in stats['car_stats'].items():
        drivers = ', '.join(car_data['drivers'])
        files = ', '.join(list(car_data['files'])[:3])
        if len(car_data['files']) > 3:
            files += f" ... (еще {len(car_data['files']) - 3})"
        
        car_reports.append(f"🚗 {car_plate}\n"
                         f"• Поездок: {car_data['trips_count']}\n"
                         f"• Водители: {drivers}\n"
                         f"• Файлы: {files}\n"
                         f"• Общая сумма: {car_data['total_amount']:,.0f} руб.\n")
    
    response = f"""
📊 ОТЧЕТ ПО АВТОМОБИЛЯМ

Всего автомобилей: {stats['unique_cars']}
Общая сумма: {stats['total_amount']:,.0f} руб.

{chr(10).join(car_reports)}
    """
    
    if len(response) > 4000:
        parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
        for part in parts:
            await message.answer(part)
            await asyncio.sleep(0.5)
    else:
        await message.answer(response)

@dp.message(Command("drivers"))
async def drivers_handler(message: Message):
    """Отчет только по водителям"""
    user_id = message.from_user.id
    user_data = user_data_store.get(user_id, [])
    
    if not user_data:
        await message.answer("📭 Нет данных для отчета. Сначала отправьте файлы.")
        return
    
    # Статистика по водителям
    driver_stats = {}
    for item in user_data:
        driver = item['Водитель']
        if driver not in driver_stats:
            driver_stats[driver] = {
                'total_amount': 0,
                'trips_count': 0,
                'cars': set(),
                'files': set()
            }
        driver_stats[driver]['total_amount'] += item['Стоимость']
        driver_stats[driver]['trips_count'] += 1
        driver_stats[driver]['cars'].add(item['Гос_номер'])
        driver_stats[driver]['files'].add(item['Файл'])
    
    driver_reports = []
    for driver, driver_data in driver_stats.items():
        if driver == "Фамилия не найдена":
            continue
        cars = ', '.join(driver_data['cars'])
        files = ', '.join(list(driver_data['files'])[:3])
        if len(driver_data['files']) > 3:
            files += f" ... (еще {len(driver_data['files']) - 3})"
        
        driver_reports.append(f"👤 {driver}\n"
                            f"• Поездок: {driver_data['trips_count']}\n"
                            f"• Автомобили: {cars}\n"
                            f"• Файлы: {files}\n"
                            f"• Общая сумма: {driver_data['total_amount']:,.0f} руб.\n")
    
    response = f"""
📊 ОТЧЕТ ПО ВОДИТЕЛЯМ

Всего водителей: {len([d for d in driver_stats.keys() if d != "Фамилия не найдена"])}
Общая сумма: {sum(d['total_amount'] for d in driver_stats.values()):,.0f} руб.

{chr(10).join(driver_reports)}
    """
    
    if len(response) > 4000:
        parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
        for part in parts:
            await message.answer(part)
            await asyncio.sleep(0.5)
    else:
        await message.answer(response)

@dp.message(Command("clear"))
async def clear_handler(message: Message):
    """Очистка всех данных"""
    user_id = message.from_user.id
    if user_id in user_data_store:
        user_data_store[user_id] = []
        await message.answer("✅ Все данные очищены!")
    else:
        await message.answer("📭 Нет данных для очистки.")

@dp.message()
async def handle_text_message(message: Message):
    """Обработка текстовых сообщений для поиска по водителям и автомобилям"""
    user_id = message.from_user.id
    user_data = user_data_store.get(user_id, [])
    
    if not user_data:
        await message.answer("📭 Нет данных для поиска. Сначала отправьте файлы.")
        return
    
    search_text = message.text.strip()
    
    # Если это команда - пропускаем (обрабатывается другими хендлерами)
    if search_text.startswith('/'):
        return
    
    # Поиск по номеру автомобиля (цифры)
    if search_text.isdigit():
        car_results = [item for item in user_data if search_text in item['Гос_номер']]
        
        if car_results:
            car_plates = set(item['Гос_номер'] for item in car_results)
            total_trips = len(car_results)
            total_amount = sum(item['Стоимость'] for item in car_results)
            drivers = set(item['Водитель'] for item in car_results if item['Водитель'] != "Фамилия не найдена")
            files = set(item['Файл'] for item in car_results)
            
            response = f"""
🔍 РЕЗУЛЬТАТЫ ПОИСКА ПО АВТОМОБИЛЮ: {search_text}

Найдено автомобилей: {len(car_plates)}
• Номера: {', '.join(car_plates)}
• Поездок: {total_trips}
• Водители: {', '.join(drivers) if drivers else 'Не указаны'}
• Файлов: {len(files)}
• Общая сумма: {total_amount:,.0f} руб.

Детали поездок:
"""
            
            for i, item in enumerate(car_results[:10], 1):
                response += f"\n{i}. {item['Дата']} - {item['Водитель']} - {item['Стоимость']:,.0f} руб."
            
            if len(car_results) > 10:
                response += f"\n\n... и еще {len(car_results) - 10} поездок"
                
            await message.answer(response)
        else:
            await message.answer(f"❌ Автомобиль с номером '{search_text}' не найден")
    
    # Поиск по фамилии водителя (текст)
    else:
        driver_results = [item for item in user_data if search_text.lower() in item['Водитель'].lower()]
        
        if driver_results:
            drivers_found = set(item['Водитель'] for item in driver_results)
            total_trips = len(driver_results)
            total_amount = sum(item['Стоимость'] for item in driver_results)
            cars = set(item['Гос_номер'] for item in driver_results)
            files = set(item['Файл'] for item in driver_results)
            
            response = f"""
🔍 РЕЗУЛЬТАТЫ ПОИСКА ПО ВОДИТЕЛЮ: {search_text}

Найдено водителей: {len(drivers_found)}
• Фамилии: {', '.join(drivers_found)}
• Поездок: {total_trips}
• Автомобили: {', '.join(cars)}
• Файлов: {len(files)}
• Общая сумма: {total_amount:,.0f} руб.

Детали поездок:
"""
            
            for i, item in enumerate(driver_results[:10], 1):
                response += f"\n{i}. {item['Дата']} - {item['Гос_номер']} - {item['Стоимость']:,.0f} руб."
            
            if len(driver_results) > 10:
                response += f"\n\n... и еще {len(driver_results) - 10} поездок"
                
            await message.answer(response)
        else:
            await message.answer(f"❌ Водитель '{search_text}' не найден")

# Добавьте сюда вашу существующую функцию обработки файлов
@dp.message(lambda message: message.document)
async def handle_document(message: Message):
    """Обработка загружаемых файлов"""
    # Ваша существующая логика обработки Excel файлов
    user_id = message.from_user.id
    # ... ваш код обработки файла ...
    await message.answer("📁 Файл получен! Используйте команды /report, /cars, /drivers для просмотра отчетов.")

async def main():
    logging.info("Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
