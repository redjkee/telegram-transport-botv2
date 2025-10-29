from aiogram.filters import Command
from aiogram.types import Message
import asyncio

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
