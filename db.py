# db.py (Версия 6.1 - ИСПРАВЛЕННАЯ)

import os
import logging
import asyncpg
import pandas as pd
from datetime import datetime
from telegram import Update

pool = None

async def init_db():
    """Создает все необходимые таблицы с правильными связями."""
    global pool
    if pool is not None: return True
    try:
        pool = await asyncpg.create_pool(dsn=os.getenv("DATABASE_URL"))
        async with pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT,
                    first_seen TIMESTAMPTZ DEFAULT NOW(),
                    last_seen TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # Справочник машин
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cars (
                    car_id SERIAL PRIMARY KEY,
                    plate_number TEXT NOT NULL UNIQUE
                );
            """)
            # Справочник водителей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS drivers (
                    driver_id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                );
            """)
            # Таблица фактов о поездках
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trips (
                    trip_id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    car_id INT REFERENCES cars(car_id) ON DELETE RESTRICT,
                    driver_id INT REFERENCES drivers(driver_id) ON DELETE RESTRICT,
                    source_file TEXT,
                    trip_date DATE,
                    route TEXT,
                    amount REAL
                );
            """)
        logging.info("Database tables initialized successfully.")
        return True
    except Exception as e:
        logging.critical(f"Failed to initialize database: {e}")
        pool = None
        return False

# --- ВОТ ОНА, НЕДОСТАЮЩАЯ ФУНКЦИЯ ---
async def get_or_create_user(update: Update):
    """Обновляет или создает пользователя в БД при каждом его действии."""
    user = update.effective_user
    if not pool or not user: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, first_name, last_name, username, last_seen)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                last_seen = NOW();
        """, user.id, user.first_name, user.last_name, user.username)

async def get_or_create_id(conn, table_name, column_name, value):
    """Универсальная функция для получения ID из справочника."""
    id_column = f"{table_name[:-1]}_id" # cars -> car_id
    record_id = await conn.fetchval(f"SELECT {id_column} FROM {table_name} WHERE {column_name} = $1", value)
    if record_id:
        return record_id
    return await conn.fetchval(f"INSERT INTO {table_name} ({column_name}) VALUES ($1) RETURNING {id_column}", value)

async def add_trips_from_df(user_id: int, df: pd.DataFrame):
    """Добавляет поездки, получая ID из справочников."""
    if not pool: return
    async with pool.acquire() as conn:
        records_to_insert = []
        for _, row in df.iterrows():
            car_id = await get_or_create_id(conn, "cars", "plate_number", str(row['Гос_номер']))
            driver_id = await get_or_create_id(conn, "drivers", "name", str(row['Водитель']))
            try:
                trip_date_str = str(row.get('Дата', ''))
                trip_date = datetime.strptime(trip_date_str, '%d.%m.%y').date() if trip_date_str != 'Дата не найдена' else None
            except (ValueError, TypeError):
                trip_date = None
            records_to_insert.append((user_id, car_id, driver_id, row['Источник'], trip_date, row['Маршрут'], row['Стоимость']))
        
        if records_to_insert:
            await conn.executemany("""
                INSERT INTO trips (user_id, car_id, driver_id, source_file, trip_date, route, amount)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, records_to_insert)

async def get_all_trips_as_df(user_id: int) -> pd.DataFrame:
    """Получает все данные с именами из справочников с помощью JOIN."""
    if not pool: return pd.DataFrame()
    query = """
        SELECT
            t.source_file AS "Источник",
            TO_CHAR(t.trip_date, 'DD.MM.YY') AS "Дата",
            t.route AS "Маршрут",
            t.amount AS "Стоимость",
            c.plate_number AS "Гос_номер",
            d.name AS "Водитель"
        FROM trips t
        LEFT JOIN cars c ON t.car_id = c.car_id
        LEFT JOIN drivers d ON t.driver_id = d.driver_id
        WHERE t.user_id = $1
        ORDER BY t.trip_date, t.trip_id;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query, user_id)
        return pd.DataFrame(records, [desc.name for desc in records[0].keys()]) if records else pd.DataFrame()

async def get_processed_files(user_id: int) -> set:
    """Получает множество имен уже обработанных файлов."""
    if not pool: return set()
    async with pool.acquire() as conn:
        records = await conn.fetch("SELECT DISTINCT source_file FROM trips WHERE user_id = $1", user_id)
        return {record['source_file'] for record in records}

async def clear_user_data(user_id: int):
    """Удаляет только поездки пользователя, не трогая справочники."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM trips WHERE user_id = $1", user_id)
