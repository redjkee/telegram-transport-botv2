# db.py

import os
import logging
import asyncio
import asyncpg
import pandas as pd

# Глобальная переменная для хранения пула соединений
pool = None

async def init_db():
    """Инициализирует пул соединений и создает таблицу, если ее нет."""
    global pool
    if pool is None:
        try:
            pool = await asyncpg.create_pool(dsn=os.getenv("DATABASE_URL"))
            logging.info("Database connection pool created successfully.")
            async with pool.acquire() as connection:
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS trips (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        source_file VARCHAR(255) NOT NULL,
                        trip_date VARCHAR(20),
                        route TEXT,
                        amount REAL,
                        car_plate VARCHAR(20),
                        driver_name VARCHAR(255)
                    );
                """)
                logging.info("Table 'trips' is ready.")
        except Exception as e:
            logging.error(f"Failed to initialize database: {e}")
            pool = None

async def check_if_file_processed(user_id: int, file_name: str) -> bool:
    """Проверяет, был ли файл уже обработан для данного пользователя."""
    if not pool: await init_db()
    async with pool.acquire() as connection:
        result = await connection.fetchval(
            "SELECT 1 FROM trips WHERE user_id = $1 AND source_file = $2 LIMIT 1",
            user_id, file_name
        )
        return result is not None

async def add_trips_from_df(user_id: int, df: pd.DataFrame):
    """Добавляет записи из DataFrame в базу данных."""
    if not pool: await init_db()
    # Преобразуем DataFrame в список кортежей для вставки
    records_to_insert = [
        (user_id, row['Источник'], row['Дата'], row['Маршрут'], row['Стоимость'], row['Гос_номер'], row['Водитель'])
        for _, row in df.iterrows()
    ]
    if not records_to_insert:
        return
        
    async with pool.acquire() as connection:
        await connection.executemany("""
            INSERT INTO trips (user_id, source_file, trip_date, route, amount, car_plate, driver_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, records_to_insert)
    logging.info(f"Added {len(records_to_insert)} records for user {user_id}.")

async def get_all_trips_as_df(user_id: int) -> pd.DataFrame:
    """Получает все записи для пользователя и возвращает их в виде DataFrame."""
    if not pool: await init_db()
    async with pool.acquire() as connection:
        records = await connection.fetch("SELECT * FROM trips WHERE user_id = $1", user_id)
        if not records:
            return pd.DataFrame()
        # Преобразуем записи в DataFrame
        columns = [
            'id', 'user_id', 'Источник', 'Дата', 'Маршрут', 
            'Стоимость', 'Гос_номер', 'Водитель'
        ]
        df = pd.DataFrame(records, columns=columns)
        # Удаляем технические колонки перед возвратом пользователю
        return df.drop(columns=['id', 'user_id'])

async def get_processed_files_count(user_id: int) -> int:
    """Считает количество уникальных обработанных файлов."""
    if not pool: await init_db()
    async with pool.acquire() as connection:
        count = await connection.fetchval(
            "SELECT COUNT(DISTINCT source_file) FROM trips WHERE user_id = $1",
            user_id
        )
        return count or 0

async def clear_user_data(user_id: int):
    """Удаляет все данные для указанного пользователя."""
    if not pool: await init_db()
    async with pool.acquire() as connection:
        await connection.execute("DELETE FROM trips WHERE user_id = $1", user_id)
    logging.info(f"All data for user {user_id} has been cleared.")
