# db.py (ИСПРАВЛЕННАЯ ВЕРСИЯ)
import os
import logging
import asyncio
import asyncpg
import pandas as pd

pool = None

async def init_db():
    """Инициализирует пул соединений и возвращает True в случае успеха."""
    global pool
    if pool is not None:
        return True
        
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.critical("DATABASE_URL environment variable not set.")
        return False
        
    try:
        pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5)
        async with pool.acquire() as connection:
            await connection.execute("SELECT 1")
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
                    driver_name VARCHAR(255),
                    UNIQUE(user_id, source_file, trip_date, route, amount, car_plate, driver_name)
                );
            """)
            await connection.execute("CREATE INDEX IF NOT EXISTS idx_user_id_file ON trips (user_id, source_file);")
            logging.info("Table 'trips' is ready.")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        pool = None
        return False

# ... (остальные функции в db.py без изменений) ...
async def check_if_file_processed(user_id: int, file_name: str) -> bool:
    if not pool: await init_db()
    async with pool.acquire() as connection:
        result = await connection.fetchval("SELECT 1 FROM trips WHERE user_id = $1 AND source_file = $2 LIMIT 1", user_id, file_name)
        return result is not None
async def add_trips_from_df(user_id: int, df: pd.DataFrame):
    if not pool: await init_db()
    records_to_insert = [(user_id, row['Источник'], row['Дата'], row['Маршрут'], row['Стоимость'], row['Гос_номер'], row['Водитель']) for _, row in df.iterrows()]
    if not records_to_insert: return
    async with pool.acquire() as connection:
        # Добавляем ON CONFLICT DO NOTHING для защиты от дубликатов на уровне БД
        await connection.executemany("""
            INSERT INTO trips (user_id, source_file, trip_date, route, amount, car_plate, driver_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
        """, records_to_insert)
    logging.info(f"Attempted to add {len(records_to_insert)} records for user {user_id}.")
async def get_all_trips_as_df(user_id: int) -> pd.DataFrame:
    if not pool: await init_db()
    async with pool.acquire() as connection:
        records = await connection.fetch("SELECT source_file, trip_date, route, amount, car_plate, driver_name FROM trips WHERE user_id = $1", user_id)
        if not records: return pd.DataFrame()
        columns = ['Источник', 'Дата', 'Маршрут', 'Стоимость', 'Гос_номер', 'Водитель']
        return pd.DataFrame(records, columns=columns)
async def get_processed_files_count(user_id: int) -> int:
    if not pool: await init_db()
    async with pool.acquire() as connection:
        count = await connection.fetchval("SELECT COUNT(DISTINCT source_file) FROM trips WHERE user_id = $1", user_id)
        return count or 0
async def clear_user_data(user_id: int):
    if not pool: await init_db()
    async with pool.acquire() as connection:
        await connection.execute("DELETE FROM trips WHERE user_id = $1", user_id)
    logging.info(f"All data for user {user_id} has been cleared.")
