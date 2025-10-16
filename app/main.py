import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()


def get_option_data(currency):
    url = 'https://deribit.com/api/v2/public/get_book_summary_by_currency'
    params = {'currency': currency, 'kind': 'option', 'expired': 'false'}

    response = requests.get(url, params=params).json()
    df = pd.DataFrame(response['result'])
    df['time'] = pd.to_datetime(df['creation_timestamp'], unit='ms').dt.strftime('%H:%M')
    df['date'] = pd.to_datetime(df['creation_timestamp'], unit='ms').dt.date
    
    return df


current_batch = pd.concat(
    [get_option_data('BTC'), get_option_data('ETH')], 
    ignore_index=True
)


# Создаем подключение к БД
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = '5432'
db_name = os.getenv('DB_NAME')

connection_string = \
    f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
engine = create_engine(connection_string)

# Проверка подключения
try:
    with engine.connect() as conn:
        print("✅ Подключение к PostgreSQL успешно!")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")


# Загрузка данных

try:
    current_batch.to_sql('option_data', engine, if_exists='append', index=False)
    print("✅ Данные загружены в БД!")
except Exception as e:
    print(f"❌ Ошибка загрузки: {e}")