import aiohttp
import asyncio
import pandas as pd
import time
from datetime import datetime

import requests
from joblib import Parallel, delayed
from pathlib import Path


# Функция для обработки одной группы(группа города и сезона)
def process_group(group):
    mean = group['temperature'].mean()
    std = group['temperature'].std()
    lower_bound = mean - 2 * std
    upper_bound = mean + 2 * std
    # Выявление анномалий
    group['is_anomaly'] = (group['temperature'] < lower_bound) | (group['temperature'] > upper_bound)
    group['average'] = mean
    group['std'] = std
    group['lower_bound'] = lower_bound
    group['upper_bound'] = upper_bound
    return group


# Распараллеливание обработки по группам
def parallel_apply_groups(grouped, func, n_jobs=4):
    groups = [group for _, group in grouped]
    results = Parallel(n_jobs=n_jobs)(delayed(func)(group) for group in groups)
    return pd.concat(results)


# Последовательное выполнение
def serial_apply_groups(grouped, func):
    return pd.concat([func(group) for _, group in grouped])


def benchmark(func, *args, **kwargs):
    """
    Бенчмарк для замера времени
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    print(f"Время выполнения: {end_time - start_time:.2f} секунд")
    return result


def get_current_temperature(city, api_key):
    """
    Получение текущей температуры для указанного города через OpenWeatherMap API.
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        temperature = data['main']['temp']
        return temperature
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе API: {e}")
        return None

async def async_get_current_temperature(city, api_key):
    """
    Получение текущей температуры для указанного города через OpenWeatherMap API.
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
    }

    url = f"{base_url}?q={params['q']}&appid={params['appid']}&units={params['units']}"

    try:
        response = await fetch_data(url)
        temperature = response['main']['temp']
        return temperature
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе API: {e}")
        return None


def get_current_season():
    """
    Определяет текущий сезон на основе даты.
    """
    date = datetime.now()

    month = date.month

    # Определение сезона по месяцам и дням
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "fall"

async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

def get_options_cities(df):
    return df['city'].unique()

def is_current_temp_anomaly(temp_curr, data, selected_city, season_curr):
    avg_temp_cities = data[(data['city'] == selected_city) & (data['season'] == season_curr)]
    avg_temp_from_history = avg_temp_cities.iloc[0]['average']
    return temp_curr < avg_temp_from_history - 2 * avg_temp_cities.iloc[0]['std'] or temp_curr > avg_temp_from_history + 2 * avg_temp_cities.iloc[0]['std']

async def main():
    my_file = Path("temperature_data.csv")
    if not my_file.is_file():
        print("Файл 'temperature_data.csv' не найден.")
        return

    df = pd.read_csv("temperature_data.csv")

    # Расчёт скользящего среднего до группировки
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature_ma_30'] = df['temperature'].rolling(window=30).mean()

    # Группировка по городу и сезону
    grouped = df.groupby(['city', 'season'])

    # Сравнение
    print("Последовательное выполнение:")
    result_serial = benchmark(serial_apply_groups, grouped, process_group)

    print("Распараллеливание:")
    result_parallel = benchmark(parallel_apply_groups, grouped, process_group, n_jobs=4)

    # Проверка идентичности результатов
    print(f"Результаты идентичны: {result_serial.equals(result_parallel)}")

    # Мы видим что последовательное выполнение даже быстрее распараллеливания

    api_key = "7351568487f157e6ae573f6d33d5cf77"

    # Город для мониторинга
    city = "New York"

    # Получение текущей температуры
    # Сравнение
    print("Синхронный вызов:")
    start_time = time.time()
    result_sync = get_current_temperature(city, api_key)
    end_time = time.time()
    print(f"Время выполнения синхронной ф-ции: {end_time - start_time:.2f} секунд")
    print(result_sync, "Результат выполнения синхронной ф-ции.")
    print("Асинхронный вызов:")
    start_time = time.time()
    current_temp_async = await async_get_current_temperature(city, api_key)
    end_time = time.time()
    print(f"Время выполнения ассинхронной ф-ции: {end_time - start_time:.2f} секунд")
    print(current_temp_async, "Результат выполнения асинхронной ф-ции.")
    print("Результат выполнения асинхронной ф-ции.")
    # При нескольких прогонах побеждала ассинхронный вызов ф-ции либо результаты замера скорости синхронной и ассинхронной были равны

    if result_sync is not None:
        print(f"Текущая температура в городе {city}: {result_sync}°C")
    else:
        print("Не удалось получить данные о температуре.")

    current_season = get_current_season()
    print(f"Текущий сезон: {current_season}")

    # Получение результата для города New York
    avg_temp_cities = result_serial[(result_serial['city'] == 'New York') & (result_serial['season'] == current_season)]
    avg_temp_from_history = avg_temp_cities.iloc[0]['average']
    print(avg_temp_from_history, "avg_temp_from_history")


if __name__ == "__main__":
    asyncio.run(main())
