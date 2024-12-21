import pandas as pd
import time
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

def main():
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

if __name__ == "__main__":
    main()