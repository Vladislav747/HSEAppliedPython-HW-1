import time
import streamlit as st
import pandas as pd
import asyncio
import matplotlib.pyplot as plt
from process_data import async_get_current_temperature, serial_apply_groups, process_group, \
    get_current_season, get_options_cities, is_current_temp_anomaly, get_current_temperature, is_data_correct

st.title('Приложение для анализа и визуализации данных о температуре')
st.header('Загрузите файл с историческими данными о температуре')

@st.cache_data
def load_data(filepath):
    return pd.read_csv(filepath)

uploaded_file = st.file_uploader('Загрузите файл с историческими данными о температуре', type=['csv'])

if uploaded_file is not None:
    data = load_data(uploaded_file)
    if is_data_correct(data) is False:
        st.error('Необходимые столбцы: city, season, temperature, timestamp - отсутствуют в вашем файле.')
    else:
        # Расчёт скользящего среднего до группировки
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['temperature_ma_30'] = data['temperature'].rolling(window=30).mean()

        # Группировка по городу и сезону
        grouped = data.groupby(['city', 'season'])
        res = serial_apply_groups(grouped, process_group)
        st.dataframe(res)
else:
    st.write('Файл не выбран.')

if uploaded_file is not None and is_data_correct(data) is True:
    api_key = st.text_input(label='Введите API ключ для OpenWeatherMap')


    if api_key != '':
        cities_options = get_options_cities(data)
        option = st.selectbox(
            "Выберите город - для получения текущей температуры:",
            cities_options,
        )

        st.write("Вы выбрали:", option)

        if st.button("Получить текущую температуру"):
            async def fetch_temperature():
                return await async_get_current_temperature(option, api_key)

            # Получение текущей температуры
            # Сравнение
            print("Синхронный вызов:")
            start_time = time.time()
            result_sync = get_current_temperature(option, api_key)
            end_time = time.time()
            time_sync = end_time - start_time
            print(f"Время выполнения синхронной ф-ции: {time_sync:.2f} секунд")
            print(result_sync, "Результат выполнения синхронной ф-ции.")

            print("Асинхронный вызов:")
            start_time = time.time()
            # Запуск асинхронной функции
            temp = asyncio.run(fetch_temperature())
            end_time = time.time()
            time_async = end_time - start_time
            print(f"Время выполнения ассинхронной ф-ции: {end_time - start_time:.2f} секунд")
            # При нескольких прогонах побеждала ассинхронный вызов ф-ции либо результаты замера скорости синхронной и ассинхронной были равны

            # Визуализация
            plt.figure(figsize=(8, 5))
            plt.bar(["Синхронный вызов", "Асинхронный вызов"], [time_sync, time_async], color=["blue", "green"])
            plt.title("Сравнение времени выполнения: синхронный vs асинхронный вызов")
            plt.ylabel("Время выполнения (секунды)")
            plt.xlabel("Тип вызова")
            st.pyplot(plt)

            current_season = get_current_season()
            st.subheader(f"Текущий сезон: {current_season}")

            # Временной ряд с аномалиями
            st.subheader("Временной ряд температуры с выделением аномалий")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(res['timestamp'], res['temperature'], label="Температура", color="blue")
            anomalies = res[res['is_anomaly']]
            ax.scatter(anomalies['timestamp'], anomalies['temperature'], color="red", label="Аномалии")
            ax.set_title(f"Временной ряд температуры для {option}")
            ax.set_xlabel("Дата")
            ax.set_ylabel("Температура")
            ax.legend()
            st.pyplot(fig)

            st.subheader("Распределение аномалий температуры")
            anomalies = res[res['is_anomaly']]
            if anomalies.empty:
                st.write("Аномалий не обнаружено.")
            else:
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.hist(anomalies['temperature'], bins=15, color='red', alpha=0.7, label="Аномалии")
                ax.set_title(f"Распределение аномалий температуры для {option}")
                ax.set_xlabel("Температура")
                ax.set_ylabel("Частота")
                ax.legend()
                st.pyplot(fig)

            st.subheader(f"Описательная статистика по историческим данным для города {option}")
            st.dataframe(res[res['city'] == option].describe())

            st.subheader(f"Сезонные профили для города {option}")
            seasonal_stats = res.groupby('season').agg(
                mean_temp=('temperature', 'mean'),
                std_temp=('temperature', 'std')
            ).reset_index()

            fig, ax = plt.subplots(figsize=(10, 6))

            seasons = seasonal_stats['season']
            mean_temps = seasonal_stats['mean_temp']
            std_temps = seasonal_stats['std_temp']

            ax.plot(seasons, mean_temps, marker='o', label="Средняя температура", color="blue")
            ax.fill_between(seasons, mean_temps - std_temps, mean_temps + std_temps, color="blue", alpha=0.2, label="± Стандартное отклонение")

            ax.set_title("Сезонные профили температуры")
            ax.set_xlabel("Сезоны")
            ax.set_ylabel("Температура (°C)")
            ax.legend()

            st.pyplot(fig)

            # Отображение текущей температуры
            if temp is not None:
                st.success(f"Текущая температура в городе {option}: {temp}°C")
            else:
                st.error("Не удалось получить данные о температуре.")

            # Является ли текущая температура аномальной
            if temp is not None and is_current_temp_anomaly(temp, res, option, current_season):
                st.info("Текущая температура является аномальной.")
            else:
                st.info("Текущая температура не является аномальной.")
