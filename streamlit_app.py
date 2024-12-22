import streamlit as st
import pandas as pd
import asyncio
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from process_data import get_current_temperature, async_get_current_temperature, serial_apply_groups, process_group, \
    get_current_season

api_key = "7351568487f157e6ae573f6d33d5cf77"

st.title('Приложение для анализа и визуализации данных о температуре')
st.header('Загрузите файл с историческими данными о температуре')

@st.cache
def load_data(filepath):
    return pd.read_csv(filepath)

uploaded_file = st.file_uploader('Загрузите файл с историческими данными о температуре', type=['csv'])
if uploaded_file is not None:
    data = load_data(uploaded_file)
    # Расчёт скользящего среднего до группировки
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data['temperature_ma_30'] = data['temperature'].rolling(window=30).mean()

    # Группировка по городу и сезону
    grouped = data.groupby(['city', 'season'])
    res = serial_apply_groups(grouped, process_group)
    st.dataframe(res)
else:
    st.write('No file uploaded.')

if uploaded_file is not None:

    option = st.selectbox(
        "Выберите город - для получения текущей температуры:",
        ("New York", "Mexico City", "Sydney"),
    )

    st.write("You selected:", option)

    # Кнопка для запуска асинхронного вызова
    if st.button("Получить текущую температуру"):
        async def fetch_temperature():
            return await async_get_current_temperature(option, api_key)

        # Запуск асинхронной функции
        temp = asyncio.run(fetch_temperature())

        current_season = get_current_season()
        st.write(f"Текущий сезон: {current_season}")

        # Получение результата для города New York
        # avg_temp_cities = data[(data['city'] == 'New York') & (data['season'] == current_season)]
        # avg_temp_from_history = avg_temp_cities.iloc[0]['average']
        # print(avg_temp_from_history, "avg_temp_from_history")

        # Отображение результата
        if temp is not None:
            st.success(f"Текущая температура в городе {option}: {temp}°C")
        else:
            st.error("Не удалось получить данные о температуре.")


