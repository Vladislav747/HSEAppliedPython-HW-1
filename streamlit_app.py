import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

st.title('Data Analysis App')
st.header('Choose a dataset')

@st.cache
def load_data(filepath):
    return pd.read_csv(filepath)

uploaded_file = st.file_uploader('Upload a CSV file', type=['csv'])
if uploaded_file is not None:
    data = load_data(uploaded_file)
    st.dataframe(data)
else:
    st.write('No file uploaded.')

if uploaded_file is not None:
    st.header('Шаг 2: Очистка данных')

    if st.checkbox('Удалить пустые значения'):
        data = data.dropna()
        st.write("Пустые значения удалены.")
        st.dataframe(data)

    if st.checkbox('Заменить пустые значения на нули'):
        data = data.fillna(data.mean())
        st.write("Пустые значения заменены на среднее.")
        st.dataframe(data)

if uploaded_file is not None:
    st.header('Шаг 3: Анализ статистических показателей')
    if st.checkbox('Показать статистические показатели'):
        st.subheader('Статистические показатели:')
        st.write(data.describe())

if uploaded_file is not None:
    st.header('Шаг 4: Визуализация данных')
    st.subheader('Гистограмма:')
    column = st.selectbox('Выберите столбец для гистограммы', data.columns)
    bins = st.slider('Количество интервалов', 5, 50, 10)

    fig, ax = plt.subplots()
    ax.hist(data[column], bins=bins, color='skyblue', edgecolor='red')
    st.pyplot(fig)

    st.subheader('Корреляционная матрица:')

    if st.checkbox('Показать корреляционную матрица'):
        fig, ax = plt.subplots()
        sns.heatmap(data.corr(), annot=True, cmap='coolwarm', ax=ax)
        st.pyplot(fig)


from sklearn import model_selection, linear_model, metrics

if uploaded_file is not None:
    st.header('Шаг 5: Построение модели линейной регрессии')

    target_column = st.selectbox('Выберите столбец для зависимой переменной(y)', data.columns)
    feature_columns = st.multiselect('Выберите столбцы для признаков(X)', [col for col in data.columns if col!= target_column])

    if st.button('Построить модель'):
        X = data[feature_columns]
        y = data[target_column]

        X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2, random_state=42)

        model = linear_model.LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mse = metrics.mean_squared_error(y_test, y_pred)

        st.write(f"Средняя квадратичная ошибка: {mse:.2f}")

        model = linear_model.LinearRegression()
        model.fit(X_train, y_train)

        st.subheader('Результаты модели:')
        st.write('Коэффициенты регрессии:')
        st.write(model.coef_)

        st.write('Средняя квадратичная ошибка:')
        y_pred = model.predict(X_test)
        fig = px.scatter(data, x=data.columns[0], y=data.columns[1], color='blue')
        st.plotly_chart(fig)

        st.write(metrics.mean_squared_error(y_test, y_pred))

st.header("Управление состоянием")

# Инициализация состояния
if "count" not in st.session_state:
    st.session_state.count = 0

# Кнопки для управления состоянием
if st.button("Increment"):
    st.session_state.count += 1

if st.button("Decrement"):
    st.session_state.count -= 1

st.write(f"Current count: {st.session_state.count}")

st.header("Markdown")

st.markdown("""
# Заголовок 1

""")

st.header("Классификация")


