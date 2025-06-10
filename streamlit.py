import psycopg2
import pandas as pd
import ast
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import altair as alt
import plotly.express as px
# from wordcloud import WordCloud

st.set_page_config(
    page_title="Dashboard",
    page_icon="📊",
    layout="wide",  # Chế độ toàn màn hình
    initial_sidebar_state="collapsed"  # Ẩn thanh sidebar mặc định
)

@st.cache_resource
def connect_to_azure_postgres(host, database, user, password, port=5432):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        print("Kết nối thành công tới Azure PostgreSQL!")
        return conn
    except Exception as e:
        print("Không thể kết nối tới Azure PostgreSQL:")
        print(e)
        return None   

# Kết nối đến PostgreSQL
host = "shopee.postgres.database.azure.com"
database = "delivery_info"
user = "Numpy"
password = "!Namphuong592003"
conn = connect_to_azure_postgres(host, database, user, password)

@st.cache_data
def preprocess_menu(menu_df: pd.DataFrame) -> pd.DataFrame:
    # Convert the "menu" column into an appropriate data type
    menu_df["menu"] = menu_df["menu"].apply(lambda item: ast.literal_eval(item))
    menu_df = menu_df.explode("menu").dropna(subset=["menu"])
    
    # Create new columns
    menu_df["food_name"] = menu_df["menu"].apply(lambda item: item["name"])
    menu_df["food_name"] = menu_df["food_name"].str.strip().str.lower()
    menu_df["food_price"] = menu_df["menu"].apply(lambda item: item["price"])

    # Drop the "menu" column
    return menu_df.drop(columns="menu")

@st.cache_data
def process_tfidf(df: pd.DataFrame, ngram: int) -> pd.DataFrame:
    df["food_name"] = df["food_name"].str.strip().str.lower()
    food_names = df["food_name"].values

    tfidf_vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(ngram, ngram))
    tfidf_matrix = tfidf_vectorizer.fit_transform(food_names)
    feature_names = tfidf_vectorizer.get_feature_names_out()
    tfidf_scores = tfidf_matrix.sum(axis=0).A1  # Tổng trọng số của từng từ

    tfidf_df = pd.DataFrame({
        "keyword": feature_names,
        "score": tfidf_scores
    }).sort_values(by="score", ascending=False)

    return tfidf_df

restaurant_query = """
    SELECT *
    FROM restaurant
"""
menu_query = """
    SELECT restaurant_id, menu
    FROM menu
    WHERE is_current = TRUE;
"""

@st.cache_data
def get_data(query, _conn):
    return pd.read_sql(query, _conn)

menu_df = get_data(menu_query, conn)
restaurant_df = get_data(restaurant_query, conn)

# Preprocess data
menu_df = preprocess_menu(menu_df)
df = restaurant_df.merge(menu_df, how="inner", on="restaurant_id")

# Visualize data
# Dropdown list options
district_options = sorted(df["district"].unique().tolist())
category_options = sorted(df["category"].unique().tolist())

filtered_df = df.copy()
# selected_districts = st.multiselect("Lọc theo Quận/Huyện", district_options)
# selected_categories = st.multiselect("Lọc theo Loại hình quán ăn", category_options)
# if selected_districts:
#     filtered_df = filtered_df[filtered_df["district"].isin(selected_districts)]
# if selected_categories:
#     filtered_df = filtered_df[filtered_df["category"].isin(selected_categories)]

with st.sidebar:
    st.title('🏙️Dashboard')

    # Lọc theo Quận/Huyện
    selected_districts = st.multiselect("Lọc theo Quận/Huyện", district_options, default='Thành Phố Thủ Đức')
    filtered_df = filtered_df[filtered_df["district"].isin(selected_districts)]
    # Lọc theo Loại hình quán ăn
    selected_categories = st.multiselect("Lọc theo Loại hình quán ăn", category_options, default='Ăn vặt/vỉa hè')
    filtered_df = filtered_df[filtered_df["category"].isin(selected_categories)]
    # Slider to select how many rows to display (ngram)
    selected_ngram = st.slider("Chọn số lượng từ", min_value=1, max_value=6, value=6)

col1, col2 = st.columns((3, 5), gap='medium')
top_keywords = None
with col1:
    if (selected_districts or selected_categories) and not filtered_df.empty:
        tfidf_df = process_tfidf(filtered_df, selected_ngram)
        top_keywords = tfidf_df.head(10)

        # Top 10 Từ khóa phổ biến using Altair bar chart
        top_keywords_chart = alt.Chart(top_keywords).mark_bar().encode(
            x=alt.X('score:Q', title='Điểm TF-IDF'),
            y=alt.Y('keyword:N', title='Từ khóa', sort='-x'),
            color=alt.Color('score:Q', scale=alt.Scale(range=["#2196F3", "#1976D2", "#0D47A1"]), legend=None),
            tooltip=['keyword', 'score']
        ).properties(
            title="Top Từ khóa phổ biến trong thực đơn",
            width=500,
            height=600
        ).configure_title(
            fontSize=16,  # Thay đổi kích thước tiêu đề
            font='Arial',  # Chọn font
            anchor='middle'  # Căn giữa tiêu đề
        )
        st.altair_chart(top_keywords_chart)

with col2:
    if top_keywords is not None and not top_keywords.empty:
        filtered_menus = []
        for keyword in top_keywords["keyword"]:
            keyword_menus = filtered_df[filtered_df["food_name"].str.contains(keyword, case=False, na=False)].copy()
            keyword_menus["keyword"] = keyword
            filtered_menus.append(keyword_menus)

        keyword_price_df = pd.concat(filtered_menus, ignore_index=True)

        if not keyword_price_df.empty:
            # Box plot for price distribution using Altair
            price_chart = alt.Chart(keyword_price_df).mark_boxplot().encode(
                x=alt.X('keyword:N', title='Từ khóa', sort='-x'),
                y=alt.Y('food_price:Q', title='Giá'),
                tooltip=['keyword', 'food_price'],
                color=alt.Color('keyword:N', scale=alt.Scale(range=["#2196F3", "#1976D2", "#0D47A1"]), legend=None),
            ).properties(
                title="Phân khúc giá của các từ khóa phổ biến",
                width=800,
                height=600
            ).configure_title(
                fontSize=16,  # Thay đổi kích thước tiêu đề
                font='Arial',  # Chọn font
                anchor='middle'  # Căn giữa tiêu đề
            )
            st.altair_chart(price_chart)
        else:
            st.warning("Không có món ăn nào phù hợp với các từ khóa.")


if top_keywords is not None:
    selected_keywords = st.multiselect("Chọn từ khóa", top_keywords["keyword"].tolist())
        
    if selected_keywords:
        filtered_menus = filtered_df[filtered_df["food_name"].str.contains('|'.join(selected_keywords), case=False, na=False)]
        max_price = filtered_menus["food_price"].max()
        min_price = filtered_menus["food_price"].min()
    else:
        min_price = 0
        max_price = 500000  # Default if no keywords selected

    # Slider to select price range
    min_price, max_price_slider = st.slider(
        "Chọn phạm vi giá", 
        min_value=min_price, 
        max_value=max_price, 
        value=(min_price, max_price),
        step=10000
    )

        # Filter menus based on keywords and price range
    if selected_keywords and not filtered_df.empty:
        filtered_menus = filtered_df[filtered_df["food_name"].str.contains('|'.join(selected_keywords), case=False, na=False)]
        filtered_menus = filtered_menus[(filtered_menus["food_price"] >= min_price) & (filtered_menus["food_price"] <= max_price_slider)]
            
        if not filtered_menus.empty:
            filtered_menus_sorted = filtered_menus.sort_values(by="food_price", ascending=False)
            filtered_menus_sorted = filtered_menus_sorted.reset_index(drop=True)
            st.write(f"Hiển thị {len(filtered_menus_sorted)} món ăn phù hợp với bộ lọc:")
            st.dataframe(filtered_menus_sorted[["food_name", "food_price"]])
        else:
            st.warning("Không có món ăn nào phù hợp với bộ lọc.")
    else:
        st.warning("Vui lòng chọn ít nhất một từ khóa và thiết lập phạm vi giá.")
