import streamlit as st
import pandas as pd
import requests
import io
import urllib
import json
from streamlit_tags import st_tags
import pytrends
from pytrends.request import TrendReq
from streamlit_tags import st_tags
from tqdm import tqdm
from datetime import datetime, timedelta, time, date
import altair as alt
import concurrent.futures
import numpy as np



@st.cache

def convert_df(keywords):
    return keywords.to_csv().encode('utf-8')

hide_table_row_index = """
            <style>
            tbody th {display:none}
            .blank {display:none}
            </style>
            """


st.markdown(hide_table_row_index, unsafe_allow_html=True)

st.markdown(
    """<style>
        .dataframe {text-align:center !important}
    </style>
    """, unsafe_allow_html=True) 


def f(dff_spread):
    if dff_spread["p"] == 0:
        val = "no ranking"

    if dff_spread["p"] <= 3:
        val = "top 3"

    if dff_spread["p"] >= 4 and dff_spread["p"] <= 10 :
        val = "top 10"

    if dff_spread["p"] >=11 and dff_spread["p"] <= 20:
        val = "top 20"
        
    if dff_spread["p"] >20:
        val = "Sup top 20"
        
    return val


dates = requests.get("https://api1.seobserver.com/organic_keywords/list_dates.json?api_key=60f82fae5f469aa7018b4616a96479aa23c58eb9ce42b6c6b86fbb1f8&base=manomano_fr_fr&offset=0")
dates = dates.json()["data"]
dates_recent = dates[0]
WoW = dates[2]
MoM = dates[6]
QoQ = dates[18]
dates = [dates_recent,WoW, MoM, QoQ]

###### FORMULAIRE 🇫🇷 ########

st.title('SEO visibility metrics 🇫🇷')
st.write(f"last update {dates_recent}")
url = f"AGG_DATA/final_data/raw_data_manomano_fr_fr_{dates_recent}.csv"
df_fr = pd.read_csv(url,index_col=0)
st.dataframe(df_fr)
header =st.header("Get ranking share 🇫🇷")
df_spread = pd.read_csv(f"RAW_DATA/raw_data_manomano_fr_fr_{dates_recent}.csv")
chart_2 = alt.Chart(df_spread).mark_bar().encode(x="date", y="count",color="Ranking range",tooltip=('Share','count')).properties(title="Share of ranking range")
st.altair_chart(chart_2, use_container_width=True)


st.title('SEO visibility metrics 🇪🇸')
st.write(f"last update {dates_recent}")
url = f"AGG_DATA/final_data/agg_data_manomano_es_es_{dates_recent}.csv"
df_es = pd.read_csv(url,index_col=0)
st.write(df_es)
header =st.header("Get ranking share 🇪🇸")
raw_data_url = f"RAW_DATA/raw_data_manomano_es_es_{dates}.csv"
raw_data_fr = pd.read_csv(raw_data_url)
test = raw_data_fr[["domain","keyword_title","p"]]
total_queries = test.groupby(['domain']).size() 
total_queries = total_queries[0]
total_queries = total_queries / 4
df_fr_spread = raw_data_fr.filter(items=['keyword_title','p','domain','date'])
df_fr_spread['Ranking range'] = df_fr_spread.apply(f, axis=1)
df_fr_spread = df_fr_spread.groupby(['date','Ranking range']).size().reset_index().rename(columns={0:"count"}) 
df_fr_spread["Share"] = df_fr_spread["count"] / total_queries
df_fr_spread[["Share"]] = ( 100 * df_fr_spread[["Share"]] ).round(2).astype(str) + "%" # The rounding is optional
chart_2 = alt.Chart(df_fr_spread).mark_bar().encode(x="date", y="count",color="Ranking range",tooltip=('Share','count')).properties(title="Share of ranking range")
st.altair_chart(chart_2, use_container_width=True)


st.title('SEO visibility metrics 🇮🇹')
st.write(f"last update {dates_recent}")
url = f"AGG_DATA/final_data/agg_data_manomano_it_it_{dates_recent}.csv"
df_it = pd.read_csv(url,index_col=0)
st.write(df_it)
header =st.header("Get ranking share 🇮🇹")
raw_data_url = f"RAW_DATA/raw_data_manomano_it_it_{dates}.csv"
raw_data_fr = pd.read_csv(raw_data_url)
test = raw_data_fr[["domain","keyword_title","p"]]
total_queries = test.groupby(['domain']).size() 
total_queries = total_queries[0]
total_queries = total_queries / 4
df_fr_spread = raw_data_fr.filter(items=['keyword_title','p','domain','date'])
df_fr_spread['Ranking range'] = df_fr_spread.apply(f, axis=1)
df_fr_spread = df_fr_spread.groupby(['date','Ranking range']).size().reset_index().rename(columns={0:"count"}) 
df_fr_spread["Share"] = df_fr_spread["count"] / total_queries
df_fr_spread[["Share"]] = ( 100 * df_fr_spread[["Share"]] ).round(2).astype(str) + "%" # The rounding is optional
chart_2 = alt.Chart(df_fr_spread).mark_bar().encode(x="date", y="count",color="Ranking range",tooltip=('Share','count')).properties(title="Share of ranking range")
st.altair_chart(chart_2, use_container_width=True)



st.title('SEO visibility metrics 🇬🇧')
st.write(f"last update {dates_recent}")
url = f"AGG_DATA/final_data/agg_data_manomano_co_uk_en_{dates_recent}.csv"
df_uk = pd.read_csv(url,index_col=0)
st.write(df_uk)
header =st.header("Get ranking share 🇬🇧")
raw_data_url = f"RAW_DATA/raw_data_manomano_co_uk_en_{dates}.csv"
raw_data_fr = pd.read_csv(raw_data_url)
test = raw_data_fr[["domain","keyword_title","p"]]
total_queries = test.groupby(['domain']).size() 
total_queries = total_queries[0]
total_queries = total_queries / 4
df_fr_spread = raw_data_fr.filter(items=['keyword_title','p','domain','date'])
df_fr_spread['Ranking range'] = df_fr_spread.apply(f, axis=1)
df_fr_spread = df_fr_spread.groupby(['date','Ranking range']).size().reset_index().rename(columns={0:"count"}) 
df_fr_spread["Share"] = df_fr_spread["count"] / total_queries
df_fr_spread[["Share"]] = ( 100 * df_fr_spread[["Share"]] ).round(2).astype(str) + "%" # The rounding is optional
chart_2 = alt.Chart(df_fr_spread).mark_bar().encode(x="date", y="count",color="Ranking range",tooltip=('Share','count')).properties(title="Share of ranking range")
st.altair_chart(chart_2, use_container_width=True)



st.title('SEO visibility metrics  🇩🇪')
st.write(f"last update {dates_recent}")
url = f"AGG_DATA/final_data/agg_data_manomano_de_de_{dates_recent}.csv"
df_de = pd.read_csv(url,index_col=0)
st.write(df_de)
header =st.header("Get ranking share 🇩🇪")
raw_data_url = f"RAW_DATA/raw_data_manomano_de_de_{dates}.csv"
raw_data_fr = pd.read_csv(raw_data_url)
test = raw_data_fr[["domain","keyword_title","p"]]
total_queries = test.groupby(['domain']).size() 
total_queries = total_queries[0]
total_queries = total_queries / 4
df_fr_spread = raw_data_fr.filter(items=['keyword_title','p','domain','date'])
df_fr_spread['Ranking range'] = df_fr_spread.apply(f, axis=1)
df_fr_spread = df_fr_spread.groupby(['date','Ranking range']).size().reset_index().rename(columns={0:"count"}) 
df_fr_spread["Share"] = df_fr_spread["count"] / total_queries
df_fr_spread[["Share"]] = ( 100 * df_fr_spread[["Share"]] ).round(2).astype(str) + "%" # The rounding is optional
chart_2 = alt.Chart(df_fr_spread).mark_bar().encode(x="date", y="count",color="Ranking range",tooltip=('Share','count')).properties(title="Share of ranking range")
st.altair_chart(chart_2, use_container_width=True)
