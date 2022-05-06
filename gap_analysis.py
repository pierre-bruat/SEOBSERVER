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

def call(date,domains):
    df_list = []
    for domain in domains:
        for o in offset_list:
            print(
                f"requête seobserver avec offset {o} pour le keyword {keywords}, pour la date {date} et le domaine {domain}"
            )
            if tag == "":
                response = requests.get(
                    f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&{keywords}&{limit}&offset={o}&date={date}",
                    {"item_type": "domain", "item_value": [domain]},
                )
            else:
                response = requests.get(
                    f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&conditions[tags]={tag}&{keywords}&{limit}&offset={o}&date={date}",
                    {"item_type": "domain", "item_value": [domain]},
                )
            df = pd.json_normalize(response.json()["data"])

            if df.empty:
                break
            elif o == 0 and len(df) < 1000:
                df_list.append(df)
                break
            else:
                df_list.append(df)
    return pd.concat(df_list,ignore_index=True)

def seobserver(domains, dates, max_threads=8):
    df_list = []
    n_threads = min(max_threads,len(dates))
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        future_to_kw = {
            executor.submit(call, date, domains): date 
            for date in dates
        }
        for future in concurrent.futures.as_completed(future_to_kw):
            kw = future_to_kw[future]
            try:
                res = future.result()
                if res is not None: 
                    df_list.append(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (kw, exc))

    df_final = pd.concat(df_list).drop_duplicates()
    if df_final.date.nunique() == len(dates):
        print("Fin du traitement OK. Le nombre de dates correspond aux dates demandées")
        return df_final
    else:
        print("Attention, le nombre de dates ne correspond pas aux dates demandées")
        return df_final


def f(dff_spread):
    if dff_spread["p"] <= 3:
        val = "top 3"
    elif dff_spread["p"] <= 10:
        val = "top 10"
    elif dff_spread["p"] <= 20:
        val = "top 20"
    else:
        val = "sup top 20"
    return val


def convert_df(keywords):
    return keywords.to_csv().encode('utf-8')

# CONDITION #

limit = "limit=10000"
api_key ="api_key=60f82fae5f469aa7018b4616a96479aa23c58eb9ce42b6c6b86fbb1f8" 
kw=""
offset_start = 0
offset_end = 10000
offset_list = [i for i in range(offset_start, offset_end, 1000)]


###### FORMULAIRE ########
header = st.title('Gap analysis 🕵')
form = st.form(key='my-form')
database = form.selectbox("Country",["manomano_fr_fr"])
tag = form.selectbox("Family", ("cuisine", "outillage", "jardin-piscine","mobilier-d-interieur","salle-de-bain-wc","plomberie-chauffage","luminaire","electricite","revetement-sol-et-mur","animalerie","quincaillerie","construction-materiaux",""))
#kw = form.text_input("Keywords")
#compet = form.text_input("Competitors, ie. leroymerlin.fr, cdiscount.com")
#dates_selected = form.selectbox(f"Compare {last_monday} with ...",(days_15, days_30,days_60,days_120,days_180,days_360))
dates_call = requests.get("https://api1.seobserver.com/organic_keywords/list_dates.json?api_key=60f82fae5f469aa7018b4616a96479aa23c58eb9ce42b6c6b86fbb1f8&base=manomano_fr_fr&offset=0")
#dates_past = dates_call.json()["data"][:20]
#dates_list = dates_call.json()["data"][1:]
#prev_snap = form.selectbox("Dates",dates_list)
last_snap = dates_call.json()["data"][0]
submit = form.form_submit_button('Submit')
if submit:
    if database == "manomano_fr_fr":
        domains = ["manomano.fr"]
    
    #dates = (last_snap, prev_snap)
    keywords = f"conditions[keyword_title like]=%{kw}%"
    scope = f"conditions[tags]={tag}"
    base = f"base={database}"
    #domains = list(compet.split(","))
    dff = seobserver(domains, last_snap)
    st.write(dff)
    CSV = convert_df(dff)
    st.download_button(label="Download data as CSV", data=CSV, file_name="my_data.csv",mime='text/csv')

    
    



  

