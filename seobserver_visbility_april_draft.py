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

def seobserver(domains, dates, max_threads=16):
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


# CONDITION #
#domains = domains_list
#dates = ["2021-10-11","2021-11-30"]
#scope = f"conditions[tags]={tag}"
limit = "limit=10000"
#base = f"base={country}"
api_key ="api_key=60f82fae5f469aa7018b4616a96479aa23c58eb9ce42b6c6b86fbb1f8" 
kw=""
offset_start = 0
offset_end = 10000
offset_list = [i for i in range(offset_start, offset_end, 1000)]


###### FORMULAIRE ########
header = st.title('SEO visibility metrics 🕵')
form = st.form(key='my-form')
database = form.selectbox("Country",("manomano_fr_fr","manomano_es_es","manomano_it_it","manomano_de_de","manomano_co_uk_en"))
tag = form.selectbox("Family", ("cuisine", "outillage", "jardin-piscine","mobilier-d-interieur","salle-de-bain-wc","plomberie-chauffage","luminaire","electricite","revetement-sol-et-mur","animalerie","quincaillerie","construction-materiaux",""))
kw = form.text_input("Keywords")
#compet = form.text_input("Competitors, ie. leroymerlin.fr, cdiscount.com")
#dates_selected = form.selectbox(f"Compare {last_monday} with ...",(days_15, days_30,days_60,days_120,days_180,days_360))
dates_call = requests.get("https://api1.seobserver.com/organic_keywords/list_dates.json?api_key=60f82fae5f469aa7018b4616a96479aa23c58eb9ce42b6c6b86fbb1f8&base=manomano_fr_fr&offset=0")
dates_past = dates_call.json()["data"][:20]
dates_list = dates_call.json()["data"][1:]
prev_snap = form.selectbox("Dates",dates_list)
last_snap = dates_call.json()["data"][0]
submit = form.form_submit_button('Submit')
if submit:
    if database == "manomano_fr_fr":
        domains = ["manomano.fr","leroymerlin.fr","amazon.fr","cdiscount.com","castorama.fr"]
    if database == "manomano_es_es":
        domains = ["manomano.es","leroymerlin.es","amazon.es","bauhaus.es","lionshome.es"]
    if database == "manomano_it_it":
        domains = ["manomano.it","leroymerlin.it","amazon.it","bricoman.it","obi-italia.it"]
    if database == "manomano_de_de":
        domains = ["manomano.de","obi.de","amazon.de","hornbach.de","home24.de"]
    if database == "manomano_co_uk_en":
        domains = ["manomano.co.uk","screwfix.com","wayfair.co.uk","amazon.co.uk","diy.com"]

    dates = (last_snap, prev_snap)
    keywords = f"conditions[keyword_title like]=%{kw}%"
    scope = f"conditions[tags]={tag}"
    base = f"base={database}"
    #domains = list(compet.split(","))
    dff = seobserver(domains, dates)
    st.write(dff)
    CSV = convert_df(dff)
    st.download_button(label="Download data as CSV", data=CSV, file_name="my_data.csv",mime='text/csv')
    df_visibility_sum = dff.groupby(['date','domain']).sum()[['visibility']]
    df_visibility_sum = df_visibility_sum.rename(columns = lambda x: x.strip("visibility "))
    df_visibility_sum = df_visibility_sum.unstack()   
    df_visibility_sum_growth = df_visibility_sum.pct_change().mul(100).round(2)
    
    st.subheader(f"👉 Visibility score evolution btw {prev_snap} and {last_snap}")
    
    col2, col1 = st.columns(2)
    df_ranking_2 = df_visibility_sum.iloc[0]
    df_ranking_2 = df_ranking_2.sort_values(ascending=False)
    df_ranking_2 = df_ranking_2.to_frame()
    real_date_before = df_ranking_2.columns[0]
    col2.write(df_ranking_2)   

    df_ranking = df_visibility_sum.iloc[1]
    df_ranking = df_ranking.sort_values(ascending=False)
    df_ranking = df_ranking.to_frame()
    #df_ranking = df_ranking.iloc[: , 1:]
    real_date_after = df_ranking.columns[0]
    col1.write(df_ranking)
 
    
    st.subheader("👉 visibility growth 📈")
    plat_1 = df_visibility_sum_growth.columns[0]
    evol_1 = df_visibility_sum_growth.iloc[1,0]
    score_1 = df_visibility_sum.iloc[1,0]

    plat_2 = df_visibility_sum_growth.columns[1]
    evol_2 = df_visibility_sum_growth.iloc[1,1] 
    score_2 = df_visibility_sum.iloc[1,1]

    plat_3 = df_visibility_sum_growth.columns[2]
    evol_3 = df_visibility_sum_growth.iloc[1,2]
    score_3 = df_visibility_sum.iloc[1,2]

    plat_4 = df_visibility_sum_growth.columns[3]
    evol_4 = df_visibility_sum_growth.iloc[1,3]
    score_4 = df_visibility_sum.iloc[1,3]

    plat_5 = df_visibility_sum_growth.columns[4]
    evol_5 = df_visibility_sum_growth.iloc[1,4]
    score_5 = df_visibility_sum.iloc[1,4]

    plat_1 = pd.DataFrame(plat_1)
    plat_1 = plat_1.iloc[1,0]

    plat_2 = pd.DataFrame(plat_2)
    plat_2 = plat_2.iloc[1,0]

    plat_3 = pd.DataFrame(plat_3)
    plat_3 = plat_3.iloc[1,0]

    plat_4 = pd.DataFrame(plat_4)
    plat_4 = plat_4.iloc[1,0]

    plat_5 = pd.DataFrame(plat_5)
    plat_5 = plat_5.iloc[1,0]
    
    col1, col2, col3, col4, col5  = st.columns(5)
    col1.metric(plat_1,score_1,evol_1)
    col2.metric(plat_2,score_2,evol_2)
    col3.metric(plat_3,score_3,evol_3)
    col4.metric(plat_4,score_4,evol_4)
    col5.metric(plat_5,score_5,evol_5)



    #st.subheader("👉 Visibility growth over time 📈")
    #dff_3 = seobserver(domains, dates_past)
    #df_visibility_sum = dff_3.groupby(['date','domain']).sum()[['visibility']]
    #df_visibility_sum = df_visibility_sum.reset_index()
    #df_visibility_sum = df_visibility_sum.rename(columns = lambda x: x.strip("visibility "))
    #df_visibility_sum = df_visibility_sum.unstack() 
    #st.write(df_visibility_sum)
    #st.line_chart(df_visibility_sum)
    #chart = alt.Chart(df_visibility_sum).mark_line().encode(x=alt.X('date'),y=alt.Y('visibility'),color=alt.Color("domain")).properties(title="Visibility growth over time")
    #chart = alt.Chart(df_visibility_sum).mark_area().encode(x="date", y="visibility",color="domain",tooltip=['date', 'visibility']).properties(title="Visibility growth over time")
    #st.altair_chart(chart, use_container_width=True)



    st.subheader("👉 Keywords ranking evolution")
    dff_filtered = dff.filter(items=['keyword_title','search_volume','p','url','date','domain'])
    top_fail_avant = dff_filtered[dff_filtered["date"] == f"{real_date_before}"]
    top_fail_avant =  top_fail_avant[top_fail_avant['domain'].str.contains("manomano")]
    top_fail_avant = top_fail_avant.drop_duplicates("keyword_title")
    top_fail_après =  dff_filtered[dff_filtered['date'] == f"{real_date_after}"]
    top_fail_après =  top_fail_après[top_fail_après['domain'].str.contains("manomano")]
    top_fail_après = top_fail_après.drop_duplicates("keyword_title")
    result = top_fail_après.merge(top_fail_avant, how='left', on='keyword_title')
    result_filtered = result.filter(items=['keyword_title','search_volume_x','p_x','p_y'])
    result_filtered['Position diff'] = result_filtered["p_y"] - result_filtered["p_x"]
    result_filtered = result_filtered.rename(columns={result_filtered.columns[0]: 'Keyword'})
    result_filtered = result_filtered.rename(columns={result_filtered.columns[1]: 'Search Volume'})
    result_filtered = result_filtered.rename(columns={result_filtered.columns[2]: 'Position now'})
    result_filtered = result_filtered.rename(columns={result_filtered.columns[3]: 'Position then'})
    result_filtered = result_filtered.rename(columns={result_filtered.columns[4]: 'Evolution'})
    result_filtered = result_filtered.dropna(subset=['Position then'])
    result_filtered = result_filtered.dropna(subset=['Evolution'])
    result_filtered["Position then"] = result_filtered['Position then'].apply(lambda x: round(x))
    result_filtered["Evolution"] = result_filtered['Evolution'].apply(lambda x: round(x))
    #result_filtered["Evolution"] = result_filtered['Evolution'].astype(int)
    result_filtered = result_filtered.sort_values(by="Evolution",ascending=True)
    st.write(result_filtered)

    
    
    st.subheader("👉 Keywords spread")
    dff_spread = dff.filter(items=['keyword_title','p','domain'])
    dff_spread['Ranking range'] = dff_spread.apply(f, axis=1)
    #dff_spread = dff.filter(items=['keyword_title','domain','Ranking range'])
    dff_spread = dff_spread.groupby(['domain','Ranking range']).size().reset_index().rename(columns={0:"count"}) 
    dff_spread = dff_spread.sort_values(by="Ranking range")
    chart_2 = alt.Chart(dff_spread).mark_bar().encode(x="count", y="domain",color="Ranking range",tooltip='count').properties(title="Share of ranking range")
    st.altair_chart(chart_2, use_container_width=True)


    st.subheader("👉 Ranking > position 20)")
    dff_2 = dff.filter(items=['keyword_title','search_volume','p','url'])
    keywords_20 =  dff_2[dff_2['p']>=21]
    keywords_20 =  keywords_20[keywords_20['url'].str.contains("manomano")]
    keywords_20 = keywords_20.sort_values(by=["search_volume","p"],ascending=[False,True])
    keywords_20 = keywords_20.drop_duplicates(subset="keyword_title",keep="first")
    st.write(keywords_20)

    st.subheader("👉 Ranking btw position 4 & 10")
    dff_2 = dff.filter(items=['keyword_title','search_volume','p','url'])
    keywords_4_10 =  dff_2[dff_2['p']>=4]
    keywords_4_10 =  keywords_4_10[keywords_4_10['p']<=10]
    keywords_4_10 =  keywords_4_10[keywords_4_10['url'].str.contains("manomano")]
    keywords_4_10 = keywords_4_10.sort_values(by=["search_volume","p"],ascending=[False,True])
    keywords_4_10 = keywords_4_10.drop_duplicates(subset="keyword_title",keep="first")
    st.write(keywords_4_10)

    st.subheader("👉 Ranking btw position 10 & 20")
    dff_2 = dff.filter(items=['keyword_title','search_volume','p','url'])
    keywords_10_20 =  dff_2[dff_2['p']>=11]
    keywords_10_20 =  keywords_10_20[keywords_10_20['p']<=20]
    keywords_10_20 =  keywords_10_20[keywords_10_20['url'].str.contains("manomano")]
    keywords_10_20 = keywords_10_20.sort_values(by=["search_volume","p"],ascending=[False,True])
    keywords_10_20 = keywords_10_20.drop_duplicates(subset="keyword_title",keep="first")
    st.write(keywords_10_20)


    #st.subheader("Share of keywords in TOP ")
    
    #keywords_LP = dff_filtered[dff_filtered['url'].str.contains("/[a-z0-9-]+-[0-9]+(?:-[a-z]*[0-9]+)?$", regex=True)]
    #keywords_LP_in_top20 = keywords_LP[keywords_LP['p']<=20]
    #st.write(keywords_LP_in_top20)
    #st.write(keywords_LP_in_top20['url'].value_counts())



  

