### RECUP DES DONNEES BRUT ####

def seobserver(domains,dates):
    for date in dates:
        df_list = []
        for domain in domains:
            if tag != "":
                response = requests.get(f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&date={date}&conditions[tags]={tag}&{keywords}&{limit}&offset=0", {'item_type': 'domain', 'item_value':[domain]})
            else: 
                response = requests.get(f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&date={date}&{keywords}&{limit}&offset=0", {'item_type': 'domain', 'item_value': [domain]})
            
            for i in offset_list:
                if tag != "":
                            response = requests.get(f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&date={date}&conditions[tags]={tag}&{keywords}&{limit}&offset={i}", {'item_type': 'domain', 'item_value':[domain]})
                else: 
                            response = requests.get(f"https://api1.seobserver.com/organic_keywords/index.json?{api_key}&base={database}&date={date}&{keywords}&{limit}&offset={i}", {'item_type': 'domain', 'item_value': [domain]})
                df = pd.json_normalize(response.json()['data'])
                df_list.append(df)
                df = pd.concat(df_list)
                #df = df[["domain","visibility","date"]]
                #df = df.groupby(["date","domain"]).sum()
                #df = df.reset_index()
                #df = df[["domain","visibility"]]
                df.to_csv(f"/Users/pierre.b/Documents/GitHub/STREAMLIT_RANKING_SEOBSERVER/FULL_DATA/ranking_{database}_{date}.csv")

    return df