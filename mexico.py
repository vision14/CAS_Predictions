import pandas as pd
from datetime import datetime, timedelta
from statsmodels.tsa.arima_model import ARIMA
from time import time
import requests
import json
from pymongo import MongoClient

def covid_prediction(series):
    X = series.values
    history = [i for i in X]
    predictions = list()
    for t in range(30):
        model = ARIMA(history, order=(10,1,0))
        model_fit = model.fit(disp=0)
        output = model_fit.forecast()
        yhat = output[0]
        predictions.append(int(round(yhat[0])))
        history.append(yhat)

    return predictions

print("Executing Mexico.py")
start = time()
my_url = "https://covid19.who.int/page-data/region/amro/country/mx/page-data.json"
s_date = "2020-02-28"

data = requests.get(my_url).json()
data = data["result"]["pageContext"]["countryGroup"]

total_cases = []
total_deaths = []
new_cases = []
new_deaths = []

for i in data["data"]["rows"]:
    new_deaths.append(i[2])
    total_deaths.append(i[3])
    new_cases.append(i[7])
    total_cases.append(i[8])

date_list = pd.date_range(start=s_date, end=datetime.today()).tolist()

if(len(date_list) > len(new_cases)):
    date_list = date_list[:len(new_cases)]

data_dict = {"Date": date_list,
             "New Cases": new_cases,
             "Total Cases": total_cases,
             "New Deaths": new_deaths,
             "Total Deaths": total_deaths}

mongo_data = []
for i in range(len(date_list)):
    mongo_data.append({"Date": date_list[i],
                       "New Cases": new_cases[i],
                       "Total Cases": total_cases[i],
                       "New Deaths": new_deaths[i],
                       "Total Deaths": total_deaths[i]})

try:
    client = MongoClient("mongodb+srv://user_1:USER_1@cluster0.0oqke.mongodb.net/<dbname>?retryWrites=true&w=majority")
    db = client.get_database('covid_db')
    db_data = db.mexico
    db_data.delete_many({})
    db_data.insert_many(mongo_data)
    print("Data dumped successfully")
except:
    print("Unkown error occurred")

df = pd.DataFrame.from_dict(data_dict)
df = df.set_index("Date")
df = df.asfreq(pd.infer_freq(df.index))
df.columns = df.columns.str.replace(' ', '_')
new_cases_df = df.iloc[:, 0]
total_cases_df = df.iloc[:, 1]
new_deaths_df = df.iloc[:, 2]
total_deaths_df = df.iloc[:, 3]
print("Data Retrieved")

print("Predictions Started")
nc_preds = covid_prediction(new_cases_df)
nd_preds = covid_prediction(new_deaths_df)
tc_preds = [int(abs(nc_preds[0] + total_cases_df.iloc[-1]))]
td_preds = [int(abs(nd_preds[0] + total_deaths_df.iloc[-1]))]
for i in range(1, len(nc_preds)):
    tc_preds.append(int(abs(nc_preds[i]+tc_preds[i-1])))
    td_preds.append(int(abs(nd_preds[i]+td_preds[i-1])))

for i in range(len(nc_preds)):
    if(nc_preds[i] < 0):
        nc_preds[i] = 0
    if(tc_preds[i] < 0):
        tc_preds[i] = 0
    if(nd_preds[i] < 0):
        nd_preds[i] = 0
    if(td_preds[i] < 0):
        td_preds[i] = 0

start_date = data_dict["Date"][-1]+timedelta(days=1)
end_date = start_date+timedelta(days=30)

date_list = pd.date_range(start=start_date, end=end_date).tolist()
if(len(date_list) > len(nc_preds)):
    date_list = date_list[:len(nc_preds)]    

print("Predictions Completed")

mongo_data = []
for i in range(len(date_list)):
    mongo_data.append({"Date": date_list[i],
                       "New Cases": nc_preds[i],
                       "Total Cases": tc_preds[i],
                       "New Deaths": nd_preds[i],
                       "Total Deaths": td_preds[i]})

try:
    db_data = db.mexico_predictions
    db_data.delete_many({})
    db_data.insert_many(mongo_data)
    print("Data dumped successfully")
except:
    print("Unkown error occurred")

end = time()
print("Total time taken:", end-start)
print("")
