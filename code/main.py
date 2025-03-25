#%%
from pymongo import MongoClient
import pandas as pd # 命名模組
from bson.objectid import ObjectId
import kagglehub

import os
from dotenv import load_dotenv

#%%
# 下載資料
path = kagglehub.dataset_download("shriyasingh900/covid19-dataset")

#%%
# 讀取CSV檔案
csv_file = '../data/covid_19_clean_complete.csv'
df = pd.read_csv(csv_file)

#%%
# 建立連線函式
def monCon(monip,mondb,moncol):
    monclient = MongoClient('mongodb://'+monip+':27017/') 
    mondb = monclient[mondb]
    momCol = mondb[moncol]
    return momCol

#%%
# 將 DataFrame 轉換為字典列表
data_dict = df.to_dict(orient='records')

#%%
monip = os.getenv("MON_IP")
mondbName = os.getenv("MONDBNAME")
moncolName = os.getenv("MONCLIENT")
monclient = MongoClient('mongodb://'+monip+':27017/') 
mondb = monclient[mondbName]
momCol = mondb[moncolName] 

# 插入資料到MongoDB
momCol.insert_many(data_dict)

#%%
monip = os.getenv("MON_IP")
mondbName = os.getenv("MONDBNAME")
moncolName = os.getenv("MONCLIENT")
# 台灣確診人數最多的前五天
agg1 = [
    {
        "$match": {
            "Country/Region": "Taiwan*"
        }
    },
    {
         "$group": {
            "_id": "$Date", 
            "確診人數": {"$sum": "$Confirmed"} 
        }
    },
    {
        "$sort": {"確診人數": -1}
    },
        {"$limit": 5
    },
    {
        "$project": { 
            "Date": "$_id",
            "確診人數": 1, 
            "_id": 0 
        }
    }
]
momCol = monCon(monip,mondbName,moncolName)
result1 = momCol.aggregate(agg1)

print("台灣確診人數最多的前五天：")
for doc in result1:
    print(doc)
#%%
# 台灣死亡人數最多的前五天
agg2 = [
    {
        "$match": {
            "Country/Region": "Taiwan*"
        }
    },
    {
         "$group": {
            "_id": "$Date", 
            "死亡人數": {"$sum": "$Deaths"} 
        }
    },
    {
        "$sort": {"死亡人數": -1}
    },
        {"$limit": 5
    },
    {   
        "$project": { 
            "Date": "$_id",
            "死亡人數": 1,
            "_id": 0 
        }
    }
]
momCol = monCon(monip,mondbName,moncolName)
result2 = momCol.aggregate(agg2)

print("台灣死亡人數最多的前五天：")
for doc in result2:
    print(doc)
#%%