import requests
import pandas as pd
from urllib.request import Request, urlopen
from datetime import date
import time
import bs4 as bs
import urllib.request
import pandas as pd
import re
import logging
import random
import os
from tqdm import tqdm
import requests
from concurrent.futures import ThreadPoolExecutor as PoolExecutor


start_time = time.time()

Medicine_Data = requests.get("https://www.planetdrugsdirect.com/index/keywords").json()
Data_Notebook = []

for i in Medicine_Data:
    Records = []
    for information,values in i.items() :
        Records.append(values)
    Data_Notebook.append((Records[0],Records[1],Records[2]))
    
Data_Notebook = pd.DataFrame(Data_Notebook,columns = ["Medicine","Drug_Name","In_Demand"])

Data_Notebook["Name"] = ""
Data_Notebook["Other_Name"] = ""


for row in tqdm(range(0,len(Data_Notebook))):
    Data_Notebook["Name"][row] = Data_Notebook["Medicine"][row].split('(')[0]
    Data_Notebook["Other_Name"][row] = Data_Notebook["Medicine"][row].split('(')[1].replace(')','')
    
user_agent_list = [
        
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

RECORDS = []

FAILED_QUEUE = []


def medicine_crawler(item):
    
    try:
        
        address = "https://www.planetdrugsdirect.com/drugs/"+str(Data_Notebook["Drug_Name"][item])
        
        
        print("-------------")
        
        user_agent = random.choice(user_agent_list)
        #req = Request(address,headers={'User-Agent': user_agent})
        #source = urlopen(req).read()
        #soup = bs.BeautifulSoup(source,'lxml')

        r = requests.get(address, headers={'User-Agent': user_agent}, timeout=10)
        soup = bs.BeautifulSoup(r.content)
        
        Values = soup.find_all('div', attrs={'class': 'drug-list drug-list-brand'})

        Rows = soup.find_all('div', attrs={'class': 'row'})

        print("Crawling " + str(address))
        

        for row in Rows:

            try:

                Data = row.find('div', attrs={'class': 'col-sm-5'})

                Data = Data.get_text()

                Name = "".join(re.findall("[a-zA-Z0-9()]",Data.split('\n')[1]))
                print(Name)

                Data = row.find('div', attrs={'class': 'col-sm-7'})

                Stocks = []

                Precription_Required = soup.find('div', attrs={'class': 'col-xs-12 tiny'}).get_text().split('\n')[2]
                Shipped_From = soup.find('div', attrs={'class': 'col-xs-12 tiny'}).get_text().split('\n')[-4]

                for i in Data.find_all('option'):

                    RECORDS.append((
                        
                        Data_Notebook["Drug_Name"][item],
                        Data_Notebook["Medicine"][item],
                        Data_Notebook["Other_Name"][item],
                        Name,
                        "".join(i.get_text().split('-')[0]),
                        "".join(i.get_text().split('-')[1]),
                        Precription_Required,
                        Shipped_From

                            ))

            except:

                print("----")

        
        time.sleep(5)
         
    except:
        
        print("Failed for " + str(item))
        FAILED_QUEUE.append(item)


from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(medicine_crawler, x) for x in range(len(Data_Notebook))]

 

today = date.today()

today = today.strftime("%m/%d/%Y")

timestr = time.strftime("%Y-%m-%d")


RECORDS = pd.DataFrame(RECORDS,columns =[
    
    "Drug_Name",
    "Medicine",
    "Other_Name",
    "Name",
    "Dose",
    "Price",
    "Prescription",
    "Shipping"
])

RECORDS["Date"] = today

RECORDS = RECORDS[[
    
    "Date",
    "Drug_Name",
    "Medicine",
    "Other_Name",
    "Name",
    "Dose",
    "Price",
    "Prescription",
    "Shipping"
    
]]

RECORDS  = RECORDS.drop_duplicates(subset = list(RECORDS.columns), keep = "first") 

Data_Notebook["Date"] = today

Data_Notebook = Data_Notebook[[
    "Date",
    "Medicine",
    "Drug_Name",
    "In_Demand",
    "Name",
    "Other_Name"
]]


saved_named = "Records/Medicine-Drug-Information_"+str(timestr)+'.xlsx'

writer = pd.ExcelWriter(saved_named, engine='xlsxwriter')

RECORDS.to_excel(writer, sheet_name='Price_Information',index=False)

Data_Notebook.to_excel(writer,sheet_name='Trend_Information',index=False)

writer.save()

print(FAILED_QUEUE)


end_time = time.time()

print("--- %s seconds ---" % (end_time - start_time))


