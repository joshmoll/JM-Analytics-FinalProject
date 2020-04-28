import requests
import os
import json
import sqlite3
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
 
def API_data_retrieval():
   base_url = "https://energ.ee/covid19-us-api/states.json"
   response = requests.get(base_url)
   covid_tracking_data = response.json()
   return covid_tracking_data
 
 
def database_setup(db_name):
   path = os.path.dirname(os.path.abspath(__file__))
   conn = sqlite3.connect("flights(v2).db")
   cur = conn.cursor()
   return cur, conn
 

def daily_case_avg_graph(cur, conn):
   #cur.execute('SELECT April FROM case_info limit 37, 44') #integers
   april_case_lst = [140081, 149401, 159937, 170512, 180458, 188694, 195031, 202208]
   #cur.execute('SELECT Date FROM case_info limit 37, 44') #strings
   april_dates_lst = ['2020-04-07', '2020-04-08', '2020-04-09', '2020-04-10', '2020-04-11', '2020-04-12', '2020-04-13', '2020-04-14']
   plt.bar(april_dates_lst, april_case_lst, color = 'black', label = 'Confirmed Cases')
   plt.title('NY COVID-19 Cases in April')
   plt.xlabel('Date')
   plt.ylabel('Confirmed Cases')
   plt.legend()
   plt.show()

 
#dates april 7-14, case # a day, total cases for each date and divide 7, average cases from 7-14
def calculations(case_data):
   case_numbers = []
   for case in case_data['New York']:
       case_numbers.append(int(case['confirmed'])) #83889 on APR 1. confirmed_cases.append(case) returns a list of dictionaries starting from march 1
   confirmed_cases = case_numbers[37:45] #list of confirmed cases APRIL 7-14
   total_cases = sum(confirmed_cases)
   avg  = total_cases//7
   print(avg)
   return avg
  
 
def main():
   cur, conn = database_setup("flights(v2).db")
 
   daily_case_avg_graph(cur, conn)
   calculations(API_data_retrieval())
 
if __name__ == "__main__":
   main()
