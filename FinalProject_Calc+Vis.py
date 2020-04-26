import requests
import os
import json
import sqlite3
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

def ReadDataFromApi(begin, end):

    username = "joshmoll"
    password = "206finalproject"
    base_url = "@opensky-network.org/api/flights/departure?"
    new_url = "https://" + username + ":" + password + base_url + "airport=KJFK" + "&" + "begin={}".format(begin) + "&" + "end={}".format(end)   
    apr_json_data = requests.get(new_url).json()
    return apr_json_data
    
#Connect to an sql server and create a cursor
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect("flights(v2).db")
    cur = conn.cursor()
    return cur, conn

def calc_percent_decrease(feb_table, apr_table, cur, conn):

    cur.execute("SELECT airline_name FROM {}".format(feb_table))        #dict with keys as flights, values as frequency. feb only
    all_rows = cur.fetchall()
    feb_airline_dict = {}
    for airline in all_rows:
        if airline not in feb_airline_dict:
            feb_airline_dict[airline] = 1
        else:
            feb_airline_dict[airline] += 1
    
    feb_total = sum(feb_airline_dict.values())                          #find avg number of flights per day. 
    #print(feb_total)
    feb_average = feb_total/7
    

    cur.execute("SELECT airline_name FROM {}".format(apr_table))        #dict with keys as flights, values as frequency. apr only
    all_rows = cur.fetchall()
    apr_airline_dict = {}
    for airline in all_rows:
        if airline not in apr_airline_dict:
            apr_airline_dict[airline] = 1
        else:
            apr_airline_dict[airline] += 1

    apr_total = sum(apr_airline_dict.values())                          #find avg number of flights per day.
    #print(apr_total)
    apr_average = apr_total/7


    difference = apr_average - feb_average                              #apply percent decrease equation
    decimal_value = difference / feb_average                                
    percent_decrease = (decimal_value * 100) * -1
    #print(percent_decrease)
    print("There was a " + str(percent_decrease) + "% percent decrease in flights leaving JFK in April compared to Februrary")

    with open("Percent_decrease.txt", "w") as outfile:
        outfile.write(str(percent_decrease))

def graph_Totaldata():
    apr_columns = ["DAL", "AAL", "ASA", "AAR", "FRH", "JBU", "EDV", "N92", "ATN", "FDX", "UPS", "DHK", "THY", "RPA", "N56", "N91", "CLX", "SLQ",
    "KAL", "GEC", "ABX", "VIR", "SV8", "BAW", "CPA", "AFR", "KLM", "EVA", "EIN", "AZA", "GTI", "CAO", "QFA", "CAL", "ELY", "SOO", "N97", "EEV",
    "TAY", "N23", "QTR", "CKS", "GJE", "DPJ", "JAL", "CKK", "CSN", "ASG", "AFL", "AHO", "TPA", "AMX", "N63", "N49", "N73", "N14", "N52", "N44",
    "N57", "N85", "N38", "N13", "N40", "TAM", "N33"]
    apr_flight_frequency = [29, 7, 6, 2, 4, 67, 16, 1, 3, 9, 6, 1, 5, 2, 1, 2, 8, 1, 8, 1, 2, 5, 3, 2, 6, 3, 1, 1, 1, 1, 8, 3, 2, 1, 1, 2, 1, 1, 
    2, 1, 1, 1, 2, 2, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

    feb_columns = ['AAL', 'EDV', "JBU", 'KAP', 'DAL', 'ASA', 'RPA', 'ENY', 'NRS', 'N98', 'N40', 'JB2', 'TAI', "BWA", "FDX","N45", "DHK", "UPS",
     "VIR", "BAW", "AA2", "AVA", "ICE", "SLQ", "VOI", "GEC", "AIJ", "AMX", "PAL", "LNE", "N91", "KAL", "CPA", "AAR", "CAL", "AUI", "EVA",
     "NAX", "AFR", "THY", "TAP", "LAN", "UAE", "ABX", "LOT", "KLM", "AZA", "EIN", "IBE", "SWR", "QTR", "CLX", "RAM", "N31", "SIA", "067", "AFL",
     "KAC", "FIN", "MSR", "QFA", "BEL", "AUA", "DLH","XOJ", "TAM", "ANA", "WJA", "N88", "ISS", "EJA", "SLI", "SVA", "RAX", "N68", "CMP", "VIV",
     "ICL", "JAL", "AIC", "U2B", "SAA", "CMB", "GAJ", "AA7", "EJM", "HAL"]

    feb_flight_frequency = [58, 61, 157, 2, 138, 13, 14, 11, 3, 2, 2, 1, 2, 6, 5, 2, 1, 3, 6, 10, 1, 3, 1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3, 2, 2, 
    1, 1, 6, 4, 2, 1, 1, 3, 1, 2, 1, 2, 2, 3, 2, 3, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 
    1, 1, 1, 1, 1, 1, 1]

    plt.bar(feb_columns, feb_flight_frequency, color = 'red', label = "February")
    plt.bar(apr_columns, apr_flight_frequency, color = "blue", label = "April")

    plt.xticks(rotation = 90, fontsize = 8, fontname = "Times New Roman")

    plt.title("Total Flights out of JFK ((February 7th, 5pm - February 14th, 5pm and April 7th, 5pm - April 14th, 5pm")
    plt.ylabel("Flights")
    plt.xlabel("Airline")
    plt.tick_params(axis='x', pad=5)
    plt.legend()
    plt.show()
   
def main():
    
    cur, conn = setUpDatabase('flights.db')
    calc_percent_decrease("totalFebFlights", "totalAprilFlights", cur, conn)
    graph_Totaldata()

if __name__ == "__main__":
    main()
