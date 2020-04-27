import requests
import os
import json
import sqlite3
from opensky_api import OpenSkyApi

api = OpenSkyApi()

#Write a function so that the code obtained from the read in file in converted to JSON and returns a dictionary

def ReadDataFromApi(begin, end):

    username = "joshmoll"
    password = "206finalproject"
    base_url = "@opensky-network.org/api/flights/departure?"
    new_url = "https://" + username + ":" + password + base_url + "airport=KJFK" + "&" + "begin={}".format(begin) + "&" + "end={}".format(end)   
    json_data = requests.get(new_url).json()
    return json_data

def API_data_retrieval():
    base_url = "https://energ.ee/covid19-us-api/states.json"
    response = requests.get(base_url)
    covid_tracking_data = response.json()
    return covid_tracking_data

#Connect to an sql server and create a cursor
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect("flights(v2).db")
    cur = conn.cursor()
    return cur, conn


#Create the FebFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
def setUpFebFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE FebFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS FebFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, departure_airport TEXT, intended_arrival_airport TEXT)")

    airline_list = []
    count = 0
    for flight in data:
        if count == 20:
            break
        x = flight["callsign"]
        cur.execute("SELECT callsign FROM FebFlights WHERE callsign = ?", (x,))
        callsign_test = cur.fetchone()
        if callsign_test:
            continue
        else:
            cur.execute("INSERT OR IGNORE INTO FebFlights (callsign, airline_name, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["estDepartureAirport"], flight["estArrivalAirport"]))  
            airline_list.append(flight)
            count += 1

    conn.commit()

#Create the AprilFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
def setUpAprilFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE AprilFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS AprilFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, departure_airport TEXT, intended_arrival_airport TEXT)")

    airline_list = []         
    count = 0
    for flight in data: 
        if count == 20:
            break
        x = flight["callsign"]                           
        cur.execute("SELECT callsign FROM AprilFlights WHERE callsign = ?", (x,))
        callsign_test = cur.fetchone()
        if callsign_test:
            continue
        else:
            cur.execute("INSERT OR IGNORE INTO AprilFlights (callsign, airline_name, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["estDepartureAirport"], flight["estArrivalAirport"])) #apply category id here
            airline_list.append(flight)
            count += 1
        
    conn.commit()

def share_key(cur):
    cur.execute("SELECT totalFebFlights.callsign, totalAprilFlights.callsign, totalFebFlights.intended_arrival_airport, totalAprilFlights.intended_arrival_airport FROM totalFebFlights JOIN totalAprilFlights ON totalFebFlights.callsign = totalAprilFlights.callsign")
    rows = cur.fetchall()
    #print(rows)

#Create the totalFebFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
#This table will load all the February data by 200, useful for visualization and calculations
def setUpTotalFebFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE totalFebFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS totalFebFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, departure_airport TEXT, intended_arrival_airport TEXT)")

    airline_list = []
    count = 0
    for flight in data:
        if count == 200:
            break
        x = flight["callsign"]
        cur.execute("SELECT callsign FROM totalFebFlights WHERE callsign = ?", (x,))
        callsign_test = cur.fetchone()
        if callsign_test:
            continue
        else:
            cur.execute("INSERT OR IGNORE INTO totalFebFlights (callsign, airline_name, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["estDepartureAirport"], flight["estArrivalAirport"]))  
            airline_list.append(flight)
            count += 1
            
    conn.commit()

#Create the totalAprilFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
#This table will load all the April data by 200, useful for visualization and calculations
def setUpTotalAprilFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE totalAprilFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS totalAprilFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, departure_airport TEXT, intended_arrival_airport TEXT)")

    airline_list = []         
    count = 0
    for flight in data: 
        if count == 200:
            break
        x = flight["callsign"]                            
        cur.execute("SELECT callsign FROM totalAprilFlights WHERE callsign = ?", (x,))
        callsign_test = cur.fetchone()
        if callsign_test:
            continue
        else:
            cur.execute("INSERT OR IGNORE INTO totalAprilFlights (callsign, airline_name, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["estDepartureAirport"], flight["estArrivalAirport"])) #apply category id here
            airline_list.append(flight)
            count += 1
           

    conn.commit()

def case_info_setup(case_data, cur, conn):
    cur.execute('DROP TABLE case_info')
    cur.execute("CREATE TABLE IF NOT EXISTS case_info (Date TEXT PRIMARY KEY, April TEXT)")
    NY_cases = []
    count = 0
    for key in case_data['New York']:
        if count == 20:
            break
        i = key['date']
        cur.execute("SELECT Date FROM case_info WHERE Date = ?", (i,))
        row = cur.fetchone()
        if row:
            continue
        else:
            cur.execute("INSERT OR IGNORE INTO case_info (Date, April) VALUES (?, ?)", (key['date'], key['confirmed']))
            NY_cases.append(key)
            count += 1
    conn.commit()

def main():
    cur, conn = setUpDatabase('flights.db')

    feb_json_data = ReadDataFromApi("1581098400","1581703200")
    setUpFebFlightsTable(feb_json_data, cur, conn)

    apr_json_data = ReadDataFromApi("1586278800", "1586883600")  
    setUpAprilFlightsTable(apr_json_data, cur, conn)

    feb_json_data = ReadDataFromApi("1581098400","1581703200")
    setUpTotalFebFlightsTable(feb_json_data, cur, conn)

    apr_json_data = ReadDataFromApi("1586278800", "1586883600")  
    setUpTotalAprilFlightsTable(apr_json_data, cur, conn)

    share_key(cur)

    
    NY_json_data = API_data_retrieval()
    case_info_setup(NY_json_data, cur, conn)

if __name__ == "__main__":
    main()
