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
    apr_json_data = requests.get(new_url).json()
    return apr_json_data
    
#Connect to an sql server and create a cursor
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect("flights(v2).db")
    cur = conn.cursor()
    return cur, conn

#Create the FebFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
def setUpFebFlightsTable(data, cur, conn):

    #cur.execute("DROP TABLE FebFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS FebFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, date INTEGER, departure_airport TEXT, intended_arrival_airport TEXT)")

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
            cur.execute("INSERT OR IGNORE INTO FebFlights (callsign, airline_name, date, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["lastSeen"], flight["estDepartureAirport"], flight["estArrivalAirport"]))  
            airline_list.append(flight)
            count += 1

    conn.commit()

#Create the AprilFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
def setUpAprilFlightsTable(data, cur, conn):

    #cur.execute("DROP TABLE AprilFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS AprilFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, date INTEGER, departure_airport TEXT, intended_arrival_airport TEXT)")

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
            cur.execute("INSERT OR IGNORE INTO AprilFlights (callsign, airline_name, date, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["lastSeen"], flight["estDepartureAirport"], flight["estArrivalAirport"])) #apply category id here
            airline_list.append(flight)
            count += 1
        
    conn.commit()

def share_key(cur):
    cur.execute("SELECT totalFebFlights.callsign, totalAprilFlights.callsign, totalFebFlights.intended_arrival_airport, totalAprilFlights.intended_arrival_airport FROM totalFebFlights JOIN totalAprilFlights ON totalFebFlights.callsign = totalAprilFlights.callsign")
    rows = cur.fetchall()
    print(rows)

def setUptotalFebFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE totalFebFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS totalFebFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, date INTEGER, departure_airport TEXT, intended_arrival_airport TEXT)")

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
            cur.execute("INSERT OR IGNORE INTO totalFebFlights (callsign, airline_name, date, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["lastSeen"], flight["estDepartureAirport"], flight["estArrivalAirport"]))  
            airline_list.append(flight)
            count += 1

    conn.commit()

#Create the AprilFlights table within the database, include a column for callsign as primary key, airline_name, date, departure airport, and intended arrival airport
def setUptotalAprilFlightsTable(data, cur, conn):

    cur.execute("DROP TABLE totalAprilFlights")
    cur.execute("CREATE TABLE IF NOT EXISTS totalAprilFlights (callsign TEXT PRIMARY KEY, airline_name TEXT, date INTEGER, departure_airport TEXT, intended_arrival_airport TEXT)")

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
            cur.execute("INSERT OR IGNORE INTO totalAprilFlights (callsign, airline_name, date, departure_airport, intended_arrival_airport) VALUES (?, ?, ?, ?, ?)", (flight["callsign"], flight["callsign"][0:3], flight["lastSeen"], flight["estDepartureAirport"], flight["estArrivalAirport"])) #apply category id here
            airline_list.append(flight)
            count += 1
        
    conn.commit()
    conn.close()

def main():
    cur, conn = setUpDatabase('flights.db')

    feb_json_data = ReadDataFromApi("1581098400","1581703200")
    setUpFebFlightsTable(feb_json_data, cur, conn)

    apr_json_data = ReadDataFromApi("1586278800", "1586883600")  
    setUpAprilFlightsTable(apr_json_data, cur, conn)

    share_key(cur)

if __name__ == "__main__":
    main()
