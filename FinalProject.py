
import requests
import os
import json
import sqlite3
from opensky_api import OpenSkyApi
api = OpenSkyApi()

#Write a function so that the code obtained from the read in file in converted to JSON and returns a dictionary
def ReadDataFromApi():

    username = "joshmoll"
    password = "206finalproject"

    base_url = "@opensky-network.org/api/flights/departure?"
    new_url = "https://" + username + ":" + password + base_url + "airport=KJFK" + "&" + "begin=1580576400"+ "&" + "end=1580662800"    #feb 1-2flights
    converted_data = requests.get(new_url).json()
    print(converted_data)
    
#Connect to an sql server and create a cursor
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect("flights.db")
    cur = conn.cursor()
    return cur, conn

#Create the FebFlights table within the database, include a column for airline, departure airport, and intended arrival airport
def setUpFebFlightsTable(cur, conn):
    #cur.execute("CREATE TABLE IF NOT EXISTS FebFlights (airline TEXT, departure airport INTEGER, intended arrivial airport TEXT)") #code is invalid becuase I used the alter table below


   # cur.execute("ALTER TABLE FebFlights RENAME TO FebFlights_old6")
   # cur.execute("CREATE TABLE FebFlights (airline_code TEXT PRIMARY KEY, departure_airport TEXT, intended_arrival_airport TEXT)")

    airline_list = []
    count = 0
    for flight in data[0]:
        cur.execute("INSERT INTO FebFlights(airline_code, departure_airport, intended_arrival_airport VALUES (?, ?, ?)", (flight["callsign"], flight["estDepartureAirport"], flight["estArrivalAirport"]))
        airline_list.append(flight)
        count += 1
        if count == 20:
            break

    conn.commit()
    conn.close()
    

#def setUpAprilFlightsTable(cur, conn):

    #cur.execute("CREATE TABLE AprilFlights (airline_code TEXT PRIMARY KEY, departure_airport TEXT, intended_arrival_airport TEXT)")

 #   conn.commit()
  #  conn.close()
    
def main():
    ReadDataFromApi()
    cur, conn = setUpDatabase('flights.db')
    setUpFebFlightsTable(cur, conn)
    setUpAprilFlightsTable(cur, conn)

if __name__ == "__main__":
    main()


