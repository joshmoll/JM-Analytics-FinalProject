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
    conn = sqlite3.connect("final_project.db")
    cur = conn.cursor()
    return cur, conn

def case_info_setup(case_data, cur, conn):
    #cur.execute('DROP TABLE case_info')
    cur.execute("CREATE TABLE IF NOT EXISTS case_info (Date TEXT PRIMARY KEY, February TEXT NULL, April TEXT)")
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

def april_dates(cur, conn):
    cur.execute('SELECT Date FROM case_info limit 37, 44')
    april_dates = cur.fetchall()
    

def april_cases(cur, conn):
    cur.execute('SELECT April FROM case_info limit 37, 44')
    april_cases = cur.fetchall()


def main():
    cur, conn = database_setup("final_project.db")
    NY_json_data = API_data_retrieval()
    case_info_setup(NY_json_data, cur, conn)
    april_dates(cur, conn)
    april_cases(cur, conn)


if __name__ == "__main__":
    main()