# -*- coding: utf-8 -*-
"""
Created on Sat Sep 17 01:54:59 2016

Simple server for Fuel PFM app

@author: gabrielcuendet
"""

import os
import pyrebase
import json
import base64
import struct
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import time


def read_fuel_from_json(filename):
    """ Read the fuel level (ENH_DASHBOARD_FUEL) and the date and time from
    JSON file
    """
    fuel = {}
    dtime = []
    gps = []

    with open(filename, 'r') as f:
        for row in f.readlines():
            try:
                json_dict = json.loads(row)

                if json_dict["fields"]:
                    if "ENH_DASHBOARD_FUEL" in json_dict["fields"].keys():
                        dtime.append(pd.to_datetime(json_dict["recorded_at"],
                                                    '%Y-%m-%dT%H:%M:%SZ'))
                        if json_dict["asset"] not in fuel.keys():
                            fuel[json_dict["asset"]] = []
                        fuel[json_dict["asset"]].append(float(base64.b64decode(json_dict["fields"]["ENH_DASHBOARD_FUEL"]["b64_value"])))
            except ValueError:
                print 'problem'
    # DEBUG PURPOSE
    # print fuel
    pd_fuel = pd.DataFrame(fuel, index=dtime)
    return pd_fuel


def connect_to_db(username, passwd):
    """ Connect to the Firebase database and returns a reference to the database
    service
    """
    config = {"apiKey": "AIzaSyAnFixYXiBCDMLPR-30d3syF-VLOgdGzAc",
              "authDomain": "fuelpfm.firebaseapp.com",
              "databaseURL": "https://fuelpfm.firebaseio.com",
              "storageBucket": "fuelpfm.appspot.com",
              }

    app = pyrebase.initialize_app(config)

    # Get a reference to the auth service
    auth = app.auth()
    user = auth.sign_in_with_email_and_password('gabriel.cuendet@gmail.com',
                                                'fire_AS3x!985')

    # Get a reference to the database service
    db = app.database()
    return db, user


def execute():
    """ """
    user = 'gabriel.cuendet@gmail.com'
    passwd = 'fire_AS3x!985'
    db, user = connect_to_db(user, passwd)


def parse_all_files(folder):
    list_files = os.listdir(folder)

    pd_fuel = read_fuel_from_json(os.path.join(folder, list_files[1]))
    print pd_fuel

    for file in list_files[2:]:
        print file
        new_fuel = read_fuel_from_json(os.path.join(folder, file))
        pd_fuel = pd.concat([pd_fuel, new_fuel])

    # DEBUG PURPOSE
    # print pd_fuel

    plt.plot(pd_fuel,'-x')
    plt.legend(pd_fuel.columns.values, loc='lower left')
    plt.title('ENH_DASHBOARD_FUEL')
    fig = plt.gcf()
    fig.set_size_inches(12, 8)
    plt.savefig('enh_dashboard_fuel.pdf')

    return pd_fuel


def process_fuel(pd_fuel, threshold=5.0):
    event = {}
    headers = pd_fuel.columns.values
    
    for i in pd_fuel.size:
        if pd_fuel[i]-pd_fuel[]


def populate_db(folder):
    """ Parse the datasets from AMAG and populate the databases accordingly """
    pd_fuel = parse_all_files(folder)
    process_fuel(pd_fuel)


if __name__ == "__main__":
    # execute()
    populate_db('/Users/gabrielcuendet/Documents/perso/source/python/HackZurich2016/Amag/json/')
