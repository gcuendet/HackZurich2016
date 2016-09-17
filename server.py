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
import matplotlib.pyplot as plt
import pandas as pd
import csv


def read_fuel_from_json(filename):
    """ Read the fuel level (ENH_DASHBOARD_FUEL) and the date and time from
    JSON file
    """
    fuel = {}
    dtime = []
    gps = []  # Missing GPS data in AMAG dataset! -> Get that from ContoVista

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


def read_contovista_from_csv(filename):
    dict_financial = []
    with open(filename, 'rU') as f:  
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            dict_financial.append(row)

    return dict_financial


def parse_all_fuel_files(folder):
    list_files = os.listdir(folder)

    pd_fuel = read_fuel_from_json(os.path.join(folder, list_files[1]))

    for file in list_files[2:]:
        print file
        new_fuel = read_fuel_from_json(os.path.join(folder, file))
        pd_fuel = pd.concat([pd_fuel, new_fuel])

    # DEBUG PURPOSE
    # print pd_fuel

    plt.plot(pd_fuel, '-x')
    plt.legend(pd_fuel.columns.values, loc='lower left')
    plt.title('ENH_DASHBOARD_FUEL')
    fig = plt.gcf()
    fig.set_size_inches(12, 8)
    plt.savefig('enh_dashboard_fuel.pdf')

    return pd_fuel


def process_offline(pd_fuel, dict_financial):
    #---PARAMETER----.
    threshold = 5.0 #| in liters, corresponds to minimum refuel volume
    #----------------.
    events = {}
    headers = pd_fuel.columns.values

    for header in headers:
        events[header] = []
        pd_fuel_nonNaN = pd_fuel[header].dropna()
        for i, el in enumerate(pd_fuel_nonNaN):
            diff_lt = el - pd_fuel_nonNaN[max(i-1, 0)]
            if pd_fuel_nonNaN[i] - pd_fuel_nonNaN[i-1] > threshold:
                events[header].append({'timestamp': str(pd_fuel_nonNaN.index[i]),
                                       'vehicle_data': {'gps_lat': None,
                                                        'gps_long': None,
                                                        'lt_pump': str(diff_lt),
                                                        'vehicle_id': header}})    
    
    # To fuse AMAG <-> ContoVista, you could compare GPS position in addition
    # to timestamp
    fin_events = {}
    for row in dict_financial:
        if row['ACCOUNT_ID'] not in fin_events.keys():
            fin_events[row['ACCOUNT_ID']] = []
        fin_events[row['ACCOUNT_ID']].append({row['TRANSACTION_DATE']: {'pfm_data': {'account_id': row['ACCOUNT_ID'],
                                                                  'amount': row['AMOUNT'],
                                                                  'city': row['CITY'],
                                                                  'counterparty': row['COUNTERPARTY'],
                                                                  'country': row['COUNTRY'],
                                                                  'currency': row['CURRENCY'],
                                                                  'gps_lat': row['GEO_LATITUDE'],
                                                                  'gps_long': row['GEO_LONGITUDE'],
                                                                  'street': row['STREET'],
                                                                  'timestamp': row['TRANSACTION_DATE'],
                                                                  'transaction_type': row['TRANSACTION_TYPE'],
                                                                  'zip': row['ZIP']}}})
        
    return events, fin_events


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


def populate_db(amag_folder, cv_filename):
    """ Parse the datasets from AMAG and ContoVista and populate the databases
    accordingly """
    dict_financial = read_contovista_from_csv(cv_filename)
    pd_fuel = parse_all_fuel_files(amag_folder)
    amag, cv = process_offline(pd_fuel, dict_financial)

    username = 'gabriel.cuendet@gmail.com'
    passwd = 'fire1234'
    db, user = connect_to_db(username, passwd)        

    for vehicle in amag.keys():
        usr = db.child('vehicle_id').child(vehicle).get()
        db.child('data2').child(usr.val()).set(amag[vehicle])

    for account in cv.keys():
        usr = db.child('account_id').child(account).get()
        dates = db.child('data2').child(usr.val()).get()
        for i, amag_date in enumerate(dates.val()):
            for cv_date in cv[account]:
                # If you wanted to match GPS as well.... that would be here!
                if cv_date.keys()[0] in amag_date['timestamp']:
                    db.child('data2').child(usr.val()).child(str(i)).update(cv_date[cv_date.keys()[0]])


if __name__ == "__main__":
    populate_db('/Users/gabrielcuendet/Documents/perso/source/python/HackZurich2016/Amag/json/',
                '/Users/gabrielcuendet/Documents/perso/source/python/HackZurich2016/ContoVista/PFM_TRANSACTION_GAS.csv')
