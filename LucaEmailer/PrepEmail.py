import sqlite3
from datetime import datetime, date
import io
import numpy as np
import matplotlib.dates as dates

def CrtConnObject():
    sqliteConnection = 0
    # file_name = '"{}"'.format(file_name)
    try:
        sqliteConnection = sqlite3.connect('mail_metrics.db', detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = sqliteConnection.cursor()
        return sqliteConnection
    except sqlite3.Error as ERROR:
        print("Error while working with SQLite", ERROR)

    return  sqliteConnection

def Data_Retrival(ConnObj):
    list_x = []
    list_y = []

    try:
        CursorObj = ConnObj.cursor()
        sqlite_flowLvl_insert_param_1 = " SELECT EMAIL_ID from RECEPIENTS"
        sqlite_flowLvl_insert_param_2 = " SELECT MESSAGE from RECEPIENTS"

        CursorObj.execute(sqlite_flowLvl_insert_param_1)
        mail_ids = CursorObj.fetchall()
        CursorObj.execute(sqlite_flowLvl_insert_param_2)
        messages = CursorObj.fetchall()
        # print(dates)
        for mail,msg in zip(mail_ids, messages):
            list_x.append(mail[0])
            list_y.append(msg[0])


        CursorObj.close()
        # print(list_y)

    except sqlite3.Error as error:
        print("Error while working with SQLite", error)
    # print("list value is ", list_x[0])
    # print("list y value is ", str(list_y[0]))

    return list_x,list_y     #  returns only time list

# conn_obj = CrtConnObject()
# list_x = addFlowLevel(conn_obj)