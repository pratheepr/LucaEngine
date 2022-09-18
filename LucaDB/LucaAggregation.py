import pandas as pd
from LucaDB import DBAccess as db

if __name__ == "__main__":
    DB_Location = '/Users/pratheepravysandirane/Downloads/cnrl_alerts_22oct.db'
    DbConnObj = db.CrtConnObject(DB_Location)

