import pandas as pd
from LucaDB import DBAccess as db

if __name__ == "__main__":
    DB_Location = '/Users/pratheepravysandirane/Downloads/cnrl_alerts_22oct.db'
    SqliteConnObj = db.CrtConnObject(DB_Location)
    rcd = db.Luca_Aggregate_by_opctag(SqliteConnObj)
    print(rcd)

    PGConnObj = db.CrtPGConnObject()
    ret = db.move_aggr_to_postgres(PGConnObj, rcd)