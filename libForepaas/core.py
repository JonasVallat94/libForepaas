# -*- coding: utf-8 -*-
from . import helpers
import datetime
from cryptography.fernet import Fernet
#from forepaas.worker.connect import connect
#from forepaas.worker.connector import bulk_insert
import pandas as pd

def getToday():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"

def addToDf(df, values):
    for x in range(len(df.keys())):
        df[list(df.keys())[x]].append(values[x])
    return df

def decryptValue(encryptedMessage, key):
    fernet = Fernet(key)
    return fernet.decrypt(encryptedMessage.encode()).decode()

def insertDataIntoTable(df, tableName, connectPath=None):
    if len(df[list(df.keys())[0]])>0:
        connectPath = "dwh/data_prim/"+tableName if connectPath==None else connectPath
        destination = connect(connectPath)
        df = pd.DataFrame(df)
        stats, error = bulk_insert(destination, tableName, df)
        return True
    return False

def updateErrorReports(cn, source):
    cn.query("UPDATE report_workflow SET data_added='True' WHERE status='FAILED' AND source='"+source+"' AND data_added='False'")
    
def testPrint():
    print("testPrint")