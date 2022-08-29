# -*- coding: utf-8 -*-
import datetime
from cryptography.fernet import Fernet
from forepaas.worker.connect import connect
from forepaas.worker.connector import bulk_insert
import pandas as pd

def getToday():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+"Z"

def addToDf(df, values):
    for x in range(len(df.keys())):
        df[list(df.keys())[x]].append(values[x])
    return df

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
    
def newFernetKey():
    return str(Fernet.generate_key()).split("'")[1]
    
def encryptValue(decryptedMessage, key):
    key = Fernet(key)
    return key.encrypt(decryptedMessage.encode()).decode()

def decryptValue(encryptedMessage, key):
    key = Fernet(key)
    return key.decrypt(encryptedMessage.encode()).decode()

def getDictSensorOriginsToMeasures(cn, source=None, idUsageCategory=None):
    if source==None:
        sensors = cn.query("SELECT sensor.id_sensor_origin, sensor_measure.id_sensor_measure FROM sensor INNER JOIN sensor_measure ON sensor.id_sensor=sensor_measure.id_sensor")
    else:
        sensors = cn.query("SELECT sensor.id_sensor_origin, sensor_measure.id_sensor_measure FROM source INNER JOIN sensor ON sensor.id_source=source.id_source INNER JOIN sensor_measure ON sensor.id_sensor=sensor_measure.id_sensor WHERE source.name='"+source+("'" if idUsageCategory is None else "' AND id_usage_category = '"+idUsageCategory+"'"))

    sensor_origins_to_measures = {}
    for x in range(len(sensors)):
        id_sensor_origin = sensors["id_sensor_origin"][x]
        id_sensor_measure = sensors["id_sensor_measure"][x]
        if id_sensor_origin!=None:
            if id_sensor_origin in sensor_origins_to_measures.keys():
                sensor_origins_to_measures[id_sensor_origin].append(id_sensor_measure)
            else:
                sensor_origins_to_measures[id_sensor_origin]=[id_sensor_measure]
    return sensor_origins_to_measures

def testPrint():
    print("testPrint")