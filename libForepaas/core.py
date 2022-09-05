import datetime
from cryptography.fernet import Fernet
from forepaas.worker.connect import connect
from forepaas.worker.connector import bulk_insert
import pandas as pd
import os

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

#Returns a dict with all sensor.id_sensor_origin as keys and a list of the corresponding sensor_measure.id_sensor_measure as values
#source and idUsageCategory are optinals
#source          - only the sensors linked to the corresponding source.name will be returned
#idUsageCategory - only the sensor_measures with the same sensor_measure.id_usage_category will be returned
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

def sendAndResetReportDF(df=None):
    if df!=None:
        insertDataIntoTable(df, "report_sensor_measure")
    df = {"id" : [], "source" : [], "id_sensor_measure" : [], "nb_inputs" : [], "nb_actions" : [], "retrieval_date" : [], "lastupdate" : []}
    return df

def addToSensorDataDict(sensorData, id_sensor_measure, id_usage_category, value=1):
    key = id_sensor_measure + "-" + id_usage_category
    if key in sensorData.keys():
        sensorData[key] += value
    else:
        sensorData[key] = value
    return sensorData

def reportDataToDF(sensorData, source, xDaysAgo, df, nb_inputs, timestamp):
    for sensor in sensorData.keys():
        id_sensor_measure = sensor.split("-")[0]
        id_usage_category = sensor.split("-")[1]
        nb_actions = sensorData[sensor]
        id = source + "_" + id_sensor_measure + "_" + xDaysAgo
        
        df = addToDf(df, [id, source, id_sensor_measure, nb_inputs, nb_actions, xDaysAgo, timestamp])
    return df
    
#Get id_usage_category of a given id_sensor_measure. Keep track of already done queries
def getUsageCategoryFromSensorMeasure(cn, sensorToCatDict, id_sensor_measure):
    if id_sensor_measure in sensorToCatDict.keys():
        id_usage_category = sensorToCatDict[id_sensor_measure]
    else:
        id_usage_category = cn.query("SELECT * FROM sensor_measure WHERE id_sensor_measure= '"+ id_sensor_measure +"'")["id_usage_category"][0]
        sensorToCatDict[id_sensor_measure] = id_usage_category
    return sensorToCatDict, id_usage_category

    sensorData = getDefaultReportValues(sensorData, "technics")

#If sensorData is empty, get default values (load_report_sensor_measure)
def getDefaultReportValues(cn, sensorData, tableName):
    if len(sensorData.keys())==0:
        id_sensor_measure = cn.query("SELECT * FROM " + tableName + " ORDER BY lastupdate DESC LIMIT 1")["id_sensor_measure"]
        if len(id_sensor_measure)>0:
            id_sensor_measure = id_sensor_measure[0]
            id_usage_category = cn.query("SELECT * FROM sensor_measure WHERE id_sensor_measure= '"+ id_sensor_measure +"'")["id_usage_category"][0]
        else:
            id_sensor_measure = "None"
            id_usage_category = "None"
        sensorData[id_sensor_measure+"-"+id_usage_category] = 0
    return sensorData

#(re)initialise values for the script(s) load_report_sensor_measure
def initReportValues():
    nbDays      = int(os.getenv('DAYS_RANGE'))
    threshold   = int(os.getenv('THRESHOLD'))

    timestamp = getToday()
    xDaysAgo =  (datetime.datetime.now() - datetime.timedelta(days = nbDays)).strftime('%Y-%m-%d')
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    dfReport = sendAndResetReportDF()
    cn = connect("dwh/data_prim/")
    sensorData={}

    return nbDays, threshold, timestamp, xDaysAgo, today, dfReport, cn, sensorData

#Extract data from a table to later insert it into report_sensor_measure
def getDataForReportSensorMeasure(cn, tableName, today, xDaysAgo, sensorData):
    data = cn.query("SELECT * FROM " + tableName + " WHERE lastupdate >'" + xDaysAgo + "' AND lastupdate <'" + today + "'")
    nb_inputs = len(data)
    sensorToCatDict={}
    for x in range(0, nb_inputs):
        id_sensor_measure = data["id_sensor_measure"][x]
        sensorToCatDict, id_usage_category = getUsageCategoryFromSensorMeasure(cn, sensorToCatDict, id_sensor_measure)
        
        sensorData = addToSensorDataDict(sensorData, id_sensor_measure, id_usage_category)
        
    return sensorData, nb_inputs

#full process of extracting and treating data from a table and insert it into report_sensor_measure
def reportSensorMeasureRegularProcess(source, tableName):
    #Initialise values
    nbDays, threshold, timestamp, xDaysAgo, today, dfReport, cn, sensorData = initReportValues()
    
    #Get data
    sensorData, nb_inputs = getDataForReportSensorMeasure(cn, tableName, today, xDaysAgo, sensorData)

    #Default values if no data was found
    sensorData = getDefaultReportValues(cn, sensorData, tableName)
        
    #Store data
    dfReport = reportDataToDF(sensorData, source, xDaysAgo, dfReport, nb_inputs, timestamp)

    #insert data
    sendAndResetReportDF(dfReport)
    
def testPrint():
    print("testPrint")