#------------------------------------------
#--- Date:  09/04/2024
#--- Python Ver: 3.12.3
#------------------------------------------
import json
import sqlite3

# SQLite DB Name
DB_Name =  "IoT.sqlite3"

# Database Manager Class
class DatabaseManager():
    def __init__(self):
        print("Initializing DatabaseManager...")
        self.conn = sqlite3.connect(DB_Name)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()
        
    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()

# Function to save Temperature to DB Table
def Temperature_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Temperature_Data (SensorID, Date_n_Time, Temperature) VALUES (?,?,?)", [SensorID, Data_and_Time, Temperature])
    del dbObj
    print("Inserted Temperature Data into Database.")
    print("")

# Function to save Humidity to DB Table
def Humidity_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Humidity_Data (SensorID, Date_n_Time, Humidity) VALUES (?,?,?)", [SensorID, Data_and_Time, Humidity])
    del dbObj
    print("Inserted Humidity Data into Database.")
    print("")

# Function to save Flow Rate to DB Table
def Flow_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Flow = json_Dict['Flow']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Flow_Data (SensorID, Date_n_Time, Flow) VALUES (?,?,?)", [SensorID, Data_and_Time, Flow])
    del dbObj
    print("Inserted Flow Data into Database.")
    print("")

# Function to save Position to DB Table
def Position_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Position = json_Dict['Position']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Position_Data (SensorID, Date_n_Time, Position) VALUES (?,?,?)", [SensorID, Data_and_Time, Position])
    del dbObj
    print("Inserted Position Data into Database.")
    print("")

# Function to save PaintLevel to DB Table
def PaintLevel_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    PaintLevel = json_Dict['PaintLevel']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO PaintLevel_Data (SensorID, Date_n_Time, PaintLevel) VALUES (?,?,?)", [SensorID, Data_and_Time, PaintLevel])
    del dbObj
    print("Inserted PaintLevel Data into Database.")
    print("")

# Function to save SurfaceQuality to DB Table
def SurfaceQuality_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    SurfaceQuality = json_Dict['SurfaceQuality']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO SurfaceQuality_Data (SensorID, Date_n_Time, SurfaceQuality) VALUES (?,?,?)", [SensorID, Data_and_Time, SurfaceQuality])
    del dbObj
    print("Inserted SurfaceQuality Data into Database.")
    print("")

# Master Function to Select DB Function based on MQTT Topic
def sensor_Data_Handler(Topic, jsonData):
    if Topic == "Factory/Machine1/1/Temperature":
        Temperature_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/2/Humidity":
        Humidity_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/3/Flow":
        Flow_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/4/Position":
        Position_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/5/PaintLevel":
        PaintLevel_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/6/SurfaceQuality":
        SurfaceQuality_Data_Handler(jsonData)




