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
        print("connect to db")
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
# Function to save Temperature to DB Table
def A_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    A = json_Dict['A']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO A_Data (SensorID, Date_n_Time, A) VALUES (?,?,?)", [SensorID, Data_and_Time, A])
    del dbObj
    print("Inserted A Data into Database.")
    print("")

# Function to save Humidity to DB Table
def B_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    B = json_Dict['B']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO B_Data (SensorID, Date_n_Time, B) VALUES (?,?,?)", [SensorID, Data_and_Time, B])
    del dbObj
    print("Inserted B Data into Database.")
    print("")

# Function to save Flow Rate to DB Table
def W_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    W = json_Dict['W']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO W_Data (SensorID, Date_n_Time, W) VALUES (?,?,?)", [SensorID, Data_and_Time, W])
    del dbObj
    print("Inserted W Data into Database.")
    print("")

# Function to save X to DB Table
def X_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    X = json_Dict['X']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO X_Data (SensorID, Date_n_Time, X) VALUES (?,?,?)", [SensorID, Data_and_Time, X])
    del dbObj
    print("Inserted X Data into Database.")
    print("")

# Function to save Y to DB Table
def Y_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Y = json_Dict['Y']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Y_Data (SensorID, Date_n_Time, Y) VALUES (?,?,?)", [SensorID, Data_and_Time, Y])
    del dbObj
    print("Inserted Y Data into Database.")
    print("")

# Function to save Z to DB Table
def Z_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Z = json_Dict['Z']
    
    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO Z_Data (SensorID, Date_n_Time, Z) VALUES (?,?,?)", [SensorID, Data_and_Time, Z])
    del dbObj
    print("Inserted Z Data into Database.")
    print("")

# Master Function to Select DB Function based on MQTT Topic
def sensor_Data_Handler(Topic, jsonData):
    print("im being called")
    if Topic == "Factory/Machine1/1/A":
        A_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/2/B":
        B_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/3/W":
        W_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/4/X":
        X_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/5/Y":
        Y_Data_Handler(jsonData)
    elif Topic == "Factory/Machine1/6/Z":
        Z_Data_Handler(jsonData)
