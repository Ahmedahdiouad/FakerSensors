import json
import sqlite3
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite DB Name
DB_Name = "IoT.sqlite3"

# Database Manager Class
class DatabaseManager:
    def __init__(self):
        logging.info("Initializing DatabaseManager...")
        self.conn = sqlite3.connect(DB_Name)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        try:
            self.cur.execute(sql_query, args)
            self.conn.commit()
            logging.info("Database operation successful.")
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            self.conn.rollback()

    def fetch_records(self, sql_query, args=()):
        try:
            self.cur.execute(sql_query, args)
            rows = self.cur.fetchall()
            return rows
        except sqlite3.Error as e:
            logging.error(f"Database fetch error: {e}")
            return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            logging.error(f"Exception caught: {exc_type}, {exc_val}")
        self.cur.close()
        self.conn.close()

# Resource Management Class
class ResourceManager:
    def identify_resource(self, jsonDict):
        return jsonDict.get('SensorID ', 'Unknown')

    def register_resource(self, resource):
        # Enregistrement de la ressource dans la base de données
        logging.info(f"Resource {resource} registered.")
        try:
            with DatabaseManager() as db:
                db.add_del_update_db_record("INSERT INTO Resources (ResourceID) VALUES (?)", (resource,))
        except Exception as e:
            logging.error(f"Error registering resource: {e}")

# Event Management Class
class EventManager:
    def handle_event(self, event):
        # Journalisation des événements dans une table de la base de données
        logging.info(f"Event {event} handled.")
        try:
            with DatabaseManager() as db:
                db.add_del_update_db_record("INSERT INTO Events (Event) VALUES (?)", (event,))
        except Exception as e:
            logging.error(f"Error handling event: {e}")

# Data Management Class
class DataManager:
    def store_data(self, table, data):
        # Stockage des données brutes dans une table générique
        logging.info(f"Data {data} stored in {table}.")
        try:
            with DatabaseManager() as db:
                db.add_del_update_db_record(f"INSERT INTO {table} (RawData) VALUES (?)", (data,))
        except Exception as e:
            logging.error(f"Error storing data: {e}")

# Recovery Management Class
class RecoveryManager:
    def recover_data(self):
        # Logique de récupération des données
        logging.info("Data recovery process started.")
        try:
            with DatabaseManager() as db:
                # récupération des données
                rows = db.fetch_records("SELECT * FROM BackupData")
                for row in rows:
                    logging.info(f"Recovered data: {row}")
        except Exception as e:
            logging.error(f"Error recovering data: {e}")

# Function to save Temperature to DB Table
def Temperature_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO Temperature_Data (SensorID, Date_n_Time, Temperature) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, Temperature])
        logging.info("Inserted Temperature Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting Temperature data: {e}")

# Function to save Humidity to DB Table
def Humidity_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO Humidity_Data (SensorID, Date_n_Time, Humidity) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, Humidity])
        logging.info("Inserted Humidity Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting Humidity data: {e}")

# Function to save Flow Rate to DB Table
def Flow_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    Flow = json_Dict['Flow']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO Flow_Data (SensorID, Date_n_Time, Flow) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, Flow])
        logging.info("Inserted Flow Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting Flow data: {e}")

# Function to save Position to DB Table
def Position_Data_Handler(jsonData):
    # Parse Data 
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    Position = json_Dict['Position']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO Position_Data (SensorID, Date_n_Time, Position) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, Position])
        logging.info("Inserted Position Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting Position data: {e}")

# Function to save PaintLevel to DB Table
def PaintLevel_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    PaintLevel = json_Dict['PaintLevel']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO PaintLevel_Data (SensorID, Date_n_Time, PaintLevel) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, PaintLevel])
        logging.info("Inserted PaintLevel Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting PaintLevel data: {e}")

# Function to save SurfaceQuality to DB Table
def SurfaceQuality_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Date_and_Time = json_Dict['Date']
    SurfaceQuality = json_Dict['SurfaceQuality']

    # Push into DB Table
    try:
        with DatabaseManager() as db:
            db.add_del_update_db_record("INSERT INTO SurfaceQuality_Data (SensorID, Date_n_Time, SurfaceQuality) VALUES (?,?,?)",
                                        [SensorID, Date_and_Time, SurfaceQuality])
        logging.info("Inserted SurfaceQuality Data into Database.")
    except Exception as e:
        logging.error(f"Error inserting SurfaceQuality data: {e}")

# Master Function to Select DB Function based on MQTT Topic
def sensor_Data_Handler(Topic, jsonData):
    resource_manager = ResourceManager()
    resource_id = resource_manager.identify_resource(json.loads(jsonData))
    resource_manager.register_resource(resource_id)
    event_manager = EventManager()
    event_manager.handle_event(Topic)
    data_manager = DataManager()
    data_manager.store_data("RawData", jsonData)  # Stocker les données brutes dans une table générique
    recovery_manager = RecoveryManager()
    recovery_manager.recover_data()

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

        