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
    def identify_resource(self, topic, jsonDict):
        machine = "Unknown"
        if "Machine1" in topic:
            machine = "Machine1"
        elif "Machine2" in topic:
            machine = "Machine2"
        # Add more machines as needed

        if 'Temperature' in jsonDict:
            return f"Temperature_{machine}"
        elif 'Humidity' in jsonDict:
            return f"Humidity_{machine}"
        elif 'Flow' in jsonDict:
            return f"Flow_{machine}"
        elif 'Position' in jsonDict:
            return f"Position_{machine}"
        elif 'PaintLevel' in jsonDict:
            return f"PaintLevel_{machine}"
        elif 'SurfaceQuality' in jsonDict:
            return f"SurfaceQuality_{machine}"
        elif 'debit' in jsonDict:
            return f"debit_{machine}"
        else:
            return f"Unknown_{machine}"

    def register_resource(self, resource):
        # Enregistrement de la ressource dans la base de données
        logging.info(f"Resource {resource} registered.")
        try:
            with DatabaseManager() as db:
                db.add_del_update_db_record("INSERT INTO Resources (ResourceID) VALUES (?)", (resource,))
        except Exception as e:
            logging.error(f"Error registering resource: {e}")

# Event Manager Class
class EventManager:
    def __init__(self):
        logging.info("Initializing EventManager...")

    def route_event(self, topic, jsonData):
        if topic == "Factory/Machine1/event/Temperature":
            self.store_temperature_event(jsonData)
        elif topic == "Factory/Machine1/prossice/Temperature":
            self.process_temperature_event(jsonData)
        elif topic == "Factory/Machine1/event/Humidity":
            self.store_humidity_event(jsonData)
        elif topic == "Factory/Machine1/prossice/Humidity":
            self.process_humidity_event(jsonData)
        elif topic == "Factory/Machine1/event/Flow":
            self.store_flow_event(jsonData)
        elif topic == "Factory/Machine1/prossice/Flow":
            self.process_flow_event(jsonData)
        elif topic == "Factory/Machine1/event/Position":
            self.store_position_event(jsonData)
        elif topic == "Factory/Machine1/prossice/Position":
            self.process_position_event(jsonData)
        elif topic == "Factory/Machine1/event/PaintLevel":
            self.store_paint_level_event(jsonData)
        elif topic == "Factory/Machine1/prossice/PaintLevel":
            self.process_paint_level_event(jsonData)
        elif topic == "Factory/Machine1/event/SurfaceQuality":
            self.store_surface_quality_event(jsonData)
        elif topic == "Factory/Machine1/prossice/SurfaceQuality":
            self.process_surface_quality_event(jsonData)
        else:
            logging.warning(f"Unhandled topic: {topic}")

    def store_temperature_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Temperature = json_Dict['Temperature']

        try:
            with DatabaseManager() as db:
                # Insert into Temperature_Data table
                db.add_del_update_db_record("INSERT INTO Temperature_Data (SensorID, Date_n_Time, Temperature) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, Temperature])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'Temperature', Temperature])
            logging.info("Inserted Temperature Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting Temperature data: {e}")

    def process_temperature_event(self, jsonData):
        logging.info("Processing temperature data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Temperature = json_Dict['Temperature']
        
        # Filtering step: Filter out any invalid data (e.g., temperature out of expected range)
        if not (Temperature < -50 or Temperature > 150):  # Example range check
            # Processing step: For demonstration, we'll convert Celsius to Fahrenheit
            if 'Celsius' in json_Dict:
                Temperature = Temperature * 9/5 + 32
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)

            
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating temperature data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO Temperature_Data (SensorID, Date_n_Time, Temperature) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, Temperature])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed Temperature', Temperature])
                logging.info("Inserted Processed Temperature Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed Temperature data: {e}")
        else:
            logging.warning(f"Filtered out invalid temperature data: {Temperature}")

    def store_humidity_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Humidity = json_Dict['Humidity']

        try:
            with DatabaseManager() as db:
                # Insert into Humidity_Data table
                db.add_del_update_db_record("INSERT INTO Humidity_Data (SensorID, Date_n_Time, Humidity) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, Humidity])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'Humidity', Humidity])
            logging.info("Inserted Humidity Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting Humidity data: {e}")

    def process_humidity_event(self, jsonData):
        logging.info("Processing humidity data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Humidity = json_Dict['Humidity']
        
        # Filtering step: Filter out any invalid data (e.g., humidity out of expected range)
        if not (Humidity < 0 or Humidity > 100):  # Example range check
            # Processing step: Placeholder for any processing logic
            processed_humidity = Humidity
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating humidity data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO Humidity_Data (SensorID, Date_n_Time, Humidity) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, processed_humidity])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed Humidity', processed_humidity])
                logging.info("Inserted Processed Humidity Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed Humidity data: {e}")
        else:
            logging.warning(f"Filtered out invalid humidity data: {Humidity}")

    def store_flow_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Flow = json_Dict['Flow']

        try:
            with DatabaseManager() as db:
                # Insert into Flow_Data table
                db.add_del_update_db_record("INSERT INTO Flow_Data (SensorID, Date_n_Time, Flow) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, Flow])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'Flow', Flow])
            logging.info("Inserted Flow Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting Flow data: {e}")

    def process_flow_event(self, jsonData):
        logging.info("Processing flow data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Flow = json_Dict['Flow']
        
        # Filtering step: Filter out any invalid data
        if Flow >= 0:  # Example check
            # Processing step: Placeholder for any processing logic
            processed_flow = Flow
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating flow data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO Flow_Data (SensorID, Date_n_Time, Flow) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, processed_flow])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed Flow', processed_flow])
                logging.info("Inserted Processed Flow Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed Flow data: {e}")
        else:
            logging.warning(f"Filtered out invalid flow data: {Flow}")

    def store_position_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Position = json_Dict['Position']

        try:
            with DatabaseManager() as db:
                # Insert into Position_Data table
                db.add_del_update_db_record("INSERT INTO Position_Data (SensorID, Date_n_Time, Position) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, Position])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'Position', Position])
            logging.info("Inserted Position Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting Position data: {e}")

    def process_position_event(self, jsonData):
        logging.info("Processing position data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        Position = json_Dict['Position']
        
        # Filtering step: Filter out any invalid data
        if Position >= 0:  # Example check
            # Processing step: Placeholder for any processing logic
            processed_position = Position
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating position data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO Position_Data (SensorID, Date_n_Time, Position) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, processed_position])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed Position', processed_position])
                logging.info("Inserted Processed Position Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed Position data: {e}")
        else:
            logging.warning(f"Filtered out invalid position data: {Position}")

    def store_paint_level_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        PaintLevel = json_Dict['PaintLevel']

        try:
            with DatabaseManager() as db:
                # Insert into PaintLevel_Data table
                db.add_del_update_db_record("INSERT INTO PaintLevel_Data (SensorID, Date_n_Time, PaintLevel) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, PaintLevel])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'PaintLevel', PaintLevel])
            logging.info("Inserted PaintLevel Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting PaintLevel data: {e}")

    def process_paint_level_event(self, jsonData):
        logging.info("Processing paint level data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        PaintLevel = json_Dict['PaintLevel']
        
        # Filtering step: Filter out any invalid data
        if PaintLevel >= 0:  # Example check
            # Processing step: Placeholder for any processing logic
            processed_paint_level = PaintLevel
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating paint level data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO PaintLevel_Data (SensorID, Date_n_Time, PaintLevel) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, processed_paint_level])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed PaintLevel', processed_paint_level])
                logging.info("Inserted Processed PaintLevel Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed PaintLevel data: {e}")
        else:
            logging.warning(f"Filtered out invalid paint level data: {PaintLevel}")

    def store_surface_quality_event(self, jsonData):
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        SurfaceQuality = json_Dict['SurfaceQuality']

        try:
            with DatabaseManager() as db:
                # Insert into SurfaceQuality_Data table
                db.add_del_update_db_record("INSERT INTO SurfaceQuality_Data (SensorID, Date_n_Time, SurfaceQuality) VALUES (?,?,?)",
                                            [SensorID, Date_and_Time, SurfaceQuality])
                # Insert into Events table
                db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                            [SensorID, Date_and_Time, 'SurfaceQuality', SurfaceQuality])
            logging.info("Inserted SurfaceQuality Data into Database and recorded event.")
        except Exception as e:
            logging.error(f"Error inserting SurfaceQuality data: {e}")

    def process_surface_quality_event(self, jsonData):
        logging.info("Processing surface quality data...")
        
        # Parse the JSON data
        json_Dict = json.loads(jsonData)
        SensorID = json_Dict['Sensor_ID']
        Date_and_Time = json_Dict['Date']
        SurfaceQuality = json_Dict['SurfaceQuality']
        
        # Filtering step: Filter out any invalid data
        if SurfaceQuality >= 0:  # Example check
            # Processing step: Placeholder for any processing logic
            processed_surface_quality = SurfaceQuality
            
            # Aggregation step: Example of aggregating data over a period (this is simplified)
            # Here, we're just logging the aggregation as a placeholder for actual implementation
            logging.info(f"Aggregating surface quality data for SensorID {SensorID}")

            # Collection step: Collect the processed data into the database
            try:
                with DatabaseManager() as db:
                    db.add_del_update_db_record("INSERT INTO SurfaceQuality_Data (SensorID, Date_n_Time, SurfaceQuality) VALUES (?,?,?)",
                                                [SensorID, Date_and_Time, processed_surface_quality])
                    # Insert into Events table
                    db.add_del_update_db_record("INSERT INTO Events (SensorID, Date_n_Time, EventType, Value) VALUES (?,?,?,?)",
                                                [SensorID, Date_and_Time, 'Processed SurfaceQuality', processed_surface_quality])
                logging.info("Inserted Processed SurfaceQuality Data into Database and recorded event.")
            except Exception as e:
                logging.error(f"Error inserting processed SurfaceQuality data: {e}")
        else:
            logging.warning(f"Filtered out invalid surface quality data: {SurfaceQuality}")

# Master Function to Select DB Function based on MQTT Topic
def sensor_Data_Handler(Topic, jsonData):
    resource_manager = ResourceManager()
    json_Dict = json.loads(jsonData)
    resource_id = resource_manager.identify_resource(Topic, json_Dict)
    resource_manager.register_resource(resource_id)
    event_manager = EventManager()
    event_manager.route_event(Topic, jsonData)
