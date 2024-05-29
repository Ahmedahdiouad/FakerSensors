
import sqlite3
import random
from datetime import datetime
import time


def calculate_availability(running_time, planned_production_time):
    """
    Calculate availability rate.

    Parameters:
        running_time (float): Total time the equipment was running (in the same units as planned_production_time).
        planned_production_time (float): Total planned production time (in the same units as running_time).

    Returns:
        float: Availability rate (0-100).
    """
    return (running_time / planned_production_time) * 100


def calculate_performance(ideal_cycle_time, total_count, running_time):
    """
    Calculate performance rate.

    Parameters:
        ideal_cycle_time (float): Ideal cycle time for production.
        total_count (float): Total count of units produced (including defective ones).
        running_time (float): Total time the equipment was running.

    Returns:
        float: Performance rate (0-100).
    """
    return (ideal_cycle_time * total_count / running_time) * 100


def calculate_quality(good_count, total_count):
    """
    Calculate quality rate.

    Parameters:
        good_count (float): Count of good or acceptable units produced.
        total_count (float): Total count of units produced (including defective ones).

    Returns:
        float: Quality rate (0-100).
    """
    return (good_count / total_count) * 100


def calculate_oee(availability, performance, quality):
    """
    Calculate Overall Equipment Effectiveness (OEE) given availability, performance, and quality.

    Parameters:
        availability (float): Availability rate (0-100).
        performance (float): Performance rate (0-100).
        quality (float): Quality rate (0-100).

    Returns:
        float: Overall Equipment Effectiveness (OEE) rate (0-100).
    """
    return (availability / 100) * (performance / 100) * (quality / 100) * 100


# Function to create the database tables
def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS machine_1 (
                        id INTEGER PRIMARY KEY,
                        running_time REAL,
                        planned_production_time REAL,
                        ideal_cycle_time REAL,
                        total_count INTEGER,
                        good_count INTEGER,
                        availability REAL,
                        performance REAL,
                        quality REAL,
                        Overall_Equipment_Effectiveness REAL,
                        timestamp TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS machine_2 (
                        id INTEGER PRIMARY KEY,
                        running_time REAL,
                        planned_production_time REAL,
                        ideal_cycle_time REAL,
                        total_count INTEGER,
                        good_count INTEGER,
                        availability REAL,
                        performance REAL,
                        quality REAL,
                        Overall_Equipment_Effectiveness  REAL,
                        timestamp TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS machine_3 (
                        id INTEGER PRIMARY KEY,
                        running_time REAL,
                        planned_production_time REAL,
                        ideal_cycle_time REAL,
                        total_count INTEGER,
                        good_count INTEGER,
                        availability REAL,
                        performance REAL,
                        quality REAL,
                        Overall_Equipment_Effectiveness  REAL,
                        timestamp TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS machine_4 (
                        id INTEGER PRIMARY KEY,
                        running_time REAL,
                        planned_production_time REAL,
                        ideal_cycle_time REAL,
                        total_count INTEGER,
                        good_count INTEGER,
                        availability REAL,
                        performance REAL,
                        quality REAL,
                        Overall_Equipment_Effectiveness  REAL,
                        timestamp TEXT)''')

# Function to insert data into the database
def insert_data(cursor, conn, table_name, data):
    cursor.execute(f'''INSERT INTO {table_name}
                    (running_time, planned_production_time, ideal_cycle_time, total_count, good_count,
                    availability, performance, quality, Overall_Equipment_Effectiveness , timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    

# Connect to the database
conn = sqlite3.connect('IoT.sqlite3')
print("Database connected successfully!")
cursor = conn.cursor()

# Create the tables if they don't exist
create_tables(cursor)

# Repeat the process every minute
while True:
    for machine in range(1, 5):
        print(f"Generating data for machine {machine}...")
        
        running_time = random.uniform(490, 500)
        planned_production_time = random.uniform(590, 600)
        ideal_cycle_time = random.uniform(5, 8)
        total_count = random.randint(57, 60)
        good_count = random.randint(50, total_count)

        availability = calculate_availability(running_time, planned_production_time)
        performance = calculate_performance(ideal_cycle_time, total_count, running_time)
        quality = calculate_quality(good_count, total_count)
        Overall_Equipment_Effectiveness  = calculate_oee(availability, performance, quality)
        
        # Current timestamp
        timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        # Insert data into the database
        table_name = f"machine_{machine}"
        data = (running_time, planned_production_time, ideal_cycle_time, total_count, good_count, 
                availability, performance, quality, Overall_Equipment_Effectiveness, timestamp)
        insert_data(cursor, conn, table_name, data)

    # Wait for 1 seconds before repeating
    time.sleep(1)

# Close the connection
conn.close()
