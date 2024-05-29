#------------------------------------------
#--- Python Ver: 3.12.3
#------------------------------------------
import sqlite3

# SQLite DB Name
DB_Name =  "IoT.sqlite3"

# SQLite DB Table Schema
TableSchema="""
drop table if exists Temperature_Data ;
create table Temperature_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Temperature text
);
drop table if exists Humidity_Data ;
create table Humidity_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Humidity text
);

create table if not exists Position_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Position text
);


create table if not exists PaintLevel_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  PaintLevel text
);


create table if not exists SurfaceQuality_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  SurfaceQuality text
);


create table if not exists Flow_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Flow text
);





create table if not exists Resources (
  id integer primary key autoincrement,
  ResourceID text
);


create table if not exists Events (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time,
  EventType text,
  Value text
);



"""

#Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

#Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

#Close DB
curs.close()
conn.close()
