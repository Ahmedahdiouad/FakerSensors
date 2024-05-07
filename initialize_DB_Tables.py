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
drop table if exists Position_Data ;
create table Position_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Position text
);

drop table if exists PaintLevel_Data ;
create table PaintLevel_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  PaintLevel text
);

drop table if exists SurfaceQuality_Data ;
create table SurfaceQuality_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  SurfaceQuality text
);

drop table if exists Flow_Data ;
create table Flow_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Flow text
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
