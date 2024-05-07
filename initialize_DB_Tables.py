#------------------------------------------
#--- Python Ver: 3.12.3
#------------------------------------------
import sqlite3

# SQLite DB Name
DB_Name =  "IoT.sqlite3"

# SQLite DB Table Schema
TableSchema="""
drop table if exists A_Data ;
create table A_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  A text
);


drop table if exists B_Data ;
create table B_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  B text
);
drop table if exists X_Data ;
create table X_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  X text
);

drop table if exists Y_Data ;
create table Y_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Y text
);

drop table if exists Z_Data ;
create table Z_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  Z text
);

drop table if exists W_Data ;
create table W_Data (
  id integer primary key autoincrement,
  SensorID text,
  Date_n_Time text,
  W text
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
