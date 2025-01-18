import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd
app = Flask(__name__) 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

@app.route('/')
@app.route('/homepage/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()
    
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()
    
def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trips = get_trips_id(trip_id, conn)
    return trips.to_json()

def get_trips_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

@app.route('/trips/add', methods=['POST'], endpoint='add_trip') 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

@app.route('/trips/average_duration') 
def route_trips_bike():
    conn = make_connection()
    result = calc_trips_bike(conn)
    return result.to_json()

def calc_trips_bike(conn):
    query="""
    select a.*
    from
        (select tr.bikeid
                ,sum(tr.duration_minutes) sum_duration_mins
                ,count(tr.id) total_trip
                ,avg(tr.duration_minutes) avg_duration_mins
        from trips tr
        group by tr.bikeid) a
    order by a.sum_duration_mins desc, a.total_trip desc, a.bikeid asc
            """
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/trips/average_duration/<bike_id>')
    # code here
def route_trips_bikeid(bike_id):
    conn = make_connection()
    result = calc_trips_bikeid(bike_id, conn)
    return result.to_json()

def calc_trips_bikeid(bike_id, conn):
    query=f"""
    select a.*
    from
        (select  tr.bikeid
                ,sum(tr.duration_minutes) sum_duration_mins
                ,count(tr.id) total_trip
                ,avg(tr.duration_minutes) avg_duration_mins
        from trips tr
        where tr.bikeid={bike_id}
        group by tr.bikeid) a
    order by a.sum_duration_mins desc, a.total_trip desc, a.bikeid asc
            """
    result = pd.read_sql_query(query, conn)
    return result    

@app.route('/trips/average_duration/<bike_id>')
def route_stationId_duration():
    req = request.get_json(force=True)  # Parse the incoming JSON data as Dictionary
    station_id = req['station_id']
    endstation_id = req['endstation_id']
    period = req['period']
        
    conn = make_connection()
    result_period = calc_stationId_specifiedtime_trips(station_id, endstation_id, period, conn)
    return result_period.to_json()

def calc_stationId_specifiedtime_trips(station_id, endstation_id, period, conn):
    query = f"""
        select start_station_id,
               end_station_id,
               sum(duration_minutes) duration_intime,
               count(id) total_trip_intime,
               avg(duration_minutes) avg_duration_intime
        from trips
        where start_station_id = {station_id}
          and end_station_id = {endstation_id}
          and start_time like ('{period}%')
    """
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/average_duration_station', methods=['POST']) 
def route_stationid_duration():
    req = request.get_json(force=True)  # Parse the incoming JSON data as Dictionary
    station_id = req['station_id']
    endstation_id = req['endstation_id']
    period = req['period']
        
    conn = make_connection()
    result_period = calc_stationid_specifiedtime_trips(station_id, endstation_id, period, conn)
    return result_period.to_json()

def calc_stationid_specifiedtime_trips(station_id, endstation_id, period, conn):
    query = f"""
        select start_station_id,
               end_station_id,
               sum(duration_minutes) duration_intime,
               count(id) total_trip_intime,
               avg(duration_minutes) avg_duration_intime
        from trips
        where start_station_id = {station_id}
          and end_station_id = {endstation_id}
          and start_time like ('{period}%')
    """
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)
