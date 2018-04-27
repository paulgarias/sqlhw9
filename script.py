from flask import Flask,jsonify
from flask_restful import Resource, Api

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import dateutil.parser

# Create the connection engine
engine = create_engine("sqlite:///hawaii.sqlite")

# Create the inspector and connect it to the engine
inspector = inspect(engine)

# Collect the names of tables within the database
print(inspector.get_table_names())

# Declare a Base using `automap_base()`
Base = automap_base()

# Use the Base class to reflect the database tables
Base.prepare(engine, reflect=True)

# Print all of the classes mapped to the Base
print(Base.classes.keys())

# Assign the measurements class to a variable called `Measurements`
Measurements = Base.classes.measurements

# Assign the stations class to a variable called `Stations`
Stations = Base.classes.stations

# Create a session
session = Session(engine)

def leap_year(y):
    if y % 400 == 0:
        return True
    if y % 100 == 0:
        return False
    if y % 4 == 0:
        return True
    else:
        return False

def monthDiff(dt,numMonths):
    monthRing = [1,2,3,4,5,6,7,8,9,10,11,12]
    yearsBack = np.int(np.floor(numMonths/12))
    monthsBack = numMonths % 12
    # check for month
    monthCheck = monthRing[dt.month-(monthsBack+1)]
    if monthCheck in [1,3,5,7,8,10,12]:
        return datetime(dt.year-yearsBack,monthCheck,dt.day)
    elif monthCheck in [4,6,9,11]:
        if dt.day > 30:
            return datetime(dt.year-yearsBack,monthCheck,30)
        else:
            return datetime(dt.year-yearsBack,monthCheck,dt.day)
    elif monthCheck in [2]:
        if leap_year(datetime(dt.year-yearsBack,4,1).year) & (dt.day>=29):
            print("ding")
            return datetime(dt.year-yearsBack,monthCheck,29)
        else:
            return datetime(dt.year-yearsBack,monthCheck,28)
            
        return 28
    # need to return a timestamp that is year, month now
    return leapFlag,monthRing[dt.month-(monthsBack+1)],yearsBack

def getTempMinMaxAvg(sDate,eDate):
    tempObs = session.query(Measurements.date,Measurements.tobs)\
        .filter(Measurements.date>=sDate)\
        .filter(Measurements.date<=eDate).all()
    df1 = pd.DataFrame(tempObs)
    dfmean = df1.groupby('date').mean().reset_index().rename(columns={"tobs":"T (mean)"})
    dfmax = df1.groupby('date').max().reset_index().rename(columns={"tobs":"T (max)"})
    dfmin = df1.groupby('date').min().reset_index().rename(columns={"tobs":"T (min)"})
    dfT = dfmean.set_index('date').join(dfmax.set_index('date').join(dfmin.set_index('date')))
    return dfT

def getTempMinMaxAvgAfterStart(sDate):
    tempObs = session.query(Measurements.date,Measurements.tobs)\
        .filter(Measurements.date>=sDate).all()
    df1 = pd.DataFrame(tempObs)
    dfmean = df1.groupby('date').mean().reset_index().rename(columns={"tobs":"T (mean)"})
    dfmax = df1.groupby('date').max().reset_index().rename(columns={"tobs":"T (max)"})
    dfmin = df1.groupby('date').min().reset_index().rename(columns={"tobs":"T (min)"})
    dfT = dfmean.set_index('date').join(dfmax.set_index('date').join(dfmin.set_index('date')))
    return dfT
    
def calc_temps(startDate,endDate):
    sDate = dateutil.parser.parse(startDate)
    eDate = dateutil.parser.parse(endDate)
    return getTempMinMaxAvg(sDate,eDate)

def calc_temps_After(startDate):
    sDate = dateutil.parser.parse(startDate)
    return getTempMinMaxAvgAfterStart(sDate)


app = Flask(__name__)
api = Api(app)

class TemperatureRange(Resource):
	def get(self,startDate,endDate):
		dfTrange = calc_temps(startDate,endDate)
		return jsonify(dfTrange.reset_index().to_dict(orient='records'))

class TemperatureAfter(Resource):
	def get(self,startDate):
		dfTrange = calc_temps_After(startDate)
		return jsonify(dfTrange.reset_index().to_dict(orient='records'))

class Temperature(Resource):
	def get(self):
		tempObs = session.query(Measurements.date,Measurements.tobs) \
    		.filter(Measurements.date<=datetime.now()) \
    		.filter(Measurements.date>=monthDiff(datetime.now(),12)) \
    		.all()
		newTempObs = [{'date':item[0].isoformat(),'tobs':item[1]} for item in tempObs]
		return jsonify(newTempObs)	

class Precipitation(Resource):
	def get(self):
		preciptQuery = session.query(Measurements.date,Measurements.prcp) \
    		.filter(Measurements.date<=datetime.now()) \
    		.filter(Measurements.date>=monthDiff(datetime.now(),12)) \
    		.all()
		newPrcpObs = [{'date':item[0].isoformat(),'prcp':item[1]} for item in preciptQuery]
		return jsonify(newPrcpObs)

class StationsInHawaii(Resource):
	def get(self):
		stationsQuery = session.query(Stations.station,\
			Stations.name,\
			Stations.latitude,\
			Stations.longitude,
			Stations.elevation) \
			.all()
		stationsObjs = [{'station':item[0],\
				'name':item[1],\
				'latitude':item[2],\
				'longitude':item[3],\
				'elevation':item[4] } for item in stationsQuery]
		return jsonify(stationsObjs)
		

api.add_resource(TemperatureRange,'/api/v1.0/<string:startDate>/<string:endDate>')
api.add_resource(TemperatureAfter,'/api/v1.0/<string:startDate>')
api.add_resource(Temperature,'/api/v1.0/tobs')
api.add_resource(Precipitation,'/api/v1.0/precipitation')
api.add_resource(StationsInHawaii,'/api/v1.0/stations')

if __name__ == '__main__':
    app.run(debug=True)
