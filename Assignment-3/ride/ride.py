from flask import Flask, render_template,\
jsonify,request,abort
from flask_sqlalchemy import SQLAlchemy 
from sqlalchemy import create_engine
from multiprocessing import Value
from collections import OrderedDict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,ForeignKey ,DateTime
from sqlalchemy.orm import sessionmaker,scoped_session
import requests
from flask import Response
import json
import re
from datetime import datetime
counter = Value('i', 0)
ride_count = Value('i',0)
#engine = create_engine('sqlite:///:cloud1:', echo=True) 
app=Flask(__name__)

@app.errorhandler(405)
def method_not_found(e):
	r = request.path
	if(r == "/api/v1/rides"):
		with counter.get_lock():
			counter.value+=1
		return jsonify({}),405

	
@app.route("/api/v1/rides",methods=["POST"])
def create_ride():
	with counter.get_lock():
		counter.value += 1

	created_by = request.get_json()["created_by"]
	timestamp = request.get_json()["timestamp"]
	source = int(request.get_json()["source"])
	destination =int(request.get_json()["destination"])
	if source == destination:
		return jsonify("Source and Destination are same"),400
	if source not in range(1,199):
		return jsonify("Source doesn't exist"),400
	if destination not in range(1,199):
		return jsonify("Destination doesn't exist"),400
	pattern = re.compile(r'\d\d-\d\d-\d\d\d\d:\d\d-\d\d-\d\d')
	if(re.match(pattern,timestamp) == None):
		return Response("Timestamp format not correct", status=400, mimetype='application/json')

	read_request = { "flag" : "1",
					"name" : created_by
					}
	#r = requests.get(url = "http://Users-Rides-2129458677.us-east-1.elb.amazonaws.com/api/v1/users")
	r = requests.get(url = "http://user:8080/api/v1/users")
	if(r.text != "None" and created_by in json.loads(r.text)):
		write_request = {	
				"flag" : "2",	
				"created_by" : created_by,
				"timestamp": timestamp,
				"source": source,
				"destination": destination
				}
		r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
		if r.text == "success":
			with ride_count.get_lock():
				ride_count.value += 1
			return jsonify("Ride Created"),201
		else:
			return jsonify("{400:failed}"),400
		
	else:
		return Response("Username doesn't exist", status=400, mimetype='application/json')


@app.route("/api/v1/rides",methods = ["GET"])
def get_ride_id1():
	with counter.get_lock():
		counter.value += 1
	source = int(request.args.get("source"))
	destination = int(request.args.get("destination"))
	read_request = { "flag" : "3",
					"source" : source,
					"destination" : destination
					}

	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	
	res = json.loads(r.text)
	if len(res) == 0:
		return jsonify(""),204
	else:
		res = json.loads(r.text)
		return jsonify(res),200
	 

	
@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def get_rideDetails(rideId):
	with counter.get_lock():
		counter.value += 1
	read_request = { "flag" : "4",
					"rideid": rideId
	}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	
	if r.text == "204":
		return jsonify(""),204
	else:
		res = json.loads(r.text)
		return jsonify(res),200

@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):
	with counter.get_lock():
		counter.value += 1
	username = request.get_json()["username"]
	read_request = { "flag" : "5",
					"name" : username,
					"rideid": rideId
	}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	#return r.text
	
	if(int(r.text) ==0 ):
		return jsonify("User Created the ride"),400
	elif(int(r.text) ==1 ):
		return jsonify("User has already joined the ride"),400
	elif(int(r.text) ==2 ):
		return jsonify("Username doesn't exist"),400
	elif(int(r.text) ==3 ):
		return jsonify("RideId doesn't exist"),400
	if(int(r.text) ==5 ):
		write_request = { "flag" : "3",
					"name" : username,
					"rideid": rideId
						}
		r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
		
		if r.text== "success":
			return jsonify("Joined successfully"),201
		else:
			return jsonify("{2:fail}"),400	

	
		
@app.route("/api/v1/rides/<rideId>",methods=["DELETE"])
def delete_ride(rideId):
	with counter.get_lock():
		counter.value += 1
	read_request = { "flag" : "6",
					"rideid": rideId
	}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	if r.text == "None":
		return Response("RideId doesn't exist", status=400, mimetype='application/json')
	else:
		write_request = { "flag" : "5",
					"rideid": rideId
					}
		r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
		if r.text == "success":
			with ride_count.get_lock():
				ride_count.value -= 1
			return Response("RideId Deleted", status=200, mimetype='application/json')
@app.route("/api/v1/_count",methods = ["GET"])
def count():
	l = [counter.value]
	return jsonify(l),200
@app.route("/api/v1/_count",methods = ["DELETE"])
def reset_count():
	counter.value = 0
	return jsonify({}),200


@app.route("/api/v1/rides/count",methods = ["GET"])
def r_count():
	with counter.get_lock():
		counter.value += 1
	with counter.get_lock():
		l = [ride_count.value]
	
	return jsonify(l),200		
		

@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	write_request = { "flag" : "6",
					
					}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
	if r.text== "success":
			return jsonify("DB cleared"),200



if __name__ == '__main__':
	
	app.debug=True
	
	app.run(host = '0.0.0.0',port = 80)
