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
#engine = create_engine('sqlite:///:cloud1:', echo=True) 
app=Flask(__name__)


@app.errorhandler(405)
def method_not_found(e):
	r = request.path
	if(r == "/api/v1/users"):
		with counter.get_lock():
			counter.value+=1
		return jsonify({}),405	
@app.route("/api/v1/users",methods=["PUT"])
def adduser():
	with counter.get_lock():
		counter.value += 1
	
	name=request.get_json()["username"]
	password=request.get_json()["password"]
	read_request = { "flag" : "1",
					"name" : name
					}
	
	pattern = re.compile(r'\b[0-9a-f]{40}\b')
	if(re.match(pattern,password) == None):
		return Response("password not in sha1", status=400, mimetype='application/json')
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	if(r.text == "None"):
		write_request = { "flag" : "1",
					"name" : name,
					"password": password
					}
		r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
		if r.text == "success":
			return jsonify("Username added successfully"),201
		else:
			return jsonify("{400:failed}"),400
	else:
		return jsonify("Username already exist"),400
	
	
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def delete_user(username):
	with counter.get_lock():
		counter.value += 1
	read_request = { "flag" : "1",
					"name" : username
					}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	if(r.text != "None"):
		write_request =  {
					"flag" : "4",
					 "name": username
		}
		r = requests.delete(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
		if r.text == "success":
			return jsonify("deleted"),200
		else:
			return jsonify(""),400
	else:
			return jsonify("Username doesn't exist"),400


@app.route("/api/v1/db/clear",methods=["POST"])
def db_clear():
	write_request = { "flag" : "6",
					"rideid": rideId
					}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/write",json = write_request )
	if r.text== "success":
			#return jsonify("DB cleared"),200
		return jsonify("Database cleared"),200

@app.route("/api/v1/users",methods=["GET"])
def list_users():
	with counter.get_lock():
		counter.value += 1
	read_request = { "flag" : "2",
					
					}
	r = requests.post(url = "http://18.233.169.145/api/v1/db/read",json = read_request )
	res = json.loads(r.text)
	if len(res) == 0:
		return jsonify("None"),204
	return jsonify(res),200



@app.route("/api/v1/_count",methods = ["GET"])
def api_count():
	l = [counter.value]
	return jsonify(l),200	
@app.route("/api/v1/_count",methods=["DELETE"])
def reset_count():
	with counter.get_lock():
		counter.value = 0
	return jsonify({}),200


db.create_all()	
app.debug=True

app.run(host = '0.0.0.0',port=80)