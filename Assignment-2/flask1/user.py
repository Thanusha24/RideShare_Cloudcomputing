from flask import Flask, render_template,\
jsonify,request,abort
from flask_sqlalchemy import SQLAlchemy 
from sqlalchemy import create_engine
from collections import OrderedDict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,ForeignKey ,DateTime
from sqlalchemy.orm import sessionmaker,scoped_session
import requests
from flask import Response
import json
import re
from datetime import datetime
#engine = create_engine('sqlite:///:cloud1:', echo=True) 
app=Flask(__name__)
app.config['SQLALCHEMY_DATAdb.Model_URI'] = 'sqlite:///:c:' 
app.config['SQLALCHEMY_TRACK_MODIFICATIONS_URI'] = False
db = SQLAlchemy(app)
Session = scoped_session(sessionmaker(bind=db))



class Username(db.Model):
    __tablename__ = 'users'

    name = Column(String, primary_key=True)
    password = Column(String(40))

    def __init__(self,name,password):
    	self.password = password
    	self.name = name

@app.route("/")
def hello():
	return "hello"
    

@app.route("/api/v1/users",methods=["PUT"])
def adduser():
	
	name=request.get_json()["username"]
	password=request.get_json()["password"]
	read_request = { "flag" : "1",
					"name" : name
					}
	
	pattern = re.compile(r'\b[0-9a-f]{40}\b')
	if(re.match(pattern,password) == None):
		return Response("password not in sha1", status=400, mimetype='application/json')
	r = requests.post(url = "http://localhost:80/api/v1/db/read",json = read_request )
	if(r.text == "None"):
		write_request = { "flag" : "1",
					"name" : name,
					"password": password
					}
		r = requests.post(url = "http://localhost:80/api/v1/db/write",json = write_request )
		if r.text == "success":
			return jsonify("Username added successfully"),201
		else:
			return jsonify("{400:failed}"),400
	else:
		return jsonify("Username already exist"),400
	
	
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def delete_user(username):
	read_request = { "flag" : "1",
					"name" : username
					}
	r = requests.post(url = "http://localhost:80/api/v1/db/read",json = read_request )
	if(r.text != "None"):
		write_request =  {
					"flag" : "4",
		             "name": username
		}
		r = requests.delete(url = "http://localhost:80/api/v1/db/write",json = write_request )
		if r.text == "success":
			return jsonify("deleted"),200
		else:
			return jsonify(""),400
	else:
			return jsonify("Username doesn't exist"),400
@app.route("/api/v1/users",methods=["GET"])
def list_users():
	res = Username.query.all()
	l = []
	if res == None:
		return jsonify("None"),204
	for i in res:
		l.append(i.name)
	return jsonify(l),201

@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	Username.query.delete()
	db.session.commit()
	return jsonify("Database cleared"),200

@app.route("/api/v1/db/read",methods=["POST"])
def readdb():
	flag = request.get_json()["flag"]
	if int(flag) == 1:
		name = request.get_json()["name"]
		res = Username.query.filter_by(name  = name ).first()
		if res == None:
			return "None"
		else:
			return name
	if int(flag) ==2:
		res = Username.query.all()
		l = []
		if res == None:
			return jsonify("None"),204
		for i in res:
			l.append(i.name)
		return jsonify(l),201
		 


@app.route("/api/v1/db/write",methods=["POST","DELETE"])
def writedb():
	flag = request.get_json()["flag"]
	if int(flag) == 1:
		name = request.get_json()["name"]
		password=request.get_json()["password"]
		u = Username(name,password)
		db.session.add(u)
		db.session.commit()
		return "success"
	
	if int(flag) == 4:
		username = request.get_json()["name"]
		Username.query.filter_by(name = username).delete()
		#Riders.query.filter_by(name = username).delete()
		db.session.commit()
		return "success"
	

if __name__ == '__main__':
	db.create_all()	
	app.debug=True
	
	app.run(host = '0.0.0.0',port=80)

