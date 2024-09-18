#!/usr/bin/env python
import pika
import sys
from flask import Flask, render_template,\
jsonify,request,abort
from flask_sqlalchemy import SQLAlchemy 
from threading import Thread, Lock
from multiprocessing import Value
from sqlalchemy import create_engine
from collections import OrderedDict
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,ForeignKey ,DateTime
from sqlalchemy.orm import sessionmaker,scoped_session
import requests
from flask import Response
import json
import re
import uuid
from datetime import datetime
import docker
import subprocess
import operator
import os,sqlite3
from threading import Thread, Lock
import tarfile
import time
import threading
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
logging.basicConfig()
zk = KazooClient(hosts='zookeeper:2181')
zk.start()
zk.ensure_path("/slaves")
#engine = create_engine('sqlite:///:cloud1:', echo=True) 
app=Flask(__name__)
counter = Value('i', 0) #https://stackoverflow.com/questions/42680357/increment-counter-for-every-access-to-a-flask-view
flag=1
#scaling_check_interval = 5
scaling_container_bracket = 20
container_list = {}
container_list_lock = Lock()
num_of_slaves = Value('i',0)
connection = pika.BlockingConnection(
	pika.ConnectionParameters(host='rmq'))

channel = connection.channel()
#channel.queue_delete(queue='WRITE_Q')
channel.queue_declare(queue='WRITE_Q', durable=True)
master=""




class Calling(object):

	def __init__(self):
		self.connection = pika.BlockingConnection(
			pika.ConnectionParameters(host='rmq'))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue = result.method.queue

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response,
			auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = json.loads(body)

	def call(self, n):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(
			exchange='',
			routing_key='READ_Q',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=json.dumps(n))
		while self.response is None:
			self.connection.process_data_events()
		return self.response




@app.route("/api/v1/db/read",methods=["POST"])
def readdb():
	global flag
	with counter.get_lock():
		counter.value += 1
	#Thread(target=auto_scale).start()
	if counter.value==1 and flag==1:
		threading.Timer(120,auto_scale).start()
		flag=0
	d = {}
	flag = request.get_json()["flag"]
	if int(flag) == 1:
		name= request.get_json()["name"]
		obj1 = Calling()

		response = obj1.call({'flag':1})
		if name not in response:
			return "None"
		else:
			return name
	if int(flag) ==2:
		obj2 = Calling()

		response = obj2.call({'flag':1})
		return jsonify(response)
		res = Username.query.all()
		l = []
		if res == None:
			return jsonify("None"),204
		for i in res:
			l.append(i.name)
		return jsonify(l),201
	if int(flag) == 3:
		d['source'] = int(request.get_json()["source"])
		d['destination'] = int(request.get_json()["destination"])
		d['flag'] = 3
		obj1 = Calling()

		response = obj1.call(d)
		
		return jsonify(response),200
	if int(flag) == 4:
		d['flag'] = 4
		d['rideId'] = request.get_json()["rideid"]
		obj1 = Calling()

		response = obj1.call(d)
		if response == "None":
			return "204"
		return Response(json.dumps(response), status=201, mimetype='application/json')
		
	if int(flag) == 5:
		username = request.get_json()["name"]
		rideId = request.get_json()["rideid"]
		read_request = { "flag" : "2",
					
					}
		#r = requests.get(url = "http://Users-Rides-2129458677.us-east-1.elb.amazonaws.com/api/v1/users")
		r = requests.get(url = "http://user:8080/api/v1/users")
		if r.text == "None":
			return 2
		res = json.loads(r.text)
		if username not in res:
			return "2"
		read_request = { "flag" : "4",
					"rideid": rideId
		}
		res = requests.post(url = "http://orch:9000/api/v1/db/read",json = read_request )
		
		if res.text== "204":
			return "3"

		response = json.loads(res.text)
		if response == None:
			return "3"
		print("response for join ride ",response)
		if response['Created_by'] == username:
			return "0"

		if username in response["users"]:
			return "1"
		else:
			return "5"
	if(int(flag) == 6):
		d['flag'] = 6
		d['rideId'] = request.get_json()["rideid"]
		obj1 = Calling()
		res= obj1.call(d)
		
		return res

@app.route("/api/v1/db/write",methods=["POST","DELETE"])
def WRITEdb():
	flag = request.get_json()["flag"]
	d = {}
	#creating user
	if int(flag) == 1:
		
		d['flag'] = 1
		d['name'] = request.get_json()["name"]
		d['password']=request.get_json()["password"]
		#l= [ d['name'],d['password']]
		print("sending ",d)
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))
		return "success"
	#creating ride
	if int(flag) == 2:
		d['flag'] = 2
		d['created_by'] = request.get_json()["created_by"]
		d['timestamp'] = request.get_json()["timestamp"]
		d['source'] = int(request.get_json()["source"])
		d['destination'] =int(request.get_json()["destination"])
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))
		return "success"
	#user joiing ride
	if int(flag) == 3:
		d['flag'] = 3
		d['name'] = request.get_json()["name"]
		d['rideId'] = request.get_json()["rideid"]
		print("calling queue")
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))

		return "success"
	#delete user
	if int(flag) == 4:
		d = {}
		d['flag'] = 4
		d['name'] = request.get_json()["name"]
		
		
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))
		return "success"
	#delete ride
	if int(flag) == 5:
		d["flag"] = 5

		d['rideId'] = request.get_json()["rideid"]
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))
		
		
		return "success"
	if int(flag) == 6:
		d["flag"] = 6
		channel.basic_publish(
			exchange='',
			routing_key='WRITE_Q',
			body=json.dumps(d),
			properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
	   			 ))
		return "success"


def start_last_container():
	print("starting a container")
	with num_of_slaves.get_lock():
		num_of_slaves.value += 1
	print("value = ",num_of_slaves.value)
	container2 = client.containers.run( privileged = True,  image = 'handsonsession_worker:latest',
											   name = 'slave'+str(num_of_slaves.value),
											   command = 'sh -c "sleep 15 && python worker.py"',
											   environment = {'WORKER':'SLAVE'},
											   links = {'handsonsession_rmq_1':'rmq','handsonsession_zoo_1':'zoo'},
											   network = 'handsonsession_default',
											   detach = True)
	id_name=str(client.containers.get(container2.id))+":code/:c:"
	#copy_to("master:code/:c::",id_name)
	time.sleep(10)
	container_list_lock.acquire()
	container_list[container2.id] = client.containers.get(container2.id).attrs['State']['Pid']
	container_list_lock.release()
	print("containers :",container_list)
def fail_func():
	c = zk.get_children("/slaves")
	print("There are %s children with names %s" % (len(c), c))
	print("slave with the highest pid is killed")
	print("starting a container")
	with num_of_slaves.get_lock():
		num_of_slaves.value += 1
		
	print("value = ",num_of_slaves.value)
	container2 = client.containers.run( privileged = True,  image = 'handsonsession_worker:latest',
											   name = 'slave'+str(num_of_slaves.value),
											   command = 'sh -c "sleep 15 && python worker.py"',
											   environment = {'WORKER':'SLAVE'},
											   links = {'handsonsession_rmq_1':'rmq','handsonsession_zoo_1':'zoo'},
											   network = 'handsonsession_default',
											   detach = True,tty=True)
	time.sleep(5)
	container_list_lock.acquire()
	container_list[container2.id] = client.containers.get(container2.id).attrs['State']['Pid']
	container_list_lock.release()
	print("Container:",container_list)
	print('crashed slave and created a new slave')

def kill_last_container():
	
	#client = docker.APIClient(base_url='unix:///var/run/docker.sock')
	container_list_lock.acquire()
	print(container_list)
	with num_of_slaves.get_lock():
		num_of_slaves.value -= 1
	max_cont_id = max(container_list.items(), key = operator.itemgetter(1))[0] 
	print(max_cont_id)
	cnt=client.containers.get(max_cont_id)
	cnt.stop(timeout=5)
	cnt.remove(v=True,force=True)
	print("killed container",max_cont_id)

	del(container_list[max_cont_id])
	container_list_lock.release()
	print(container_list)
	
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	print("Crashing slave")
	#start_last_container()
	#time.sleep(5)
	r=[]
	max_cont_id = max(container_list.items(), key = operator.itemgetter(1))[0]
	r.append(container_list[max_cont_id])
	kill_last_container()
	zk.get_children("/slaves",watch=fail_func())
	return jsonify(r),200
#autoscaling 
@app.route("/api/v1/worker/list",methods=["GET"])
def workers_list():
	l=list(container_list.values())
	print(l)
	cl=docker.from_env()
	m=cl.containers.get("master").attrs['State']['Pid']
	l.append(m)
	l.sort()
	print("l1 ",l)
	return jsonify(l),200

def auto_scale():
	
	print('Auto Scaling Started')
	#time.sleep(scaling_check_interval)
		#num_slaves=0
	num_cont_needed = (counter.value // scaling_container_bracket) + 1
	print("Number of containers needed:"+str(num_cont_needed),file=sys.stderr) 
	if(num_of_slaves.value != num_cont_needed):
		if(num_of_slaves.value < num_cont_needed):
			no_of_extra_containers = num_cont_needed - num_of_slaves.value
			for i in range(no_of_extra_containers):
				start_last_container()
				time.sleep(1)
			#print(container_dictionary,file=sys.stderr)
		else:
			no_of_extra_containers = abs(num_cont_needed - num_of_slaves.value)
			while(no_of_extra_containers != 0):
				kill_last_container()
				no_of_extra_containers = no_of_extra_containers - 1
			
	else:
		print("Same number of containers",file=sys.stderr)
		print("Number of read requests:",counter.value,file=sys.stderr)
	with counter.get_lock():
		counter.value = 0
	threading.Timer(120,auto_scale).start()

if __name__ == '__main__':
	

	print("creating master")
	client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
				
	container1 = client.containers.run( privileged = True,
											   image = 'handsonsession_worker:latest',
											   name = 'master',
											   command = 'sh -c "sleep 15 && python worker.py"',
											   environment = {'WORKER':'MASTER'},
											   links = {'handsonsession_rmq_1':'rmq','handsonsession_zoo_1':'zoo'},
											   network = 'handsonsession_default',
											   detach = True)
	master=container1
	container2 = client.containers.run( privileged = True,
											   image = 'handsonsession_worker:latest',
											   name = 'slave1',
											   command = 'sh -c "sleep 15 && python worker.py"',
											   environment = {'WORKER':'SLAVE'},
											   links = {'handsonsession_rmq_1':'rmq','handsonsession_zoo_1':'zoo'},
											   network = 'handsonsession_default',
											   detach = True)

	container_list_lock.acquire()
	container_list[container2.id] = client.containers.get(container2.id).attrs['State']['Pid']
		
	container_list_lock.release()
	print(container_list)
	
	#Thread(target=auto_scale).start()
	with num_of_slaves.get_lock():
			num_of_slaves.value += 1
	
		
	app.debug=True
	
	app.run(use_reloader = False,host = '0.0.0.0',port=80)
