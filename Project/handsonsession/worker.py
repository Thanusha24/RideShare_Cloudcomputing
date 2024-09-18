#!/usr/bin/env python
import pika
import time
import json
from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from collections import OrderedDict
import re
import os
import sqlite3
import docker
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
import random 
import uuid
'''client = docker.from_env()
container_list=client.containers.list()
for i in container_list():
    container = client.containers.get(i)
    print()
    
#checking master or slave
if os.environ['WORKER'] == 'SLAVE':
    print("this is slave")
else:
    print("this is master")
'''
#creating table
logging.basicConfig()
zk = KazooClient(hosts='zookeeper:2181')
zk.start()
zk.ensure_path("/slaves")
Base = declarative_base()
engine = create_engine('sqlite:///:c:', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

class Username(Base): #user table
    __tablename__ = 'users'

    name = Column(String, primary_key=True)
    password = Column(String(40))

    def __init__(self,name,password):
        self.password = password
        self.name = name


class Ride(Base): #ride table
    __tablename__ = 'ride'

    id = Column(Integer, primary_key =  True)
    created_by = Column(String)
    timestamp = Column(String)
    source = Column(Integer)
    destination = Column(Integer)
    

    def __init__(self,created_by,timestamp,source,destination):
        
        self.created_by = created_by
        self.timestamp = timestamp
        self.source = source
        self.destination = destination

class Riders(Base): #riders who joined the ride
    __tablename__ = 'riders'
    id = Column(Integer, primary_key =  True)
    rideid = Column(Integer)
    name =Column(String)
    def __init__(self,rideid,name):
        self.rideid = rideid
        self.name =name

Base.metadata.create_all(engine) 
def check_znode(slave_no):
    kz_pth="/slaves/slave_"+str(slave_no)
    if zk.exists(kz_pth):
        sl_n=random.randrange(30, 50, 1)
        kz_pth="/slaves/slave_"+str(sl_n)
    return kz_pth

#creating queue
if os.environ['WORKER'] == 'SLAVE':
    print("this is slave")
    print("Creating slave znode")
    children=zk.get_children('/slaves')
    slave_no=len(children)+1
    kz_pth="/slaves/slave_"+str(slave_no)
    kz_pth=check_znode(slave_no)
    print(kz_pth)
    if zk.exists(kz_pth):
        print("existing")
    else:
        zk.create(path=kz_pth, value=b"This is a slave",ephemeral=True)
        print('Created a slave znode')

if os.environ['WORKER'] == 'SLAVE':
    class CopyRpcClient(object):

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
        def delete(self):
            print("deleting")
            self.channel.queue_delete(queue='COPY_Q')

        def on_response(self, ch, method, props, body):
            print("onresponse")
            if self.corr_id == props.correlation_id:
                self.response = json.loads(body)

        def call(self):
            print("calling")
            self.response = None
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key='COPY_Q',
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=str("get"))
            while self.response is None:
                self.connection.process_data_events()
            return self.response
    obj1 = CopyRpcClient()
    response = obj1.call()
    for i in response['User']:
        u = Username(i[0],i[1])
        session.add(u)
    for i in response['Rides']:
        r = Ride(i[0],i[1],i[2],i[3])
        session.add(r)
    for i in response['Riders']:
        r = Riders(i[0],i[1])
        session.add(r)
    session.commit()
    #obj1.delete()
    #del obj1
if os.environ['WORKER'] == 'MASTER':
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))

    channel = connection.channel()
    channel.queue_declare(queue='COPY_Q')
    def on_request(ch, method, props, body):
        print("Received copy request")
        d = {}
        d['User'] = []
        d['Rides'] = []
        d['Riders'] = []
        res = session.query(Username)
        if res != None:
                for i in res:
                        d['User'].append((i.name , i.password))
        res = session.query(Ride)
        
        if res != None:
            for i in res:
                d['Rides'].append((i.created_by,i.timestamp,i.source,i.destination))
        res = session.query(Riders)
        if res != None:
            for i in res:
                d['Riders'].append((i.rideid,i.name))




        response = d

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=json.dumps(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
print(' [*] Waiting for messages.',print(os.environ['WORKER']))

def compare_time(t1,t2):
    l1 = t1.split(" ")
    l2 = t2.split(":")
    date1 = l1[0].split("-")
    date2 = l2[0].split("-")
    time1 = l1[1].split(":")
    time2 = l2[1].split("-")
    d1,m1,y1 = int(date1[2]) ,int(date1[1]) ,int(date1[0])
    d2,m2,y2 = int(date2[0]) ,int(date2[1]) ,int(date2[2])
    s1,min1,h1 = int(time1[2][:2]) ,int(time1[1]) ,int(time1[0])
    s2,min2,h2 = int(time2[0]) ,int(time2[1]) ,int(time2[2])
    #print("1 ",y1,m1,d1,h1,min1,s1)
    #print("2 ",y2,m2,d2,h2,min2,s2)
    if(y1>y2):
        return 0
    elif y2>y1:
        return 1
    if(m1>m2):
        return 0
    elif m2>m1:
        return 1
    if(d1>d2):
        return 0
    elif d2>d1:
        return 1
    if(h1>h2):
        return 0
    elif h2>h1:
        return 1
    if(min1>min2):
        return 0
    elif min2>min1:
        return 1
    if(s1>s2):
        return 0

    return 1
 

if os.environ['WORKER'] == 'SLAVE':

    def read(ch, method, props, body):
        print("Reading")
        print(" [x] Reading Received %r" % json.loads(body))
        n = json.loads(body)
        d = json.loads(body)
        if(n['flag'] == 1):
            l= []
            select = session.query(Username)
            if select == None:
                l = []   
            else:
                for u in select :
                    l.append(u.name)
                print("user list = ",l)
            ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=json.dumps(l))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        if(n['flag'] == 3 ):
            print("get ride id called ")
            source = d['source']
            destination = d['destination']
            f = 0
            res = session.query(Ride).filter_by(source = source).filter_by(destination = destination)
            l = []
            if(res == None):
                return jsonify("{}"),204
            for i in res:

                c = compare_time(str(datetime.now()),i.timestamp)
                
                if(c == 1):
                    
                    f = 1
                    r1 = OrderedDict([("rideId",i.id),("username",i.created_by),("timestamp",i.timestamp)])
                    l.append(r1)
                else:
                    
                    continue
            ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=json.dumps(l))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        if d['flag'] == 4:
            rideId = d['rideId']
            res = session.query(Ride).filter_by(id = int(rideId)).first()
            
            res1 = session.query(Riders).filter_by(rideid = int(d['rideId'])).all()
            if res != None :

                r1 = {}
                r1["rideId"] = d['rideId']
                r1["users"] = []
                for i in res1:
                    r1["users"].append(i.name)
                r1["Created_by"] = res.created_by
                r1["Timestamp"] = res.timestamp
                r1["source"] = res.source
                r1["destination"] = res.destination
            else:
                r1 = "None"
            ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=json.dumps(r1))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        if d['flag'] == 6:
            print("checking rideId")
            rideId = d['rideId']
            res = session.query(Ride).filter_by(id = int(rideId)).first()
            if res == None:
                r =  "None"
            else:
                r = str(rideId) 

            ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=json.dumps(r))
            ch.basic_ack(delivery_tag=method.delivery_tag)

 

def write(ch, method, properties, body):
    print("writig ")
    print(" Writing  [x] Received %r" % json.loads(body))
    d = json.loads(body)
    
    print("type of d",type(d))
    if d["flag"] == 1:
        try:
            
            print(type(d))
            u = Username(d["name"],d["password"])
            session.add(u)
            session.commit()
            
            time.sleep(body.count(b'.'))
            print(" [x] Done")
            
        except:
            print("error could not insert")
            print(" [x] Done")
            
    elif d['flag'] == 2:
        print("creating ride")
        r = Ride(d['created_by'],d['timestamp'],d['source'],d['destination'])
        session.add(r)
        session.commit()
        
    elif d['flag'] == 3:
        print("joining  ride")
        r = Riders(d['rideId'],d['name'])
        session.add(r)
        session.commit()
        print("ride joined")
        
    elif d["flag"] == 4:
        print("delete called")
        session.query(Username).filter(Username.name == d["name"]).delete()
        session.commit()
        
    elif d['flag'] == 5:
        print("deleting ride")
        rideId = d["rideId"]
        session.query(Ride).filter_by(id = int(rideId)).delete()
        session.query(Riders).filter_by(rideid = int(rideId)).delete()
        session.commit()
        
    elif d['flag'] == 6:
        print("clearing db")
        session.query(Ride).filter().delete()
        session.query(Riders).filter().delete()
        session.query(Username).filter().delete()
        session.commit()
    
    #if os.environ['WORKER'] == 'MASTER':
    #    ch.basic_publish(exchange='SYNC_Q', routing_key='', body=json.dumps(d))
    if os.environ['WORKER'] == 'MASTER':
                ch.basic_publish(exchange='SYNC_Q', routing_key='', body=json.dumps(d))   
    ch.basic_ack(delivery_tag=method.delivery_tag)    



if os.environ['WORKER'] == 'SLAVE':
    print("this is slave")
    
    channel.queue_declare(queue='READ_Q',durable = True)

    #channel.basic_qos(prefetch_count=1)
    channel.exchange_declare(exchange='SYNC_Q', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='SYNC_Q', queue=queue_name)
    channel.basic_consume(queue='READ_Q', on_message_callback=read)
    #channel.basic_consume(queue='WRITE_Q', on_message_callback=write)
    channel.basic_consume(queue=queue_name, on_message_callback=write)
    channel.start_consuming()

elif os.environ['WORKER'] == 'MASTER':
    print("this is master")
    channel.basic_consume(queue='COPY_Q', on_message_callback=on_request)
    channel.queue_declare(queue='WRITE_Q', durable=True)
    channel.exchange_declare(exchange='SYNC_Q', exchange_type='fanout')
    channel.basic_consume(queue='WRITE_Q', on_message_callback=write)


channel.start_consuming()