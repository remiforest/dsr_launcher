#! /usr/bin/python
# coding: utf-8

"""

MapR DSR Launcher

Starts and configures DSR containers

"""

import subprocess
import requests
import threading
import time

from flask import Flask, render_template, request, Response, flash, redirect, url_for
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory


CLUSTER_IP = "10.0.0.11"
username = "mapr"
password = "mapr"
PEM_FILE = ""
SECURE_MODE = False

LAUNCHER_TABLE = "/dsr_launcher/launcher_table"
MAPR_CLUSTER = "demo.mapr.com"
HOST_IP = "10.0.0.11"
CONTAINER_BASE_NAME = "dsr_" # model = dsr_username_portnum


STARTING_PORT = 9995


# Initialize databases
if SECURE_MODE:
    connection_str = "{}:5678?auth=basic;" \
                     "user={};" \
                     "password={};" \
                     "ssl=true;" \
                     "sslCA={};" \
                     "sslTargetNameOverride={}".format(CLUSTER_IP,username,password,PEM_FILE,CLUSTER_IP)
else:
    connection_str = "{}:5678?auth=basic;user={};password={};ssl=false".format(CLUSTER_IP,username,password)

connection = ConnectionFactory().get_connection(connection_str=connection_str)
launcher_table = connection.get_or_create_store(LAUNCHER_TABLE)



def launch_dsr(port,username="mapr",password="mapr"):
    port = str(port)
    name = "{}{}_{}".format(CONTAINER_BASE_NAME,username,port)
    id_infos = subprocess.check_output(['id',username]).split(' ')
    uid =  id_infos[0].split('=')[1].split('(')[0]
    gid =  id_infos[1].split('=')[1].split('(')[0]
    group =  id_infos[1].split('=')[1].split('(')[1][:-1]

    subprocess.call(['docker','run','-it','--detach',
                     '--name',name,
                     '-p',port + ':9995',
                     '-e','HOST_IP=' + HOST_IP,
                     '-e','MAPR_CLUSTER=' + MAPR_CLUSTER,
                     '-e','MAPR_CLDB_HOSTS=' + CLUSTER_IP,
                     '-e','MAPR_CONTAINER_USER=' + username,    # the user uden which the container will run
                     '-e','MAPR_CONTAINER_PASSWORD=' + password, # only this user can log into Zeppelin
                     '-e','MAPR_CONTAINER_GROUP=' + group, # the group for the container user
                     '-e','MAPR_CONTAINER_UID=' + uid,
                     '-e','MAPR_CONTAINER_GID=' + gid,
                     '-e','MAPR_MOUNT_PATH=' + "/mapr",
                     '-e','ZEPPELIN_NOTEBOOK_DIR=' + "/mapr/"+MAPR_CLUSTER+"/notebooks/"+username+"/",
                     '-e','MAPR_TZ=' + "Europe/Paris",
                     '-v','/sys/fs/cgroup:/sys/fs/cgroup:ro',
                     '--cap-add','SYS_ADMIN',
                     '--cap-add','SYS_RESOURCE',
                     '--device','/dev/fuse',
                     'maprtech/data-science-refinery:v1.3.2_6.1.0_6.1.0_centos7'])
    launcher_table.insert_or_replace({"_id":name,"port":port,"status":"starting","owner":username})



def start_dsr(container):
    subprocess.call(['docker','start',container])

def stop_dsr(container):
    subprocess.call(['docker','stop',container])

def delete_dsr(container):
    subprocess.call(['docker','rm',container])
    global active_containers
    launcher_table.delete(container)


def check_dsr(container):
    port = container.split("_")[2]
    try:
        if "Zeppelin" in requests.get("https://127.0.0.1:{}".format(port),verify=False).content:
            launcher_table.update(_id=container,mutation={"$put":{'status':"started"}})
            return "started"
    except Exception as e:
        msg = str(e)
        print(msg)
        try:
            if "Connection refused" in msg:
                launcher_table.update(_id=container,mutation={"$put":{'status':"stopped"}})
                return "stopped"
            if "bad handshake" in msg:
                launcher_table.update(_id=container,mutation={"$put":{'status':"starting"}})
                return "starting"
            return "undefined" 
        except:
            return "deleted"


def list_containers():
    # List running DSR containers
    ps_output = subprocess.check_output(['docker', 'ps','-a']).decode('utf-8').strip().split('\n')[1:]
    for line in ps_output:
        if CONTAINER_BASE_NAME in line:
            columns = line.split(' ')
            container = columns[-1]
            port = container.split('_')[2]
            if "Exited" in line:
                launcher_table.update(_id=container,mutation={"$put":{'status':"stopped"}})
            else:
                launcher_table.update(_id=container,mutation={"$put":{'status':"started"}})


list_containers()


app = Flask(__name__)


# Main UI
@app.route('/')
def home():
    list_containers()
    active_containers = launcher_table.find()
    print(active_containers)
    return render_template("launcher.html",active_containers=active_containers)




@app.route('/new_container',methods=["POST"])
def new_container():
    new_port = STARTING_PORT
    free_port_found = False
    while not free_port_found:
        free_port_found = True
        query_result = launcher_table.find({"$where": {"$eq": {"port": str(new_port)}}})
        for doc in query_result:
             free_port_found = False
             new_port += 1
             continue
    print("launching dsr on port {}".format(new_port))
    launch_dsr(new_port)
    return str(new_port)



@app.route('/stop_container',methods=["POST"])
def stop_container():
    container = request.form["container"]
    stop_dsr(container)
    return "{} stopped".format(container)

@app.route('/start_container',methods=["POST"])
def start_container():
    container = request.form["container"]
    start_dsr(container)
    return "{} stopped".format(container)

@app.route('/delete_container',methods=["POST"])
def delete_container():
    container = request.form["container"]
    delete_dsr(container)
    return "{} deleted".format(container)

@app.route('/check_status',methods=["POST"])
def check_status():
    container = request.form["container"]
    return check_dsr(container)


app.run(debug=True,host='0.0.0.0',port=80)





