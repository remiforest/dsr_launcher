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
import pam
import sys
import signal
import traceback

from flask import Flask, render_template, request, Response, flash, redirect, url_for, session
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


CLUSTER_IP = "10.0.0.11"
username = "mapr"
password = "mapr"
PEM_FILE = ""
SECURE_MODE = False

LAUNCHER_TABLE = "/dsr_launcher/launcher_table"
MAPR_CLUSTER = "demo.mapr.com"
HOST_IP = "10.0.0.11"
CONTAINER_BASE_NAME = "dsr" # model = dsr_username_portnum


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
stop_threads = False



# DB connection management 

def get_db_connection():
    global connection
    global stop_threads
    global connection_thread_active
    connection_thread_active = True
    i = 0
    while not stop_threads:
        if i == 1500 :
            connection = ConnectionFactory().get_connection(connection_str=connection_str)
            i = 0
        time.sleep(1)
    connection_thread_active = False



#### Main features

# Create a new container

def create_dsr(port,owner,username,password):
    try:
        port = str(port)
        name = "{}_{}_{}_{}".format(CONTAINER_BASE_NAME,owner,username,port)
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
        launcher_table.insert_or_replace({"_id":name,"port":port,"status":"starting","owner":owner,"username":username})
        return name
    except Exception as e: 
        traceback.print_exc()
        return "failed"

# Stop a container
def stop_dsr(container):
    print("stopping {}".format(container))
    subprocess.call(['docker','stop',container])


# Start a cotnainer
def start_dsr(container):
    print("starting {}".format(container))
    subprocess.call(['docker','start',container])

# Delete a container
def delete_dsr(container):
    subprocess.call(['docker','rm',container])
    launcher_table.delete(_id=container)

# List containers for a given user
def list_containers(owner=False):
    active_containers = []
    
    if owner:
        query_results = launcher_table.find({"$where": {"$eq": {"owner": session["username"]}}})
    else:
        query_results = launcher_table.find()

    for container in query_results:
        active_containers.append(container)
    
    return active_containers



# update the list of existing containers in the database
def init_containers():
    ps_output = subprocess.check_output(['docker', 'ps','-a']).decode('utf-8').strip().split('\n')[1:]
    for line in ps_output:
        if CONTAINER_BASE_NAME in line:
            columns = line.split(' ')
            container = columns[-1]
            properties = container.split('_')
            owner = properties[1]
            username = properties[2]
            port = properties[3]
            create_container = True
            for doc in launcher_table.find_by_id(container):
                create_container = False
            if create_container:
                launcher_table.insert_or_replace({"_id":container,"owner":owner,"username":username,"port":port,"status":"started"})
            if "Exited" in line:
                launcher_table.update(_id=container,mutation={"$put":{'status':"stopped"}})

# Updates the status of the containers
def update_containers_status():
    for container in launcher_table.find():
        print("checking {} status".format(container["_id"]))
        try:
            content = requests.get("https://127.0.0.1:{}".format(container["port"]),verify=False).content
            if "Zeppelin" in content:
                launcher_table.update(_id=container["_id"],mutation={"$put":{'status':"running"}})
            if "HTTP ERROR: 503" in content:
                launcher_table.update(_id=container["_id"],mutation={"$put":{'status':"error"}})
        except Exception as e:
            msg = traceback.format_exc()
            try:
                if "Connection refused" in msg:
                    launcher_table.update(_id=container["_id"],mutation={"$put":{'status':"stopped"}})
                if "bad handshake" in msg:
                    launcher_table.update(_id=container["_id"],mutation={"$put":{'status':"starting"}})
            except:
                pass




# Cleans the containers database
def clean_database():
    for container in launcher_table.find():
        launcher_table.delete(_id=container["_id"])
        print("{} deleted").format(container["_id"])


# Reset application : Stops and deletes all containers
def reset_application():
    ps_output = subprocess.check_output(['docker', 'ps','-a']).decode('utf-8').strip().split('\n')[1:]
    for line in ps_output:
        if CONTAINER_BASE_NAME + "_" in line:
            columns = line.split(' ')
            container = columns[-1]
            if not "Exited" in line:
                stop_dsr(container)
            delete_dsr(container)
    clean_database()


app = Flask(__name__)

# Set the secret key to some random bytes. Keep this really secret!
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

# Main UI
@app.route('/')
def home():
    if session.get("logged_in"):
        return render_template("launcher.html",active_containers=list_containers(session["username"]))
    else:
        return render_template("login.html")


@app.route('/login',methods=["POST"])
def login():
    username = request.form["username"]
    password = request.form["password"]
    is_auth = pam.authenticate(username,password)
    if is_auth:
        session["logged_in"] = True
        session["username"] = username
        session["auth_message"] = None
    else:
        session["auth_message"]="authentication failed for {}".format(username)
    return home() 


@app.route("/logout")
def logout():
    session['logged_in'] = False
    return home()


@app.route('/new_container',methods=["POST"])
def new_container():
    new_port = STARTING_PORT
    username = request.form["username"]
    password = request.form["password"]
    free_port_found = False
    while not free_port_found:
        free_port_found = True
        query_result = launcher_table.find({"$where": {"$eq": {"port": str(new_port)}}})
        for doc in query_result:
             free_port_found = False
             new_port += 1
             continue
    print("launching new dsr on port {}".format(new_port))
    container = create_dsr(new_port,session["username"],username,password)
    return "{} launched".format(container)



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
    return launcher_table.find_by_id(request.form["container"])["status"]

@app.route('/reset',methods=["POST"])
def reset():
    reset_application()
    return "Application reset"

@app.route('/refresh_db',methods=["POST"])
def refresh_db():
    clean_database()
    init_containers()
    display_containers()
    return render_template("auth_failed.html",username=username)

def handler(signal, frame):
    global stop_threads
    global monitoring_thread_active
    global connection_thread_active
    stop_threads = True
    while monitoring_thread_active and connection_thread_active:
        print(".")
        time.sleep(1)
    subprocess.call(['./clean.sh'])
    sys.exit(0)

signal.signal(signal.SIGINT, handler)


auth_thread = threading.Thread(target=get_db_connection)
auth_thread.start()


def monitoring_function():
    global stop_threads
    global monitoring_thread_active
    monitoring_thread_active = True
    while not stop_threads:
        init_containers()
        update_containers_status()
        time.sleep(5)
    monitoring_thread_active = False

monitoring_thread = threading.Thread(target=monitoring_function)
monitoring_thread.start()



try:
    app.run(debug=True,host='0.0.0.0',port=80)
except:
    subprocess.call(['./clean.sh'])




