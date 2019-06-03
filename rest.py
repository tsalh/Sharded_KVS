from flask import Flask, redirect, request, jsonify,json
from flask import jsonify
from kvs import KVS
from os import environ
import requests
import socket
import sys
import time
from multiprocessing import Process




app = Flask(__name__)
# Import view after Flask App intialization to create
# view endpoint
# import view

# kvs will be used as our key-value-store
kvs = KVS()
# testing purposes
kvs.put('x', ('test_version', 5))
kvs.put('y', ('test_version', 5))
kvs.put('z', ('test_version', 5))

# IP address of this node
#SOCKET_ADDRESS = environ['SOCKET_ADDRESS']
# List of IP addresses for all nodes
#SYS_VIEW = environ['VIEW'].split(',')

# IP address of this node
SOCKET_ADDRESS = environ['SOCKET_ADDRESS']
# List of IP addresses for all nodes

TIMEOUT = 2.00

# Version: Casual dependencies
CAUSAL_HISTORY = {'test_version':'test_metadata'}
VERSION = int(0)

def initializeView(view_string):
    view = view_string.split(",")
    view_map = {}
    for replica in view:
        view_map[replica] = False
    return view_map

global SYS_VIEW
SYS_VIEW = initializeView(environ['VIEW'])

global SHARD_COUNT
SHARD_COUNT = int(environ['SHARD_COUNT'])

def initializeShardMembers(sys_view, shard_count):
    view = list(sys_view.keys())
    view.sort()
    shard_members = [[] for i in range(SHARD_COUNT)]
    shard_id = 0 
    for replica in view:
        shard_members[shard_id].append(replica)
        shard_id += 1
        if (shard_id == shard_count):
            shard_id = 0

    return shard_members

global SHARD_MEMBERS
SHARD_MEMBERS = initializeShardMembers(SYS_VIEW, SHARD_COUNT)

def getShardID(replica):
    for shard_id in range(SHARD_COUNT):
        if replica in SHARD_MEMBERS[shard_id]:
            return shard_id
    return -1


@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_shard_ids():
    id_str = ""
    for i in range(SHARD_COUNT):
        id_str += (str(i) + ',')
    id_str = id_str.rstrip(',')
    response = jsonify()
    response.data = json.dumps({"message":"Shard IDs retrieved successfully", "shard-ids":id_str})
    response.status_code = 200
    return response

@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_node_shard_id():
    shard_id = getShardID(SOCKET_ADDRESS)
    response = jsonify()
    response.data = json.dumps({"message":"Shard ID of the node retrieved successfully", "shard-id":shard_id})
    response.status_code = 200
    return response

@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def get_shard_id_members(shard_id):
    shard_members = ','.join(SHARD_MEMBERS[int(shard_id)])
    response = jsonify()
    response.data = json.dumps({"message":"Members of shard ID retrieved successfully", "shard-id-members":shard_members})
    response.status_code = 200
    return response

# Returns the forward url
def get_forward_url(forward_address, key):
    return "http://" + forward_address + "/key-value-store/" + key

def broadcast_request(key, version, metadata, json):
    global SYS_VIEW
    print(SYS_VIEW, file=sys.stderr)
    print("test", file=sys.stderr)
    app.logger.info(SYS_VIEW)
    # Broadcast a DELETE call to every replica in View
    global VERSION
    for replica in SYS_VIEW:
        if SYS_VIEW[replica] and replica != SOCKET_ADDRESS:
            url = get_forward_url(replica, key)
            app.logger.info('[%s] Broadcasting to replica %s...',SOCKET_ADDRESS, replica)
            payload = {'version_num':int(VERSION), 'version':version, 'broadcasted':True, 'metadata':metadata}
            try:
                response = requests.put(url, json=json, params=payload, timeout=TIMEOUT)
            except requests.exceptions.RequestException:
                app.logger.info("broadcast to [%s] has failed", replica)
                print("Removing from view", file=sys.stderr)
                print(replica, file=sys.stderr)
                SYS_VIEW[replica] = False
                delete_url = "http://" + SOCKET_ADDRESS + "/key-value-store-view"
                json = {"delete_replica":replica}
                requests.delete(delete_url, json=json)
                print(SYS_VIEW, file=sys.stderr)
                # Delete View from replica
                # Broadcast Delete
                ''' 
                for up_replica in SYS_VIEW:
                    if SYS_VIEW[up_replica] and up_replica != replica:
                        delete_url = "http://" + up_replica + "/key-value-store-view"
                        json = {"delete_replica":replica}
                        requests.delete(url, json=json)
                '''

    print(SYS_VIEW, file=sys.stderr)
    print("DONE", file=sys.stderr)
    app.logger.info("Done broadcasting for %s", SOCKET_ADDRESS)
    return

# Returns the forward response used by the forwarding instance
def get_forward_response(forward_url, method, json=None):
    response = None
    try:
        if method == "PUT":
            forward_response = requests.put(forward_url, json=json, timeout=TIMEOUT)
            response = forward_response.content, forward_response.status_code
        if method == "GET":
            forward_response = requests.get(forward_url, timeout=TIMEOUT)
            response = forward_response.content, forward_response.status_code
        if method == "DELETE":
            forward_response = requests.delete(forward_url, timeout=TIMEOUT)
            response = forward_response.content, forward_response.status_code
    except requests.exceptions.RequestException:
        response = jsonify(error="Main instance is down", message="Error in " + method), 503

    return response

def convert_to_set(metadata_string):
    return metadata_string.split("'")

def get_new_version():
    global VERSION
    new_version_val = VERSION + 1
    new_version = 'V' + str(new_version_val)
    VERSION += 1
    return new_version

@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):
    global VERSION
    content = request.get_json()
    value = content.get('value', '') if content else ''
    metadata = content.get('causal-metadata', '') if content else ''

    # If request is sent from a replica, update kvs, CAUSAL_HISTORY, version, and return
    if request.args.get('broadcasted'):
        app.logger.info("[%s] a broadcasted message", SOCKET_ADDRESS)
        version = request.args.get('version')
        new_metadata = request.args.get('metadata')
        
        CAUSAL_HISTORY[version] = new_metadata
        version_num = request.args.get('version_num')
        kvs.put(key, (version, value))
        VERSION = int(version_num)
        app.logger.info("[%s] successfully broadcasted version %s", SOCKET_ADDRESS, str(VERSION))


        return "Successful broadcasted message", 200

    if metadata:
        for version in metadata.split(","):
            app.logger.info("waiting for version %s", version)
            while not version in CAUSAL_HISTORY:
                pass

    if not value:
        return jsonify(error="Value is missing", message="Error in PUT"), 400
    if len(key) > 50:
        return jsonify(error="Key is too long", message="Error in PUT"), 400

    new_version = get_new_version()
    if metadata:
        metadata = metadata + ',' + new_version
    else:
        metadata = new_version
    if kvs.key_exists(key):
        kvs.put(key, (new_version, value))
        CAUSAL_HISTORY[new_version] = metadata
        new_data = json.dumps({"message":"Updated successfully", "version":new_version, "causal-metadata":metadata})
        json_data = jsonify(message="Added successfully", version=new_version)
        json_data.data = new_data
        response = json_data, 200
    else:
        kvs.put(key, (new_version, value))
        CAUSAL_HISTORY[new_version] = metadata
        new_data = json.dumps({"message":"Added successfully", "version":new_version, "causal-metadata":metadata})
        json_data = jsonify(message="Added successfully", version=new_version)
        json_data.data = new_data
        response = json_data, 201
    # Open new process to call broadcast_put so we don't have to wait
    broadcast = Process(target = broadcast_request, args=(key, new_version, metadata, request.get_json()))
    broadcast.start()
    return response

@app.route('/key-value-store/<key>', methods=['GET'])
def get(key):
    global VERSION
    storedKey = kvs.get(key)
    value = storedKey[1]
    version = storedKey[0]
    metadata = CAUSAL_HISTORY[version]

    # Main instance execution
    if kvs.key_exists(key):
        new_data = json.dumps({"message":"Retrieved successfully", "version":version, "causal-metadata":metadata, "value":value})
        json_data = jsonify(message="Added successfully", version=version)
        json_data.data = new_data
        return json_data, 200
    else:
        return jsonify(doesExist=False, error="Key does not exist", message="Error in GET"), 404

@app.route('/key-value-store/<key>', methods=['DELETE'])
def delete(key):
    global VERSION
    content = request.get_json()

    # If request is sent from a replica, update kvs, CAUSAL_HISTORY, version, and return
    if request.args.get('broadcasted'):
        app.logger.info("[%s] a broadcasted message", SOCKET_ADDRESS)
        version = request.args.get('version')
        
        CAUSAL_HISTORY[version] = metadata
        version_num = request.args.get('version_num')
        kvs.put(key, (version, None))
        VERSION = int(version_num)
        app.logger.info("[%s] successfully broadcasted version %s", SOCKET_ADDRESS, str(VERSION))


        return "Successful broadcasted message", 200

    metadata = content.get('causal-metadata', '') if content else ''
    if metadata:
        for version in metadata.split(","):
            # Only move on after version from causal metadata is in this replicas causal history
            app.logger.info("waiting for version %s", version)
            while not version in CAUSAL_HISTORY:
                pass
    if kvs.key_exists(key):
        # Get new version
        new_version = get_new_version()
        if metadata:
            metadata + ',' + new_version
        else:
            metadata = new_version
        kvs.put(key, (new_version, None))
        CAUSAL_HISTORY[new_version] = metadata
        new_data = json.dumps({"message":"Deleted successfully", "version":new_version, "causal-metadata":metadata})
        json_data = jsonify(message="Added successfully", version=new_version)
        json_data.data = new_data
        response = json_data, 200
    else:
        return jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404
    # Use Process to broadcast so we don't have to wait
    broadcast = Process(target = broadcast_request, args=(key, new_version, metadata, request.get_json()))
    broadcast.start()
    return response




# Delete a node from the system
@app.route( '/key-value-store-view', methods=['DELETE'] )
def deleteView():
    content  = request.get_json()
    view_to_delete = content.get("delete_replica", '') if content else ''
    if view_to_delete:
        if not SYS_VIEW[view_to_delete]:
            return jsonify( error="Socket address does not exist in the view", message="Error in DELETE"), 404
        else:
            SYS_VIEW[view_to_delete] = False
            return jsonify(message="Replica successfully deleted from view"), 200
    return jsonify(error="No delete data",), 404
            
@app.route( '/connect', methods=['GET'])
def connect():
    response = []
    response.append(CAUSAL_HISTORY)
    return json.dumps(response)

@app.route( '/kvs', methods=['GET'])
def get_kvs():
    response = []
    dictionary = kvs.get_dictionary()
    response.append(kvs.get_dictionary())
    return json.dumps(response)

# Return all the nodes in the system
@app.route( '/key-value-store-view', methods=['GET'])
def getView():
    global SYS_VIEW
    print("TESTING GET VIEW", file=sys.stderr)
    for replica in SYS_VIEW:
        url = "http://" + replica + "/connect"
        print(url, file=sys.stderr)
        try:
            if replica != SOCKET_ADDRESS:
                response = requests.get(url, timeout=TIMEOUT)
        except requests.exceptions.RequestException:
            delete_url = "http://" + SOCKET_ADDRESS + "/key-value-store-view"
            response = requests.delete(delete_url, json={"delete_replica":replica}, timeout=1)
            app.logger.info(response)

    view_string = ''
    print(SYS_VIEW, file=sys.stderr)
    for key in SYS_VIEW:
        if SYS_VIEW[key]:
            view_string = view_string + key + ','
    

    return jsonify( message="View retrieved successfully", view=view_string[:-1]), 200

# Add a Node to the system
@app.route( '/key-value-store-view', methods=['PUT'] )
def putView():
    content  = request.get_json()
    add_replica = content.get("replica_sender") if content else ''
    if add_replica:
        if SYS_VIEW[add_replica]:
            return jsonify(error="Socket address already exists in the view", 
                    message="Error in PUT"), 404
        else:
            SYS_VIEW[add_replica] = True
            print(SYS_VIEW)
            return jsonify(message="Replica added successfully to the view"), 201
    return jsonify(error="No put data"), 404

def append_causal(causal):
    for data in causal:
        if CAUSAL_HISTORY.get(data) is None:
            CAUSAL_HISTORY[data] = causal[data]

def append_kvs(kvs_import):
    for key in kvs_import:
        if not kvs.key_exists(key):
            kvs.put(key, kvs_import[key])
def setup(view_string, replica_name):
    view = view_string.split(",")
    SYS_VIEW[replica_name] = True
    print(view)

    crashed_replicas = []
    for replica in view:
        if SYS_VIEW[replica] == False:
            try:
                url = "http://" + replica + "/key-value-store-view"
                print("Put URL:" + url)
                response = requests.put(url, json={"replica_sender":replica_name}, timeout=TIMEOUT)
                SYS_VIEW[replica] = True
                print(SYS_VIEW)
                print(replica)
            except requests.exceptions.RequestException:
                app.logger.info("Unable to connect to replica %s", replica)
                crashed_replicas.append(replica)
    # Delete Crashed replicas
    app.logger.info("Deleting replicas from view")
    for replica in crashed_replicas:
        print("Crashed_replicas:")
        print(crashed_replicas)
        try:
            url = "http://" + replica + "/key-value-store-view"
            response = requests.delete(url, json={"delete_replica":replica}, timeout=1)
            app.logger.info(response)
        except requests.exceptions.RequestException:
            print("Here")
            
    for replica in SYS_VIEW:
        if replica != replica_name and SYS_VIEW[replica] == True:
            kvs_url = "http://" + replica + "/kvs"
            causal_url = "http://" + replica + "/connect"
            kvs_response = requests.get(kvs_url)
            causal_response = requests.get(causal_url)
            kvs_list = json.loads(kvs_response.content)
            causal_list = json.loads(causal_response.content)
            append_causal(causal_list[0])
            append_kvs(kvs_list[0])
            print(CAUSAL_HISTORY)
            print(kvs.get_dictionary())
with app.app_context():
    setup(environ['VIEW'], SOCKET_ADDRESS)
    

if __name__ == '__main__':
   #setup(environ['VIEW'], SOCKET_ADDRESS)
   hostPort = SOCKET_ADDRESS.split( ':' )
   #app.run(host=hostPort[0], port=hostPort[1])
   # app.logger.info("START")
   app.run(host='0.0.0.0', port=8080, debug=True)
