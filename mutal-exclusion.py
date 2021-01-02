import redis
import sys
import json

redis_host = '127.0.0.1'
redis_port = 6379


available_nodes = set(['node1', 'node2', 'node3'])

resource_list = [
    "a",
    "b",
    "c"
]

resource_locks = {}
resource_lock_request_timestamps = {}
resource_waiting_nodes = {}
resource_ok_list = {}
for resource in resource_list:
    resource_locks[resource] = False
    resource_lock_request_timestamps[resource] = None
    resource_waiting_nodes[resource] = set()
    resource_ok_list[resource] = set()


# The logical timestamp of this node
timestamp = 0

print("Choose from these node options:")
for name in available_nodes:
    print("\t-{}".format(name))

input_name = ""
while input_name not in available_nodes:
    input_name = input("Type a valid name for this node: ")

# The name used to send messages to this node
node_name = input_name

other_nodes = available_nodes - set([input_name])


def increment_timestamp(event_timestamp=0):
    """ Updates the timestamp to the event timestamp if it is bigger and
    increments the timestamp by one

    Args:
        event_timestamp (int, optional): The event_timestamp. Defaults to 0.
    """
    global timestamp
    timestamp = max([timestamp, event_timestamp]) + 1
    print("Timestamp is now:", timestamp)


def run_middleware(message):
    global timestamp
    global node_name
    data = message["data"]
    parsed_data = {}
    try:
        parsed_data = json.loads(data)
    except json.JSONDecodeError:
        print("Received a message not in JSON format, ignoring it: ", data)
        raise ValueError('Ignore Message')
    sender = parsed_data["sender"]
    if sender == node_name:
        raise ValueError('Ignore Message')
    else:
        print("Received message from ", sender)
        increment_timestamp(parsed_data["timestamp"])

    return parsed_data["data"]


def send_ok_message(target, resource):
    send_message(target, {
        "type": "resource lock response",
        "response": "ok",
        "resource": resource,
        "sender": node_name,
    })


def handle_lock_request(data):
    global timestamp
    resource = data['resource']
    sender = data['sender']
    sender_timestamp = data['timestamp']

    # If I am the one that has the resource lock
    if resource_locks[resource]:
        resource_waiting_nodes[resource].add(sender)
        print("""{sender} wants to use {resource}, but I am already using it.
        Put {sender} on the waiting list for resource {resource}. Current queue for resource {resource} is: {queue}""".format_map(
            {
                "sender": sender,
                "resource": resource,
                "queue": resource_waiting_nodes[resource]
            }
        ))

    # If I am not trying to access the lock for the request at the moment
    elif resource_lock_request_timestamps[resource] == None:
        send_ok_message(sender, resource)
        print("""{sender} wants to use {resource}, sent ok as I am not using it or requesting it!""".format_map(
            {
                "sender": sender,
                "resource": resource,
            }
        ))
    # If I am trying to access the same resource but do not have the lock at the moment
    else:
        request_timestamp = resource_lock_request_timestamps[resource]
        print("""{sender} wants to use {resource}, but I also requested the same resource!""".format_map(
            {
                "sender": sender,
                "resource": resource,
            }
        ))
        if request_timestamp <= sender_timestamp:
            resource_waiting_nodes[resource].add(sender)
            print(""" My timestamp ({timestamp}) is smaller than theirs ({sender_timestamp}), so I got priority!
            Put {sender} on the waiting list for resource {resource}. Current queue for resource {resource} is: {queue}""".format_map(
                {
                    "sender": sender,
                    "resource": resource,
                    "queue": resource_waiting_nodes[resource],
                    "timestamp": request_timestamp,
                    "sender_timestamp": sender_timestamp,
                }
            ))
        elif request_timestamp > sender_timestamp:
            # The other node got priority
            print(""" Their timestamp ({sender_timestamp}) is smaller than mine ({timestamp}), so they got priority!
            Sending ok message to {sender}!""".format_map(
                {
                    "sender": sender,
                    "timestamp": request_timestamp,
                    "sender_timestamp": sender_timestamp,
                }
            ))
            send_ok_message(sender, resource)



def handle_lock_response(data):
    resource = data['resource']
    sender = data['sender']
    print("Got ok for resource {} from {}".format(
        resource, sender))
    resource_ok_list[resource].add(sender)

    if resource_ok_list[resource] == other_nodes:
        print("I got all the necessary approvals for resource {}! I can now write on it!".format(resource))
        resource_locks[resource] = True


def receive_direct_message(message):
    data = {}
    try:
        data = run_middleware(message)
    except ValueError:
        return
    message_type = data['type']
    if message_type == 'resource lock response':
        handle_lock_response(data)


def receive_broadcast_message(message):
    data = {}
    try:
        data = run_middleware(message)
    except ValueError:
        return
    message_type = data['type']
    if message_type == 'resource lock request':
        handle_lock_request(data)


def send_message(target, data):
    global r
    increment_timestamp()
    r.publish(target, json.dumps({
        "timestamp": timestamp,
        "sender": node_name,
        "data": data
    }))
    print("Sent message to", target)


def operate_over_resource(split_input):
    global r
    operation = split_input[0]
    resource_name = split_input[1]
    legal_operations = ['read', 'lock', 'write', 'free']
    if operation not in legal_operations or resource_name not in resource_list:
        return

    if operation == 'read':
        print('Current value for resource {} is {}'.format(
            resource_name, r.get(resource_name)))
    elif operation == 'write':
        if not resource_locks[resource_name]:
            print("You do not have the permission to write the {} resource, type LOCK {} to get lock".format(
                resource_name, resource_name))
            return
        else:
            value = split_input[2]
            r.set(resource_name, value)
            print("The {} resource is now {}!".format(resource_name, value))
    elif operation == 'free':
        resource_locks[resource_name] = False
        resource_lock_request_timestamps[resource_name] = None
        print("Freed lock from resource", resource_name)
        for node in resource_waiting_nodes[resource_name]:
            send_ok_message(node, resource_name)
        resource_waiting_nodes[resource_name] = []

    elif operation == 'lock':
        if resource_lock_request_timestamps[resource_name] != None:
            print('ERROR: Already requested a lock for this resource, still waiting for approval')
            return
        resource_lock_request_timestamps[resource_name] = timestamp
        send_message('all', {
            "type": "resource lock request",
            "resource": resource_name,
            "timestamp": timestamp,
            "sender": node_name
        })
        print("Sent lock request to all nodes!")


r = redis.Redis(host=redis_host, port=redis_port)
p = r.pubsub()
p.subscribe(**{"all": receive_broadcast_message,
               node_name: receive_direct_message})
thread = p.run_in_thread(sleep_time=0.001)

print("""
Event loop started. Type LOCK [resource_name] or READ [resource_name] to operate over a resource.
 - READ operation will always read the current value of a certain distributed resource
 - LOCK operation will attempt to lock the resource to this node with mutual exclusion.
    - If another node has a lock with the same resource, it may take a while before they free it
    - After a resource is locked for this node, you can type WRITE [resource_name] [value], to write a new value for the resource
    - When you are done using a resource, type FREE [resource_name] to free the lock so others may use it!
Type 'timestamp' for the current logical timestamp.
List of resources:""")
for resource in resource_list:
    print("\t-{}".format(resource))

while True:
    input_str = input().lower()
    split_input = input_str.split()
    if input_str == 'timestamp':
        print("Current timestamp is:", timestamp)
    elif len(split_input) > 1:
        operate_over_resource(split_input)
