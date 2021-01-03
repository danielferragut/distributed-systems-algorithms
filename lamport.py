import redis
import sys
import json

redis_host = '127.0.0.1'
redis_port = 6379

# Available nodes in for naming in the cluster
available_nodes = set(['node1', 'node2', 'node3'])

# The logical timestamp of this node
timestamp = 0
# The name used to send messages to this node
node_name = None

print("Choose from these node options:")
for name in available_nodes:
    print("\t-{}".format(name))

input_name = ""
while input_name not in available_nodes:
    input_name = input("Type a valid name for this node: ")
node_name = input_name


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
    """ Simulates a middleware in the application layer, gets a message
    and operate over the headers, returning only the data to the original
    function

    Args:
        message (dict): The message that comes directly from the redis lib

    Raises:
        ValueError: If a value error is raised, the messaged is supposed to be
        ignored

    Returns:
        dict: The data without the middleware headers
    """
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
        print("Independent event happened in this node!")
    else:
        print("Received message from ", sender)
        increment_timestamp(parsed_data["timestamp"])

    return parsed_data["data"]


def receive_direct_message(message):
    """ Handles direct messsages for this node, i.e messages in the {node_name}
    channel in redis

    Args:
        message (dict): Message from another node, this comes from the redis lib
    """
    try:
        run_middleware(message)
    except ValueError:
        return


def receive_broadcast_message(message):
    """ Handles direct messsages for this node, i.e messages in the "all"
    channel in redis

    Args:
        message (dict): Message from another node, this comes from the redis lib
    """
    try:
        run_middleware(message)
    except ValueError:
        return

def send_message(target, data):
    """ Wrapper that publishes a message on a target(channel in redis).
    Increments logical timestamp by one
    Args:
        target (string): Target (channel in redis) that this message is directed to
        data (dict): Data that is going to be sent with the messsage (normally has a 'type' field to categorize it)
    """
    global r
    r.publish(target, json.dumps({
        "timestamp": timestamp,
        "sender": node_name,
        "data": data,
    }))
    print("Sent message to", target)
    increment_timestamp()


r = redis.Redis(host=redis_host, port=redis_port)
p = r.pubsub()
p.subscribe(**{"all": receive_broadcast_message,
               node_name: receive_direct_message})
thread = p.run_in_thread(sleep_time=0.001)

print("""
Event loop started. Type the name of a node to send a event.
If you type the name of this node({}), then a independent event for this node will happen.
Type 'timestamp' for the current logical timestamp.
List of nodes:""".format(node_name))
for name in available_nodes:
    print("\t-{}".format(name))
while True:
    input_command = input()
    if input_command in available_nodes:
        send_message(input_command, {})
    elif input_command == 'timestamp':
        print("Current timestamp is:", timestamp)
