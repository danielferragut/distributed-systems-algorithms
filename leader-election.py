import redis
import sys
import json
import os
import threading

redis_host = '127.0.0.1'
redis_port = 6379

# The logical timestamp of this node
timestamp = 0
# The name used to send messages to this node
node_name = None
leader_name = None

input_name = ""
while not input_name.isalnum():
    input_name = input("Type a valid name for this node: ")
node_name = input_name

election_request_denials = 0


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


def receive_direct_message(message):
    data = {}
    try:
        data = run_middleware(message)
    except ValueError:
        return
    if data['type'] == 'election denial':
        global election_request_denials
        election_request_denials += 1


def receive_broadcast_message(message):
    data = {}
    try:
        data = run_middleware(message)
    except ValueError:
        return
    if data['type'] == 'election':
        print("{} sent an election request!".format(data['leader_candidate']))
        if data['priority'] >= os.getpid():
            print("Their priority of {} is bigger than mine of {}, so I accept the new leader!".format(data['priority'], os.getpid()))
            global leader_name
            leader_name = data['leader_candidate']
        else:
            print("Their priority of {} is smaller than mine of {}, so I refuse this candidate!".format(data['priority'], os.getpid()))
            send_election_denial_message(data['leader_candidate'])
            if leader_name != node_name:
                start_election()


def send_message(target, data):
    global r
    r.publish(target, json.dumps({
        "timestamp": timestamp,
        "sender": node_name,
        "data": data,
    }))
    print("Sent message to", target)
    increment_timestamp()

def send_election_denial_message(target):
    send_message(target, {
        "type": "election denial",
        "priority": os.getpid()
    })

def check_election_responses():
    global leader_name, election_request_denials, node_name
    print("Election timeout reached, checking results")
    if election_request_denials == 0:
        print("Election ended and I am the leader!")
        leader_name = node_name
        election_request_denials = 0
    else:
        print("Got at least one denial, I lost the election :(")


def start_election():
    if leader_name == node_name:
        print("I ({}) am already the leader, there is no need for an election".format(node_name))
        return
    print("Starting election with me as the leader!")
    send_message("all", {
        "type": "election",
        "leader_candidate": node_name,
        "priority": os.getpid(),
    })
    thread = threading.Timer(1, check_election_responses)
    thread.start()

def print_leader():
    if leader_name == None:
        print("There is no leader at the moment! Election may be ongoing")
    else:
        print("Current leader is", leader_name)


r = redis.Redis(host=redis_host, port=redis_port)
p = r.pubsub()
p.subscribe(**{"all": receive_broadcast_message,
               node_name: receive_direct_message})
thread = p.run_in_thread(sleep_time=0.001)

print("""
Event loop started. Type the LEADER to see the current leader or ELECTION to start a new election.
Type 'timestamp' for the current logical timestamp.""")
while True:
    input_command = input().lower()
    if input_command == 'timestamp':
        print("Current timestamp is:", timestamp)
    elif input_command == 'election':
        start_election()
    elif input_command == 'leader':
        print_leader()
