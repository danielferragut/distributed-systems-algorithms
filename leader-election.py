import redis
import sys
import json
import os
import threading

redis_host = '127.0.0.1'
redis_port = 6379

# The name used to send messages to this node
node_name = None
leader_name = None

input_name = ""
while not input_name.isalnum():
    input_name = input("Type a valid name for this node: ")
node_name = input_name

election_request_denials = 0

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
    return parsed_data["data"]


def receive_direct_message(message):
    """ Handles direct messsages for this node, i.e messages in the {node_name}
    channel in redis

    Args:
        message (dict): Message from another node, this comes from the redis lib
    """
    data = {}
    try:
        data = run_middleware(message)
    except ValueError:
        return
    if data['type'] == 'election denial':
        global election_request_denials
        election_request_denials += 1


def receive_broadcast_message(message):
    """ Handles that were broadcasted to all nodes, i.e messages in the "all"
    channel in redis

    Args:
        message (dict): Message from another node, this comes from the redis lib
    """
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
    """ Wrapper that publishes a message on a target(channel in redis).
    Args:
        target (string): Target (channel in redis) that this message is directed to
        data (dict): Data that is going to be sent with the messsage (normally has a 'type' field to categorize it)
    """
    global r
    r.publish(target, json.dumps({
        "sender": node_name,
        "data": data,
    }))
    print("Sent message to", target)

def send_election_denial_message(target):
    """ Wrapper that sends a election request denial to a leader candidate.
    This means that the leader candidate will not be a leader and this node
    will have started a new election.
    Args:
        target (string): Target (channel in redis) that this message is directed to (i.e the leader candidate name)
    """
    send_message(target, {
        "type": "election denial",
        "priority": os.getpid()
    })

def check_election_responses():
    """ Async function that will run after a timeout period after an election
    has started. This function will check for election denials, if there is
    at least one, the election is lost, else the election is won.
    """
    global leader_name, election_request_denials, node_name
    print("Election timeout reached, checking results")
    if election_request_denials == 0:
        print("Election ended and I am the leader!")
        leader_name = node_name
        election_request_denials = 0
    else:
        print("Got at least one denial, I lost the election :(")


def start_election():
    """ Starts an election with this node running for leader. Will send an
    election request to all nodes and will check asynchronously if the election
    was won after a timeout period.
    """
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
    """ Prints the current leader
    """
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
Event loop started. Type the LEADER to see the current leader or ELECTION to start a new election.""")
while True:
    input_command = input().lower()
    if input_command == 'election':
        start_election()
    elif input_command == 'leader':
        print_leader()
