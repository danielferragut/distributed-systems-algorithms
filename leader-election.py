import redis
import sys
import json
import os
import threading
import time

_REDIS_HOST = '127.0.0.1'
_REDIS_PORT = 6379
_TIMEOUT_PERIOD = 0.5

class Node:
    def __init__(self, redis_cli, name, priority):
        self._r = redis_cli
        self._node_name = name
        self.election_request_denials = 0
        self.priority = priority
        self.leader_name = None
        self.leader_priority = None
        self.other_nodes = set()

    def destroy(self, thread):
        self._send_message('all', {
            "type": "node disconnect",
            "sender": self.name
        })
        thread.stop()

    @property
    def name(self):
        return self._node_name

    def _run_middleware(self, message):
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
        data = message["data"]
        parsed_data = {}
        try:
            parsed_data = json.loads(data)
        except json.JSONDecodeError:
            print("Received a message not in JSON format, ignoring it: ", data)
            raise ValueError('Ignore Message')
        sender = parsed_data["sender"]
        if sender == self.name:
            raise ValueError('Ignore Message')
        else:
            print("Received message from ", sender)

        return parsed_data["data"]

    def enter_cluster(self):
        """ Enters the cluster by sending a message to all nodes and expecting
        a response from the leader
        """
        print("Entering the cluster! Asking for the leader...")
        self._send_message('all', {
            'type': 'new node',
            'sender': self.name,
        })
        thread = threading.Timer(_TIMEOUT_PERIOD, self._check_for_leader_response)
        thread.start()

    def _check_for_leader_response(self):
        """Async function that checks for the leader welcome message. If
        there is no leader, a election is started
        """
        if self.leader_name == None:
            print("There is no leader in the cluster! Starting an election!")
            self.start_election()
        else:
            print("Leader {} responded! Entered cluster successfully".format(self.leader_name))
            if self.priority > self.leader_priority:
                print("The leader priority, however, is smaller than mine! Starting an election!")
                self.start_election()


    def receive_direct_message(self, message):
        """ Handles direct messsages for this node, i.e messages in the {node_name}
        channel in redis

        Args:
            message (dict): Message from another node, this comes from the redis lib
        """
        data = {}
        try:
            data = self._run_middleware(message)
        except ValueError:
            return
        if data['type'] == 'election denial':
            self.election_request_denials += 1
        elif data['type'] == 'leader welcome':
            self.leader_name = data['sender']
            self.leader_priority = data['priority']
            self.other_nodes = set([self.leader_name]).union(
                set(data['other_nodes']))

    def receive_broadcast_message(self, message):
        """ Handles that were broadcasted to all nodes, i.e messages in the "all"
        channel in redis

        Args:
            message (dict): Message from another node, this comes from the redis lib
        """
        data = {}
        try:
            data = self._run_middleware(message)
        except ValueError:
            return
        if data['type'] == 'election':
            print("{} sent an election request!".format(
                data['leader_candidate']))
            if data['priority'] >= self.priority:
                print("Their priority of {} is bigger than mine of {}, so I accept the new leader!".format(
                    data['priority'], self.priority))
            else:
                print("Their priority of {} is smaller than mine of {}, so I refuse this candidate!".format(
                    data['priority'], self.priority))
                self._send_election_denial_message(data['leader_candidate'])
                if self.leader_name != self.name:
                    self.start_election()
        elif data['type'] == 'new node':
            new_node = data['sender']
            print("New node {} in arrived in cluster!".format(new_node))
            if self.leader_name == self.name:
                print("As I am the leader, welcoming {} to the cluster...".format(new_node))
                self._send_message(new_node, {
                    'type': 'leader welcome',
                    'sender': self.name,
                    'other_nodes': list(self.other_nodes),
                    'priority': self.priority
                })
            self.other_nodes.add(new_node)
        elif data['type'] == 'new leader':
            self.leader_name = data['sender']
        elif data['type'] == 'node disconnect':
            self.other_nodes.remove(data['sender'])

    def _send_message(self, target, data):
        """ Wrapper that publishes a message on a target(channel in redis).
        Args:
            target (string): Target (channel in redis) that this message is directed to
            data (dict): Data that is going to be sent with the messsage (normally has a 'type' field to categorize it)
        """
        self._r.publish(target, json.dumps({
            "sender": self.name,
            "data": data,
        }))
        print("Sent message to", target)

    def _send_election_denial_message(self, target):
        """ Wrapper that sends a election request denial to a leader candidate.
        This means that the leader candidate will not be a leader and this node
        will have started a new election.
        Args:
            target (string): Target (channel in redis) that this message is directed to (i.e the leader candidate name)
        """
        self._send_message(target, {
            "type": "election denial",
            "priority": self.priority
        })

    def start_election(self):
        """ Starts an election with this node running for leader. Will send an
        election request to all nodes and will check asynchronously if the election
        was won after a timeout period.
        """
        if self.leader_name == self.name:
            print("I ({}) am already the leader, there is no need for an election".format(
                self.name))
            return
        print("Starting election with me as the leader!")
        self._send_message("all", {
            "type": "election",
            "leader_candidate": self.name,
            "priority": self.priority,
        })
        thread = threading.Timer(_TIMEOUT_PERIOD, self._check_election_responses)
        thread.start()

    def _check_election_responses(self):
        """ Async function that will run after a timeout period after an election
        has started. This function will check for election denials, if there is
        at least one, the election is lost, else the election is won.
        """
        print("Election timeout reached, checking results")
        if self.election_request_denials == 0:
            print("Election ended and I am the leader!")
            self.leader_name = self.name
            self.election_request_denials = 0
            self._send_message('all', {
                "type": "new leader",
                "sender": self.name
            })
        else:
            print("Got at least one denial, I lost the election :(")

    def print_leader(self):
        """ Prints the current leader
        """
        print("Current leader is", self.leader_name)


def main():
    r = redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT)

    input_name = input("Type a name for this node: ")
    node = Node(r, input_name, int(sys.argv[1]))

    p = r.pubsub()
    p.subscribe(**{
        "all": node.receive_broadcast_message,
        node.name: node.receive_direct_message
    })
    thread = p.run_in_thread(sleep_time=0.001)
    node.enter_cluster()
    # Wait for node to enter the cluster
    time.sleep(_TIMEOUT_PERIOD * 4)

    try:
        print("""Event loop started:
        Type the LEADER to see the current leader;
        Type ELECTION to start a new election;
        Type LIST to see current nodes on the cluster;
        Type QUIT to stop the program;""")
        while True:
            try:
                input_command = input().lower()
                if input_command == 'election':
                    node.start_election()
                elif input_command == 'leader':
                    node.print_leader()
                elif input_command == 'list':
                    print("Other nodes in cluster are:")
                    for other_node in node.other_nodes:
                        print("\t-{}{}".format(
                            "[LEADER]: " if other_node == node.leader_name else "",
                            other_node))

                elif input_command == 'quit':
                    break
            except KeyboardInterrupt:
                break
    finally:
        node.destroy(thread)


if __name__ == '__main__':
    main()
