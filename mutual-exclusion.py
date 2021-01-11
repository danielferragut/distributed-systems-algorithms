import redis
import json
import time


_REDIS_HOST = '127.0.0.1'
_REDIS_PORT = 6379


class Resource():
    def __init__(self, name, is_list=False):
        self.name = name
        self.has_lock = False
        self.lock_request_timestamp = None
        self.waiting_nodes = set()
        self.ok_list = set()
        self.lock_request_other_nodes = set()
        self.is_list = is_list

    def has_all_necessary_approvals(self):
        return self.lock_request_other_nodes.issubset(self.ok_list)

    def get_value(self, r):
        if self.is_list:
            values = r.lrange(self.name, 0, -1)
            if not values:
                values = []
            return [value.decode('utf8') for value in values]
        else:
            return r.get(self.name)

    def lock(self):
        print("Locking resource {}! I can now write on it!".format(
            self.name))
        self.has_lock = True
        self.ok_list = set()


class Node:
    def __init__(self, redis_cli, name, resources):
        self._r = redis_cli
        self.resources = resources
        self._node_name = name
        # The logical timestamp of this node
        self._timestamp = 0
        self._r.lpush('nodes', self._node_name)

    def destroy(self, thread):
        nodes_resource = self.resources['nodes']
        self.operate_over_resource(['lock', 'nodes'])
        while not nodes_resource.has_lock:
            print("Waiting for lock...")
            time.sleep(0.3)
        self._r.lrem('nodes', 0, self._node_name)
        for resource in self.resources.values():
            self.operate_over_resource(['free', resource.name])
        thread.stop()

    @property
    def name(self):
        return self._node_name

    @property
    def timestamp(self):
        return self._timestamp

    def _get_other_nodes(self):
        """ Returns a set with all the node names in the cluster that aren't this one

        Returns:
            set(string): The set with the names of other nodes
        """
        nodes = self._r.lrange('nodes', 0, -1)
        if not nodes:
            nodes = []

        return set([node.decode('utf8') for node in nodes if node.decode('utf8') != self.name])

    def _increment_timestamp(self, event_timestamp=0):
        """ Updates the timestamp to the event timestamp if it is bigger and
        increments the timestamp by one

        Args:
            event_timestamp (int, optional): The event_timestamp. Defaults to 0.
        """
        self._timestamp = max([self._timestamp, event_timestamp]) + 1
        print("Timestamp is now:", self._timestamp)

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
            self._increment_timestamp(parsed_data["timestamp"])

        return parsed_data["data"]

    def _receive_direct_message(self, message):
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
        message_type = data['type']
        if message_type == 'resource lock response':
            self._handle_lock_response(data)

    def _receive_broadcast_message(self, message):
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
        message_type = data['type']
        if message_type == 'resource lock request':
            self._handle_lock_request(data)

    def _handle_lock_request(self, data):
        """ Function that takes care of any request for locks on shared resources

        Args:
            data (dict): The data contained in the message of the request
        """
        resource_name = data['resource']
        sender = data['sender']
        sender_timestamp = data['timestamp']

        # If resource is not present in the shared resources list
        if resource_name not in self.resources.keys():
            return
        resource = self.resources[resource_name]

        # If I am the one that has the resource lock
        if resource.has_lock:
            resource.waiting_nodes.add(sender)
            print("{sender} wants to use {resource}, but I am already using it.\nPut {sender} on the waiting list for resource {resource}.\nCurrent queue for resource {resource} is: {queue}".format_map(
                {
                    "sender": sender,
                    "resource": resource.name,
                    "queue": resource.waiting_nodes
                }
            ))

        # If I am not trying to access the lock for the request at the moment
        elif resource.lock_request_timestamp == None:
            self._send_ok_message(sender, resource)
            print("""{sender} wants to use {resource}, sent ok as I am not using it or requesting it!""".format_map(
                {
                    "sender": sender,
                    "resource": resource.name,
                }
            ))
        # If I am trying to access the same resource but do not have the lock at the moment
        else:
            request_timestamp = resource.lock_request_timestamp
            print("""{sender} wants to use {resource}, but I also requested the same resource!""".format_map(
                {
                    "sender": sender,
                    "resource": resource.name,
                }
            ))
            if request_timestamp <= sender_timestamp:
                resource.waiting_nodes.add(sender)
                print("""My timestamp ({timestamp}) is smaller than theirs ({sender_timestamp}), so I got priority!
                Put {sender} on the waiting list for resource {resource}.
                Current queue for resource {resource} is: {queue}""".format_map(
                    {
                        "sender": sender,
                        "resource": resource.name,
                        "queue": resource.waiting_nodes,
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
                self._send_ok_message(sender, resource)

    def _handle_lock_response(self, data):
        """Handles responses related to previous lock requests

        Args:
            data (dict): The data from the response message
        """
        resource_name = data['resource']
        sender = data['sender']
        # If invalid resource name, ignore
        if resource_name not in self.resources.keys():
            return
        resource = self.resources[resource_name]
        print("Got ok for resource {} from {}".format(
            resource.name, sender))
        resource.ok_list.add(sender)

        if resource.has_all_necessary_approvals():
            print("I got all necessary approvals for resource {}!".format(
                resource.name))
            resource.lock()

    def _send_message(self, target, data):
        """ Wrapper that publishes a message on a target(channel in redis).
        Increments logical timestamp by one
        Args:
            target (string): Target (channel in redis) that this message is directed to
            data (dict): Data that is going to be sent with the messsage (normally has a 'type' field to categorize it)
        """
        self._r.publish(target, json.dumps({
            "timestamp": self.timestamp,
            "sender": self.name,
            "data": data,
        }))
        print("Sent message to", target)
        self._increment_timestamp()

    def _send_ok_message(self, target, resource):
        """ Send an approval message to a target, allowing the lock on a resource

        Args:
            target (string): The node target of this message (the channel on redis pubsub)
            resource (Resource): Instance of the resource being allowed
        """
        self._send_message(target, {
            "type": "resource lock response",
            "response": "ok",
            "resource": resource.name,
            "sender": self._node_name,
        })

    def operate_over_resource(self, split_input):
        """ Helper function that deals with the 4 operations over a shared resource

        Args:
            split_input ([string]): The user input split by spaces
            resource_list ([string]): The dict for all resource instances
        """
        operation = split_input[0]
        resource_name = split_input[1]
        legal_operations = ['read', 'lock', 'write', 'free']
        if operation not in legal_operations or resource_name not in self.resources.keys():
            return

        resource = self.resources[resource_name]

        if operation == 'read':
            print('Current value for resource {} is {}'.format(
                resource.name, resource.get_value(self._r)))

        elif operation == 'write':
            if not resource.has_lock:
                print("You do not have the permission to write the {} resource, type LOCK {} to get lock".format(
                    resource.name, resource.name))
                return
            else:
                value = split_input[2]
                self._r.set(resource.name, value)
                print("The {} resource is now {}!".format(resource.name, value))

        elif operation == 'free':
            resource.has_lock = False
            resource.lock_request_timestamp = None
            print("Freed lock from resource", resource.name)
            for node in resource.waiting_nodes:
                self._send_ok_message(node, resource)
            resource.waiting_nodes = set()

        elif operation == 'lock':
            if resource.lock_request_timestamp != None:
                print(
                    'ERROR: Already requested a lock for this resource, still waiting for approval')
                return
            resource.lock_request_timestamp = self.timestamp
            resource.lock_request_other_nodes = self._get_other_nodes()

            # If this node is alone in the cluster
            if resource.lock_request_other_nodes == set():
                print(
                    "There is no one else in the cluster, getting lock for resource", resource.name)
                resource.lock()
            else:
                self._send_message('all', {
                    "type": "resource lock request",
                    "resource": resource_name,
                    "timestamp": self.timestamp,
                    "sender": self._node_name
                })
                print("Sent lock request to all nodes!")


def main():
    r = redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT)

    resources = {
        "nodes": Resource("nodes", is_list=True),
        "a": Resource("a"),
        "b": Resource("b"),
        "c": Resource("c")
    }

    def _print_resources():
        for resource in resources.values():
            print("\t-{}".format(resource.name))

    input_name = input("Type a name for this node: ")
    node = Node(r, input_name, resources)

    p = r.pubsub()
    p.subscribe(**{
        "all": node._receive_broadcast_message,
        node.name: node._receive_direct_message
    })
    thread = p.run_in_thread(sleep_time=0.001)

    try:
        print("""
        Event loop started. Type LOCK [resource_name] or READ [resource_name] to operate over a resource.
        - READ operation will always read the current value of a certain distributed resource
        - LOCK operation will attempt to lock the resource to this node with mutual exclusion.
            - If another node has a lock with the same resource, it may take a while before they free it
            - After a resource is locked for this node, you can type WRITE [resource_name] [value], to write a new value for the resource
            - When you are done using a resource, type FREE [resource_name] to free the lock so others may use it!
        Type 'timestamp' for the current logical timestamp.
        Type 'list' for a list of the shared resources.
        Type 'quit' to kill this process.
        List of resources:""")
        _print_resources()
        while True:
            try:
                input_command = input().lower()
                split_input = input_command.split()
                if input_command == 'timestamp':
                    print("Current timestamp is:", node.timestamp)
                elif input_command == 'list':
                    _print_resources()
                elif input_command == 'quit':
                    break
                elif len(split_input) > 1:
                    node.operate_over_resource(split_input)
            except KeyboardInterrupt:
                break
    finally:
        node.destroy(thread)

if __name__ == '__main__':
    main()
