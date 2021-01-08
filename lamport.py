import redis
import json


_REDIS_HOST = '127.0.0.1'
_REDIS_PORT = 6379
_LOCK_TIMEOUT = 5


class Node:
    def __init__(self, redis_cli, name):
        self._r = redis_cli
        self._node_name = name
        # The logical timestamp of this node
        self._timestamp = 0

        with self._r.lock('nodes-lock', blocking_timeout=_LOCK_TIMEOUT):
            self._r.lpush('nodes', self._node_name)

    def __del__(self):
        with self._r.lock('nodes-lock', blocking_timeout=_LOCK_TIMEOUT):
            self._r.lrem('nodes', 0, self._node_name)

    @property
    def name(self):
        return self._node_name

    @property
    def timestamp(self):
        return self._timestamp
        
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
            print("Independent event happened in this node!")
        else:
            print("Received message from ", sender)
            self._increment_timestamp(parsed_data["timestamp"])

        return parsed_data["data"]

    def receive_direct_message(self, message):
        """ Handles direct messsages for this node, i.e messages in the {node_name}
        channel in redis

        Args:
            message (dict): Message from another node, this comes from the redis lib
        """
        try:
            self._run_middleware(message)
        except ValueError:
            return

    def receive_broadcast_message(self, message):
        """ Handles messages that were broadcastes to all nodes, i.e messages in the "all"
        channel in redis

        Args:
            message (dict): Message from another node, this comes from the redis lib
        """
        try:
            self._run_middleware(message)
        except ValueError:
            return

    def send_message(self, target, data):
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


def _get_available_nodes(r):
    with r.lock('nodes-lock', blocking_timeout=_LOCK_TIMEOUT):
        nodes = r.lrange('nodes', 0, -1)
        if not nodes:
            nodes = []
        return [node.decode('utf8') for node in nodes]


def main():
    r = redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT)

    input_name = input("Type a name for this node: ")
    node = Node(r, input_name)

    p = r.pubsub()
    p.subscribe(**{
        "all": node.receive_broadcast_message,
        node.name: node.receive_direct_message
    })
    p.run_in_thread(sleep_time=0.001)

    try:
        print("""
        Event loop started. Type the name of a node to send a event.
        If you type the name of this node({}), then a independent event for this node will happen.
        Type 'timestamp' for the current logical timestamp.
        List of nodes:""".format(node.name))
        for name in _get_available_nodes(r):
            print("\t-{}".format(name))
        while True:
            input_command = input(f'nodes{_get_available_nodes(r)}: ')
            if input_command in _get_available_nodes(r):
                node.send_message(input_command, {})
            elif input_command == 'timestamp':
                print("Current timestamp is:", node.timestamp)
    finally:
        del node
        p.unsubscribe()
        p.close()


if __name__ == '__main__':
    main()