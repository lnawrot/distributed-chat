import sys
import time
import uuid
import socket, select
import json
import atexit

HOST = "localhost"
PORT = 50000
TIMEOUT = 2
BUFFER_SIZE = 1024

class Client:
    def __init__(self, name):
        self.running = True

        self.id = str(uuid.uuid4())
        self.name = name
        self.clients = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(TIMEOUT)

        self.register()
        print("get-clients || send <number> <message> || exit")
        print_prompt()
        self.events_loop()
        atexit.register(self.unregister)

    def events_loop(self):
        socket_list = [sys.stdin, self.socket]
        while self.running:
            sys.stdout.flush()
            read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [], 1)

            for s in read_sockets:
                #incoming message from remote server
                if s == self.socket:
                    data = s.recv(BUFFER_SIZE)
                    if not data:
                        print('Disconnected from node.')
                        self.unregister()
                    else :
                        resp = json.loads(data.decode('ascii'))
                        self.parse_response(resp)

                #user entered a message
                else :
                    cmd = sys.stdin.readline()
                    self.parse_command(cmd)

    def send(self, message):
        #print("Sending message:", message)
        print_prompt()

        try:
            self.socket.sendall(json.dumps(message).encode("ascii"))
        except socket.error as serr:
            print(serr.strerror, serr.errno)
            self.unregister()

    def parse_command(self, cmd):
        cmd = cmd.rstrip().split(" ")

        if cmd[0] == "get-clients":
            self.get_clients_list()

        if cmd[0] == "exit":
            self.unregister()

        if cmd[0] == "send":
            self.send_message(int(cmd[1]), " ".join(cmd[2:]))

    def parse_response(self, resp):
        if resp['type'] == "clients-list":
            self.received_clients_list(resp['clients'])
            return

        if resp['type'] == "message":
            self.received_message(resp)
            return

        if resp['type'] == "message-ack":
            return

        if resp['type'] == "message-fail":
            number =[i for i, x in enumerate(self.clients) if x['id'] == resp['client-from']['id']]
            if number:
                print("\nMessage sent by you to", resp['client-from']['name'], "(", number[0] ,") failed to arrive. Probably client is down.")
                print_prompt()

            return

    # ------------------ registration ---------------------------
    def register(self):
        try:
            self.socket.connect((HOST, PORT))
            message = {
                "type": "connect",
                "id": self.id,
                "name": self.name
            }
            self.send(message)

        except socket.error as serr:
            print("Connection to the node could not be established.")
            self.unregister()

    def unregister(self):
        self.running = False
        self.socket.close()

        try:
            sys.exit(0)
        except:
            print("Exiting.")

    # ------------------ commands ---------------------------
    def get_clients_list(self):
        message = {
            "type": "get-clients"
        }
        self.send(message)

    def send_message(self, client_number, text):
        if client_number < 0 or client_number >= len(self.clients):
            print("Wrong client number.")
            print_prompt()
            return

        client_id = self.clients[client_number]['id']
        client_name = self.clients[client_number]['name']

        message = {
            "type": 'message',
            "client-from": {
                "id": self.id,
                "name": self.name
            },
            "client-to": {
                "id": client_id,
                "name": client_name
            },
            "message": text,
            "timestamp": get_timestamp()
        }
        self.send(message)

        print("Message sent to", client_name, "(", client_number ,"):", text)
        print_prompt()

    # ------------------ responses ---------------------------
    def received_message(self, message):
        number =[i for i, x in enumerate(self.clients) if x['id'] == message['client-from']['id']]
        if not number:
            print("\nMessage from", message['client-from']['name'], ":", message['message'])
        else:
            print("\nMessage from", message['client-from']['name'], "(", number ,"):", message['message'])

        print_prompt()

    def received_clients_list(self, list):
        self.clients = list

        # remove myself from list of available clients
        index = [i for i, x in enumerate(self.clients) if x['id'] == self.id][0]
        del self.clients[index]

        print("\nList of available clients:")
        for i, client in enumerate(self.clients):
            print("  " + str(i) + ". " + client['name'])

        print_prompt()


def get_timestamp():
    return int(round(time.time() * 1000))

def print_prompt():
    sys.stdout.write("cmd>")
    sys.stdout.flush()

if len(sys.argv) != 2:
    print("Proper script call: python3 client.py your_name")
    exit(1)

client = Client(sys.argv[1])
