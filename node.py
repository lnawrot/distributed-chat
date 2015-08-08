import sys
import socket, select
import json
import server
import atexit
import struct

HOST = ""
CLIENT_PORT = 50000
NODE_PORT = 45000
TIMEOUT = 2
SELECT_TIMEOUT = 0.1
BUFFER_SIZE = 1024

class Node:
    def __init__(self, hosts, ip = None):
        self.clients_node = ClientsNode(self)
        self.nodes_node = NodesNode(self, hosts, ip)
        self.server = None

        self.events_loop()

    def events_loop(self):
        while 1:
            self.clients_node.events_loop()
            self.nodes_node.events_loop()
            sys.stdout.flush()


# ====================================================================================
# ==========================     NODES NODE     ======================================
# ====================================================================================
class NodesNode:
    def __init__(self, n, hosts, ip = None):
        print("NN. Init")
        self.node = n
        self.hosts = hosts

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(TIMEOUT)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(ip)

        self.ip = self.socket.getsockname()[0]

        self.server_ip = None
        self.election_number = 0

        self.connection_list = [self.socket]
        self.connections_ips = []
        self.do_election()
        atexit.register(self.disconnect)

    def bind(self, ip = None):
        if ip == None:
            ip = HOST

        try:
            self.socket.bind((ip, NODE_PORT))
            self.socket.listen(4)
        except socket.error as serr:
            print("NN.Bind", serr, serr.errno)

    def events_loop(self):
        read_sockets,write_sockets,error_sockets = select.select(self.connection_list,[],[], SELECT_TIMEOUT)

        for s in read_sockets:
            #New connection
            if s == self.socket:
                new_connection, addr = self.socket.accept()
                if addr[0] != self.ip:
                    try:
                        index = [i for i, x in enumerate(self.connection_list[1:]) if x.getpeername()[0] == new_connection.getpeername()[0]]
                        if index:
                            print("NN. Removing node:", self.connection_list[index[0] + 1])
                            self.connection_list[index[0] + 1].shutdown(socket.SHUT_RDWR)
                            self.connection_list[index[0] + 1].close()
                            del self.connection_list[index[0] + 1]
                    except Exception as e:
                        print("Error while handling new connection:", e)

                self.connection_list.append(new_connection)
                self.connections_ips.append((addr[0], new_connection))
                print("NN. New node:", addr)

            #Some incoming message from other node
            else:
                try:
                    data = s.recv(BUFFER_SIZE)
                    if data:
                        messages = data.decode('ascii').replace("}{", "}|#|{").split("|#|")
                        for m in messages:
                            resp = json.loads(m)
                            print("\033[92m" + "NN.R:", resp, "\033[0m")
                            self.parse(resp, s)
                    else:
                        raise socket.error

                except socket.error as serr:
                    # if error is not "connection reset by peer" or "timeout"
                    if serr.errno != 104 or serr.errno != 110:
                        try:
                            s.shutdown(socket.SHUT_RDWR)
                            s.close()
                        except Exception as e:
                            print("Error while closing connection:", e)

                    if s in self.connection_list:
                        self.connection_list.remove(s)

                    index = [i for i, x in enumerate(self.connections_ips) if x[1] == s]
                    ip = None
                    if index:
                        ip = self.connections_ips[index[0]][0]
                        del self.connections_ips[index[0]]

                        print("NN. Node (", ip, ") is offline. Error:", serr)
                        print("NN. Active connections:", len(self.connection_list))

                        # if we are server, then remove node clients from list
                        if self.ip == self.server_ip:
                            j = [i for i, x in enumerate(self.node.server.clients) if x['ip'] == ip]
                            if j:
                                del self.node.server.clients[j[0]]

                        if ip != self.ip and ip == self.server_ip:
                            self.do_election()
                        elif self.ip != ip and self.server_ip != None:
                            self.send_node_down(ip)

                    continue

                except Exception as e:
                    print("NN. Events loop error:", e)
                    continue

    def parse(self, resp, dest_socket):
        if "elNo" in resp and resp['type'] != "election-ack" and resp['elNo'] < self.election_number and self.server_ip != None:
            self.send_election_break(dest_socket)
            return

        if resp["type"] == "election":
            made_circle = [x for x in resp['members'] if x['ip'] == self.ip]
            if not made_circle:
                sent_election_break = False
                # check all previous nodes. Maybe we have higher election number so we know which node is the server.
                for member in resp['members']:
                    if member['elNo'] < self.election_number and self.server_ip != None:
                        sent_election_break = True
                        self.send_election_break(member['ip'])

                if not sent_election_break:
                    self.send({'type': "election-ack"}, dest_socket.getpeername()[0])
                    self.send_election(resp)
            else:
                server = max(resp['members'], key=lambda item: item['ip'].split(".")[3])
                for member in resp['members']:
                    member['received'] = False

                    # increment server election number
                    if member['ip'] == server['ip']:
                        member['elNo'] += 1

                self.server_ip = server['ip']
                self.election_number = server['elNo']

                message = {
                    'type': 'election-done',
                    'elNo': server['elNo'],
                    'members': resp['members']
                }
                self.send({'type': "election-done-ack"}, dest_socket.getpeername()[0])
                self.send_election_done(message)

                if self.server_ip == self.ip:
                    self.init_server(resp['members'])

            return

        # everything is ok
        if resp["type"] == "election-ack":
            return

        # everything is ok
        if resp["type"] == "election-done-ack":
            return

        # other node has election number higher then ours, update server info
        if resp["type"] == "election-break":
            # if this node was a server and no longer is
            if self.server_ip == self.ip:
                del self.node.server

            if resp['server'] == self.ip:
                self.election_number = resp['elNo']
                self.do_election()
                return

            self.server_ip = resp['server']
            self.election_number = resp['elNo']

            self.send_node_update()

            return

        # election finished and coordinator has been choosen
        if resp["type"] == "election-done":
            # find server - node with highest IP
            server = max(resp['members'], key=lambda item: item['ip'].split(".")[3])

            # if this node was a server and no longer is
            if self.server_ip == self.ip and server['ip'] != self.ip:
                del self.node.server
                self.node.server = None

            self.server_ip = server['ip']
            self.election_number = resp['elNo']

            self.send_election_done(resp)


            if self.server_ip == self.ip and self.node.server == None:
                self.init_server(resp['members'])

            #TODO czy to dziala
            #if self.server_ip == self.ip:
            #    self.node.server = None
            #    self.init_server(resp['members'])

            return

        # node update went smoothly
        if resp["type"] == "node-update-ack":
            return

        # node received response for clients list update
        if resp["type"] == "clients-list":
            self.node.clients_node.update_clients_list(resp['clients'])
            return

        # server wants to know my clients list
        if resp["type"] == "ping":
            self.send_node_update(dest_socket.getpeername()[0])
            return

        # other node's client sent message to one of our clients pass it to clients_node
        if resp["type"] == "message":
            self.node.clients_node.received_message(resp)
            return

        if resp["type"] == "message-ack":
            self.node.clients_node.received_message_ack(resp)
            return

        if resp["type"] == "message-fail":
            self.node.clients_node.received_message_fail(resp)
            return

        # if this node is a server, then parse message and see if it was for server
        if self.server_ip == self.ip:
            self.node.server.parse(resp, dest_socket)


    def send(self, message, sock = None):
        if sock is None:
            sock = self.socket

        if isinstance(sock, str):
            sock = self.get_connection(sock)

        if sock == False:
            print("NN. Cannot send message", message)
            return False

        try:
            sock.sendall(json.dumps(message).encode("ascii"))
            sock.sendall(b'')
            print("\033[95m" + "NN.S:", sock.getpeername()[0], ":", message, "\033[0m")
            return True
        except socket.error as serr:
            print("NN.sending error", message, sock, serr.strerror, serr.errno)

            if sock in self.connection_list:
                self.connection_list.remove(sock)

            index = [i for i, x in enumerate(self.connections_ips) if x[1] == sock]
            if index:
                ip = self.connections_ips[index[0]][0]
                del self.connections_ips[index[0]]

            return False

    def send_ack(self, type, ip):
        message = {
            "type": type + "-ack"
        }

        self.send(message, self.get_connection(ip))

    def send_election(self, message):
        message['members'].append({
            "ip": self.ip,
            "elNo": self.election_number
        })

        on_list = [o['ip'] for o in message['members']]
        if len(on_list) == len(HOST):
            return

        index = (self.hosts.index(self.ip) + 1) % len(self.hosts)

        result = False
        for i in range(0, len(self.hosts) - 1):
            ip = self.hosts[(index + i) % len(self.hosts)]


            result = self.send(message, self.get_connection(ip))
            print("NN. Connected to", ip, ":", result)
            if result:
                return

        #we are alone!
        self.election_number = 0
        self.server_ip = None


    def send_election_break(self, sock):
        message = {
            "type": "election-break",
            "elNo": self.election_number,
            "server": self.server_ip
        }

        self.send(message, sock)

    def send_election_done(self, message):
        index = [i for i, x in enumerate(message['members']) if x['ip'] == self.ip][0]
        message['members'][index]['received'] = True

        # find first not already informed node
        index = [i for i, x in enumerate(message['members']) if x['received'] == False]

        # everyone got election-done
        if not index:
            return
        # send election to next ip
        else:
            ip = message['members'][index[0]]['ip']
            self.send(message, self.get_connection(ip))


    def send_node_update(self, ip = None):
        if ip == None and self.server_ip == None:
            print("NN. Server not yet chosen.")
            self.node.clients_node.update_clients_list([])
            return

        if ip == None and self.server_ip != None:
            ip = self.get_connection(self.server_ip)

        message = {
            "type": "node-update",
            "elNo": self.election_number,
            "clients": self.node.clients_node.get_my_clients()
        }
        result = self.send(message, ip)
        if not result:
            print("NN. Server is probably down. Election needed.")
            self.do_election()

    def send_get_clients(self):
        message = {
            "type": "get-clients",
            "elNo": self.election_number
        }
        result = self.send(message, self.get_connection(self.server_ip))
        if not result:
            print("NN. Server is down. Election needed.")
            self.do_election()

    def send_node_down(self, ip):
        message = {
            "type": "node-down",
            "elNo": self.election_number,
            "ip": ip
        }

        self.send(message, self.server_ip)

    def do_election(self):
        print("NN. Starting election.")
        sys.stdout.flush()

        self.server_ip = None
        self.node.server = None

        message = {
            'type': "election",
            'members': []
        }
        self.send_election(message)

    # =================== utils ================================
    def get_connection(self, ip):
        if ip == self.ip:
            for conn in self.connection_list[1:]:
                if conn.getpeername()[0] == ip:
                    return conn
        else:
            for conn in self.connection_list[1:]:
                if conn.getpeername()[0] == ip or conn.getsockname()[0] == ip:
                    return conn

        return self.create_connection(ip)

    def create_connection(self, host):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        try:
            s.connect((host, NODE_PORT))
        except socket.error as serr:
            print("NN. Connection to (", host, ") could not be established.", serr.strerror, serr.errno)
            return False

        # TODO: czy to jest dobre?
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        s.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 1)
        s.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        s.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 10)
        self.connection_list.append(s)
        self.connections_ips.append((host, s))
        return s

    def init_server(self, members):
        self.node.server = server.Server(self.node, members)

    def disconnect(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()

            for sock in self.connection_list[1:]:
                self.send({"type": "bye"}, sock)
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()

        except Exception:
            print("Cannot close connection.")

# ====================================================================================
# ==========================    CLIENTS NODE    ======================================
# ====================================================================================
class ClientsNode:
    def __init__(self, n):
        self.node = n
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(TIMEOUT)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not self.bind():
            print("CN. Could not establish connection")
            sys.exit(0)

        self.my_clients = []
        self.other_clients = []
        self.clients_waiting_for_list = []
        self.connection_list = [self.socket]

    def bind(self):
        try:
            self.socket.bind(("localhost", CLIENT_PORT))
            self.socket.listen(5)
            return True
        except socket.error as serr:
            print("CN. bind", serr, serr.errno)
            return False

    def events_loop(self):
        read_sockets,write_sockets,error_sockets = select.select(self.connection_list,[],[], SELECT_TIMEOUT)

        for s in read_sockets:
            #New connection
            if s == self.socket:
                new_connection, addr = self.socket.accept()
                self.client_connected(new_connection)

            #Some incoming message from a client
            else:
                try:
                    data = s.recv(BUFFER_SIZE)
                    if data:
                        messages = data.decode('ascii').replace("}{", "}|#|{").split("|#|")
                        for m in messages:
                            resp = json.loads(m)
                            print("CN.recv", resp)
                            self.parse(resp, s)
                    else:
                        self.client_disconnected(s)

                except socket.error as serr:
                    print("CN. Socket exception:", serr)
                    s.close()
                    self.client_disconnected(s)
                    continue

                except Exception as e:
                    print("CN. Events loop exception:", e)
                    continue

    # parse whatever is sent from client to node
    def parse(self, resp, dest_socket):
        if resp['type'] == "connect":
            client = {
                "id": resp['id'],
                "name": resp['name']
            }
            client['socket'] = dest_socket
            self.my_clients.append(client)
            self.add_to_waiting_for_list(dest_socket)

            self.node.nodes_node.send_node_update()
            self.node.nodes_node.send_get_clients()
            return

        if resp['type'] == "get-clients":
            self.node.nodes_node.send_get_clients()
            self.add_to_waiting_for_list(dest_socket)

            return

        if resp['type'] == "message":
            ip = [x for x in self.other_clients if x['id'] == resp['client-to']['id']]

            if not ip:
                self.send_message_fail(resp)
            else:
                if not self.node.nodes_node.send(resp, ip[0]['node']):
                    self.send_message_fail(resp)

            return

    # sending messages
    def send(self, message, socket = None):
        if socket is None:
            socket = self.socket

        try:
            socket.sendall(json.dumps(message).encode("ascii"))
            return True
        except socket.error as serr:
            print("CN. sending error", serr.strerror, serr.errno)
            return False

    # respond to the sender that message was received
    def send_message_ack(self, message):
        temp = message['client-to']
        message['client-to'] = message['client-from']
        message['client-from'] = temp
        message['type'] = "message-ack"
        del message['message']

        ip = [x for x in self.other_clients if x['id'] == message['client-to']['id']]
        if ip:
            self.node.nodes_node.send(message, ip[0]['node'])

    # respond to the sender that message sending failed
    def send_message_fail(self, message):
        temp = message['client-to']
        message['client-to'] = message['client-from']
        message['client-from'] = temp
        message['type'] = "message-fail"
        del message['message']

        ip = [x for x in self.other_clients if x['id'] == message['client-to']['id']]
        if ip:
            self.node.nodes_node.send(message, ip[0]['node'])

    # send updated list of all clients to client that asked for it
    def send_clients_list(self, socket, list = None):
        if not list:
            list = self.prepare_clients_list()

        message = {
            'type': 'clients-list',
            'clients': list
        }

        self.send(message, socket)

    # message received for client in our node from other node
    def received_message(self, message):
        list = [x for x in self.my_clients if x['id'] == message['client-to']['id']]
        if not list:
            self.send_message_fail(message)
            return
        else:
            socket = list[0]['socket']
            self.send(message, socket)
            self.send_message_ack(message)

    # message_fail received for client in our node from other node
    def received_message_fail(self, message):
        socket = [x for x in self.my_clients if x['id'] == message['client-to']['id']][0]['socket']
        self.send(message, socket)

    # message_ack received for client in our node from other node
    def received_message_ack(self, message):
        return

    # nodes-node got clients list and clients-node now has to forward it to clients
    def update_clients_list(self, clients_list):
        self.other_clients = clients_list
        all_clients = self.prepare_clients_list()

        for s in self.clients_waiting_for_list:
            self.send_clients_list(s, all_clients)

        #empty list, because all waiting clients has been notified
        del self.clients_waiting_for_list[:]

    # ========================== UTILS =====================
    def client_connected(self, socket):
        self.connection_list.append(socket)
        print("CN. Client connected.")

    # remove from list of active clients
    def client_disconnected(self, socket):
        socket.close()

        index = [i for i, x in enumerate(self.my_clients) if x['socket'] == socket][0]
        del self.my_clients[index]
        self.connection_list.remove(socket)
        self.node.nodes_node.send_node_update()

        print("CN. Client offline.")

    def prepare_clients_list(self):
        other_clients = []
        my_clients = []
        for client in self.other_clients:
            other_clients.append({
                "id": client['id'],
                "name": client['name']
            })

        for client in self.my_clients:
            c = {
                "id": client['id'],
                "name": client['name']
            }
            if c not in other_clients:
                my_clients.append()

        return my_clients + other_clients

    def get_my_clients(self):
        clients = []
        for c in self.my_clients:
            clients.append({
                "id": c['id'],
                "name": c['name']
            })

        return clients

    def add_to_waiting_for_list(self, socket):
        if socket not in self.clients_waiting_for_list:
            self.clients_waiting_for_list.append(socket)
