import sys
import time
import socket, select
import json

# ====================================================================================
# ==========================     SERVER     =========================================
# ====================================================================================
class Server:
    def __init__(self, n, members):
        print("Server init.")
        self.node = n
        self.nodes = map(lambda x: x['ip'], members)
        self.ip = self.node.nodes_node.ip
        self.ping_all()

        # clients = [{ip: node_ip, clients: []}]
        self.clients = []

    def parse(self, message, socket):
        if message['type'] == "node-update":
            index = [i for i, x in enumerate(self.clients) if x['ip'] == self.get_socket_ip(socket)]
            if not index:
                node = {
                    'ip': self.get_socket_ip(socket),
                    'clients': message['clients']
                }
                self.clients.append(node)

            else:
                self.clients[index[0]]['clients'] = message['clients']

            self.send_node_update_ack(self.get_socket_ip(socket))

            return

        if message['type'] == "node-bye":
            index = [i for i, x in enumerate(self.clients) if x['ip'] == self.get_socket_ip(socket)]
            if index:
                del self.clients[index[0]]

            return

        if message['type'] == "node-down":
            result = self.send_ping(message['ip'])
            # node not responding, it might be down, so remove it
            if not result:
                index = [i for i, x in enumerate(self.clients) if x['ip'] == message['ip']]
                if index:
                    del self.clients[index[0]]

            return

        if message['type'] == "get-clients":
            self.send_clients_list(self.get_socket_ip(socket))
            return

    def send_ping(self, ip):
        message = {
            'type': "ping",
            'elNo': self.node.nodes_node.election_number
        }
        return self.node.nodes_node.send(message, ip)

    def send_node_update_ack(self, ip):
        message = {
            "type": "node-update-ack"
        }
        self.node.nodes_node.send(message, ip)

    def send_clients_list(self, ip):
        all_list = []
        for node in self.clients:
            list = []
            for c in node['clients']:
                tmp = c
                tmp['node'] = node['ip']
                list.append(tmp)

            all_list += list

        message = {
            'type': "clients-list",
            'clients': all_list
        }
        self.node.nodes_node.send(message, ip)

    def ping_all(self):
        for ip in self.nodes:
            #if ip != self.ip:
                self.send_ping(ip)

    def get_socket_ip(self, socket):
        if socket == self.node.nodes_node.socket:
            return self.ip

        result = socket.getpeername()[0]
        if result == self.ip:
            result = socket.getsockname()[0]

        return result