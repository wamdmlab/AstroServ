#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

import socket
import threading
import SocketServer
import sys

def client(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))

 #   try:
  #      print "Send: {}".format(message)
    sock.sendall(message)
    #    response = sock.recv(1024)
    #    jresp = json.loads(response)
    #    print "Recv: ",jresp

  #  finally:
    sock.close()

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = sys.argv[1], int(sys.argv[2]) 
    if len(sys.argv)<4:
       msg1=sys.stdin.read()
    else:
       msg1 = sys.argv[3]
       msg1=msg1+'\n'
#    print msg1
   # msg2 = [{'src':"ln", 'dst':"lndst"}]
   # msg3 = [{'src':"xj", 'dst':"xjdst"}]

    #jmsg1 = json.dumps(msg1)
    #jmsg2 = json.dumps(msg2)
    #jmsg3 = json.dumps(msg3)

    client(HOST, PORT, msg1)
    #client(HOST, PORT, jmsg2)
    #client(HOST, PORT, jmsg3)

