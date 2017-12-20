#!/usr/bin/env python
# -*- coding:utf-8 -*-
#

import socket
import threading
import SocketServer
import json, types,string
import os, time
import sys  
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        #jdata = json.loads(data)
        #print "Receive data from '%r'"% (data)
        #print "Receive jdata from '%r'"% (jdata)
        #rec_src = jdata[0]['src']
        #rec_dst = jdata
        #print filename
        file_object = open('listen_stop.txt','a')
        file_object.write(data)
        file_object.close
        #cur_thread = threading.current_thread()
        #response = [{'thread':cur_thread.name,'src':rec_src,'dst':rec_dst}]

        #jresp = json.dumps(response)
        #self.request.sendall(jresp)
        #rec_cmd = "proccess "+rec_src+" -o "+rec_dst
        #print "CMD '%r'" % (rec_cmd)
        #os.system(rec_cmd)
           
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = sys.argv[1], int(sys.argv[2])
    #print HOST
    stoppid="lsof -Pnl +M -i4 | grep %d | awk \'{print $2}\' | xargs kill >/dev/null 2>&1" %PORT
    os.system(stoppid)   
    filename='listen_stop.txt'
    clean=":>%s" %filename
    os.system(clean)
    SocketServer.TCPServer.allow_reuse_address = True
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    #print "Server loop running in thread:", server_thread.name
    #print " .... waiting for connection"

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
