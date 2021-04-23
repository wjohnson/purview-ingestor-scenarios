from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

import json
import os

class GP(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
    def do_HEAD(self):
        self._set_headers()
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        print(self.path)
        
        content = r"{}"
        with open(os.path.join(os.getcwd(), self.path[1:])) as fp:
            content = fp.read()

        self.wfile.write(bytes(content, 'utf-8'))
        
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        print(self.rfile)
        output = {"a":1,"b":"abc"}
        self.wfile.write(bytes(json.dumps(output,indent=2), "utf-8"))

def run(server_class=HTTPServer, handler_class=GP, port=8088):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print('Server running at localhost:8088...')
    httpd.serve_forever()

run()