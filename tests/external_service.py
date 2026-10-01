"""An independent fake business API that enforces the supplied idempotency key."""
import http.server,json,threading
class ExternalService:
    def __init__(self):
        self.requests=0;self.effects=0;self.outputs={};self.lock=threading.Lock();parent=self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                key=json.loads(self.rfile.read(int(self.headers['Content-Length'])))['key']
                with parent.lock:
                    parent.requests+=1
                    if key not in parent.outputs:
                        parent.effects+=1;parent.outputs[key]={'effect_id':parent.effects}
                    output=json.dumps(parent.outputs[key]).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(output)));self.end_headers();self.wfile.write(output)
        self.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.url=f'http://127.0.0.1:{self.server.server_port}/effect'
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def close(self):self.server.shutdown();self.server.server_close();self.thread.join()
