"""Real S3 HTTP traffic fault proxy for the local RustFS backend only."""
import http.client, http.server, socket, threading, time
class FaultProxy:
    def __init__(self, target_port=19000, state_read_delay=0):
        self.ready=threading.Event();self.release=threading.Event();self.lock=threading.Lock();self.mode=None;self.triggered=0
        self.state_reads=0;self.reads_in_flight=0;self.peak_state_reads=0
        parent=self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def handle_request(self):
                body=self.rfile.read(int(self.headers.get('Content-Length',0)))
                headers=dict(self.headers);headers.pop('Connection',None)
                conn=http.client.HTTPConnection('127.0.0.1',target_port,timeout=30)
                conn.request(self.command,self.path,body,headers)
                response=conn.getresponse();payload=response.read()
                if state_read_delay and self.command=='GET' and self.path.endswith('/state.json'):
                    with parent.lock:
                        parent.state_reads+=1;parent.reads_in_flight+=1
                        parent.peak_state_reads=max(parent.peak_state_reads,parent.reads_in_flight)
                    time.sleep(state_read_delay)
                    with parent.lock:parent.reads_in_flight-=1
                selected=False
                with parent.lock:
                    if parent.mode=='lost-state-response' and self.command=='PUT' and self.path.endswith('/state.json') and b'"status":"completed"' in body and response.status==200:
                        selected=True;parent.mode=None
                    if parent.mode=='pause-result-response' and self.command=='PUT' and '/results/' in self.path and response.status==200:
                        selected=True;parent.mode=None
                        pause=True
                    elif parent.mode=='pause-signal-state-response' and self.command=='PUT' and self.path.endswith('/state.json') and b'"signals":{"' in body and response.status==200:
                        selected=True;parent.mode=None
                        pause=True
                    elif parent.mode=='pause-retry-state-response' and self.command=='PUT' and self.path.endswith('/state.json') and b'\"last_retry_operation\":\"' in body and b'\"status\":\"queued\"' in body and response.status==200:
                        selected=True;parent.mode=None
                        pause=True
                    elif parent.mode=='pause-schedule-intent-response' and self.command=='PUT' and '/schedules/' in self.path and b'"pending":{' in body and response.status==200:
                        selected=True;parent.mode=None
                        pause=True
                    elif parent.mode=='pause-scheduled-task-response' and self.command=='PUT' and '/tasks/' in self.path and self.path.endswith('/state.json') and b'"schedule":{' in body and b'"status":"queued"' in body and response.status==200:
                        selected=True;parent.mode=None
                        pause=True
                    else:pause=False
                if selected:
                    parent.triggered+=1;parent.ready.set()
                    if pause:parent.release.wait(30)
                    self.close_connection=True
                    try:self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:pass
                    self.connection.close();conn.close();return
                self.send_response(response.status)
                for k,v in response.getheaders():
                    if k.lower() not in ['transfer-encoding','connection','content-length']:self.send_header(k,v)
                self.send_header('Content-Length',str(len(payload)));self.end_headers()
                try:self.wfile.write(payload)
                except (BrokenPipeError,ConnectionResetError):pass
                conn.close()
            do_GET=do_PUT=do_POST=do_DELETE=do_HEAD=handle_request
        self.server=http.server.ThreadingHTTPServer(('127.0.0.1',19002),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
    def start(self):self.thread.start()
    def arm(self,mode):
        self.mode=mode;self.ready.clear();self.release.clear()
    def close(self):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join()
