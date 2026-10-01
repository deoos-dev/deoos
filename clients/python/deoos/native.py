"""ctypes calls a Rust library in THIS process; never starts a server or subprocess."""
import ctypes, json, os, pathlib, platform, threading
class NativeEngine:
    def __init__(self, config):
        name = {'Darwin':'libdeoos_engine.dylib','Linux':'libdeoos_engine.so','Windows':'deoos_engine.dll'}.get(platform.system())
        if name is None:raise RuntimeError(f'Unsupported native platform: {platform.system()}')
        path = os.environ.get('DEOOS_NATIVE_LIBRARY',str(pathlib.Path(__file__).parent/'native'/name))
        self.lib = ctypes.CDLL(path)
        self.lib.deoos_open.argtypes=[ctypes.c_char_p,ctypes.POINTER(ctypes.c_void_p)]
        self.lib.deoos_open.restype=ctypes.c_void_p
        self.lib.deoos_request.argtypes=[ctypes.c_void_p,ctypes.c_char_p]
        self.lib.deoos_request.restype=ctypes.c_void_p
        self.lib.deoos_close.argtypes=[ctypes.c_void_p]
        self.lib.deoos_close.restype=None
        self.lib.deoos_string_free.argtypes=[ctypes.c_void_p]
        self.lib.deoos_string_free.restype=None
        self.lock=threading.RLock()
        error=ctypes.c_void_p()
        self.handle=self.lib.deoos_open(json.dumps(config,allow_nan=False).encode(),ctypes.byref(error))
        if not self.handle:
            message=self._decode(error.value) if error.value else {'error':'native initialization failed'}
            raise RuntimeError(message['error'])
    def _decode(self, pointer):
        try:return json.loads(ctypes.string_at(pointer))
        finally:self.lib.deoos_string_free(pointer)
    def request(self, method, path, data):
        from . import EngineError
        with self.lock:
            if not self.handle:raise RuntimeError('engine closed')
            response=self._decode(self.lib.deoos_request(self.handle,json.dumps(dict(method=method,path=path,data=data),allow_nan=False).encode()))
        if response['status']!=200:raise EngineError(response['status'],response['error'])
        return response['value']
    def close(self):
        with self.lock:
            if self.handle:
                self.lib.deoos_close(self.handle)
                self.handle=None
