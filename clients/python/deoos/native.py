"""ctypes calls a Rust library in THIS process; never starts a server or subprocess."""
import ctypes, json, os, pathlib, platform, threading
class NativeEngine:
    def __init__(self, config):
        name = {'Darwin':'libdeoos_engine.dylib','Linux':'libdeoos_engine.so','Windows':'deoos_engine.dll'}.get(platform.system())
        if name is None:raise RuntimeError(f'Unsupported native platform: {platform.system()}')
        path = os.environ.get('DEOOS_NATIVE_LIBRARY',str(pathlib.Path(__file__).parent/'native'/name))
        try:
            self.lib = ctypes.CDLL(path)
        except OSError as error:
            raise RuntimeError(
                'Cannot load the DEOOS engine in library mode. Install the prebuilt Python '
                'wheel from the release for your operating system and CPU architecture. If '
                'DEOOS_NATIVE_LIBRARY is set, check that it points to a compatible '
                'library. To connect in server mode, use Client.remote(url, token).'
            ) from error
        self.lib.deoos_open.argtypes=[ctypes.c_char_p,ctypes.POINTER(ctypes.c_void_p)]
        self.lib.deoos_open.restype=ctypes.c_void_p
        self.lib.deoos_request.argtypes=[ctypes.c_void_p,ctypes.c_char_p]
        self.lib.deoos_request.restype=ctypes.c_void_p
        self.lib.deoos_close.argtypes=[ctypes.c_void_p]
        self.lib.deoos_close.restype=None
        self.lib.deoos_string_free.argtypes=[ctypes.c_void_p]
        self.lib.deoos_string_free.restype=None
        # Protect the raw Rust handle's lifetime without holding a Python lock
        # across an FFI call. ctypes releases the GIL for CDLL calls, so
        # per-call tokens let independent requests enter the native runtime
        # concurrently while close() drains them first.
        self.condition=threading.Condition()
        self.active_calls={}
        self.closer=None
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
        thread_id=threading.get_ident()
        call_token=object()
        try:
            with self.condition:
                # Publish the active-call token before observing/capturing
                # the raw handle. A same-thread signal can re-enter close()
                # while this RLock is held; it must see this token and refuse
                # to free the handle. Revalidate after registration because a
                # signal may also close an as-yet-unadmitted handle just
                # before this critical section begins.
                self.active_calls[call_token]=thread_id
                if self.closer is not None or not self.handle:
                    del self.active_calls[call_token]
                    if not self.active_calls:self.condition.notify_all()
                    raise RuntimeError('engine closed')
                handle=self.handle
            request=json.dumps(dict(method=method,path=path,data=data),allow_nan=False).encode()
            response=self._decode(self.lib.deoos_request(handle,request))
        finally:
            with self.condition:
                if call_token in self.active_calls:
                    del self.active_calls[call_token]
                    if not self.active_calls:self.condition.notify_all()
        if response['status']!=200:raise EngineError(response['status'],response['error'])
        return response['value']
    def close(self):
        thread_id=threading.get_ident()
        close_token=object()
        try:
            with self.condition:
                # A signal handler or other reentrant caller must not wait for
                # the native request that it interrupted on this same thread.
                if thread_id in self.active_calls.values():
                    raise RuntimeError('cannot close engine from an active native request')
                while self.closer is not None:
                    if self.closer[1]==thread_id:
                        raise RuntimeError('cannot close engine reentrantly')
                    self.condition.wait()
                if not self.handle:return
                self.closer=(close_token,thread_id)
                while self.active_calls:
                    self.condition.wait()
                handle=self.handle
                # Mark the handle unavailable before calling the void FFI
                # close function. If Python interruption occurs during that
                # call, retrying could double-free an already-closed handle.
                self.handle=None
            self.lib.deoos_close(handle)
        finally:
            with self.condition:
                if self.closer is not None and self.closer[0] is close_token:
                    self.closer=None
                    self.condition.notify_all()
