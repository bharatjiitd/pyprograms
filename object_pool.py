import Queue
import threading

class ObjectPool(object):
    def __init__(self, init_size, max_size, should_wait, max_wait_time, object_factory, *args, **kwargs):
        self._init_size = max(init_size, 1)
        self._max_size = max(max_size, 1)
        self._should_wait = should_wait
        self._max_wait_time = max_wait_time
        self._object_factory = object_factory
        self._args = args
        self._kwargs = kwargs
        self._queue = Queue.Queue()
        self._in_use_objs = 0;
        self._write_mutex = threading.Lock()
        self._read_mutex = threading.Lock()

    def borrow_obj(self):
        with self._write_mutex:
            try:
                if self._queue.empty() and self._in_use_objs < self._max_size:
                    pooled_obj = self.__create_pooled_obj()                    
                    self._in_use_objs += 1
                    return pooled_obj
                pooled_obj = self._queue.get(self._should_wait, self._max_wait_time)                
                return pooled_obj
            except Exception as err:
                raise err

    def return_obj(self, pooled_obj):
        try:
            self._queue.put(pooled_obj, False)            
        except Exception as err:
            raise err

    def __create_pooled_obj(self):
        try:
            pooled_item = self._object_factory(*self._args, **self._kwargs)
            return pooled_item
        except Exception as error:
            raise error
        
    def size(self):
        return self._queue.qsize()
    
    def total_created(self):
        return self._in_use_objs

    def __repr__(self):
        return ("<pool %d: size=%d>" % (id(self), self._in_use_objs))


    def __str__(self):
        return ("<pool %d: size=%d>" % (id(self), self._in_use_objs))


