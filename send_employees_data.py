#
#                       _oo0oo_
#                      o8888888o
#                      88" . "88
#                      (| -_- |)
#                      0\  =  /0
#                    ___/`---'\___
#                  .' \\|     |// '.
#                 / \\|||  :  |||// \
#                / _||||| -:- |||||- \
#               |   | \\\  -  /// |   |
#               | \_|  ''\---/''  |_/ |
#               \  .-\__  '-'  ___/-. /
#             ___'. .'  /--.--\  `. .'___
#          ."" '<  `.___\_<|>_/___.' >' "".
#        | | :  `- \`.;`\ _ /`;.`/ - ` : | |
#         \  \ `_.   \_ __\ /__ _/   .-` /  /
#     =====`-.____`.___ \_____/___.-`___.-'=====
#                       `=---='
#
#
#     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#               Buddha bless never BUG
#
import logging
import time
import sys
import stomp
import threading
import concurrent.futures
import mysql.connector
import os
import Queue
import time
from mysql.connector import Error
from logging.config import fileConfig
from datetime import date
import atexit
import traceback
import object_pool
import employees_pb2


dbconfig = {
            "database": "employees",
            "user":     "test_company",
            "password": "Rajulj2@"
        }

args = ()
obj_pool = object_pool.ObjectPool(2, 3, True, 30, mysql.connector.connect, *args, **dbconfig)

def connect():
    """ Connect to MySQL database """
    try:
        dbconfig = {
            "database": "employees",
            "user":     "test_company",
            "password": "Rajulj2@"
        }
    
        #cnx = mysql.connector.connect(pool_name = "mypool", pool_size = 3, **dbconfig)
        cnx = obj_pool.borrow_obj()
        
        if cnx.is_connected():
            logging.info('Connected to MySQL database')
            return cnx
    except mysql.connector.OperationalError as err:
        logging.exception("Error connecting to database {}".format(err))
        raise e
    except mysql.connector.PoolError as err:
        logging.exception("error get connection from pool {}".format(err))
        raise err
    return None    

def process_context_message(message):
    logging.info("process_context_message")
    batch_context = BatchContext()
    batch_context.process(message)
    return batch_context    
          
          
class FileWriterWorker(threading.Thread):
    def __init__(self, file_name, file_queue):
        self._file_name = file_name
        self._task_queue = file_queue
        self._stomp_conn = stomp.Connection()
        self._stomp_conn.start()
        self._stomp_conn.connect('admin', 'password', wait=True)
        threading.Thread.__init__(self)

    def run(self):
        with open(self._file_name, 'a') as fwrite:
            while True:
                try:
                    task = self._task_queue.get(True, 30)                
                    #fwrite.write(task)
                    self._stomp_conn.send('/queue/test_dest',  task)
                except Queue.Empty as err:
                    logging.exception("queue is empty no more data to write")
                except Exception as err:
                    logging.exception("got error while persisting...")
                    raise err
                
class Worker(threading.Thread):
    def __init__(self, jobs, file_task_queue):
        self.conn = connect()
        
        self.jobs = jobs
        self._file_task_queue = file_task_queue
        self.success_count = 0
        self.failed_count = 0
        threading.Thread.__init__(self)

    def close_connect(self):
        if self.conn and self.conn.is_connected():
            obj_pool.return_obj(self.conn)

    def run(self):
        logging.info("Starting worker thread ...")
        while True:
            try:                
                task = self.jobs.get(True, 5)
                self.ol_message(task)
            except Queue.Empty as err:
                s = str(err)                    
                logging.exception("Error: {}".format(s))
                self.close_connect()
                break    
            except Exception as err:
                logging.exception("Error: {}".format(s))
                self.close_connect()    
                break
    
    def ol_message(self, message):        
        try:                    
            cursor = self.conn.cursor()
            select_emp = "select emp_no, first_name, last_name, hire_date, gender, birth_date from employees where emp_no = %(emp_no)s"
            cursor.execute(select_emp, { 'emp_no': message[0] })  
                                     # Insert new employee
            for result in cursor:
                emp = employees_pb2.Employee()
                emp.id = result[0]                
                emp.first_name = result[1]
                emp.last_name = result[2]
                emp.hire_date = str(result[3])
                emp.birth_date = str(result[5])
                if result[4] == 'M':
                    emp.gender = employees_pb2.Employee.M
                else:
                    emp.gender = employees_pb2.Employee.F
                    
                self._file_task_queue.put(emp.SerializeToString())

            cursor.close()
            self.conn.commit()            
            self.success_count += 1    
        except mysql.connector.OperationalError as err:
            s = str(err)
            logging.exception("Error: {}, putting task back on the queue.".format(s))              
            self.jobs.put(message)
            raise err
        except Exception as err:
            logging.exception(err)
            self.failed_count += 1    
            raise err
        finally:
              logging.info("consumed the message ...{}".format(self.jobs.qsize()))

class BatchContext():
    def __init__(self):
        self._queue = Queue.Queue()
        self._file_task_queue = Queue.Queue()        
        self._threadpool_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def schedule_workers(self, message):
        file_writer_worker = FileWriterWorker("{}.data".format(message), self._file_task_queue)
        file_writer_worker.start()
        
        worker_list = []                        
        for i in range(3):
            try:
                worker = Worker(self._queue, self._file_task_queue)            
                worker_list.append(worker)
            except Exception as e:
                logging.exception("error initializing worker...")
        map(lambda x: self._threadpool_executor.submit(x.run), worker_list)
    
    def process(self, message):
        self.query(message)
    
    def query(self, message):
        conn = connect()
        try:                        
            batch_cursor = conn.cursor(buffered=True)    
            select_stmt = "SELECT emp_no FROM current_dept_emp WHERE dept_no = %(dept_no)s"            
            logging.debug("processing query ... {} with params {}".format(select_stmt, message))            
            batch_cursor.execute(select_stmt, { 'dept_no': message })
            
            for result in batch_cursor:
                self.schedule_task(result)
    
            batch_cursor.close()
            conn.commit()
            
            self.schedule_workers(message)
        except mysql.connector.Error as err:
            s = str(err)
            logging.exception("Error: executing query {}".foramt(s))             
        finally:
            obj_pool.return_obj(conn)
    
    def stop(self, wait):
        self._threadpool_executor.shutdown(wait)        
    
    def schedule_task(self, task):
        self._queue.put(task)

class DeptMessageProcessor():
    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.processpool_executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.pool_size)    
    
    def process_context(self, message):
        future = self.processpool_executor.submit(process_context_message, message)
        future.add_done_callback(self.success_context_message)
    
    def success_context_message(future):
        logging.info("message: {} consumed status: ".format(message, future.done()))
        batch_context = future.result()
        batch_context.stop()    
    
    def stop(self, wait):
        self.processpool_executor.shutdown(wait)

        
processor = DeptMessageProcessor(2)

class DeptMsgListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        logging.info('received an error "%s"' % message)
    
    def on_message(self, headers, message):        
        future = processor.process_context(message)
    

def test_stomp():
    conn = stomp.Connection()
    conn.set_listener('', DeptMsgListener())
    conn.start()
    conn.connect('admin', 'password', wait=True)
    conn.subscribe(destination='/queue/test', id=1, ack='auto', headers=None)    

    while True:
        time.sleep(20)

    conn.disconnect()

def shutdown_hook():
    processor.stop(False)

def main():
    try:
        fileConfig('loggin_config.ini')
        logging.info('Started')        
        test_stomp()
        logging.info('Finished')
    except KeyboardInterrupt:
        print("Shutdown requested ... exiting")
    except Exception:
        traceback.print_exc(file=sys.stderr)
    finally:
        atexit.register(shutdown_hook)    

if __name__ == '__main__':
    main()
