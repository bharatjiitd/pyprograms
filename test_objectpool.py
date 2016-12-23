#!/usr/bin/python

import mysql.connector
import object_pool
import threading
import time
import unittest
import Queue

class MySQLClient(threading.Thread):
    def __init__(self, obj_pool, time_to_finish):
        self._obj_pool = obj_pool
        self._time_to_finish = time_to_finish
        threading.Thread.__init__(self)

    def run(self):        
        obj = self._obj_pool.borrow_obj()        
        time.sleep(self._time_to_finish)
        self._obj_pool.return_obj(obj)


class TestObjectPool(unittest.TestCase):
    def setUp(self):
        dbconfig = {
                    "database": "employees",
                    "user":     "test_company",
                    "password": "Rajulj2@"
                }
        args = ()
        self.obj_pool = object_pool.ObjectPool(2, 3, True, 15, mysql.connector.connect, *args, **dbconfig)
        
    def test_scenario1(self):
        #for i in range(7):
        mysql_client1 = MySQLClient(self.obj_pool, 20)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client1.setName("Thread_1")
        mysql_client1.start()
        
        mysql_client2 = MySQLClient(self.obj_pool, 18)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client2.setName("Thread_2")
        mysql_client2.start()
        
        mysql_client3 = MySQLClient(self.obj_pool, 10)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client3.setName("Thread_4")
        mysql_client3.start()
        
        mysql_client4 = MySQLClient(self.obj_pool, 4)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client4.setName("Thread_5")
        mysql_client4.start()
        
        mysql_client1.join()
        mysql_client2.join()
        mysql_client3.join()
        mysql_client4.join()
                
        self.assertEqual(self.obj_pool.total_created(), 3, "total created == 3")

    def test_scenario2(self):
        thread_list = []
        for i in range(5):
            mysql_client = MySQLClient(self.obj_pool, 15)
            mysql_client.setName("Thread_{}".format(i))
            mysql_client.start()
            thread_list.append(mysql_client)

        map(lambda x: x.join(), thread_list)# mysql_client.join()
            
        self.assertEqual(self.obj_pool.total_created(), 3, "total created == 3")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestObjectPool('test_scenario1'))
    suite.addTest(TestObjectPool('test_scenario2'))
    return suite


if __name__ == '__main__':
    unittest.main()


    