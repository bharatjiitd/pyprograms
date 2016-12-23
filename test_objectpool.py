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
        self.obj_pool = object_pool.ObjectPool(2, 3, True, 30, mysql.connector.connect, *args, **dbconfig)
        
    def test_scenario1(self):
        #for i in range(7):
        mysql_client = MySQLClient(self.obj_pool, 40)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client.setName("Thread_1")
        mysql_client.start()
        
        mysql_client = MySQLClient(self.obj_pool, 38)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client.setName("Thread_2")
        mysql_client.start()
        
        mysql_client = MySQLClient(self.obj_pool, 25)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client.setName("Thread_4")
        mysql_client.start()
        
        mysql_client = MySQLClient(self.obj_pool, 6)
        #mysql_client.setName("Thread_{}".format(i))
        mysql_client.setName("Thread_5")
        mysql_client.start()
        
        time.sleep(10)
        
        self.assertEqual(self.obj_pool.total_created(), 3, "total created == 3")

    def test_scenario2(self):
        for i in range(5):
            mysql_client = MySQLClient(self.obj_pool, 40)
            mysql_client.setName("Thread_{}".format(i))
            mysql_client.start()
            
        time.sleep(10)
        self.assertEqual(self.obj_pool.total_created(), 3, "total created == 3")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestObjectPool('test_scenario1'))
    suite.addTest(TestObjectPool('test_scenario2'))
    return suite


if __name__ == '__main__':
    unittest.main()


    