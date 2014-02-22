# -*- coding: UTF-8 -*-
# Tests 
#
# Copyright 2014 Cole Maclean
"""Unit tests for Tornpsyco."""
import unittest
import datetime

import tornado.testing

from tornpsyco import *

class TornpsycoTests(tornado.testing.AsyncTestCase):
    """Connection handling tests.
    """
    db_settings = dict(host='localhost', database='tornpsyco', user='cole')
    
    def setUp(self):
        super(TornpsycoTests, self).setUp()
        self.conn = Connection(host=self.db_settings['host'],
            database=self.db_settings['database'], user=self.db_settings['user'],
            ioloop=self.io_loop, callback=self.stop)
        self.wait() # wait for initialization
        self.conn.execute("DROP TABLE IF EXISTS test", callback=self.stop)
        self.wait() # wait for drop
        self.conn.execute("""
        CREATE TABLE "test" (
            "id" serial PRIMARY KEY,
            "an_int" integer,
            "some_chars" varchar(45),
            "more_chars" varchar(2056) DEFAULT NULL,
            "datetime" timestamp with time zone
        )
        """, callback=self.stop)
        self.wait() #allow to complete
        now = datetime.datetime.now(UTC)
        sample_rows = [
            (7, 'foobar', 'blah blah blah', now,),
            (234237, 'dfdsfasd', 'the quick brown fox', now,),
            (146, 'foobar', 'jumped over the lazy dog', now,),
        ]
        for row in sample_rows:
            self.conn.execute("""INSERT INTO "test"
                (an_int, some_chars, more_chars, datetime)
                VALUES (%s, %s, %s, %s)
                """, *row, callback=self.stop)
            self.wait() #allow to complete
        
    def tearDown(self):
        self.conn.close()
        super(TornpsycoTests, self).tearDown()
        
    def test_basic_conn(self):
        self.assertTrue(self.conn)
        self.assertFalse(self.conn.busy)
    
    def test_reconnect(self):
        self.assertTrue(self.conn)
        self.conn.reconnect(callback=self.stop)
        self.wait() # let it do it's thing
        self.assertFalse(self.conn.busy)
    
    @tornado.testing.gen_test
    def test_execute_select(self):
        query = "SELECT 42;"
        result = yield self.conn.execute(query)
        self.assertEqual(int(result[0]), 42)
    
    @tornado.testing.gen_test
    def test_execute_rowcount(self):
        query = "DELETE FROM test;"
        result = yield self.conn.execute(query, rowcount=True)
        self.assertEqual(result, 3)
    
    @tornado.testing.gen_test
    def test_execute_returning(self):
        now = datetime.datetime.now(UTC)
        result = yield self.conn.execute("""INSERT INTO "test"
        (an_int, some_chars, more_chars, datetime)
        VALUES (%s, %s, %s, %s)
        RETURNING id, datetime""",
        18, 'a few chars', 'more test text', now)
        self.assertEqual(result, (4, now,))
    
    @tornado.testing.gen_test
    def test_get_select(self):
        query = "SELECT id FROM test WHERE an_int = 7;"
        result = yield self.conn.get(query)
        self.assertEqual(result.id, 1)

    @tornado.testing.gen_test
    def test_get_object_access(self):
        query = "SELECT 42 AS id, 15 AS foo;"
        result = yield self.conn.get(query)
        self.assertEqual(result.id, 42)
        self.assertEqual(result['foo'], 15)
        # we should be able to modify records
        result['foo'] = 'bar'
        self.assertEqual(result['foo'], 'bar')
    
    @tornado.testing.gen_test
    def test_get_unicode_nicely(self):
        query = "SELECT 'ümläuts' AS test;"
        result = yield self.conn.get(query)
        self.assertEqual(result.test, 'ümläuts')
    
    @tornado.testing.gen_test
    def test_basic_query(self):
        query = "SELECT * FROM test;"
        results = yield self.conn.query(query)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].an_int, 7)
        self.assertEqual(results[2].more_chars, 'jumped over the lazy dog')
    
    @tornado.testing.gen_test
    def test_get_select_multiple(self):
        query = "SELECT id FROM test;"
        with self.assertRaises(MultipleRowsReturnedError):
            result = yield self.conn.get(query)
            
    @tornado.testing.gen_test
    def test_datetime_consistency(self):
        now = datetime.datetime.now()
        insert = yield self.conn.execute("""INSERT INTO "test"
        (an_int, some_chars, more_chars, datetime)
        VALUES (%s, %s, %s, %s) RETURNING id""",
        44, 'a few chars', 'more test text', now)
        select = yield self.conn.get("""SELECT datetime
        FROM test WHERE id = %s""", insert[0])
        self.assertEqual(select.datetime.replace(tzinfo=None), now)
        
def suite():
    suite = unittest.TestLoader().loadTestsFromTestCase(TornpsycoTests)
    return suite

if __name__ == '__main__':
    all = lambda: suite()
    tornado.testing.main()