# -*- coding: UTF-8 -*-
# Tornpsyco
# A psycopg2 async connection wrapper
#
# Copyright 2014 Cole Maclean
"""A lightweight wrapper around psycopg2.
Inspired by torndb by Ben Darnell.

Uses psycopg2 async and returns dict-based `Row` objects via futures.

    conn = Connection(host='localhost', database='test', user='test')

    ...

    @tornado.gen.coroutine
    def get(self):
        query = "SELECT id, name, email FROM users WHERE id = %s;"
        user = yield conn.get(query, (1,))
        self.write("Hello {}".format(user.name))


Datetimes returned via psycopg2 have a custom timezone set. To minimize confusion,
use the UTC offset for all datetime objects (e.g. `datetime.datetime.now(UTC)`).
"""
import functools
import queue

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.tz

from tornado.ioloop import IOLoop
from tornado.concurrent import return_future

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class Connection(object):
    """Our DB connection object.

    Arguments:
    host -- postgresql host
    database -- postgresql database

    Keyword arguments:
    user -- postgresql user
    password -- postgresql password
    port -- postgresql connection port
    ioloop -- `tornado.ioloop.IOLoop` instance to use for callbacks and fd handlers. Shouldn't normally be required.
    callback -- callback to execute on connection initialization
    """
    
    def __init__(self, host, database, **kwparameters):

        self._db = None
        self._waiting_queries = queue.Queue()
        self._ioloop = kwparameters.get('ioloop', None)
        self._dbargs = {
            'async': 1,
            'host': host,
            'database': database
        }
        for arg in ('user', 'password', 'port'):
            if kwparameters.get(arg) is not None:
                self._dbargs[arg] = kwparameters[arg]
        
        callback = kwparameters.pop('callback', None)
        self._open(callback=callback)
    
    def _open(self, **kwparameters):
        """Open the psycopg2 connction, and poll for OK."""
        self._db = psycopg2.connect(**self._dbargs)
        if not self._ioloop:
            self._ioloop = IOLoop.current()
        callback = kwparameters.pop('callback', self._on_ok)
        handler = functools.partial(self._poll, callback=callback)
        self._ioloop.add_handler(self._fd, handler, IOLoop.WRITE)
        
    def _on_ok(self, fd, events, **kwparameters):
        self._ready = True
    
    def close(self):
        """Close the database connection"""
        if getattr(self, '_db') is not None:
            self._db.close()
            self._db = None
            
    def reconnect(self, **kwparameters):
        """Close and reopen the database connection."""
        self.close()
        self._open(**kwparameters)
    
    @property
    def busy(self):
        """Check with psycopg2 if the connection
        is in use."""
        if getattr(self, '_db') is not None:
            return self._db.isexecuting()
        else:
            return None
    
    @property
    def _fd(self):
        """Get the database connection file descriptor."""
        if getattr(self, '_db') is not None:
            return self._db.fileno()
        else:
            return None
        
    def _cursor(self, **kwparameters):
        """Returns a new cursor."""
        return self._db.cursor(**kwparameters)
        
    def _poll(self, fd, events, **kwparameters):
        """Handle new data via the async connection."""
        callback = kwparameters.pop('callback', None)
        state = self._db.poll()
        if state == psycopg2.extensions.POLL_OK:
            # Connection is clear for use
            self._ioloop.remove_handler(fd)
            if callback is not None:
                callback() # results retrieved from cursor later
            self._execute_next()
        elif state == psycopg2.extensions.POLL_READ:
            # Reading
            self._ioloop.update_handler(fd, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            # Writing
            self._ioloop.update_handler(fd, IOLoop.WRITE)
        else:
            # Arithmatic (just kidding, error)
            raise psycopg2.OperationalError("poll returned a bad state: {}".format(state))

    def _poll_sync(self):
        """Poll until ready — used for sync queries.
        """
        while True:
            state = self._db.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                pass
            elif state == psycopg2.extensions.POLL_READ:
                pass
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)

    def _execute_next(self):
        try:
            bound = self._waiting_queries.get_nowait()
        except queue.Empty:
            pass
        else:
            bound()
    
    def _queue(self, result_handler, query, params):
        if self.busy:
            bound = functools.partial(self._execute, result_handler, query, params)
            self._waiting_queries.put_nowait(bound)
        else:
            self._execute(result_handler, query, params)            
    
    def _execute(self, result_handler, query, params):
        cursor = self._cursor(cursor_factory=RowCursor)
        cursor.execute(query, params)
        result_handler = functools.partial(result_handler, cursor)
        io_handler = functools.partial(self._poll, callback=result_handler)
        self._ioloop.add_handler(self._fd, io_handler, IOLoop.WRITE)
    
    @return_future
    def query(self, query, *parameters, **kwparameters):
        """Executes the query given and returns a row list. Either ordered or
        named parameters can be used (but not mixed).
        
        conn.query("SELECT name, email FROM users WHERE id = %s", 1)
        
        conn.query("SELECT id, name FROM users WHERE email = %(email)s",
            email='test@example.com')
        
        Arguments:
        query -- The query string. All data values should be replaced with %s
        *parameters -- data for substitution in the query string, in order
        
        Keyword arguments:
        **kwparameters -- data for substitution in the query string
        """
        callback = kwparameters.pop('callback', None)
        def handle_result(cursor):
            try:
                results = cursor.fetchall()
            finally:
                cursor.close()
            callback(results)
        
        self._queue(handle_result, query, kwparameters or parameters)
        
    def query_sync(self, query, *parameters, **kwparameters):
        """Sync version of query.
        """
        assert not 'callback' in kwparameters, "No callbacks!"
        self._poll_sync() # poll until we're ready to go
        cursor = self._cursor(cursor_factory=RowCursor)
        cursor.execute(query, kwparameters or parameters)
        self._poll_sync() # poll for results
        try:
            results = cursor.fetchall()
        finally:
            cursor.close()
        return results
    
    @return_future
    def get(self, query, *parameters, **kwparameters):
        """Same as `query`, but returns a single object rather than a list."""
        callback = kwparameters.pop('callback', None)
        def handle_results(rows):
            if not rows:
                callback(None)
            elif len(rows) > 1:
                raise MultipleRowsReturnedError("Multiple rows returned for get query")
            else:
                callback(rows[0])
        kwparameters.update(callback=handle_results)
        self.query(query, *parameters, **kwparameters)
        
    def get_sync(self, query, *parameters, **kwparameters):
        """Sync version of get.
        """
        results = self.query_sync(query, *parameters, **kwparameters)
        if len(results) > 1:
            raise MultipleRowsReturnedError("Multiple rows returned for get query")
        elif not results:
            return None
        else:
            return results[0]

    @return_future
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query. Lastrowid is not returned, unless you use
        RETURNING. rowcount=True will return the rowcount. Executemany is
        not supported by psycopg2 using async connections.
        
        Parameter substitution is as with `query`."""
        callback = kwparameters.pop('callback', None)
        rowcount = kwparameters.pop('rowcount', False)
        cursor = self._cursor()
        cursor.execute(query, kwparameters or parameters)
        def handle_result():
            try:
                if rowcount:
                    result = cursor.rowcount
                else:
                    try:
                        result = cursor.fetchone()
                    except ProgrammingError:
                        result = None
            finally:
                cursor.close()
            callback(result)
        handler = functools.partial(self._poll, callback=handle_result)
        self._ioloop.add_handler(self._fd, handler, IOLoop.WRITE)
      
class Row(psycopg2.extras.RealDictRow):
    """A dict that allows for object-like property access syntax.
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
            
class RowCursor(psycopg2.extras.DictCursorBase):
    """A psycopg2 cursor that returns Row objects
    (dicts with attribute access). Basically the same as
    psycopg2.extras.RealDictCursor.
    """
    def __init__(self, *args, **kwargs):
        kwargs['row_factory'] = Row
        super(RowCursor, self).__init__(*args, **kwargs)
        self._prefetch = 0
    
    def execute(self, query, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        return super(RowCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        return super(RowCursor, self).callproc(procname, vars)

    def _build_index(self):
        if self._query_executed == 1 and self.description:
            for i in range(len(self.description)):
                self.column_mapping.append(self.description[i][0])
            self._query_executed = 0

class MultipleRowsReturnedError(Exception):
    pass

# Alias exceptions    
DatabaseError = psycopg2.DatabaseError
OperationalError = psycopg2.OperationalError
IntegrityError = psycopg2.IntegrityError
DataError = psycopg2.DataError
ProgrammingError = psycopg2.ProgrammingError

# TZ offset
UTC = psycopg2.tz.FixedOffsetTimezone(offset=0, name='UTC')