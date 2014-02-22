# Tornpsyco

A lightweight wrapper around [psycopg2][psycopg]. Inspired by [torndb][torndb] by Ben Darnell.

Uses a psycopg2 asynchronous connection and returns dict-based `Row` objects via futures.

[psycopg]: "http://initd.org/psycopg/"
[torndb]: "https://github.com/bdarnell/torndb"

## Introduction

    conn = Connection(host='localhost', database='test', user='test')

    ...

    @tornado.gen.coroutine
    def get(self):
        query = "SELECT id, name, email FROM users WHERE id = %s;"
        user = yield conn.get(query, (1,))
        self.write("Hello {}".format(user.name))


## API reference


### class `Connection`
The `Connection` class initializes a connection to the database, and encapsulates all query methods.

Arguments:

 * host -- postgresql host
 * database -- postgresql database

Keyword arguments:

 * user -- postgresql user
 * password -- postgresql password
 * port -- postgresql connection port
 * ioloop -- `tornado.ioloop.IOLoop` instance to use for callbacks and fd handlers. Shouldn't normally be required.
 * callback -- callback to execute on connection initialization

#### `query`
Executes the query given and returns a row list. Either ordered or named parameters can be used (but not mixed).
        
    conn.query("SELECT name, email FROM users WHERE id = %s", 1)

    conn.query("SELECT id, name FROM users WHERE email = %(email)s",
        email='test@example.com')
        
Arguments:

 * query -- The query string. All data values should be replaced with %s
 * *parameters -- data for substitution in the query string, in order

Keyword arguments:

 * **kwparameters -- data for substitution in the query string

#### `get`
Same as `query`, but returns a single object rather than a list.

#### `execute`
Executes the given query. Lastrowid is not returned, unless you use `RETURNING`. `rowcount=True` will return the rowcount. Executemany is not supported by psycopg2 using async connections.
        
Parameter substitution is as with `query`.

#### `close`
Closes the db connection.

#### `reconnect`
Closes and reopens the db connection.

### class `Row`
The `Row` class is a dict-like structure that data is returned from the database in.


## Misc

Datetimes returned via psycopg2 have a custom timezone set. To minimize confusion,
use the UTC offset for all datetime objects (e.g. `datetime.datetime.now(UTC)`).