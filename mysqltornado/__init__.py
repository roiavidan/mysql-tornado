# -*- coding: UTF-8 -*-

import threading
import MySQLdb
import Queue

import tornado.ioloop
import tornado.gen
import tornado.concurrent


class MySQLConnection():

    class Worker(threading.Thread):

        def __init__(self, conn):
            """
            Initialize a new Worker thread.

            :return: None
            """
            self.conn = conn
            self.db = None
            super(MySQLConnection.Worker, self).__init__()

        def connect(self):
            """
            (re)Connect to a MySQL database.

            :return: None
            """
            if self.db is not None:
                self.disconnect()

            self.db = MySQLdb.connect(host=self.conn.host, port=self.conn.port, db=self.conn.db, user=self.conn.user, passwd=self.conn.pwd, use_unicode=True, charset='utf8')
            self.db.autocommit(self.conn.auto_commit)

        def disconnect(self):
            """
            Close a MySQL connection.

            :return: None
            """
            if self.db:
                try:
                    self.db.close()
                finally:
                    self.db = None

        def run(self):
            """
            Working thread main loop. Pick up jobs from the queue and execute them.

            :return: None
            """
            # First thing, create a MySQL connection for this thread
            self.connect()

            # Start thread's ioloop
            while self.conn.running:
                result = None
                error = None
                cursor = None
                try:
                    # Get next task from queue
                    task = self.conn.queue.get(True)

                    # Handle special abort command
                    if task['command'] == 'abort':
                        self.conn.queue.put(task)
                        break

                    # Get a DB cursor and execute query (at most 3 times!)
                    retries = 3
                    while retries > 0:
                        try:
                            cursor = self.db.cursor()
                            cursor.execute(task['query'])
                            error = None
                            break
                        except (AttributeError, MySQLdb.OperationalError) as e:
                            error = e
                            cursor = None
                            self.connect()

                    if error is not None:
                        raise Exception('Failed 3 reconnection attempts to MySQL server: {0}'.format(e))

                    # Determine result reading type
                    if task['command'] == 'select':
                        result = list(cursor.fetchall())
                        if len(result) == 0:
                            result = None
                    elif task['command'] == 'insert':
                        result = cursor.lastrowid
                except Exception as e:
                    error = e
                finally:
                    # Make sure we close the DB cursor!
                    if cursor is not None:
                        cursor.close()

                # Send result to the query's request-ee
                self.conn._send_result(task, result, error)

            # No more tasks. Close connection
            self.disconnect()

    def __init__(self, connection_string, worker_pool_size=10, auto_commit=True):
        """
        Initialize the Tornado Async MySQL wrapper

        :param connection_string: Connection string for establishing a server connection.
        :param worker_pool_size: Number of worker threads to initialize.
        :param auto_commit: Whether or not to auto commit statements on execution.
        :return: None
        """
        self.auto_commit = auto_commit
        self._parse_connection_string(connection_string)
        self.running = True
        self.queue = Queue.Queue()
        self.workers = []
        for i in xrange(worker_pool_size):
            w = MySQLConnection.Worker(self)
            w.start()
            self.workers.append(w)

    def _parse_connection_string(self, connection_string):
        """
        Parse the given Connection string. Only recognized parts will be used. All other parts will be ignored.

        :param connection_string: MySQL connection string.
        :return: None
        """
        self.host = '127.0.0.1'
        self.port = 3306
        self.db = None
        self.user = None
        self.pwd = None
        for part in connection_string.split(';'):
            part = part.strip()
            if part != '':
                k, v = part.split('=')
                k = k.lower()
                if k == 'server':
                    self.host = v.strip()
                elif k == 'port':
                    self.port = int(v.strip())
                elif k == 'database':
                    self.db = v.strip()
                elif k == 'uid':
                    self.user = v.strip()
                elif k == 'pwd':
                    self.pwd = v.strip()

    def _send_result(self, task, result, error):
        """
        Send a query result back to it's request-ee.

        :return: None
        """
        if error is None:
            task['future'].set_result(result)
        else:
            task['future'].set_exception(error)

    def close(self):
        """
        Shutdown this connection.

        :return: None
        """
        self.running = False
        self.queue.put({'command':'abort'})
        map(lambda w: w.join(), self.workers)

    def query(self, sql):
        """
        Perform a DB query.

        :param sql: SQL statement to execute.
        :return: Future instance
        """
        future = tornado.concurrent.TracebackFuture()
        self.queue.put({'query':sql, 'command':sql.split(None, 1)[0].lower(), 'future':future})

        return future
