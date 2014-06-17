# -*- coding: UTF-8 -*-

import threading
import MySQLdb
import Queue

import tornado.ioloop
import tornado.gen
import tornado.concurrent


class MySQLConnection():

    class Worker(threading.Thread):

        def __init__(self, conn, *args, **kwargs):
            """
            Initialize a new Worker thread.

            :return: None
            """
            self.conn = conn
            self.db = None
            self.in_tx = False
            super(MySQLConnection.Worker, self).__init__(*args, **kwargs)

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

                    # Ignore Transactions which are not this thread's
                    tx_id = task.get('tx_id')
                    if tx_id is not None:
                        if tx_id != self.name:
                            # Put task request back into queue and wait again
                            self.conn.queue.put(task)
                            continue

                    # Handle transactions
                    if task['command'] == '*begin-tx*':
                        if self.in_tx:
                            # Already attending a transaction, return request to queue
                            self.conn.queue.put(task)
                            continue
                        else:
                            # Signal this Thread will handle the Transaction!
                            self.in_tx = True
                            result = self.name
                    elif task['command'] == '*end-tx*':
                        if self.in_tx and task['tx_id'] == self.name:
                                # This is our signal to stop attending this transaction
                                self.in_tx = False
                        else:
                            # Not attending a transaction or it's not our transaction. Either way, ignore request
                            self.conn.queue.put(task)
                            continue
                    else:
                        # Get a DB cursor and execute query (at most 3 times!)
                        retries = 3
                        while retries > 0:
                            try:
                                cursor = self.db.cursor()
                                cursor.execute(task['query'])
                                error = None
                                break
                            except (AttributeError, MySQLdb.OperationalError) as e:
                                retries -= 1
                                error = e
                                cursor = None
                                self.connect()
                            except Exception as e:
                                if cursor is not None:
                                    cursor.close()
                                error = e
                                break

                        if error is not None and retries == 0:
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

    class Transaction():

        def __init__(self, conn):
            """
            Initialize a Transaction context manager.

            :param conn: Reference to the MySQL connection instance.
            """
            self.conn = conn
            self.tx_id = None

        @tornado.gen.coroutine
        def query(self, query):
            """
            Execute a query
            """
            ret = yield self.conn.query(query, tx_id=self.tx_id)
            raise tornado.gen.Return(ret)

        @tornado.gen.coroutine
        def begin(self):
            """
            Start a new Transaction.

            :return: None
            """
            if self.tx_id is None:
                self.tx_id = yield self.conn.query('*begin-tx*')
                yield self.conn.query('START TRANSACTION WITH CONSISTENT SNAPSHOT', tx_id=self.tx_id)

        @tornado.gen.coroutine
        def commit(self):
            """
            Commit transaction.

            :return: None
            """
            if self.tx_id is not None:
                try:
                    yield self.conn.query('COMMIT', tx_id=self.tx_id)
                finally:
                    yield self.conn.query('*end-tx*', tx_id=self.tx_id)
                    self.tx_id = None

        @tornado.gen.coroutine
        def rollback(self):
            """
            Rollback transaction.

            :return: None
            """
            if self.tx_id is not None:
                try:
                    yield self.conn.query('ROLLBACK', tx_id=self.tx_id)
                finally:
                    yield self.conn.query('*end-tx*', tx_id=self.tx_id)
                    self.tx_id = None

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
            w = MySQLConnection.Worker(self, name=str(i))
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

    def query(self, sql, tx_id=None):
        """
        Perform a DB query.

        :param sql: SQL statement to execute.
        :return: Future instance
        """
        future = tornado.concurrent.TracebackFuture()
        self.queue.put({'query':sql, 'command':sql.split(None, 1)[0].lower(), 'future':future, 'tx_id':tx_id})

        return future

    def transaction(self):
        """
        Return a new Transaction helper object for performing queries within a Transaction scope.

        :return: Transaction instance.
        """
        return MySQLConnection.Transaction(self)

