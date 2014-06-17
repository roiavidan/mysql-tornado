mysql-tornado
=============

MySQLdb asynchronous wrapper for Tornado.

Features:
* Thread-pool based
* Transaction support

Example usage:

    import tornado.ioloop
    import tornado.gen
    from mysqltornado import MySQLConnection
    
    db = MySQLConnection('Server=somehost;Database=mydatabase;Uid=myuser;Pwd=mypasswd')
    
    @tornado.gen.coroutine
    def test():
        try:
            result = yield db.query('select * from sometable LIMIT 10')
            print 'result', result
        except Exception as e:
            print 'error', e
    
        # End Tornado IOLoop and close MySQL connection
        tornado.ioloop.IOLoop.instance().stop()
        db.close()
    
    # Start Tornado's IOLoop and execute test routine
    tornado.ioloop.IOLoop.instance().add_callback(test)
    tornado.ioloop.IOLoop.instance().start()
