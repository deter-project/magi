import logging
import getpass
import pymongo
from datetime import datetime
from bson import InvalidDocument
from magi.util import database
from magi.testbed import testbed

class DatabaseFormatter(logging.Formatter):
    def format(self, record):
        """Format exception object as a string"""
        data = record.__dict__.copy()

        if record.args:
            record.msg = record.msg % record.args

        data.update(
            host=testbed.nodename,
            message=record.msg,
            username=getpass.getuser(),
            time=datetime.now(),
            args=tuple(unicode(arg) for arg in record.args)
        )
        
        if 'exc_info' in data and data['exc_info']:
            data['exc_info'] = self.formatException(data['exc_info'])
        
        return data

class DatabaseHandler(logging.Handler):
    """ 
    Logs all messages to a database. This  handler is designed 
    to be used with the standard python logging mechanism.
    """

    @classmethod
    def to(cls, db=database.DB_NAME, 
                collection=database.LOG_COLLECTION_NAME, 
                host=database.getCollector(), 
                port=database.DATABASE_SERVER_PORT, 
                level=logging.NOTSET):
        """ Create a handler for a given  """
        connection = database.getConnection()
        collection = pymongo.collection.Collection(database=connection[database.DB_NAME], 
                                                        name=database.LOG_COLLECTION_NAME)
        return cls(collection, level=level)
        
    def __init__(self, collection=database.LOG_COLLECTION_NAME, 
                        db=database.DB_NAME, 
                        host=database.getCollector(), 
                        port=database.DATABASE_SERVER_PORT, 
                        level=logging.NOTSET):
        """ Init log handler and store the collection handle """
        logging.Handler.__init__(self, level)
        if (type(collection) == str):
            connection = database.getConnection()
            self.collection = pymongo.collection.Collection(database=connection[database.DB_NAME], 
                                                            name=database.LOG_COLLECTION_NAME)
        else:
            self.collection = collection
        self.formatter = DatabaseFormatter()

    def emit(self, record):
        """ Store the record to the collection. Async insert """
        try:
            self.collection.save(self.format(record))
        except InvalidDocument, e:
            logging.error("Unable to save log record: %s", e.message ,exc_info=True)


