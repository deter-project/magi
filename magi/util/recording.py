
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import Column, Integer, create_engine

RecordingSession = scoped_session(sessionmaker(autocommit=False, bind=create_engine('sqlite:////tmp/magi.db')))

DBase = declarative_base()

class Record(object):
	""" Base class for all things recorded into our node database """

	@declared_attr
	def __tablename__(cls):
		return cls.__name__.lower()

	id =  Column(Integer, primary_key=True)
	timestamp = Column(Integer)




