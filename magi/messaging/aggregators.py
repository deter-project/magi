
from magi.messaging.api import MAGIMessage
from magi.messaging.processor import MessageProcessor
import logging

log = logging.getLogger(__name__)

class Aggregator(MessageProcessor):
	def setIdProcessor(self, nameid):
		self.nameid = nameid

class TextAggregator(Aggregator):
	"""
		Processor that attempts to combine text messages when it can, a little bit hardcoded.  Really need
		to store by destination but for now...
	"""
	def __init__(self):
		Aggregator.__init__(self)
		self.blobs = dict()  # map from incoming transport -> (timer, list of strings)

	def processPRE(self, msglist, now):
		passed = list()

		# See what we can absorb
		for msg in msglist:
			if 'worm' in msg.dstdocks and 'data' in msg.dstgroups:
				ifin = msg._receivedon
				if ifin not in self.blobs:
					when = now + 0.1
					self.blobs[ifin] = (when, list())
					self.msgintf.needPush('PRE', when)  # if past a current active push request, the earliest request wins

				self.blobs[ifin][1].append(msg.data)
			else:
				passed.append(msg)  # regular messages
				

		# See what now needs to be sent
		for transport, (timer, blob) in self.blobs.items():
			if timer > now:
				self.msgintf.needPush('PRE', timer) # make sure we get later pushes as needed, again min(...) wins
				continue

			del self.blobs[transport]  # pull out entry

			if len(blob) > 0:
				log.info("Sending collected worm messages (%s)", len(blob))
				msg = MAGIMessage(groups='data', docks='worm', contenttype=MAGIMessage.TEXT, data='\n'.join(blob)) 
				self.nameid.processOUT([msg])  # need a new source and valid msg ID
				msg._receivedon = transport # lets routing know where NOT to send it again, no need to smurf ourselves
				passed.append(msg)
	

		return passed


class FormattedDataAggregator(Aggregator):
	"""
		Processor to do aggregation on data formatted in our 'common' method
	"""
	def __init__(self):
		Aggregator.__init__(self)
		
	def processPRE(self, msglist, now):
		return msglist

