
import yaml
import cPickle
import logging
import inspect
import traceback
import sys
from os.path import basename
from magi.testbed import testbed
from magi.messaging.magimessage import MAGIMessage

log = logging.getLogger(__name__)

class CallException(Exception):
	""" Marker for exceptions thrown during parsing of method call """
	pass

class MethodCall(object):
	"""
		Wrapper class that does the actual extraction of method call data from a YAML message if passed yaml data.
		Otherwise, a new method call can be constructed using this class.
	"""
	def __init__(self, request=None, method=None):
		if request is not None:
			self.method = None
			self.args = None
			self.buildFromRequest(request)
		elif method is not None:
			self.version = 1.0
			self.method = method
			self.args = {}

	def buildFromRequest(self, request):
		""" populate the object with the method call values from the (parsed) yaml request. """
		try:
			log.debug('request: %s', request)
			self.version = request['version']
			if self.version == 1.0:
				self.method = request['method']
				self.args = {}
				if 'args' in request:
					args = request['args']
					if type(args) is not dict:
						raise CallException("args is type %s, not dict" % type(args))
					for k, v in args.iteritems():
						self.args[k] = v  # pull it out
			else:
				raise CallException("Unknown Version %f" % self.version)
		except Exception, e:
			log.error("Error decoding method call (%s,%s): %s" % (self.method, self.args, e))
			raise

	def __getattr__(self, key):
		""" Called when attribute not found, return None as the default value for an argument, don't throw an error """
		return None


def dispatchCall(obj, msg, data):
	""" 
		Pull out the method and args from standard YAML method call in the given (parsed) yaml message and then call that method
	"""
	try:
		call = None
		call = MethodCall(data)
		meth = getattr(obj, call.method)
		spec = inspect.getargspec(meth)

		for k in call.args.keys():
			if k not in spec.args and spec.keywords is None:
				del call.args[k]
			if k == 'msg':
				del call.args[k]
				log.error("Can't use 'msg' as an argument name")

		return meth(msg, **call.args)
		
	except Exception, e:
		(fname, lineno, fn, text) = traceback.extract_tb(sys.exc_info()[2])[-1]
		if fname != __file__ and fname+'c' != __file__:  # not caused by our meth call, don't hide the real traceback
			raise

		if isinstance(e, TypeError):
			if spec.defaults is None:
				defaultslen = "None"
			else:
				defaultslen = len(spec.defaults)
			log.error("Error assigning arguments:\n\tProvided %s\n\tCall uses %s\n\tDefaults for last %s arg\n\t%s",
					call.args.keys(), spec.args[2:], defaultslen, spec.keywords and "Has keywords" or "No keywords")
		else:
			if call is None:
				method = "N/A"
			else:
				method = call.method
			log.error("Failed to dispatch %s call: %s", method, e)
		raise CallException(e)

		# TODO: should we send an error message back now or just send to logs for retrieval?

def doMessageAction(obj, msg, messaging=None):
	"""
		The function takes a message, and demuxxes it. Based on the content of the message it 
		may take a number of actions. That number is currently one: invoke dispatchCall
		which calls a function on "this" object whatever it is. 
	"""
	
	log.debug("In doMessageAction %s %s", str(obj), str(msg))
	log.info("Content type: %d", msg.contenttype)
	
	#First deserialize the message and then switch on the action. 
	if msg.contenttype == MAGIMessage.PICKLE:
		log.info("Content type: Pickle")
		data = cPickle.loads(msg.data)
	else:
		# Default data type is YAML
		data = yaml.load(msg.data)
		
	if 'method' in data:
		try: 
			retVal = dispatchCall(obj, msg, data)
		except Exception, e:
				log.error("Agent %s threw an exception %s during main loop", str(obj), e, exc_info=1)
				log.error("Sending back a RunTimeException event. This may cause the receiver to exit.")
				exc_type, exc_value, exc_tb = sys.exc_info()
				filename, line_num, func_name, text = traceback.extract_tb(exc_tb)[-1]
				filename = basename(filename)
				messaging.trigger(event='RuntimeException', func_name=func_name, agent=str(obj),
								   nodes=[testbed.nodename], filename=filename, line_num=line_num, error=str(e))
				return
			
		if 'trigger' in data:
			if retVal is None:
				log.error("Agent %s does not provide return value for method", str(obj))
				messaging.trigger(event='RuntimeException', agent=str(obj), nodes=[testbed.nodename], error="no return value")
				return 
			else:
				messaging.trigger(event=data['trigger'], nodes=[testbed.nodename], retVal=retVal)
				
	else:
		log.warn('got message without supported (or any?) action')
		



