
from logging import Handler, Logger, RootLogger, NOTSET, INFO
import threading

class AgentLogger(Logger):
	""" This becomes the core logger class, adds the check for agent log level before continuing """

	def isEnabledFor(self, level):
		threadName = threading.current_thread().name
		if self.root.agentLevels.get(threadName, 0) > level:
			return 0
		return Logger.isEnabledFor(self, level)


class AgentRootLogger(AgentLogger):
	""" Replaces the core root logger with one that stores our agent log level map """

	def __init__(self, level):
		AgentLogger.__init__(self, "root", level)
		self.agentLevels =  dict()
		#self.setThreadLevel('MainThread', 100)

	def setThreadLevel(self, agentName, level):
		""" set agent level, this only exists in the root """
		self.agentLevels[agentName] = level


