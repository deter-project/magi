#!/usr/bin/python

# Modified version of sched.py from python core
# We don't want a main loop, but we do want to know when the next event should fire
# Also add option to remove by method pointer

from magi.util.Collection import namedtuple
import heapq
import time
import sys
import logging

log = logging.getLogger(__name__)

Event = namedtuple('Event', 'time, nextdelay, method, args')
debug = False

class Scheduler(object):
	"""
		Only passes messages if the current time is greater the msg.time header.  If the time header
		doesn't exist, they are passed right away
	"""

	def __init__(self):
		self.heap = list()  # Contains messages order by scheduled time value

	def sched_time(self, when, method, *args):
		""" Request that at time 'when', we call 'method(args)', returns the scheduled event """
		event = Event(when, 0, method, args)
		heapq.heappush(self.heap, event)
		return event

	def sched_relative(self, delay, method, *args):
		""" Request that at time 'now + delay', we call 'method(args)', returns the scheduled event """
		event = Event(time.time() + delay, 0, method, args)
		heapq.heappush(self.heap, event)
		return event

	def periodic(self, delay, method, *args):
		""" Request that at time 'now + delay', and every delay seconds there after we call 'method(args)', returns the scheduled event """
		event = Event(time.time() + delay, delay, method, args)
		heapq.heappush(self.heap, event)
		return event

	def getByMethod(self, method):
		""" Get the first event that is using the given method """
		for event in self.heap:
			if event.method == method:
				return event
		return None

	def unsched(self, event):
		""" Call to unschedule an event that hasn't fired yet.  Argument is the event returned by sched_* """
		try:
			self.heap.remove(event)
			heapq.heapify(self.heap)
		except ValueError:  # not in the schedule
			pass

	def run(self):
		""" Fire any events are ready to go, return the time until the next event when nothing else ready """
		return self._run(time.time())

	def _run(self, totime=sys.maxint):
		""" Fire all scheduled events up to given time regardless of the real time """
		while len(self.heap) > 0:
			if self.heap[0].time > totime:
				break
			event = heapq.heappop(self.heap)
			event.method(*event.args)
			if event.nextdelay > 0:  # periodic call, reschedule
				self.periodic(event.nextdelay, event.method, *event.args)
		
		if len(self.heap) > 0:
			return self.heap[0].time - totime
		return sys.maxint

	def _doall(self):
		""" some events encode new events, this lets us run all the events in the queue while ignoring new ones, used for testing """
		for event in self.heap[:]:
			event.method(*event.args)
		self.heap = list()


