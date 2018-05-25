from arago.actors.monitor import Monitor
from arago.actors.actor import Task, ActorCrashedError

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def _receive(self, msg):
		if not self._stopped.is_set():
			target = self._route(msg)
			task = target._receive(msg)
		else:
			task = Task(msg)
			task.set_exception(ActorCrashedError)
			self._logger.warn("{me} has crashed and rejects {task}".format(me=self, task=task))
		return task
