import gevent
from arago.actors.monitor import Monitor
from arago.actors.actor import Task, ActorStoppedError

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def _forward(self, task):
		try:
			target = self._route(task.msg)
			self._logger.trace("{me} is handing the task {task} to {target}".format(me=self, task=task, target=target))
			return target._enqueue(task)
		except ActorStoppedError as e:
			gevent.idle()
			self._logger.trace("{me} has failed to route {task} to {target} because {target} is stopped".format(me=self, task=task, target=target))
			task.set_exception(e)
		except Exception as e:
			gevent.idle()
			self._logger.trace("{me} has failed to route {task}: Determining target failed".format(me=self, task=task))
			task.set_exception(e)
			raise

	def _handle(self, task):
		return self._forward(task)
