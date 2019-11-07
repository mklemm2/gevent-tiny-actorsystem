import gevent
from arago.actors.monitor import Monitor
from arago.actors.actor import Task, ActorStoppedError

class Router(Monitor):
	def _route(self, msg):
		"""Override in your own Router subclass"""
		raise NotImplementedError

	def _forward(self, task):
		try:
			target = self._route(task)
			self._logger.trace("{me} is handing the task {task} to {target}".format(me=self, task=task, target=target))
			task = target._enqueue(task) if target else None
			return task
		except ActorStoppedError as e:
			gevent.idle()
			self._logger.trace("{me} has failed to route {task} to {target} because {target} is stopped".format(me=self, task=task, target=target))
			task.set_exception(e)
		except Exception as e:
			gevent.idle()
			self._logger.trace("{me} has failed to route {task}: Determining target failed with {err}".format(me=self, task=task, err=e))
			task.set_exception(e)
			raise

	def _handle(self, task):
		self._logger.trace("{me} received task {t} for routing".format(me=self, t=task))
		return self._forward(task)

	def join(self):
		self._logger.trace("{me} is waiting for all children to finish their work".format(me=self))
		[child.join() for child in self._children]
