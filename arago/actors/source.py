from arago.actors.actor import Actor


class SourceDoesntAcceptMessagesError(Exception):
	__str__ = lambda x: "SourceDoesntAcceptMessagesError"

class Source(Actor):
	def __init__(self, server, *args, **kwargs):
		super().__init__(loop=lambda: None, *args, **kwargs)
		self._server = server
		self._server.start()

	def _receive(self, msg, sender=None):
		raise SourceDoesntAcceptMessagesError

	def start(self):
		if self._stopped:
			self._server.start()
			self._stopped = False
			self._logger.debug("{me} has resumed operation.".format(me=self))
		else:
			self._logger.debug("{me} is already started.".format(me=self))

	def stop(self):
		if not self._stopped:
			self._stopped = True
			self._logger.debug("{me} received order to stop.".format(me=self))
			self._server.stop()
		else:
			self._logger.debug("{me} is already stopped.".format(me=self))

	def clear(self):
		return
