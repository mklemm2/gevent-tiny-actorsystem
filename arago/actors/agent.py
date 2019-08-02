from arago.actors import Monitor
import arago.actors.pattern_matching as matching


class Agent(Monitor):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.tell("start mission")
		self.stop()

	@matching.match(keyword="start mission")
	def handle(self, keyword, payload, sender):
		self._logger.info(f"{self} is starting its mission.")
		try:
			self.mission()
			self._logger.info(f"{self} has completed its mission.")
		except Exception:
			self._logger.error(f"{self} has failed its mission.")
			raise

	def mission(self):
		raise NotImplementedError("Please subclass arago.actors.Agent and implement the mission() method!")

