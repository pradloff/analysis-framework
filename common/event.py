class event_object():

	def __init__(self):
		self.__break__ = False

	def __contains__(self,item):
		return item in self.__dict__	

