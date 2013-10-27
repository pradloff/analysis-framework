from copy import copy

class EventObject():

	def __init__(self):
		self.__break__ = False
		return

	def Add(self,itemName,eventItem):
		self.__dict__[itemName]=eventItem

