
#===============================================================================================================
"""
Base EventFunction class, inherting class must create callable "__call__" accepting event object as input 
"""
#===============================================================================================================

import ROOT
from copy import copy

class EventFunction():
	def __init__(self,*args,**kwargs):
		self.__args__ = args
		self.__kwargs__ = kwargs
		self.items = {\
			0:[],
			1:[],
			2:[],
			3:[],
			4:[],
			5:[],
		}
		return
	
	def __call__(self,event):
		return event,True
	
	def addItems(self,itemType,*itemNames):
		for itemName in itemNames: self.addItem(itemName,itemType)
	
	def addItem(self,itemName,itemType,type_=None):
		#itemType=0: Must be requested from chain and must be added to event
		#itemType=1: Must be requested from chain
		#itemType=2: Branch is replaced by same name in TTree
		#itemType=3: Must be added to event from one of preceding functions
		#itemType=4: Added to event in this function
		#itemType=5: Excluded from skim
		
		try: 
			for it in itemType: self.items.get(it).append((itemName,type_))
		except TypeError: self.items.get(itemType).append((itemName,type_))
	
	def getItems(self,itemType):
		return [name for name,type_ in self.items.get(itemType)]

	def __copy__(self):
		return self.__class__(*self.__args__,**self.__kwargs__)
		"""
		newEventFunction = EventFunction()
		newEventFunction.__dict__.update(self.__dict__)
		newEventFunction.items = copy(self.items)
		return newEventFunction
		"""
#===============================================================================================================
"""
Base ResultFunction class, inherting class must create callable "__call__" accepting event object and result container as object.  Result container is prefilled with empty result items.
"""
#===============================================================================================================

class ResultFunction():
	def __init__(self,*args,**kwargs):
		self.__args__ = args
		self.__kwargs__ = kwargs
		self.items = {}
		return
	
	def __call__(self,event):
		return event,True
	
	def addItem(self,itemName,item):
		if isinstance(item,ROOT.ObjectProxy): ROOT.SetOwnership(item,False)
		self.items[itemName] = item

	def __copy__(self):
		return self.__class__(*self.__args__,**self.__kwargs__)

