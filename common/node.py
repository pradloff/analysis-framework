class node():
	def __init__(self,__parents__,__children__,__item__):
		self.__item__ = __item__
		self.__parents__ = []
		self.addParents(*__parents__)
		self.__children__ = []
		self.addChildren(*__children__)
		#self.__ancestor__ = {}
		#self.__decscendants__ = {}
				
	def addParents(self,*parents):
			for parent in parents: 
				if parent not in self.__parents__: 
					self.__parents__.append(parent)
					parent.addChildren(self)
					
	def addChildren(self,*children):
			for child in children:
				if child not in self.__children__:
					self.__children__.append(child)
					child.addParents(self)

	"""
	def getDescendants(self):
		descendants = {}
		for child in self.__children__:	
			descendants[child]=0

			descendants.update(dict((key,val) for key,val in child.getDescendants().items() if key not in descendants))
			#descendants.update( dict((key,val) for key,val child.getDescendants().items() if key not in descendants) )
		for descendant in descendants.keys(): descendants[descendant]+= 1
		return descendants

	def getProgenitors(self):
		progenitors = {}
		for parent in self.__parents__:	
			progenitors.update( parent.getProgenitors() )
			progenitors[parent]=0
		for progenitor in progenitors.keys(): progenitors[progenitor]-= 1
		return progenitors
	"""
