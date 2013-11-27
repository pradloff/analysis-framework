class node():
	def __init__(self,parents,children,__item__):
		self.__item__ = __item__
		self.parents = []
		self.add_parents(*parents)
		self.children = []
		self.add_children(*children)
		#self.__ancestor__ = {}
		#self.__decscendants__ = {}
				
	def add_parents(self,*parents):
			for parent in parents: 
				if parent not in self.parents: 
					self.parents.append(parent)
					parent.add_children(self)
					
	def add_children(self,*children):
			for child in children:
				if child not in self.children:
					self.children.append(child)
					child.add_parents(self)

	def __call__(self): return self.__item__

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
