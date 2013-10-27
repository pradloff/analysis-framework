from common.analysis import analysis
from common.EventFunction import EventFunction, ResultFunction
from itertools import product
from math import sqrt,cos,sin
import ROOT
import sys
from metadata import lumi

class test(analysis):
	def __init__(self,*args,**kwargs):
		analysis.__init__(self,*args,**kwargs)
		
		self.AddEventFunction(
			)

		self.AddResultFunction(
			)

		self.AddMetaResultFunction(
			lumi()
			)

		keeps = [
			'el_n'
			]

		for item,itemType in [
			]: self.AddItem(item,itemType)

		for items,itemType in [
			(keeps,0),
			]: self.AddItems(itemType,*items)


		for items in [
			keeps
			]: self.KeepBranch(*items)
