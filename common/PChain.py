
#===============================================================================================================
"""
Chain addon
"""
#===============================================================================================================

import ROOT
#import PyCintex
#PyCintex.Cintex.Enable()
from ROOT import gROOT, gInterpreter
gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
import re
from EventObject import EventObject
from array import array
import types
from copy import copy
import glob
import os


def generateDictionaries():
	cwd = os.getcwd()
	if not os.path.exists('dictionaries'): os.mkdir('dictionaries')
	os.chdir('dictionaries')
	gInterpreter.GenerateDictionary("std::vector<std::vector<float> >","vector")
	gInterpreter.GenerateDictionary("std::vector<std::vector<int> >","vector");
	gInterpreter.GenerateDictionary("std::vector<std::vector<unsigned int> >","vector");
	os.chdir(cwd)
	
generateDictionaries()


class PChain():

	def __del__(self):
		del self.__chain__

	def __init__(self,tree,__verbose__=False,keep=False):
	
		self.__tree__ = tree
		self.__chain__ = ROOT.TChain(tree)
		self.__verbose__ = __verbose__
		
		if not keep: self.__chain__.SetBranchStatus('*',0)
		self.__chain__.SetCacheSize(10000000)
		self.__chain__.SetCacheLearnEntries(10)

		self.__availableItems__ = None
		self.__availableItemNames__ = None
		
		self.__itemNames__ = set([])
		self.__branches__ = {}
		self.__items__ = {}

		self.__files__ = []
		self.__filesBranches__ = {}

		self.__CurrentTreeNumber__ = -1

	def GetEntries(self):
		return self.__chain__.GetEntries()

	def AddFiles(self,*files):
		for f in files: 
			tfile = ROOT.TFile(f)
			tree = getattr(tfile,self.__tree__,None)
			if any([
				not tree,
				not isinstance(tree,ROOT.TTree),
				]): raise Exception,'No matches for TTree "{0}" in file {1}.'.format(self.__tree__,f)
			self.__files__.append(f)
			self.__filesBranches__[f] = [b.GetName() for b in tree.GetListOfBranches()]
			self.__chain__.Add(f)

	def GetAvailableItems(self):
		if self.__availableItems__ is not None: return self.__availableItems__
		try: self.__availableItems__ = [branch for branch in self.__chain__.GetListOfBranches() if branch.GetClassName()]+[leaf for leaf in self.__chain__.GetListOfLeaves()]
		except: self.__availableItems__ = []
		return self.__availableItems__

	def GetAvailableItemNames(self,type_):
		#Type 0 items require all trees in all files to have the requested branch
		if not type_:
			fileBranches = self.__filesBranches__.values()
			if not fileBranches: return []
			if len(fileBranches)==1: return fileBranches[0]
			else: return fileBranches[0].intersection(*[set(branches) for branches in fileBranches[1:]])
		#All other types allow the possibility to react to different branches in trees
		else:
			fileBranches = self.__filesBranches__.values()
			if not fileBranches: return []
			if len(fileBranches)==1: return fileBranches[0]
			else: return fileBranches[0].union(*[set(branches) for branches in fileBranches[1:]])

	def AddItems(self,itemType,*itemNames):
		for itemName in itemNames: self.AddItem(itemName,itemType)

	def AddItem(self,itemName,itemType):
		itemMatches = [name for name in self.GetAvailableItemNames(itemType) if re.match('^'+itemName+'$',name)]
		if not itemMatches: 
			if not itemType: raise Exception,'No matches for "{0}", type 0 item.'.format(itemName)
			#else: print 'No matches for "{0}", type {1} item.'.format(itemName,itemType)
		itemMatches = [name for name in itemMatches if name not in self.__itemNames__]
		for itemMatch in itemMatches:
			setChainStatus = self.SetChainItem(itemMatch)
			if not setChainStatus: raise Exception,'The following item was not configured in chain: {0}'.format(itemName)
			self.__itemNames__.add(itemMatch)
		return

	def SetEntry(self,chainEntry):
		self.__CurrentEntry__ = self.__chain__.LoadTree(chainEntry)
		num = self.__chain__.GetTreeNumber()
		if self.__CurrentTreeNumber__ != num:
			self.__CurrentTreeNumber__ = num
			self.__branches__ = {}
			for item in set(self.__itemNames__)&set(self.__filesBranches__[self.__files__[self.__CurrentTreeNumber__]]):
				self.__branches__[item] = self.__chain__.GetBranch(item)
		
	def GetBranches(self,event,*items):
		#Add items to event which are registered and are not already in event:
		for item in set(items)&set(self.__itemNames__)-set(event.__dict__.keys()):
			#Pass-by items of type [1 or 2], all other item types should have failed registration
			try: self.__branches__[item].GetEntry(self.__CurrentEntry__)
			except: continue
			
			tempItem = self.__items__.get(item)
			if isinstance(tempItem,array): event.Add(item,tempItem[0])
			elif any([isinstance(tempItem,getattr(ROOT,type_)) for type_ in [\
				'vector<float,allocator<float> >',
				'vector<int,allocator<int> >',
				'vector<unsigned int,allocator<unsigned int> >'
				]]): event.Add(item,list(tempItem))
			elif any([isinstance(tempItem,getattr(ROOT,type_)) for type_ in [\
				'vector<vector<float,allocator<float> >>',
				'vector<vector<int,allocator<int> >>',
				'vector<vector<unsigned int,allocator<unsigned int> >>'
				]]): event.Add(item,[list(v) for v in tempItem])
			else: event.Add(item,tempItem)			
			
	def SetChainItem(self,itemName):
		availableItems = [item for item in self.GetAvailableItems() if item.GetName()==itemName]
		if not len(availableItems): 
			raise Exception,'The following item not found in chain: {0}'.format(itemName)
			return 0
		try:
			itemType = availableItems[0].GetTypeName()
		except:
			itemType = availableItems[0].GetClassName()
		if itemType in ['Char_t','Int_t','Bool_t','UInt_t','Long64_t','Float_t','Double_t']:
			if itemType in ['Char_t','Int_t','Bool_t','UInt_t']: self.__items__[itemName]=array('i',1*[0])
			elif itemType in ['Long64_t']: self.__items__[itemName]=array('l',1*[0])
			elif itemType in ['Float_t']: self.__items__[itemName]=array('f',1*[0.0])
			elif itemType in ['Double_t']: self.__items__[itemName]=array('d',1*[0.0])
			self.__chain__.SetBranchStatus(itemName,1)
			self.__chain__.SetBranchAddress(itemName,self.__items__[itemName])
			return 1
		elif itemType in ['string'] or itemType.startswith('vector'):
			if itemType in ['string']:
				self.__items__[itemName]=ROOT.std.string()
			elif itemType.startswith('vector'):				
				self.__items__[itemName] = getattr(ROOT,itemType)()
			self.__chain__.SetBranchStatus(itemName,1)
			self.__chain__.SetBranchAddress(itemName,ROOT.AddressOf(self.__items__[itemName]))
			return 1
		#self.__branches__[item] = self.__chain__.GetBranch(item)
		else:
			raise Exception,'The following item not found in chain: {0}'.format(itemName)
			return 0
