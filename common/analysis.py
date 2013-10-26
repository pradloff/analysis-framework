
#===============================================================================================================
"""
Base analysis class
"""
#===============================================================================================================

import ROOT
from PChain import PChain
import sys
import os
import re
import shutil
import traceback
from copy import copy, deepcopy
from time import sleep, time
from distutils.dir_util import mkpath
from multiprocessing import Process, Queue
from common.EventObject import EventObject
import code
import string
import random

class patch():
	def __init__(self,Queue):
		self.queue=Queue

	def flush(self):
		pass

	def write(self,x):
		if x and x != '\n': self.queue.put(x)

class analysis():
	def __init__(self,__keep__=False):
	
		self.__keep__ = __keep__
		
		self.__EventFunctions__ = []
		self.__ResultFunctions__ = []
		self.__MetaResultFunctions__ = []	
	
		self.__AdditionalItems__ = []
	
		self.__NewBranches__ = {}
		self.__KeepBranches__ = []

		self.__files__ = set([])
		self.__tree__ = 'physics'
		self.__skim__ = False
		self.__output__ = 'result.root'
		self.__GRL__ = None
		self.__processes__ = 2
		self.__verbose__ = False
		self.__Entries__ = None
		
		self.__result__ = []
	
	def __copy__(self):
		c = self.__class__()

		c.__EventFunctions__ = [copy(EF) for EF in self.__EventFunctions__]
		c.__ResultFunctions__ = [copy(EF) for EF in self.__ResultFunctions__]
		c.__MetaResultFunctions__ = [copy(EF) for EF in self.__MetaResultFunctions__]
	
		c.__AdditionalItems__ = self.__AdditionalItems__[:]
	
		c.__NewBranches__ = self.__NewBranches__.copy()
		c.__KeepBranches__ = self.__KeepBranches__[:]

		c.__files__ = self.__files__
		c.__tree__ = self.__tree__
		c.__skim__ = self.__skim__
		c.__output__ = self.__output__
		c.__GRL__ = self.__GRL__
		c.__processes__ = self.__processes__
		c.__verbose__ = self.__verbose__
		c.__Entries__ = self.__Entries__
		c.__keep__ = self.__keep__

		return c
		
	def AddEventFunction(self,*__EventFunction__):
		for EF in __EventFunction__: self.__EventFunctions__.append(EF)

	def AddResultFunction(self,*__EventFunction__):
		for EF in __EventFunction__: self.__ResultFunctions__.append(EF)

	def AddMetaResultFunction(self,*__EventFunction__):
		for EF in __EventFunction__: self.__MetaResultFunctions__.append(EF)
	
	def AddInput(self,*__input__):
		self.__files__ = self.__files__.union(set(__input__))
		
	def AddItem(self,item,itemType):
		self.__AdditionalItems__.append((item,itemType))

	def AddItems(self,itemType,*items):
		for item in items:
			self.__AdditionalItems__.append((item,itemType))
	
	def AddBranch(self,*b_tuples):
		for b_tuple in b_tuples: self.__NewBranches__[b_tuple[0]] = b_tuple[1]

	def KeepBranch(self,*bs):
		for b in bs: self.__KeepBranches__.append(b)
	
	def SetTree(self,__tree__):
		self.__tree__ = __tree__
		
	def SetSkim(self,__skim__):
		self.__skim__ = __skim__
		
	def SetOutput(self,__output__):
		self.__output__ = __output__

	def SetGRL(self,__GRL__):
		self.__GRL__ = __GRL__

	def SetProcesses(self,__processes__):
		self.__processes__ = __processes__
		
	def SetVerbose(self,__verbose__):
		self.__verbose__ = __verbose__
	
	def AddStandardFunctions(self):
		if self.__GRL__: self.__EventFunctions__ = [inGRL(self.__GRL__)]+self.__EventFunctions__
		self.__EventFunctions__ = [computeEventWeight()]+self.__EventFunctions__
		self.AddResultFunction(cutflow(self))
		
	def SetupChain(self):
		self.__chain__ = PChain(self.__tree__,keep=self.__keep__)
		self.__chain__.AddFiles(*self.__files__)
		for EF in self.__EventFunctions__:
			for i in range(3):
				self.__chain__.AddItems(i,*EF.getItems(i))
		for EF in self.__EventFunctions__:
			self.AddBranch(*[(name,type_) for name,type_ in EF.items.get(2)])
		#for EF in self.__EventFunctions__:
		#	self.CancelBranch(*[(name,type_) for name,type_ in EF.items.get(5)])
		for item,itemType in self.__AdditionalItems__:
			self.__chain__.AddItem(item,itemType)

	def Analyze(self,analysis,entryRange,queue,error,logger,i,processes,directory,progress):

		#Print statements executed in here and in Event/Result functions are redirected to the main logger
		sys.stdout = patch(logger)

		outputName = '{directory}/temp_{0:0>5}.root'.format(i,directory=directory)
		output = ROOT.TFile(outputName,'RECREATE')
		output.cd()
		#if i: 
		analysis = analysis.__copy__()
		entry=0
		try:
			analysis.AddStandardFunctions()
			analysis.SetupChain()
			if analysis.__skim__: analysis.AddResultFunction(skim(analysis.__chain__,analysis))
			
			result = {}

			start,end = entryRange

			#print start,end
			#sys.stdout.flush()
			
			#timeStart = time()

			for entry in xrange(start,end):
				event = EventObject()
				event.__stop__ = 1
				event.__entry__ = entry
				analysis.__chain__.SetEntry(entry)
				for EF in analysis.__EventFunctions__:
					analysis.__chain__.GetBranches(event,*(EF.getItems(0)+EF.getItems(1)+EF.getItems(2)))
					EF(event)
					if event.__break__: break
					event.__stop__+= 1
				for RF in analysis.__ResultFunctions__:
					RF(event)
				#if not(i) and not(entry%1000):
				#	percent = 100.*(float(entry)-start)/(float(end)-start)
				#	print '{percent}%, Event Rate: {rate} Hz\r'.format(percent=round(percent,1),rate=round(processes*(entry-start)/(time()-timeStart),2)),
				#	sys.stdout.flush()
				if not (entry-start+1)%10: progress.put(entry-start+1)
			progress.put(entry-start+1)
			#if not i: print '100%'

		except Exception:
			error.put('Exception caught in entry {0}\n'.format(entry)+traceback.format_exc())
			queue.put(None)
			sys.exit()

		try:
			for RF in analysis.__ResultFunctions__:
				for k,v in RF.items.items():
					try: v.Write()
					except: pass

			
			for RF in analysis.__MetaResultFunctions__:
				RF(analysis.__files__)
				for k,v in RF.items.items():
					output.cd()
					try: v.Write()
					except: pass
		except Exception:
			error.put(traceback.format_exc())
			queue.put(None)
			sys.exit()

		queue.put(outputName)
		output.Close()
			
		sys.exit()
	
	def __call__(self):
 		
		self.SetupChain()

		if self.__Entries__: entries = min([self.__Entries__,self.__chain__.__chain__.GetEntries()])
		else: entries = self.__chain__.__chain__.GetEntries()

		#self.__processes__ = max(min(self.__processes__,int(entries/1000.)),1)
		
		#print 'Processing {entries} entries with {processes} processes, output: {output}'.format(entries=entries,processes=self.__processes__,output=self.__output__)
		
		if not entries: return 1
		
		#Result and error queue
		queue = Queue()
		error = Queue()
		logger = Queue()
		progress = {}
		for i in range(self.__processes__): progress[i] = Queue()
		
		#Get temp directory
		while True:
			directory = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
			if directory not in os.listdir('.'):
				os.mkdir(directory)
				break

		#Instantiate and start processes
		ranges = [[i*(entries/self.__processes__),(i+1)*(entries/self.__processes__)] for i in range(self.__processes__)]; ranges[-1][-1]+=entries%(self.__processes__)
		processes = [Process(target = self.Analyze, args = (
				self,
				ranges[i],
				queue,
				error,
				logger,
				i,
				self.__processes__,
				directory,
				progress[i]
				)) for i in range(self.__processes__)]

		for process in processes: process.start()

		progressDict = {}
		for i in range(self.__processes__): progressDict[i] = 0

		#Wait for processes to complete or kill them if ctrl-c		
		finished = 0
		looped = 0
		timeStart = time()
		
		while 1:#any([process.is_alive() for process in processes]):
			try: sleep(0.1)
			except KeyboardInterrupt:
				for process in processes: process.terminate()		
				sys.exit()
			if not error.empty():
				e = error.get()
				print '\n'
				print e
				for process in processes: process.terminate(); process.join()
				
				shutil.rmtree(directory)				
				
				return 0

			#Update progress
			looped += 1
			if looped > 10:
				looped = 0
				while any([not progress[i].empty() for i in range(self.__processes__)]):
					#checked = True
					for i in range(self.__processes__):
						if progress[i].empty(): continue
						progressDict[i] = progress[i].get()
			
			#if checked: sleep(2)
			
			processed = sum(progressDict.values())
			percent = 100.*(float(processed)/float(entries))
			print '{percent}%, Event Rate: {rate} Hz, {processed}/{entries}'.format(percent=round(percent,1),rate=round(processed/(time()-timeStart),2),processed=processed,entries=entries),
			for i,v in sorted(progressDict.items()): print '{0}:{1}'.format(i,v),
			print '\r',
			sys.stdout.flush()
			
			if not queue.empty():
				self.__result__.append(queue.get())
				finished+=1
				#print finished,len(self.__result__)
				if finished == self.__processes__: break
				
		while not queue.empty():
			self.__result__.append(queue.get())
			finished+=1
		
		if finished != self.__processes__: raise Exception,'Number of results does not equal number of processes \({0}\\={1}\)'.format(finished,self.__processes__)		
				
		for process in processes: process.terminate(); process.join()
		
		#Create path to output and output ROOT file
		mkpath(os.path.dirname(self.__output__))
		
		if self.__processes__>1:
			merger = ROOT.TFileMerger()
			if os.path.exists(self.__output__): os.remove(self.__output__)
			merger.OutputFile(self.__output__)
			for i,result in enumerate(self.__result__): 
				#print i,result
				merger.AddFile(result)
			merger.Merge()			
			for result in self.__result__: os.remove(result)
		else:
			shutil.move(self.__result__[0], self.__output__)

		shutil.rmtree(directory)

		return 1
		
#===============================================================================================================
"""
Good run list parser
"""
#===============================================================================================================

from xml.dom.minidom import parseString

def parseGoodRunListXML(goodRunListXML):
	try:
		with open(goodRunListXML) as f: x = f.read()
		dom = parseString(x)
	except:
		print 'File {f} does not exist'.format(f=goodRunListXML)
		return {}

	goodRunListDict = {}
	
	for lumiBlockCollection in dom.getElementsByTagName('LumiBlockCollection'):
		run = int(lumiBlockCollection.getElementsByTagName('Run')[0].firstChild.data)
		for lBRange in lumiBlockCollection.getElementsByTagName('LBRange'):
			start = lBRange.getAttribute('Start')
			end = lBRange.getAttribute('End')
			if run not in goodRunListDict: goodRunListDict[run] = []
			goodRunListDict[run]+= range(int(start),int(end)+1)
				
	return goodRunListDict

#===============================================================================================================
"""
Standard Event and Result Functions
"""
#===============================================================================================================

from common.EventFunction import EventFunction, ResultFunction

#--------------------------------------------------------------------------------------------------------------

class computeEventWeight(EventFunction):

	def __init__(self,*args,**kwargs):
		EventFunction.__init__( self,*args,**kwargs )
		self.addItems()
		
	def addItems(self):
		self.addItem('mc_event_weight',2,type_='float')
		return
		
	def __call__(self,event):
		try:
			weight = event.mc_event_weight
		except AttributeError:
			weight = 1.

		event.mc_event_weight = weight
		event.__weight__ = weight

#--------------------------------------------------------------------------------------------------------------

class inGRL(EventFunction):

	def __init__(self,*args,**kwargs):
		EventFunction.__init__( self,*args,**kwargs )
		self.addItems()
		self.GRL = parseGoodRunListXML(args[0])
		
	def addItems(self):
		self.addItem('RunNumber',0)
		self.addItem('lbn',0)
		return
		
	def __call__(self,event):
		if event.RunNumber in self.GRL:
			if event.lbn in self.GRL.get(event.RunNumber): return
		event.__break__ = True


#--------------------------------------------------------------------------------------------------------------

class cutflow(ResultFunction):

	def __init__(self,*args,**kwargs):
		ResultFunction.__init__(self,*args,**kwargs)
		
		self.addItem('cutflow', ROOT.TH1F('cutflow','cutflow',len(args[0].__EventFunctions__)+1,0,len(args[0].__EventFunctions__)+1))		
		self.addItem('cutflow_weighted', ROOT.TH1F('cutflow_reweighted','cutflow_reweighted',len(args[0].__EventFunctions__)+1,0,len(args[0].__EventFunctions__)+1))		
		for i,name in enumerate(['input']+[e.__class__.__name__ for e in args[0].__EventFunctions__]):
			self.items.get('cutflow').GetXaxis().SetBinLabel(i+1,name)
			self.items.get('cutflow_weighted').GetXaxis().SetBinLabel(i+1,name)
		
	def __call__(self,event):
		for i in range(event.__stop__): self.items.get('cutflow').Fill(i)
		for i in range(event.__stop__): self.items.get('cutflow_weighted').Fill(i,event.__weight__)
		return

#--------------------------------------------------------------------------------------------------------------
from array import array

lookup = {
	'Char_t':'B',
	'UChar_t':'b',
	'Short_t':'S',
	'UShort_t':'s',
	'Int_t':'I',
	'UInt_t':'i',
	'Float_t':'F',
	'Double_t':'D',
	'Long64_t':'L',
	'ULong64_t':'l',
	'Bool_t':'O'
	}

lookup2 = {
	'int':'Int_t',
	'unsigned int':'UInt_t',
	'float':'Float_t',
	'string':'Char_t',
	}

class skim(ResultFunction):

	def __init__(self,*args,**kwargs):
	
		ROOT.gROOT.ProcessLine("struct Variable{Int_t variable_int; Bool_t variable_bool; Float_t variable_float;};")
		from ROOT import Variable
		
		ResultFunction.__init__(self,*args,**kwargs)
		self.__chain__ = args[0]
		self.__analysis__ = args[1]
		self.addItem('skim',ROOT.TTree(self.__chain__.__chain__.GetName(),self.__chain__.__chain__.GetTitle()))
		self.__count__ = 0
		self.__total__ = 0

		for itemName in self.__analysis__.__KeepBranches__:
			#print 'matching {0}'.format(itemName)
			#sys.stdout.flush()
			itemMatches = [name for name in self.__chain__.__items__ if re.match('^'+itemName+'$',name)]
			if not itemMatches:
				if not sum(1 for name in self.__analysis__.__NewBranches__ if re.match('^'+itemName+'$',name)):
					raise Exception,'No matches for "{0}", slim item.'.format(itemName)

			for itemName in itemMatches:
				#print '\tfound match {0}'.format(itemName)
				#sys.stdout.flush()
				availableItems = [item for item in self.__chain__.GetAvailableItems() if item.GetName()==itemName]
				if not availableItems: raise Exception,'The following item not found in chain: {0}'.format(itemName)
				try: itemType = availableItems[0].GetTypeName()
				except: itemType = availableItems[0].GetClassName()
				#print itemType
				if itemType in ['Char_t','Int_t','Bool_t','UInt_t','Long64_t','Float_t','Double_t']:
					try: desc = lookup[itemType]
					except KeyError: raise Exception,'Unknown item type {0} for {1}'.format(itemType,itemName)
					self.items.get('skim').Branch(itemName,self.__chain__.__items__[itemName],itemName+'/'+desc)
				elif itemType in ['string'] or itemType.startswith('vector'):
					#try: desc = lookup[lookup2[itemType.split('<')[-1].split('>')[0]]]
					#except KeyError: raise Exception,'Unknown item type {0} for {1}'.format(itemType,itemName)
					self.items.get('skim').Branch(itemName,itemType,ROOT.AddressOf(self.__chain__.__items__[itemName]))
				else:
					raise Exception,'Unknown item type {0} for {1}'.format(itemType,itemName)
			
		self.__items__ = {}

		for name,type_ in self.__analysis__.__NewBranches__.items():
			if type_.startswith('std.vector'):
				self.__items__[name] = ROOT.std.vector(type_.split('.')[-1])()
				if self.items.get('skim').GetBranch(name):
					self.items.get('skim').SetBranchAddress(name,self.__items__[name])
				else: self.items.get('skim').Branch(name,self.__items__[name])
			elif type_ == 'int': 
				self.__items__[name] = Variable()
				if self.items.get('skim').GetBranch(name):
					self.items.get('skim').SetBranchAddress(name,ROOT.AddressOf(self.__items__[name],'variable_int'))
				else: self.items.get('skim').Branch(name,ROOT.AddressOf(self.__items__[name],'variable_int'),name+'/I')
			elif type_ == 'bool': 
				self.__items__[name] = Variable()
				if self.items.get('skim').GetBranch(name):
					self.items.get('skim').SetBranchAddress(name,ROOT.AddressOf(self.__items__[name],'variable_bool'))
				else: self.items.get('skim').Branch(name,ROOT.AddressOf(self.__items__[name],'variable_bool'),name+'/O')
			elif type_ == 'float': 
				self.__items__[name] = Variable()
				if self.items.get('skim').GetBranch(name):
					self.items.get('skim').SetBranchAddress(name,ROOT.AddressOf(self.__items__[name],'variable_float'))
				else: self.items.get('skim').Branch(name,ROOT.AddressOf(self.__items__[name],'variable_float'),name+'/F')



		"""
		for name,type_ in self.__analysis__.__CancelBranches__.items():
			if name not in [b.GetName() for b in self.items.get('skim').GetListOfBranches()]: continue
			self.items.get('skim').SetBranchStatus(name,0)
		"""

	def __call__(self,event):
		self.__total__ +=1
		if event.__break__: return
		self.__chain__.__chain__.GetEntry(event.__entry__)
		for name,branch in self.__items__.items():
			type_ = self.__analysis__.__NewBranches__[name] 
			if type_.startswith('std.vector'):
				branch.clear()
				for v in getattr(event,name): branch.push_back(v)
			elif type_ == 'int': branch.variable_int = getattr(event,name)
			elif type_ == 'bool': branch.variable_bool = getattr(event,name)
			elif type_ == 'float': branch.variable_float = getattr(event,name)
			
		self.items.get('skim').Fill()
		self.__count__ +=1

		return

#===============================================================================================================

import random

if __name__=='__main__':
	import code
	
	a = analysis()
	a.SetProcesses(10)
	a()
	
	code.interact(local=locals())
	
	
