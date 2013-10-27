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
from standard import inGRL, skim, cutflow, computeMCEventWeight
#Patches stdout to pipe to a Queue which will handle logging
class logpatch():
	def __init__(self,Queue):
		self.queue=Queue

	def flush(self):
		pass

	def write(self,x):
		if x and x != '\n': self.queue.put(x)


def Analyze(analysis,entryRange,resultQueue,errorQueue,loggerQueue,processNumber,processes,directory):

	error = None
	#cleanup function always called no matter form of exit
	def cleanup():
		output.Close()
		#output name will be None if there is some problem
		resultQueue.put(outputName)
		#error will be None if there is NO problem
		errorQueue.put(error)
		sys.exit()

	#print statements executed in here and in Event/Result functions are redirected to the main logger
	sys.stdout = patch(loggerQueue)
	
	#Create local copy of analysis
	analysis = analysis.__copy__()

	#Create output
	outputName = '{directory}/temp_{0:0>{1}}.root'.format(processNumber,int(log(processes-1,10))+1,directory=directory)
	output = ROOT.TFile(outputName,'RECREATE')
	output.cd()

	try:
		analysis.AddStandardFunctions()
		analysis.SetupChain()
		if analysis.__skim__: analysis.AddResultFunction(skim(analysis.__chain__,analysis))

	except Exception:
		error = 'Error occured in initialization\n'+traceback.format_exc()
		outputName = None
	finally:
		cleanup()

	start,end = entryRange
	timeStart = time()

	entry=0
	try:
		for entry in xrange(start,end):
			#Create new event object (basically just a namespace)
			event = EventObject()
			event.__stop__ = 1
			event.__entry__ = entry
			analysis.__chain__.SetEntry(entry)
			for eventFunction in analysis.__EventFunctions__:
				#Get registered branches from chain
				analysis.__chain__.GetBranches(event,*(eventFunction.getItems(0)+eventFunction.getItems(1)+eventFunction.getItems(2)))
				#Call event function
				eventFunction(event)
				if event.__break__: break
				#Increment stop count (used in cutflow)
				event.__stop__+= 1
			for resultFunction in analysis.__ResultFunctions__:
				#Call result function (does not necessarily respect event.__break__, must be implemented on case by case basis in __call__ of result function)
				resultFunction(event)

			rate = (entry-start)/(time()-timeStart)
			percent = (entry-start+1)/(end-start)
		
			if (entry-start+1)%10:
				print 'Process number {0}: {1}% complete, {2} Hz'.format(processNumber,percentDone,rate)
	except Exception:
		error = 'Exception caught in entry {0}\n'.format(entry)+traceback.format_exc()
		outputName = None
	finally:
		cleanup()

	#Handle results
	try:
		for resultFunction in analysis.__ResultFunctions__:
			for v in resultFunction.items.values():
				output.cd()
				#Write result function items to output
				v.Write()

		for metaResultFunction in analysis.__MetaResultFunctions__:
			#Call meta-result function
			metaResultFunction(analysis.__files__)
			for v in metaResultFunction.items.values():
				output.cd()
				#Write meta-result function items to output
				v.Write()

	except Exception:
		error = 'Exception caught while handling results\n'+traceback.format_exc()
		outputName = None
	finally:
		cleanup()	

	cleanup()


#Base analysis class
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
		self.__EventFunctions__ = [computeMCEventWeight()]+self.__EventFunctions__
		self.AddResultFunction(cutflow(self))
		
	def SetupChain(self):
		
		self.__chain__ = PChain(self.__tree__,keep=self.__keep__)
		self.__chain__.AddFiles(*self.__files__)
		for EF in self.__EventFunctions__:
			for i in range(3):
				self.__chain__.AddItems(i,*EF.getItems(i))
		for EF in self.__EventFunctions__:
			self.AddBranch(*[(name,type_) for name,type_ in EF.items.get(2)])
		for item,itemType in self.__AdditionalItems__:
			self.__chain__.AddItem(item,itemType)

	def __call__(self):

		#Setup chain to catch any obvious problems
		self.SetupChain()

		if self.__Entries__: entries = min([self.__Entries__,self.__chain__.__chain__.GetEntries()])
		else: entries = self.__chain__.__chain__.GetEntries()

		if not entries: return 1
		
		#Result, error and log queue
		resultQueue = Queue()
		errorQueue = Queue()
		loggerQueue = Queue()
		
		#Create temp directory
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
				resultQueue,
				errorQueue,
				loggerQueue,
				i,
				self.__processes__,
				directory,
				progress[i]
				)) for i in range(self.__processes__)]

		for process in processes: process.start()


		def cleanup():
			for process in processes: 
				process.terminate()
				process.join()
			if os.path.exists(directory): shutil.rmtree(directory)		
			sys.exit()

		#Wait for processes to complete or kill them if ctrl-c		
		finished = 0
		looped = 0
		timeStart = time()

		while 1:
			try: sleep(0.1)
			except KeyboardInterrupt:
				pass
			finally: 
				cleanup()
				for process in processes: process.terminate()
				sys.exit()

			#flush logger queue
			while not loggerQueue.empty():
				print loggerQueue.get()

			while not errorQueue.empty():
				print errorQueue.get()
				cleanup()

			#flush result queue
			while not resultQueue.empty():
				finished+=1
				#flush logger
				while not loggerQueue.empty(): print loggerQueue.get()
				self.__result__.append(queue.get())
				print '{0}/{1} finished'.format(finished,self.__processes__)

			if finished==self.__processes__: break		
		
		
		#Create path to output and output ROOT file, merge results
		mkpath(os.path.dirname(self.__output__))
		
		if self.__processes__>1:
			merger = ROOT.TFileMerger()
			if os.path.exists(self.__output__): os.remove(self.__output__)
			merger.OutputFile(self.__output__)
			for result in self.__result__:
				merger.AddFile(result)
			merger.Merge()			
		else:
			shutil.move(self.__result__[0], self.__output__)

		cleanup()

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
	

import random

if __name__=='__main__':
	import code
	
	a = analysis()
	a.SetProcesses(10)
	a()
	
	code.interact(local=locals())
	
	
