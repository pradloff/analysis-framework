from xml.dom.minidom import parseString
from common.EventFunction import EventFunction, ResultFunction
import ROOT
import re

#Good run list parser
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



class computeMCEventWeight(EventFunction):

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

