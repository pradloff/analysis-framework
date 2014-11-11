import ROOT
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
from functions import memoize
import os
import tempfile
from helper import root_quiet

@memoize
def generate_dictionary(name,base):
    cwd = os.getcwd()
    os.chdir(tempfile.mkdtemp())
    with root_quiet(filters=["TClassTable::Add:0: RuntimeWarning","Note: Link requested for already"]):
	    ROOT.gInterpreter.GenerateDictionary(name,base)
    os.chdir(cwd)
    
class branch(object):
    def __init__(self,name,type_):
    	self.name = name
    	self.type = type_
    	self.tbranch = None
    	
   	def generate_dictionary(self): pass

	def link(self,pchain): raise NotImplementedError

	def get_entry(self,entry): self.tbranch.GetEntry(entry)

    def read(self,pchain):
		self.generate_dictionary()
		pchain().SetBranchStatus(self.name,1)
		self.link(pchain)

class vector_branch(branch):
	def __init__(self,name,type_):
		super(vector_branch,self).__init__(name,type_)
		
	def generate_dictionary(self):
		generate_dictionary(self.type,'vector')
	
	def link(self,pchain):
		self.value = getattr(ROOT,self.type)()
		pchain().SetBranchAddress(self.name,ROOT.AddressOf(self.value))

	def get_value(self): return self.value

class std_branch(branch):

	lookup = {
		'Char_t':'i',
		'Int_t':'i',
		'Bool_t':'i',
		'UInt_t':'i',
		'Long64_t':'l',
		'Float_t':'f',
		'Double_t':'d',
		}

	def __init__(self,name,type_):
		super(std_branch,self).__init__(name,type_)
				
	def link(self,pchain):
		if self.type not in lookup: raise TypeError('Unknown branch type {0}'.format(self.type))
		self.value = array(lookup[self.type],[0])
		pchain().SetBranchAddress(self.name,ROOT.AddressOf(self.value))	

	def get_value(self): return self.value[0]

#class

#class stub(branch):
#	def __init__(self):
#		super(stub,self).__init__()
