import ROOT
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
from functions import memoize
import os
import tempfile
from helper import root_quiet
from array import array

@memoize
def generate_dictionary(name,base):
    cwd = os.getcwd()
    os.chdir(tempfile.mkdtemp())
    with root_quiet(filters=["TClassTable::Add:0: RuntimeWarning","Note: Link requested for already"]):
        ROOT.gInterpreter.GenerateDictionary(name,base)
    os.chdir(cwd)

def generate_wrap(name):
	ROOT.gROOT.ProcessLine("struct _{0}{{{0} value;}};".format(name))
   	return getattr(ROOT,'_'+name)()

class branch(object):
    def __init__(self,name,type_,chain):
        self.name = name
        self.type = type_
        self.chain = chain
    
    @property
    def tbranch(self):
    	return self.chain.GetBranch(self.name)
        
    def generate_dictionary(self): pass

    def link(self,pchain): raise NotImplementedError

    def get_entry(self,entry): self.tbranch.GetEntry(entry)

    def read(self):
       	#self.generate_dictionary()
        self.chain.SetBranchStatus(self.name,1)
        self.read_link()

	def write_link(self):
		pass

	def write(self):
		pass
		
class vector_branch(branch):
    def __init__(self,name,type_,chain):
        super(vector_branch,self).__init__(name,type_,chain)
        
    def generate_dictionary(self):
        generate_dictionary(self.type,'vector')
    
    def overwrite(self,values):
    	self.value.clear()
    	for value in values: self.value.push_back(value)
    
    def read_link(self):
        self.value = getattr(ROOT,self.type)()
        self.chain.SetBranchAddress(self.name,ROOT.AddressOf(self.value))

    def get_value(self): return self.value

class std_branch(branch):

    #lookup = {
    #    'Char_t':'i',
    #    'Int_t':'i',
    #    'Bool_t':'i',
    #    'UInt_t':'i',
    #    'Long64_t':'l',
    #    'Float_t':'f',
    #    'Double_t':'d',
    #    }

    def __init__(self,name,type_,chain):
        super(std_branch,self).__init__(name,type_,chain)
         
         
    def overwrite(self,value):
    	self.value.value = value
    	       
    def read_link(self):
        #if self.type not in std_branch.lookup: raise TypeError('Unknown branch type {0}'.format(self.type))
        #self.value = array(std_branch.lookup[self.type],[0])
        self.value = generate_wrap(self.type)
        self.chain.SetBranchAddress(self.name,ROOT.AddressOf(self.value,'value'))

    def get_value(self): return self.value.value

#class

#class stub(branch):
#   def __init__(self):
#       super(stub,self).__init__()
