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
    def __init__(self,name,mode):
        self.name = name
        self.mode = mode
        self.open = False
        
    @property
    def tbranch(self):
    	return self.chain.GetBranch(self.name)
    
    def generate_dictionary(self): pass

    def read_link(self): raise NotImplementedError

    def read(self,chain):
    	if 'r' not in self.mode: raise RuntimeError('Branch {0} not open for reading'.format(self.name))
    	self.chain = chain
       	#self.generate_dictionary()
        self.chain.SetBranchStatus(self.name,1)
        self.read_link()
        self.open = True
        
    def update(self,entry): 
        if self.open: self.tbranch.GetEntry(entry)
        else: raise RuntimeError('Branch {0} not open for reading'.format(self.name))
        
	def write_link(self,chain): raise NotImplementedError

	def write(self,chain):
		if 'w' not in self.mode: raise RuntimeError('Branch not open for writing'.format(self.name))
		self.write_link(chain)
		pass
		
class vector_branch(branch):
    def __init__(self,name,mode,type_):
        super(vector_branch,self).__init__(name,mode)
        self.type = type_
    #def generate_dictionary(self):
    #    generate_dictionary(self.type,'vector')
    
    def overwrite(self,values):
    	self.value.clear()
    	for value in values: self.value.push_back(value)
    
    def read_link(self,):
        print self.type
        self.value = getattr(ROOT,self.type)()
        self.chain.SetBranchAddress(self.name,ROOT.AddressOf(self.value))

    def write(self,chain):
        chain.Branch(
            self.name,
            self.value,
            )
            
    @property
    def payload(self): return self.value

    @payload.setter
    def payload(self,value):
        if 'r' in self.mode: raise RuntimeError('Value of branch {0} cannot be modified, mode {1}'.format(self.name,self.mode))
        self.value.clear()
        for value in values: self.value.push_back(value)

    #def get_value(self): return self.value

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

    def __init__(self,name,mode,type_):
        super(std_branch,self).__init__(name,mode)
        self.type = type_
        
    def overwrite(self,value):
    	self.value.value = value
    	       
    def read_link(self):
        self.value = generate_wrap(self.type)
        self.chain.SetBranchAddress(self.name,ROOT.AddressOf(self.value,'value'))

    @property
    def payload(self): return self.value.value

    @payload.setter
    def payload(self,value):
        if 'r' in self.mode: raise RuntimeError('Value of branch {0} cannot be modified, mode {1}'.format(self.name,self.mode))
        self.value.value = value
        
    def write(self,chain):
        chain.Branch(
            self.name,
            ROOT.AddressOf(self.value,'value'),
            self.name+'/'+std_branch.lookup[self.type]
            )
#class

#class stub(branch):
#   def __init__(self):
#       super(stub,self).__init__()
