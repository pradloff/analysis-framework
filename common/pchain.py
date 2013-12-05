import ROOT
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
import re
from array import array
import os

def generate_dictionaries():
	cwd = os.getcwd()
	if not os.path.exists('dictionaries'): os.mkdir('dictionaries')
	os.chdir('dictionaries')
	for name,base in [
		("vector<vector<float> >","vector"),
		("vector<vector<int> >","vector"),
		("vector<vector<unsigned int> >","vector"),
		("vector<vector<string> >","vector"),
		("vector<vector<vector<float> > >","vector"),
		("vector<vector<vector<int> > >","vector"),
		("vector<vector<vector<unsigned int> > >","vector"),
		('vector<TLorentzVector>','vector;TLorentzVector.h'),
		]: ROOT.gInterpreter.GenerateDictionary(name,base)
	os.chdir(cwd)

"""
def generate_dictionaries():
	cwd = os.getcwd()
	try: os.mkdir('dictionaries')
	except OSError as error:
		if error.errno!=17: raise 
	os.chdir('dictionaries')
	if not os.path.exists('Loader.C'):
		with open('Loader.C','w') as f:
			for line in [
				'#include "TLorentzVector.h"',
				'#include <vector>',
				'#ifdef __MAKECINT__',
				'#pragma link C++ class vector<vector<bool> >+;',
				'#pragma link C++ class vector<vector<int> >+;',
				'#pragma link C++ class vector<vector<double> >+;',
				'#pragma link C++ class vector<vector<string> >+;',
				'#pragma link C++ class vector<vector<unsigned int> >+;',
				'#pragma link C++ class vector<vector<float> >+;',
				'#pragma link C++ class vector<TLorentzVector>+;',
				'#endif',
				]: f.write(line+'\n')
        ROOT.gROOT.ProcessLine('.L {0}+'.format(os.path.abspath('Loader.C')))
	os.chdir(cwd)
"""

class pchain():

	def __init__(self,tree):
	
		self.tree = tree
		self.chain = ROOT.TChain(tree)
		self.chain.SetCacheSize(10000000)
		self.chain.SetCacheLearnEntries(10)

		self.branch_names = []
		self.branch_types = {}
		self.branch_values = {}
		self.branches = {}
		self.created_branches = {}

		self.branches_union = None
		self.branches_intersection = None

		self.files = []
		self.files_branches = {}

		self.current_file_number = -1

	def __call__(self):
		return self.chain

	def get_entries(self):
		return self.chain.GetEntries()

	def add_files(self,files):
		for f in files:
			tfile = ROOT.TFile.Open(f)
			try: tree = getattr(tfile,self.tree,None)
			except AttributeError: raise OSError,'File {0} does not exist or could not be opened'.format(f)
			if any([
				not tree,
				not isinstance(tree,ROOT.TTree),
				]): raise ValueError,'No matches for TTree "{0}" in file {1}.'.format(self.tree,f)
			self.files.append(f)
			self.files_branches[f] = [branch.GetName() for branch in tree.GetListOfBranches()]
			for leaf in tree.GetListOfLeaves():
				if leaf.GetName() not in self.branch_types: self.branch_types[leaf.GetName()] = leaf.GetTypeName()
				elif leaf.GetTypeName()!=self.branch_types[leaf.GetName()]:
					raise TypeError('Branch typing for {0} changes in file {1}'.format(leaf.GetName(),f))
			tfile.Close()
			self.chain.Add(f)

	def get_available_branch_names(self,required=True):
		#require all trees in all files to have the requested branch
		if required:
			if not self.branches_intersection: 
				file_branches = self.files_branches.values()
				if not file_branches: self.branches_intersection = []
				elif len(file_branches)==1: self.branches_intersection = file_branches[0]
				else: self.branches_intersection = list(set(file_branches[0]).intersection(*[set(branches) for branches in file_branches[1:]]))
			return self.branches_intersection
		#All other types allow the possibility to react to different branches in trees
		else:
			if self.branches_union is None: 
				file_branches = self.files_branches.values()
				if not file_branches: self.branches_union = []
				elif len(file_branches)==1: self.branches_union = file_branches[0]
				else: self.branches_union = list(set(file_branches[0]).union(*[set(branches) for branches in file_branches[1:]]))
			return self.branches_union

	#creates virtual branch
	def create_branch(self,branch_name,event_function_name):
		self.created_branches[branch_name] = event_function_name
		if branch_name in self.get_available_branch_names(required=False):
			self.set_branch(branch_name)
			self.branch_names.append(branch_name)

	def create_branches(self,branch_names,event_function_name):
		for branch_name in branch_names: self.create_branch(branch_name,event_function_name)		

	def request_branch(self,branch_name):
		if branch_name in self.created_branches: return
		if branch_name in self.branch_names: return
		if branch_name not in self.get_available_branch_names(required=True):
			raise ValueError('No matches for required branch {0}'.format(branch_name))
		self.set_branch(branch_name)
		self.branch_names.append(branch_name)

	def request_branches(self,branch_names):
		for branch_name in branch_names: self.request_branch(branch_name)

	def set_entry(self,entry):
		self.current_entry = self.chain.LoadTree(entry)
		file_number = self.chain.GetTreeNumber()
		#reset if file changed
		if self.current_file_number != file_number:
			self.current_file_number = file_number
			self.branches = {}
			for branch_name in set(self.branch_names)&set(self.files_branches[self.files[self.current_file_number]]):
				self.branches[branch_name] = self.chain.GetBranch(branch_name)
		
	def get_branches(self,event,branch_names,event_function_name):
		#Add values to event which are registered and are not already in event:
		for branch_name in set(branch_names)&set(self.branch_names)-set(event.__dict__.keys()):

			try: self.branches[branch_name].GetEntry(self.current_entry)
			except KeyError:
				try: assert(self.created_branches[branch_name]==event_function_name)
				except KeyError,AssertionError: raise ValueError('Branch {0} not created as promised by event function {1}'.format(branch_name,self.created_branches[branch_name]))

			value = self.branch_values[branch_name]
			#pull single value from array
			if isinstance(value,array): event.__dict__[branch_name] = value[0]
			#convert vectors to lists
			elif any([isinstance(value,getattr(ROOT,branch_type)) for branch_type in [\
				'vector<float,allocator<float> >',
				'vector<int,allocator<int> >',
				'vector<unsigned int,allocator<unsigned int> >'
				]]): event.__dict__[branch_name] = list(value)
			#convert vectors of vectors to lists of lists
			elif any([isinstance(value,getattr(ROOT,branch_type)) for branch_type in [\
				'vector<vector<float,allocator<float> >>',
				'vector<vector<int,allocator<int> >>',
				'vector<vector<unsigned int,allocator<unsigned int> >>'
				]]): event.__dict__[branch_name] = [list(inner_vector) for inner_vector in value]
			else: event.__dict__[branch_name] = value
		for branch_name in set(branch_names)-set(self.branch_names)-set(event.__dict__.keys()):
			if not self.created_branches[branch_name]==event_function_name:
				raise ValueError('Branch {0} not created as promised by event function {1}'.format(branch_name,self.created_branches[branch_name]))			
			
	def set_branch(self,branch_name):
		branch_type = self.branch_types[branch_name]

		if branch_type in ['Char_t','Int_t','Bool_t','UInt_t']:
			self.branch_values[branch_name]=array('i',1*[0])
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,self.branch_values[branch_name])
		elif branch_type == 'Long64_t':
			self.branch_values[branch_name]=array('l',1*[0])
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,self.branch_values[branch_name])
		elif branch_type == 'Float_t':
			self.branch_values[branch_name]=array('f',1*[0.0])
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,self.branch_values[branch_name])
		elif branch_type == 'Double_t':
			self.branch_values[branch_name]=array('d',1*[0.0])
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,self.branch_values[branch_name])			
		elif branch_type == 'string':
			self.branch_values[branch_name]=ROOT.std.string()
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,ROOT.AddressOf(self.branch_values[branch_name]))
		elif branch_type.startswith('vector'):
			try:
				self.branch_values[branch_name] = getattr(ROOT,branch_type)()
			except AttributeError:
				raise TypeError('Unknown ROOT type {0} for branch {1}'.format(branch_type,branch_name))
			self.chain.SetBranchStatus(branch_name,1)
			self.chain.SetBranchAddress(branch_name,ROOT.AddressOf(self.branch_values[branch_name]))
		else:
			raise ValueError('Branch {0} could not be configured, type "{1}" not supported'.format(branch_name,branch_type))
