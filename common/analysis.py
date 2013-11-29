from pchain import pchain
from standard import in_grl, skim, cutflow, compute_mc_weight

class analysis():

	def __init__(self):
		self.event_functions = []
		self.result_functions = []
		self.meta_result_functions = []

		self.required_branches = []
		self.create_branches = {}
		self.keep_branches = []

		self.files = []
		self.tree = 'physics'
		self.grl = None

		self.keep_all = False
	
	def add_event_function(self,*event_functions):
		self.event_functions += event_functions

	def add_result_function(self,*result_functions):
		self.result_functions += result_functions

	def add_meta_result_function(self,*result_functions):
		self.meta_result_functions += result_functions
	
	def add_file(self,*files):
		for file_ in files:
			if file_ in self.files: continue
			self.files.append(file_)
		
	def add_standard_functions(self):
		if self.grl: self.event_functions = [in_grl(self.grl)]+self.event_functions
		self.event_functions = [compute_mc_weight()]+self.event_functions
		self.add_result_function(cutflow(self.event_functions))
		
	def setup_chain(self):
		self.pchain = pchain(self.tree)
		self.pchain.add_files(self.files)
		for event_function in self.event_functions:
			self.required_branches += event_function.required_branches
			self.create_branches.update(event_function.create_branches)
			self.keep_branches += event_function.keep_branches
			self.pchain.create_branches(event_function.create_branches.keys(),event_function.__class__.__name__)
		if self.keep_all:
			for branch_name in self.pchain.get_available_branch_names():
				if branch_name in self.keep_branches: continue
				self.keep_branches.append(branch_name)
		self.pchain.request_branches(self.required_branches)
		self.pchain.request_branches(self.keep_branches)

