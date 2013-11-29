from xml.dom.minidom import parseString
from common.functions import event_function, result_function
import ROOT
import os

#Good run list parser
def parse_grl_xml(grl_xml):
	if not os.path.exists(grl_xml):
		raise OSError('Good run list {0} not found'.format(grl_xml))

	with open(grl_xml) as f: dom = parseString(f.read())

	grl = {}
	try:
		for lumiblock_collection in dom.getElementsByTagName('LumiBlockCollection'):
			run = int(lumiblock_collection.getElementsByTagName('Run')[0].firstChild.data)
			for lumiblock_range in lumiblock_collection.getElementsByTagName('LBRange'):
				start = lumiblock_range.getAttribute('Start')
				end = lumiblock_range.getAttribute('End')
				if run not in grl: grl[run] = []
				grl[run]+= range(int(start),int(end)+1)
	except Exception as error:
		print 'Problem loading good run list {0}'.format(grl_xml)
		raise error

	return grl

#Sets MC event weight (1 if data) and instantiates __weight__
class compute_mc_weight(event_function):

	def __init__(self):
		event_function.__init__(self)
		self.create_branches['mc_event_weight'] = 'float'
		self.create_branches['is_mc'] = 'bool'
		
	def __call__(self,event):
		#if this function has been applied to dataset already then don't change
		if 'is_mc' in event: is_mc = event.is_mc
		#elif this function has not been applied then we look for mc_ related variable
		elif 'mc_event_weight' in event: is_mc = True
		#else this is data that has not seen this function
		else: is_mc = False

		#if this is mc or data that has seen this function then don't change
		if 'mc_event_weight' in event: weight = event.mc_event_weight
		#this is data so should have "base" weight of 1.
		else: weight = 1.

		event.mc_event_weight = weight
		event.is_mc = is_mc
		event.__weight__ = weight

#apply grl
class in_grl(event_function):

	def __init__(self,grl_xml):
		event_function.__init__(self)

		self.grl = parse_grl_xml(grl_xml)
		self.required_branches += [
			'RunNumber',
			'lbn',
			]

	def __call__(self,event):
		if event.RunNumber in self.grl:
			if event.lbn in self.grl.get(event.RunNumber): return
		event.__break__ = True

class cutflow(result_function):

	def __init__(self,event_functions):
		result_function.__init__(self)

		self.results['cutflow'] = ROOT.TH1F(
			'cutflow',
			'cutflow',
			len(event_functions)+1,
			0,
			len(event_functions)+1,
			)
		self.results['cutflow_weighted'] = ROOT.TH1F(
			'cutflow_weighted',
			'cutflow_weighted',
			len(event_functions)+1,
			0,
			len(event_functions)+1,
			)

		for i,event_function_name in enumerate(['input']+[event_function.__class__.__name__ for event_function in event_functions]):
			self.results['cutflow'].GetXaxis().SetBinLabel(i+1,event_function_name)
			self.results['cutflow_weighted'].GetXaxis().SetBinLabel(i+1,event_function_name)
		
	def __call__(self,event):
		for i in range(event.__stop__):
			self.results['cutflow'].Fill(i)
			self.results['cutflow_weighted'].Fill(i)

lookup_description = {
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

lookup_created = {
	'float':'Float_t',
	'int':'Int_t',
	'bool':'Bool_t',
	'string':'Char_t',
	}

class skim(result_function):

	def __init__(self,analysis):
		result_function.__init__(self)
		
		self.analysis = analysis
		self.pchain = self.analysis.pchain

		#Load struct for branches with types in [int,bool,float]
		ROOT.gROOT.ProcessLine("struct Variable{Int_t variable_int; Bool_t variable_bool; Float_t variable_float;};")

		self.tree = ROOT.TTree(self.pchain().GetName(),self.pchain().GetTitle())
		self.results['skim'] = self.tree

		for branch_name in sorted(self.analysis.keep_branches):
			if branch_name in self.analysis.create_branches: continue
			if branch_name not in self.pchain.get_available_branch_names(required=True):
				raise ValueError('No matches for required branch {0}'.format(branch_name))
			branch_type = self.pchain.branch_types[branch_name]
			if branch_type in lookup_description:
				self.tree.Branch(branch_name,self.pchain.branch_values[branch_name],branch_name+'/'+lookup_description[branch_type])
			elif branch_type == 'string':
				self.tree.Branch(branch_name,branch_type,ROOT.AddressOf(self.pchain.branch_values[branch_name]))
			elif branch_type.startswith('vector'):
				self.tree.Branch(branch_name,branch_type,ROOT.AddressOf(self.pchain.branch_values[branch_name]))
			else: raise ValueError('Branch {0} could not be configured, type "{1}" not supported'.format(branch_name,branch_type))
			
		self.created_branches = {}

		for branch_name,branch_type in sorted(self.analysis.create_branches.items()):
			if branch_type is None: continue
			if branch_type.startswith('std.vector.'):
				vector_type = branch_type.replace('std.vector.','',1)
				if vector_type not in [
					'float',
					'int',
					'bool',
					]: raise TypeError('Unsupported vector type "{0}" for branch {1}'.format(vector_type,branch_name))
				self.created_branches[branch_name] = ROOT.std.vector(vector_type)()
				#overwrite existing branch
				if self.tree.GetBranch(branch_name):
					self.tree.SetBranchAddress(branch_name,self.created_branches[branch_name])
				#create new branch
				else: self.tree.Branch(branch_name,self.created_branches[branch_name])
			elif branch_type in [
				'float',
				'int',
				'bool',
				]:
				self.created_branches[branch_name] = ROOT.Variable()
				#overwrite existing branch
				if self.tree.GetBranch(branch_name):
					self.tree.SetBranchAddress(branch_name,ROOT.AddressOf(self.created_branches[name],'variable_{0}'.format(branch_type)))
				#create new branch
				else: self.tree.Branch(branch_name,ROOT.AddressOf(self.created_branches[branch_name],'variable_{0}'.format(branch_type)),branch_name+'/'+lookup_description[lookup_created[branch_type]])
			else: raise ValueError('Branch {0} could not be configured, type "{1}" not supported'.format(branch_name,branch_type))

	def __call__(self,event):
		if event.__break__: return

		self.pchain().GetEntry(event.__entry__)
		for branch_name,branch_value in self.created_branches.items():
			branch_type = self.analysis.create_branches[branch_name]
			if branch_type.startswith('std.vector'):
				branch_value.clear()
				for value in getattr(event,branch_name): branch_value.push_back(value)
			elif branch_type == 'int': branch_value.variable_int = getattr(event,branch_name)
			elif branch_type == 'bool': branch_value.variable_bool = getattr(event,branch_name)
			elif branch_type == 'float': branch_value.variable_float = getattr(event,branch_name)
			
		self.tree.Fill()
