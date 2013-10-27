import ROOT

from common.EventFunction import ResultFunction

class lumi(ResultFunction):
	def __init__(self,*args,**kwargs):
		ResultFunction.__init__(self,*args,**kwargs)
		
		self.addItem(
		'lumi',
		ROOT.TTree('lumi','lumi')
		)
		self.string = ROOT.TString()
		self.items.get('lumi').Branch('grl','TString',ROOT.AddressOf(self.string),1600,0)
		
	def __call__(self,files):

		for file_ in files:
			f = ROOT.TFile(file_)
			#Look for Lumi TDirectory(ies)
			LumiDirectories = [f.Get(key.GetName()+';'+str(key.GetCycle())) for key in f.GetListOfKeys() if isinstance(f.Get(key.GetName()+';'+str(key.GetCycle())),ROOT.TDirectory)]
			for LumiDirectory in LumiDirectories:
				for key in LumiDirectory.GetListOfKeys():
					self.string.Resize(0)
					self.string.Append(LumiDirectory.Get(key.GetName()+';'+str(key.GetCycle())).String())
					self.items['lumi'].Fill()
			#Look for lumi trees
			LumiTrees = [f.Get(key.GetName()+';'+str(key.GetCycle())) for key in f.GetListOfKeys() if isinstance(f.Get(key.GetName()+';'+str(key.GetCycle())),ROOT.TTree)]
			for LumiTree in LumiTrees:
				if 'grl' not in [b.GetName() for b in LumiTree.GetListOfBranches()]: continue
				for entry in range(LumiTree.GetEntries()):
					LumiTree.GetEntry(entry)
					self.string.Resize(0)
					self.string.Append(LumiTree.grl)
					self.items['lumi'].Fill()
