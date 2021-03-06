import ROOT
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
from common.branches import auto_branch

class pchain():

    def __init__(self,tree):
    
        self.tree = tree
        self.chain = ROOT.TChain(tree)
        self.chain.SetCacheSize(10000000)
        self.chain.SetCacheLearnEntries(10)

        self.branches = {}
        self.branch_types = {}
        self.files = []
        self.current_file_number = -1
        self.first_entry_files = []

    def __call__(self):
        return self.chain

    def get_entries(self):
        return self.chain.GetEntries()

    def get_entry(self,entry):
        self.chain.GetEntry(entry)
		
    def add_file(self,*files):
        for f in files:
            self.validate_file(f)
            self.files.append(f)
            self().Add(f)
            
    def validate_file(self,f):
        tfile = ROOT.TFile.Open(f)
        try: tree = getattr(tfile,self.tree,None)
        except ReferenceError: raise OSError,'File {0} does not exist or could not be opened'.format(f)
        if any([
            not tree,
            not isinstance(tree,ROOT.TTree),
            ]): raise ValueError,'No matches for TTree "{0}" in file {1}.'.format(self.tree,f)
        if self.files:
            branch_types = dict((leaf.GetName(),leaf.GetTypeName()) for leaf in tree.GetListOfLeaves())
            if branch_types != self.branch_types:
                raise RuntimeError('Mis-match data between original file {0} and appended file {1} in branch {2}'.format(self.files[0],f,branch_name))
            #for leaf in tree.GetListOfLeaves():
            #    type_ = leaf.GetTypeName()
            #    name = leaf.GetName()
            #    branch_types[name] = type_
            #for branch_name,branch in self.branches.items():
            #    if any([
            #        branch_name not in branch_types,
            #        branch_types[branch_name] != branch.type,
            #        ]): raise RuntimeError('Mis-match data between original file {0} and appended file {1} in branch {2}'.format(self.files[0],f,branch_name))      
        else:
            self.branch_types = dict((leaf.GetName(),leaf.GetTypeName()) for leaf in tree.GetListOfLeaves())
            #for leaf in tree.GetListOfLeaves():
            #    type_ = leaf.GetTypeName()
            #    name = leaf.GetName()
            #    self.branches[name] = auto_branch(name,'r',type_)
            #    self.branches[name].chain = self.chain
        tfile.Close()
        self.chain.SetBranchStatus('*',0)

    def request_branch(self,branch):
        #self.branches[branch_name].read()
        try: return self.branches[branch.name]
        except KeyError: pass
        try: branch_type = self.branch_types[branch.name]
        except KeyError: raise AttributeError('No branch named {0} found'.format(branch.name))
        branch = auto_branch(branch.name,branch.mode,branch_type)
        #try: branch = self.branches[branch_name]
        #except KeyError: raise AttributeError('No branch named {0} found')
        branch.read(self.chain)
        self.chain.SetBranchStatus(branch.name,1)
        self.branches[branch.name] = branch
        return branch

    #def request_branches(self,branch_names):
    #    for branch_name in branch_names: self.request_branch(branch_name)

    def set_entry(self,entry):
        self.current_entry = self().LoadTree(entry)
        file_number = self().GetTreeNumber()

        if self.current_file_number != file_number:
            current_file = self.files[self.current_file_number]
            self.first_entry_files.append(current_file)
            self.current_file_number = file_number
            #for branch_name,branch in self.branches.items():
            #    branch.tbranch = self().GetBranch(branch_name)
        
    #def get_branches(self,event,branch_names):
    #    for branch_name in branch_names:
    #        if branch_name not in self.branches: raise KeyError('Unknown branch {0}'.format(branch_name))
    #        branch = self.branches[branch_name]
    #        branch.update(self.current_entry)
    #        event.__dict__[branch_name] = branch.value
            
