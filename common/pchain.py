import ROOT
ROOT.gROOT.ProcessLine("gErrorIgnoreLevel = 2001;")
from common.branches import vector_branch, std_branch

class pchain():

    def __init__(self,tree):
    
        self.tree = tree
        self.chain = ROOT.TChain(tree)
        self.chain.SetCacheSize(10000000)
        self.chain.SetCacheLearnEntries(10)

        #self.branch_names = []
        #self.branch_types = {}
        #self.branch_values = {}
        self.branches = {}
        #self.created_branches = {}

        #self.branches_union = None
        #self.branches_intersection = None

        self.files = []
        #self.files_branches = {}

        self.current_file_number = -1
        self.first_entry_files = []

    def __call__(self):
        return self.chain

    def get_entries(self):
        return self.chain.GetEntries()

    def add_files(self,files):
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
            branch_types = {}
            for leaf in tree.GetListOfLeaves():
                type_ = leaf.GetTypeName()
                name = leaf.GetName()
                d[name] = type_
            for branch_name,branch in self.branches.items():
                if any([
                    branch_name not in branch_types,
                    branch_types[branch_name] != branch.type,
                    ]): raise RuntimeError('Mis-match data between original file {0} and appended file {1} in branch {2}'.format(self.files[0],f,branch_name))      
        else:
            for leaf in tree.GetListOfLeaves():
                type_ = leaf.GetTypeName()
                name = leaf.GetName()
                if type_.startswith('vector'): self.branches[name] = vector_branch(name,type_)
                else: self.branches[name] = std_branch(name,type_)
        tfile.Close()

    def request_branch(self,branch_name):
        self.branches[branch_name].read(self)

    def request_branches(self,branch_names):
        for branch_name in branch_names: self.request_branch(branch_name)

    def set_entry(self,entry):
        self.current_entry = self().LoadTree(entry)
        file_number = self().GetTreeNumber()

        if self.current_file_number != file_number:
            current_file = self.files[self.current_file_number]
            self.first_entry_files.append(current_file)
            self.current_file_number = file_number
            for branch_name,branch in self.branches.items():
                branch.tbranch = self().GetBranch(branch_name)
        
    def get_branches(self,event,branch_names):
        for branch_name in branch_names:
            if branch_name not in self.branches: raise KeyError('Unknown branch {0}'.format(branch_name))
            branch = self.branches[branch_name]
            branch.get_entry(self.current_entry)
            event.__dict__[branch_name] = branch.get_value()
            value = self.branch_values[branch_name]
