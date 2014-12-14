from xml.dom.minidom import parseString
from common.functions import event_function, result_function, output_base
from common.branches import branch
from common.commandline import commandline, arg
from helper import root_quiet
import ROOT,os

class root_output(output_base):
    def __init__(self,directory,name):
        super(root_output,self).__init__(directory,name)
        #self.name = '/'.join(directo
        self.results = []

    def setup(self):
        self.TFile = ROOT.TFile(self.path,'RECREATE')
    
    def add_result(self,result):
        self.cd()
        try: result.SetDirectory(self.TFile)
        except AttributeError: pass
        self.results.append(result)
        
    def cd(self):
        self.TFile.cd()
        
    def close(self):
        self.cd()
        for result in self.results: result.Write()
        self.TFile.Close()

    def merge(self,directories):
        with root_quiet(filters=["\[TFile::Cp\]"]):
            merger = ROOT.TFileMerger()
            if os.path.exists(self.path): os.remove(self.path)
            merger.OutputFile(self.path)
            for directory in directories: merger.AddFile(directory+'/'+self.name)
            merger.Merge()
        print '{0} created'.format(self.path)

class root_result(result_function):

    def setup(self):
        #super(root_result,self).setup()
        self.root_output = root_output(self.analysis.dir,self.analysis.stream+'.root')
        self.root_output.setup()
        self.outputs.append(self.root_output)
        

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
        super(compute_mc_weight,self).__init__()
        self.branches += [
            branch('mc_channel_number','ru'),
            branch('mc_event_weight','ru'),
            branch('is_mc','ru'),
            ]
        
    def __call__(self,event):
        super(compute_mc_weight,self).__call__(event)
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
        if event.is_mc: event.mc_channel_number = event.mc_channel_number
        else: event.mc_channel_number = 0

        event.__weight__ = weight

#apply grl
class in_grl(event_function):

    def __init__(self,grl_xmls):
        event_function.__init__(self)

        self.grl = {}

        analysis_home = os.getenv('ANALYSISHOME')

        for grl_xml in grl_xmls:
            self.grl.update(parse_grl_xml('{0}/data/{1}'.format(analysis_home,grl_xml)))

        self.required_branches += [
            'RunNumber',
            'lbn',
            ]

    def __call__(self,event):
        super(in_grl,self).__call__(event)
        if event.is_mc: return
        if event.RunNumber in self.grl:
            if event.lbn in self.grl.get(event.RunNumber): return
        event.__break__ = True

class cutflow(root_result):

    #def setup_output(self):
    #   self.output = root_output(self.analysis.output_dir,self.analysis.stream_name+'.root')

    def __init__(self):
        super(cutflow,self).__init__()

    def setup(self):
        super(cutflow,self).setup()     
        #result_function.__init__(self)
        #analysis = self.get_analysis()
        
        break_exceptions = self.analysis.break_exceptions
        #for event_function in self.analysis.event_functions:
        #   break_exceptions += event_function.break_exceptions
            
        self.cutflow = ROOT.TH1F(
            'cutflow',
            'cutflow',
            len(break_exceptions)+1,
            0,
            len(break_exceptions)+1,
            )
            
        self.cutflow_weighted = ROOT.TH1F(
            'cutflow_weighted',
            'cutflow_weighted',
            len(break_exceptions)+1,
            0,
            len(break_exceptions)+1,
            )

        self.root_output.add_result(self.cutflow)
        self.root_output.add_result(self.cutflow_weighted)      
        
        self.break_exceptions = dict((break_exception,i+1) for i,break_exception in enumerate(break_exceptions))
        self.max = len(break_exceptions)+2

        self.cutflow.GetXaxis().SetBinLabel(1,'input')
        self.cutflow_weighted.GetXaxis().SetBinLabel(1,'input')

        names = {}
        for break_exception,i in sorted(self.break_exceptions.items(),key= lambda tup: tup[1]):
            name = break_exception.__name__
            if name not in names: names[name] = []
            names[name].append(break_exception)
            
        for break_exception,i in self.break_exceptions.items():
            name = break_exception.__name__
            if len(names[name])>1:
                name+='_{0}'.format(names[name].index(break_exception))
            self.cutflow.GetXaxis().SetBinLabel(i+1,name)
            self.cutflow_weighted.GetXaxis().SetBinLabel(i+1,name)

    def __call__(self,event):
        super(self.__class__,self).__call__(event)
        if event.__break__.__class__ in self.break_exceptions: stop = self.break_exceptions[event.__break__.__class__]
        elif event.__break__ is False: stop = self.max
        else: raise RuntimeError('Invalid break exception: {0}'.format(event.__break__))
        for i in range(1,stop):
            self.cutflow.Fill(i-0.5)
            self.cutflow_weighted.Fill(i-0.5,event.__weight__)

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




class skim(root_result):

    #def setup_output(self):
    #   self.output = root_output(self.analysis.output_dir,self.analysis.stream_name+'.root')
    @commandline(
        "skim",
        keep = arg('-k',action='store_true',help='Keep all branches from original tree'),
        )
    def __init__(
        self,
        keep = False,
        ):
        self.keep = keep
        super(skim,self).__init__()

    #def __init__(self):
    def setup(self):
        super(skim,self).setup()

        self.pchain = self.analysis.pchain

        self.tree = ROOT.TTree(self.pchain().GetName(),self.pchain().GetTitle())
        self.root_output.add_result(self.tree)

        self.created_branches = {}
        
        for event_function in self.analysis.event_functions:
            for branch in [branch_ for branch_ in event_function.branches if 'w' in branch_.mode]:
                self.created_branches[branch.name] = branch

        for branch in [branch for branch in self.pchain.branches.values() if ('k' in branch.mode or self.keep) and branch.name not in self.created_branches]:
            branch.read(self.pchain.chain)
            #print 'keeping branch {0}'.format(branch.name)
            print branch.name,type(branch)
            branch.write(self.tree)
            
        for branch in self.created_branches:
            branch.write(self.tree)
    """
        for branch_name in sorted(self.analysis.keep_branches):
            if self.analysis.create_branches.get(branch_name): continue
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
    """
    def __call__(self,event):
        if event.__break__: return
        #return

        self.pchain.get_entry(event.__entry__)
        for branch in self.created_branches.values():
            branch.payload = getattr(event,branch.name)
            
        self.tree.Fill()

