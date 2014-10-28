from pchain import pchain
from common.standard import in_grl, skim, cutflow, compute_mc_weight
from common.functions import EventBreak, output_base

class analysis():

    def __init__(self):
        self.event_functions = []
        self.result_functions = []
        self.meta_result_functions = []

        self.required_branches = []
        self.create_branches = {}
        self.keep_branches = []
        self.break_exceptions = []

        self.files = []
        self.tree = 'physics'
        self.grl = []

        self.keep_all = False
    
        self.outputs = []
    
    def add_event_function(self,*event_functions):
        self.event_functions += event_functions
        for event_function in event_functions:
            self.break_exceptions += event_function.break_exceptions
	
    def add_result_function(self,*result_functions):
        self.result_functions += result_functions
        for result_function in self.result_functions:
            self.outputs += result_function.outputs

    def add_meta_result_function(self,*result_functions):
        self.meta_result_functions += result_functions
        for meta_result_function in self.meta_result_functions:
            self.outputs += meta_result_function.outputs
                
    def add_file(self,*files):
        for file_ in files:
            if file_ in self.files: continue
            self.files.append(file_)
        
    def add_standard_functions(self):
        if self.grl: self.event_functions = [in_grl(self.grl)]+self.event_functions
        self.event_functions = [compute_mc_weight()]+self.event_functions
        self.add_result_function(cutflow(self.break_exceptions))
        
    def setup_chain(self):
        self.pchain = pchain(self.tree)
        self.pchain.add_files(self.files)
        for event_function in self.event_functions:
            self.required_branches += event_function.required_branches
            for branch_name,branch_type in event_function.create_branches.items():
                if branch_type is not None and branch_name not in self.create_branches: self.create_branches[branch_name]=branch_type
            self.keep_branches += event_function.keep_branches
            self.pchain.create_branches(event_function.create_branches.keys(),event_function.__class__.__name__)
        if self.keep_all:
            for branch_name in self.pchain.get_available_branch_names():
                if branch_name in self.keep_branches: continue
                self.keep_branches.append(branch_name)
        self.pchain.request_branches(self.required_branches)
        self.pchain.request_branches(self.keep_branches)

import os
import sys
import ROOT
from common.pchain import generate_dictionaries
from time import time
from common.event import event_object
from common.standard import skim
import code
from helper import root_quiet

class ROOT_output(output_base):
    def __init__(self,name):
        super(ROOT_output,self).__init__(name)
        self.TFile = ROOT.TFile(name,'RECREATE')
    
    def cd(self):
        self.TFile.cd()
        
    def close(self):
        self.TFile.Close()

    def merge(self,directories):
        with root_quiet(filters=["\[TFile::Cp\]"]):
            merger = ROOT.TFileMerger()
            if os.path.exists(self.name): os.remove(self.name)
            merger.OutputFile(self.name)
            for directory in directories: merger.AddFile(directory+'/'+self.name)
            merger.Merge()
        print '{0} created'.format(self.name)
        
class analyze_slice():

    def __init__(
        self,
        module_name,
        analysis_name,
        tree,
        grl,
        files,
        start,
        end,
        output_name,
        error_file_name,
        logger_file_name,
        keep,
        ):

        self.module_name = module_name
        self.analysis_name = analysis_name
        self.tree = tree
        self.grl = grl
        self.files = files
        self.start = start
        self.end = end
        self.output_name = output_name
        self.keep = keep

        self.output = None
        self.exitcode = 1

        if error_file_name: self.error_file = open(error_file_name,'w+',0)
        else: self.error_file = None
        if logger_file_name: self.logger_file = open(logger_file_name,'w+',0)
        else: self.logger_file = None
		
    def initialize(self):

        if self.logger_file:
            sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
            os.dup2(self.logger_file.fileno(),sys.stdout.fileno())
        if self.error_file:
            sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)
            os.dup2(self.error_file.fileno(),sys.stderr.fileno())

        analysis_constructor = __import__(self.module_name,globals(),locals(),[self.analysis_name]).__dict__[self.analysis_name]

        generate_dictionaries()

        #Create output
        self.output = ROOT_output(self.output_name)
        #self.output = ROOT.TFile(self.output_name,'RECREATE')

        #Create local copy of analysis
        self.analysis_instance = analysis_constructor()
        self.analysis_instance.tree = self.tree
        self.analysis_instance.grl = self.grl
        self.analysis_instance.keep_all = self.keep

        self.analysis_instance.outputs.append(self.output)

        with open(self.files) as f: files = [line.strip() for line in f.readlines() if line.strip()]
        self.analysis_instance.add_file(*files)
        self.analysis_instance.add_standard_functions()
        self.analysis_instance.setup_chain()
        self.analysis_instance.add_result_function(skim(self.analysis_instance))
        
        for result_function in self.analysis_instance.result_functions:
            for result in result_function.results.values():
                try: result.SetDirectory(self.output.TFile)
                except AttributeError: pass

        for meta_result_function in self.analysis_instance.meta_result_functions:
            for result in meta_result_function.results.values():
                try: result.SetDirectory(self.output.TFile)
                except AttributeError: pass

        if self.error_file: self.error_file.flush()
        if self.logger_file: self.logger_file.flush()

    def run(self,interactive=False):

        milestone = 0.
        time_start = time()
        entry=0
        done = 0.
        rate = 0.

        #get these outside the loop so there is no complication inside the loop
        event_function_info = [(event_function_.__call__,event_function_.__class__.__name__,event_function_.required_branches+event_function_.create_branches.keys()) for event_function_ in self.analysis_instance.event_functions]
        result_function_calls = [result_function_.__call__ for result_function_ in self.analysis_instance.result_functions]
        get_branches = self.analysis_instance.pchain.get_branches
        
        break_exceptions = tuple(self.analysis_instance.break_exceptions)

        for entry in xrange(self.start,self.end):
            #Create new event object (basically just a namespace)
            event = event_object()
            event.__stop__ = 1
            event.__entry__ = entry
            self.analysis_instance.pchain.set_entry(entry)
            for event_function_call,event_function_name,event_function_branches in event_function_info:
                #Get registered branches from chain
                get_branches(
                    event,
                    event_function_branches,
                    event_function_name
                    )
                #Call event function
                try: event_function_call(event)
                except break_exceptions as e:
			event.__break__ = e
			break
                #Increment stop count (used in cutflow)
                #event.__stop__+= 1
            for result_function_call in result_function_calls:
                #Call result function (does not necessarily respect event.__break__, must be implemented on case by case basis in __call__ of result function)
                result_function_call(event)
            if interactive: code.interact(local={'event':event})
            rate = (entry-self.start)/(time()-time_start)
            done = float(entry-self.start+1)/(self.end-self.start)*100.
    
            if done>milestone:
                milestone+=10.
                print '{0}% complete, {1} Hz'.format(round(done,2),round(rate,2))
                if self.error_file: self.error_file.flush()
                if self.logger_file: self.logger_file.flush()

        #Handle ROOT results
        for result_function in self.analysis_instance.result_functions:
            for result in result_function.results.values():
                self.output.cd()
                #Write result function items to output
                result.Write()

        for meta_result_function in self.analysis_instance.meta_result_functions:
            #Call meta-result function if we touched first entry of that file
            meta_result_function(self.analysis_instance.pchain.first_entry_files)
            for result in meta_result_function.results.values():
                self.output.cd()
                #Write meta-result function items to output
                result.Write()

        print '{0}% complete, {1} Hz'.format(round(done,2), round(rate,2))

        if self.error_file: self.error_file.flush()
        if self.logger_file: self.logger_file.flush()
        self.exitcode = 0

    def cleanup(self):
        #if self.output: self.output.Close()
        analysis_instance = getattr(self,'analysis_instance',None)
        if analysis_instance is not None:
            for output in self.analysis_instance.outputs: output.close()

        if self.logger_file:
            self.logger_file.flush()
            self.logger_file.close()
        if self.error_file:
            self.error_file.flush()
            self.error_file.close()
        sys.exit(self.exitcode)
