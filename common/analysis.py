import ROOT
from pchain import pchain, generate_dictionaries
import sys
import os
import shutil
import traceback
from copy import copy
from time import sleep, time
from distutils.dir_util import mkpath
from multiprocessing import Process, Queue
from common.event import event_object
import string
import random
from standard import in_grl, skim, cutflow, compute_mc_weight
from math import log
from common.external import call
import stat   

#Patches stdout to pipe to a Queue which will handle logging
class logpatch():
	def __init__(self,queue,prefix,suffix):
		self.queue=queue
		self.prefix = prefix
		self.suffix = suffix
		self.buffer = ''

	def flush(self):
		pass

	def write(self,log):
		if log != '\n':
			self.buffer += log
		else:
			self.queue.put(self.prefix+self.buffer.replace('\n','\n'+' '*len(self.prefix)))
			self.buffer = ''

#Patches stdout to a file
class logpatch_file():
	def __init__(self,output,prefix='',suffix=''):
		self.output = output
		self.prefix = prefix
		self.suffix = suffix
		self.buffer = ''

	def flush(self):
		self.output.flush()

	def close(self):
		self.output.flush()
		self.output.close()

	def write(self,log):
		if log != '\n':
			self.buffer += log
		else:
			self.output.write(self.prefix+self.buffer.replace('\n','\n'+' '*len(self.prefix))+'\n')
			self.flush()
			self.buffer = ''

def analyze(
	analysis_constructor,
	tree,
	grl,
	files,
	output,
	entries=None,
	num_processes=2
	):

	#generate default dictionaries needed by ROOT
	generate_dictionaries()

	print 'Setting up analysis'
	print '-'*50+'\n'

	#create initial analysis object to test for obvious problems
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	analysis_instance.add_file(*files)
	analysis_instance.setup_chain()

	print '\n'+'-'*50

	if entries is not None:
		entries = min([int(entries),analysis_instance.pchain.get_entries()])
	else: entries = analysis_instance.pchain.get_entries()

	if not entries: return 1

	print 'Processing {0} entries with {1} processes'.format(entries,num_processes)
	
	#Result, error and log queue
	result_queue = Queue()
	error_queue = Queue()
	logger_queue = Queue()

	#Create temp directory
	while True:
		directory = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
		if directory not in os.listdir('.'):
			os.mkdir(directory)
			break
	print 'Created temporary directory {0}'.format(directory)

	#Instantiate and start processes
	ranges = [[i*(entries/num_processes),(i+1)*(entries/num_processes)] for i in range(num_processes)]; ranges[-1][-1]+=entries%(num_processes)

	processes = [Process(target = analyze_slice, args = (
			analysis_constructor,
			tree,
			grl,
			files,
			ranges,
			process_number,
			directory,
			result_queue,
			error_queue,
			logger_queue,
			)) for process_number in range(num_processes)]

	time_start = time()
	for process in processes: process.start()
	print '{0} processes started'.format(num_processes)

	def cleanup():
		for process in processes: 
			process.terminate()
			process.join()
		if os.path.exists(directory): shutil.rmtree(directory)		
		sys.exit()

	#Wait for processes to complete or kill them if ctrl-c
	finished = 0
	results = []
	while 1:
		try: sleep(0.1)
		except KeyboardInterrupt: cleanup()

		#flush logger queue
		while not logger_queue.empty():
			print logger_queue.get()

		#flush error queue
		while not error_queue.empty():
			print error_queue.get()
			cleanup()

		#flush result queue
		while not result_queue.empty():
			finished+=1
			#flush logger queue
			while not logger_queue.empty():
				print logger_queue.get()
			#flush error queue
			while not error_queue.empty():
				print error_queue.get()
				cleanup()
			results.append(result_queue.get())

		if finished==num_processes: break		

	print 'Overall rate: {0} Hz'.format(round(entries/(time()-time_start),2))

	#Create path to output and output ROOT file, merge results
	mkpath(os.path.dirname(output))
	os.close(sys.stdout.fileno())
	if num_processes>1:
		merger = ROOT.TFileMerger()
		if os.path.exists(output): os.remove(output)
		merger.OutputFile(output)
		for result in results:
			merger.AddFile(result)
		merger.Merge()
	else:
		shutil.move(results[0], output)

	cleanup()

def analyze_slice(
	analysis_constructor,
	tree,
	grl,
	files,
	ranges,
	process_number,
	directory,
	result_queue,
	error_queue,
	logger_queue,
	):

	generate_dictionaries()

	error = None
	#cleanup function always called no matter form of exit
	def cleanup():
		output.Close()
		#output name will be None if there is some problem
		result_queue.put(output_name)
		#error will be None if there is NO problem
		if error: error_queue.put(error)
		sys.exit()

	#print statements executed in here and in Event/Result functions are redirected to the main logger
	os.close(sys.stdout.fileno())
	sys.stdout = logpatch(logger_queue,'Process number {0}: '.format(process_number),'')

	#Create output
	num_processes = len(ranges)
	if num_processes>1: output_name = '{directory}/temp_{0:0>{1}}.root'.format(process_number,int(log(num_processes-1,10))+1,directory=directory)
	else: output_name = '{directory}/temp.root'.format(directory=directory)
	output = ROOT.TFile(output_name,'RECREATE')
	
	#Create local copy of analysis
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	analysis_instance.add_file(*files)

	try:
		analysis_instance.add_standard_functions()
		analysis_instance.setup_chain()
		analysis_instance.add_result_function(skim(analysis_instance))

	except Exception:
		error = 'Error occured in initialization\n'+traceback.format_exc()
		output_name = None
		cleanup()

	#tie results to output file
	for result_function in analysis_instance.result_functions:
		for result in result_function.results.values():
			result.SetDirectory(output)

	for meta_result_function in analysis_instance.meta_result_functions:
		for result in meta_result_function.results.values():
			result.SetDirectory(output)

	milestone = 0.

	start,end = ranges[process_number]
	time_start = time()

	entry=0
	try:
		for entry in xrange(start,end):
			#Create new event object (basically just a namespace)
			event = event_object()
			event.__stop__ = 1
			event.__entry__ = entry
			analysis_instance.pchain.set_entry(entry)
			for event_function in analysis_instance.event_functions:
				#Get registered branches from chain
				analysis_instance.pchain.get_branches(event,event_function.required_branches+event_function.create_branches.keys(),event_function.__class__.__name__)
				#Call event function
				event_function(event)
				if event.__break__: break
				#Increment stop count (used in cutflow)
				event.__stop__+= 1
			for result_function in analysis_instance.result_functions:
				#Call result function (does not necessarily respect event.__break__, must be implemented on case by case basis in __call__ of result function)
				result_function(event)

			rate = (entry-start)/(time()-time_start)
			done = float(entry-start+1)/(end-start)*100.
		
			if done>milestone:
				milestone+=10.
				print '{0}% complete, {1} Hz'.format(round(done,2),round(rate,2))

	except Exception:
		error = 'Exception caught in entry {0}\n'.format(entry)+traceback.format_exc()
		output_name = None
		cleanup()

	#Handle results
	try:
		for result_function in analysis_instance.result_functions:
			for result in result_function.results.values():
				output.cd()
				#Write result function items to output
				result.Write()

		#Only process meta-results for first process
		if not process_number:
			for meta_result_function in analysis_instance.meta_result_functions:
				#Call meta-result function
				meta_result_function(analysis_instance.files)
				for result in meta_result_function.results.values():
					output.cd()
					#Write meta-result function items to output
					result.Write()

	except Exception:
		error = 'Exception caught while handling results\n'+traceback.format_exc()
		output_name = None
		cleanup()

	print '{0}% complete, {1} Hz'.format(round(done,2), round(rate,2))	
	print 'Sending output {0}'.format(output_name)

	cleanup()

def call_analyze_slice_condor(
	module_name,
	analysis_name,
	tree,
	grl,
	files,
	ranges,
	process_number,
	directory,
	result_queue,
	error_queue,
	logger_queue,
	):

	analysis_framework = os.getenv('ANALYSISFRAMEWORK')
	analysis_home = os.getenv('ANALYSISHOME')

	sys.stdout = logpatch(logger_queue,'Process number {0}: '.format(process_number),'')

	num_processes = len(ranges)
	if num_processes>1: condor_dir = '{directory}/condor_{0:0>{1}}'.format(process_number,int(log(num_processes-1,10))+1,directory=directory)
	else: condor_dir = '{directory}/condor'.format(directory=directory)

	os.mkdir(condor_dir)
	os.chdir(condor_dir)

	output_name = os.path.abspath('output.root')

	#cleanup function always called no matter form of exit
	def cleanup(logger_text,error_text,output_name):
		#output name will be None if there is some problem
		if logger_text: print logger_text
		if error_text:
			output_name = None
			error=error_text
		else: error=None
		result_queue.put(output_name)
		#error will be None if there is NO problem
		if error: error_queue.put(error)
		sys.exit()

	files_text = 'files.text'
	with open(files_text,'w') as f:
		for file_ in files: f.write(file_+'\n')
		
	result_file_name = 'result.out'
	error_file_name = 'error.out'
	logger_file_name = 'logger.out'

	#setup condor files
	with open('{0}/condor/default_condor.submit'.format(analysis_framework)) as f:
		with open('condor.submit','w') as f_out:
			f_out.write(f.read())

	with open('{0}/condor/default_condor_executable.sh'.format(analysis_framework)) as f:
		with open('condor_executable.sh','w') as f_out:
			f_out.write(f.read().format(
				analysis_framework = analysis_framework,
				analysis_home = analysis_home,
				module_name = module_name,
				analysis_name = analysis_name,
				tree = tree,
				grl = '\'{0}\''.format(grl) if grl is not None else None,
				files = files_text,
				start = ranges[process_number][0],
				end = ranges[process_number][1],
				output_name = output_name,
				process_number = process_number,
				error_file_name = error_file_name,
				logger_file_name = logger_file_name,	
				))
	
	#submit condor job
	try:
		print call('condor_submit condor.submit').strip()
	except Exception:
		error_text = 'Error occured in initialization\n'+traceback.format_exc()
		cleanup('',error_text,output_name)
		
	#attach to monitoring files
	error_file = None
	logger_file = None
	while(1):
		if not error_file and os.path.exists(error_file_name): error_file = open(error_file_name,'r+')
		if not logger_file and os.path.exists(logger_file_name): logger_file = open(logger_file_name,'r+')
		if None not in [
			error_file,
			logger_file,
			]: break
		if os.path.exists('done'):
			cleanup('','An unknown error has prevented log file creation before job finished.',output_name)
		sleep(1)

	#monitor job
	while not os.path.exists('done'):
		logger_text = logger_file.read()
		if logger_text: print logger_text.strip()
		error_text = error_file.read()
		if error_text: cleanup(logger_file.read(),error_text,output_name)
		sleep(0.5)

	cleanup(logger_file.read(),error_file.read(),output_name)	
		
def analyze_condor(
	analysis_constructor,
	module_name,
	analysis_name,
	tree,
	grl,
	files,
	output,
	entries=None,
	num_processes=2
	):

	#generate default dictionaries needed by ROOT
	generate_dictionaries()

	print 'Setting up analysis'
	print '-'*50+'\n'

	#create initial analysis object to test for obvious problems
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	analysis_instance.add_file(*files)
	analysis_instance.setup_chain()

	print '\n'+'-'*50

	if entries is not None:
		entries = min([int(entries),analysis_instance.pchain.get_entries()])
	else: entries = analysis_instance.pchain.get_entries()

	if not entries: return 1

	print 'Processing {0} entries with {1} processes'.format(entries,num_processes)
	
	#Result, error and log queue
	result_queue = Queue()
	error_queue = Queue()
	logger_queue = Queue()

	#Create temp directory
	while True:
		directory = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
		if directory not in os.listdir('.'):
			os.mkdir(directory)
			break
	print 'Created temporary directory {0}'.format(directory)

	#Instantiate and start processes
	ranges = [[i*(entries/num_processes),(i+1)*(entries/num_processes)] for i in range(num_processes)]; ranges[-1][-1]+=entries%(num_processes)

	processes = [Process(target = call_analyze_slice_condor, args = (
			module_name,
			analysis_name,
			tree,
			grl,
			files,
			ranges,
			process_number,
			directory,
			result_queue,
			error_queue,
			logger_queue,
			)) for process_number in range(num_processes)]

	time_start = time()
	for process in processes: process.start()
	print '{0} processes started'.format(num_processes)

	def cleanup():
		for process in processes: 
			process.terminate()
			process.join()
		#if os.path.exists(directory): shutil.rmtree(directory)		
		sys.exit()

	#Wait for processes to complete or kill them if ctrl-c
	finished = 0
	results = []
	while 1:
		try: sleep(0.1)
		except KeyboardInterrupt: cleanup()

		#flush logger queue
		while not logger_queue.empty():
			print logger_queue.get()

		#flush error queue
		while not error_queue.empty():
			print error_queue.get()
			cleanup()

		#flush result queue
		while not result_queue.empty():
			finished+=1
			#flush logger queue
			while not logger_queue.empty():
				print logger_queue.get()
			#flush error queue
			while not error_queue.empty():
				print error_queue.get()
				cleanup()
			results.append(result_queue.get())

		if finished==num_processes: break		

	print 'Overall rate: {0} Hz'.format(round(entries/(time()-time_start),2))

	#Create path to output and output ROOT file, merge results
	mkpath(os.path.dirname(output))
	os.close(sys.stdout.fileno())
	if num_processes>1:
		merger = ROOT.TFileMerger()
		if os.path.exists(output): os.remove(output)
		merger.OutputFile(output)
		for result in results:
			merger.AddFile(result)
		merger.Merge()
	else:
		shutil.move(results[0], output)

	cleanup()

def analyze_slice_condor(
	module_name,
	analysis_name,
	tree,
	grl,
	files,
	start,
	end,
	output_name,
	process_number,
	error_file_name,
	logger_file_name,
	):

	generate_dictionaries()

	error_file = open(error_file_name,'w+')
	logger_file = open(logger_file_name,'w+')

	error = None
	#cleanup function always called no matter form of exit
	def cleanup():
		output.Close()
		#error will be None if there is NO problem
		if error is not None: error_file.write(error+'\n');
		error_file.flush()
		error_file.close()
		sys.stdout.flush()
		sys.stdout.close()
		sys.exit()

	#print statements executed in here and in Event/Result functions are redirected to the log file
	sys.stdout = logpatch_file(logger_file)

	#Create output
	output = ROOT.TFile(output_name,'RECREATE')

	cwd = os.getcwd()
	os.chdir('{home}'.format(home=os.getenv('ANALYSISHOME')))

	if not os.path.exists(module_name):
		print '$ANALYSISHOME/analyses/{0} not found'.format(module_name)
		return 0
	module = '.'.join([part for part in module_name.split('/')]).rstrip('.py')
	try:
		analysis_constructor = __import__(module,globals(),locals(),[analysis_name]).__dict__[analysis_name]
	except ImportError:
		error = 'Problem importing {0} from $ANALYSISHOME/analyses/{1}\n'.format(analysis_name,module_name)+traceback.format_exc()
		print error
		return 0	
	if not issubclass(analysis_constructor,analysis):
		print '{0} in $ANALYSISHOME/analyses/{1} is not an analysis type'.format(analysis_constructor,module_name)
		return 0

	os.chdir(cwd)
	
	#Create local copy of analysis
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	with open(files) as f: files = [line.strip() for line in f.readlines() if line.strip()]
	analysis_instance.add_file(*files)

	try:
		analysis_instance.add_standard_functions()
		analysis_instance.setup_chain()
		analysis_instance.add_result_function(skim(analysis_instance))

	except Exception:
		error = 'Error occured in initialization\n'+traceback.format_exc()
		print error
		output_name = None
		cleanup()

	#tie results to output file
	for result_function in analysis_instance.result_functions:
		for result in result_function.results.values():
			result.SetDirectory(output)

	for meta_result_function in analysis_instance.meta_result_functions:
		for result in meta_result_function.results.values():
			result.SetDirectory(output)

	milestone = 0.

	time_start = time()

	entry=0
	try:
		for entry in xrange(start,end):
			#Create new event object (basically just a namespace)
			event = event_object()
			event.__stop__ = 1
			event.__entry__ = entry
			analysis_instance.pchain.set_entry(entry)
			for event_function in analysis_instance.event_functions:
				#Get registered branches from chain
				analysis_instance.pchain.get_branches(event,event_function.required_branches+event_function.create_branches.keys(),event_function.__class__.__name__)
				#Call event function
				event_function(event)
				if event.__break__: break
				#Increment stop count (used in cutflow)
				event.__stop__+= 1
			for result_function in analysis_instance.result_functions:
				#Call result function (does not necessarily respect event.__break__, must be implemented on case by case basis in __call__ of result function)
				result_function(event)

			rate = (entry-start)/(time()-time_start)
			done = float(entry-start+1)/(end-start)*100.
		
			if done>milestone:
				milestone+=10.
				print '{0}% complete, {1} Hz'.format(round(done,2),round(rate,2))

	except Exception:
		error = 'Exception caught in entry {0}\n'.format(entry)+traceback.format_exc()
		output_name = None
		cleanup()

	#Handle results
	try:
		for result_function in analysis_instance.result_functions:
			for result in result_function.results.values():
				output.cd()
				#Write result function items to output
				result.Write()

		#Only process meta-results for first process
		if not process_number:
			for meta_result_function in analysis_instance.meta_result_functions:
				#Call meta-result function
				meta_result_function(analysis_instance.files)
				for result in meta_result_function.results.values():
					output.cd()
					#Write meta-result function items to output
					result.Write()

	except Exception:
		error = 'Exception caught while handling results\n'+traceback.format_exc()
		output_name = None
		cleanup()

	print '{0}% complete, {1} Hz'.format(round(done,2), round(rate,2))	
	print 'Sending output {0}'.format(output_name)

	cleanup()


#Base analysis class
class analysis():

	def __init__(self):
		self.event_functions = []
		self.result_functions = []
		self.meta_result_functions = []

		self.required_branches = []
		self.create_branches = {}
		self.keep_branches = []

		self.files = set([])
		self.tree = 'physics'
		self.grl = None
	
	def add_event_function(self,*event_functions):
		self.event_functions += event_functions

	def add_result_function(self,*result_functions):
		self.result_functions += result_functions

	def add_meta_result_function(self,*result_functions):
		self.meta_result_functions += result_functions
	
	def add_file(self,*files):
		self.files = self.files.union(set(files))
		
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
		self.pchain.request_branches(self.required_branches)
		self.pchain.request_branches(self.keep_branches)

