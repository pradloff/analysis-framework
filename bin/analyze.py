#!/usr/bin/env python

import os
import sys
from common.analysis import analysis
import traceback
import ROOT
from common.pchain import generate_dictionaries
import shutil
from time import sleep, time
from distutils.dir_util import mkpath
from multiprocessing import Process, Queue
from common.event import event_object
import string
import random
from math import log
from common.external import call
from common.misc import logpatch, logpatch_file
from common.standard import in_grl, skim, cutflow, compute_mc_weight

def call_analyze(
	module_name,
	analysis_name,
	files=[],
	tree='physics',
	grl=[],
	num_processes=2,
	output='result.root',
	entries=None,
	keep=False,
	):

	cwd = os.getcwd()
	os.chdir(os.getenv('ANALYSISHOME'))

	if not os.path.exists(module_name):
		print '$ANALYSISHOME/{0} not found'.format(module_name)
		return 0
	module = '.'.join([part for part in module_name.split('/')]).rstrip('.py')
	try:
		analysis_constructor = __import__(module,globals(),locals(),[analysis_name]).__dict__[analysis_name]
	except ImportError:
		error = 'Problem importing {0} from $ANALYSISHOME/analyses/{1}\n'.format(analysis_name,module_name)+traceback.format_exc()
		print error
		return 0	
	if not issubclass(analysis_constructor,analysis):
		print '{0} in $ANALYSISHOME/{1} is not an analysis type'.format(analysis_constructor,module_name)
		return 0

	os.chdir(cwd)

	return analyze(
		analysis_constructor,
		tree,
		grl,
		files,
		output,
		entries=entries,
		num_processes=num_processes,
		keep=keep,
		)

def analyze(
	analysis_constructor,
	tree,
	grl,
	files,
	output,
	entries=None,
	num_processes=2,
	keep=False,
	):

	#generate default dictionaries needed by ROOT
	generate_dictionaries()

	print 'Setting up analysis'
	print '-'*50+'\n'

	#create initial analysis object to test for obvious problems
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	#analysis_instance.add_file(*files)
	#analysis_instance.setup_chain()

	print '\n'+'-'*50

	del analysis_instance

	#print 'Processing {0} entries with {1} processes'.format(entries,num_processes)
	
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


	processes = [Process(target = analyze_slice, args = (
			analysis_constructor,
			tree,
			grl,
			files,
			entries,
			process_number,
			num_processes,
			directory,
			result_queue,
			error_queue,
			logger_queue,
			keep,
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
	tick=0
	while 1:
		tick+=1
		#if not tick%1000: print 'tick'
		try: sleep(0.01)
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

			result =  result_queue.get()
			if result is None:
				sleep(1)
				while not logger_queue.empty():
					print logger_queue.get()
				#flush error queue
				while not error_queue.empty():
					print error_queue.get()
					cleanup()
				print 'An unknown error occured'
				cleanup()				
			results.append(result)

		if finished==num_processes: break		

	#print 'Overall rate: {0} Hz'.format(round(entries/(time()-time_start),2))

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
	entries,
	process_number,
	num_processes,
	directory,
	result_queue,
	error_queue,
	logger_queue,
	keep,
	):

	#print statements executed in here and in Event/Result functions are redirected to the main logger
	#os.close(sys.stdout.fileno())
	#os.close(sys.stderr.fileno())
	sys.stdout = logpatch(logger_queue,'Process number {0}: '.format(process_number),'')
	sys.stderr = logpatch(logger_queue,'Process number {0}: '.format(process_number),'')
	print 'Patched stdout stderr'

	generate_dictionaries()

	error = None
	#cleanup function always called no matter form of exit
	def cleanup(output,output_name,error):
		output.Close()
		sleep(0.5)
		#output name will be None if there is some problem
		result_queue.put(output_name)
		#error will be None if there is NO problem
		if error: error_queue.put(error)
		sys.exit()

	#Create output
	print 'Creating output'
	if num_processes>1: output_name = '{directory}/temp_{0:0>{1}}.root'.format(process_number,int(log(num_processes-1,10))+1,directory=directory)
	else: output_name = '{directory}/temp.root'.format(directory=directory)
	output = ROOT.TFile(output_name,'RECREATE')
	
	#Create local copy of analysis
	print 'Creating analysis'
	analysis_instance = analysis_constructor()
	analysis_instance.tree = tree
	analysis_instance.grl = grl
	analysis_instance.keep_all=keep
	print 'Adding files'
	analysis_instance.add_file(*files)

	try:
		print 'Adding standard functions'
		analysis_instance.add_standard_functions()
		print 'Setting up chain'
		analysis_instance.setup_chain()
		print 'Adding skim function'
		analysis_instance.add_result_function(skim(analysis_instance))

	except Exception:
		error = 'Error occured in initialization\n'+traceback.format_exc()
		output_name = None
		cleanup(output,output_name,error)

	if entries is not None:
		entries = min([int(entries),analysis_instance.pchain.get_entries()])
	else: entries = analysis_instance.pchain.get_entries()

	ranges = [[i*(entries/num_processes),(i+1)*(entries/num_processes)] for i in range(num_processes)]; ranges[-1][-1]+=entries%(num_processes)

	#tie results to output file
	print 'Initializing results'
	for result_function in analysis_instance.result_functions:
		for result in result_function.results.values():
			result.SetDirectory(output)

	for meta_result_function in analysis_instance.meta_result_functions:
		for result in meta_result_function.results.values():
			result.SetDirectory(output)

	milestone = 0.

	start,end = ranges[process_number]
	time_start = time()

	print 'Processing from {0} to {1}'.format(start,end)

	print 'Starting looping'
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
		cleanup(output,output_name,error)

	del analysis_object.pchain

	print 'Handling results'
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
		cleanup(output,output_name,error)

	print '{0}% complete, {1} Hz'.format(round(done,2), round(rate,2))	
	print 'Sending output {0}'.format(output_name)

	cleanup(output,output_name,error)
	
if __name__ == '__main__':

	import sys
	import argparse
	
	parser = argparse.ArgumentParser(prog='analyze.py',description='Useful caller for analyses.')
	parser.add_argument('-i','--input',default=[],dest='INPUT', nargs='+',help='Input file(s) to analyze.')
	parser.add_argument('-t','--textinput',default=None,dest='TEXTINPUT',help='Text file containing input file(s) to analyze.  Separate files by line.')
	parser.add_argument('-m','--module',default=None,dest='MODULE',help='Module containing analysis class.')
	parser.add_argument('-a','--analysis',default=None,dest='ANALYSIS',help='Name of analysis to use.')
	parser.add_argument('-o','--output',default='result.root',dest='OUTPUT',help='Name to give output ROOT file.')
	parser.add_argument('--entries',default=None,dest='ENTRIES',help='Number of entries to process.')	
	parser.add_argument('-n','--tree',default='physics',dest='TREE',help='TTree name which contains event information.')
	parser.add_argument('-g','--grl',default=[],dest='GRL',nargs='+',help='Good run list(s) XML file to use.')
	parser.add_argument('-p','--processes',default=2,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')

	args = parser.parse_args()
	
	allargs = True
	
	if not any([args.INPUT,args.TEXTINPUT]): print 'Must include some form of input [-i, --input], [-t, --textinput]'; allargs = False
	if not args.MODULE: print 'Must include name of module containing analysis [-m, --module]'; allargs = False
	if not args.ANALYSIS: print 'Must include name of analysis [-a, --analysis]'; allargs = False

	if not allargs: sys.exit()

	files = []
	
	if isinstance(args.INPUT,str): files.append(args.INPUT)
	elif isinstance(args.INPUT,list): files += args.INPUT
	
	if args.TEXTINPUT:
		with open(args.TEXTINPUT) as f:
			for line in f.readlines():
				if not line.strip(): continue
				files.append(line.strip())
	
	call_analyze(
		args.MODULE,
		args.ANALYSIS,
		files=files,
		tree=args.TREE,
		grl=args.GRL,
		num_processes=args.PROCESSES,
		output=args.OUTPUT,
		entries=args.ENTRIES,
		keep=args.KEEP,
		)

	
