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
	keep,
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
	sys.stderr = logpatch_file(logger_file)

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
	analysis_instance.keep_all = keep
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
