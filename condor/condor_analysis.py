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

		self.error = ''
		self.output = None

		self.error_file = open(error_file_name,'w+',0)
		self.logger_file = open(logger_file_name,'w+',0)

	def initialize(self):

		sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
		sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)

		os.dup2(self.logger_file.fileno(),sys.stdout.fileno())
		os.dup2(self.error_file.fileno(),sys.stderr.fileno())

		#sys.stdout = logpatch_file(logger_file)
		#sys.stderr = logpatch_file(logger_file)

		try:
			analysis_constructor = __import__(module_name,globals(),locals(),[analysis_name]).__dict__[analysis_name]
		except ImportError:
			self.error = 'Problem importing {0} from {1}\n'.format(analysis_name,module_name)+traceback.format_exc()
			sys.exit(1)

		#Create output
		self.output = ROOT.TFile(self.output_name,'RECREATE')

		#Create local copy of analysis
		self.analysis_instance = analysis_constructor()
		self.analysis_instance.tree = self.tree
		self.analysis_instance.grl = self.grl
		self.analysis_instance.keep_all = self.keep

		try:
			with open(self.files) as f: files = [line.strip() for line in f.readlines() if line.strip()]
			self.analysis_instance.add_file(*files)
			self.analysis_instance.add_standard_functions()
			self.analysis_instance.setup_chain()
			self.analysis_instance.add_result_function(skim(self.analysis_instance))

		except Exception:
			self.error = 'Error occured in initialization\n'+traceback.format_exc()
			sys.exit(1)

		#tie results to output file
		for result_function in self.analysis_instance.result_functions:
			for result in result_function.results.values():
				result.SetDirectory(self.output)

		for meta_result_function in self.analysis_instance.meta_result_functions:
			for result in meta_result_function.results.values():
				result.SetDirectory(self.output)

	def run(self):
		generate_dictionaries()
		milestone = 0.
		time_start = time()
		entry=0
		try:
			for entry in xrange(start,end):
				#Create new event object (basically just a namespace)
				event = event_object()
				event.__stop__ = 1
				event.__entry__ = entry
				self.analysis_instance.pchain.set_entry(entry)
				for event_function in self.analysis_instance.event_functions:
					#Get registered branches from chain
					self.analysis_instance.pchain.get_branches(event,event_function.required_branches+event_function.create_branches.keys(),event_function.__class__.__name__)
					#Call event function
					event_function(event)
					if event.__break__: break
					#Increment stop count (used in cutflow)
					event.__stop__+= 1
				for result_function in self.analysis_instance.result_functions:
					#Call result function (does not necessarily respect event.__break__, must be implemented on case by case basis in __call__ of result function)
					result_function(event)

				rate = (entry-start)/(time()-time_start)
				done = float(entry-start+1)/(end-start)*100.
		
				if done>milestone:
					milestone+=10.
					print '{0}% complete, {1} Hz'.format(round(done,2),round(rate,2))

		except Exception:
			self.error = 'Exception caught in entry {0}\n'.format(entry)+traceback.format_exc()
			sys.exit(1)

		#Handle results
		try:
			for result_function in self.analysis_instance.result_functions:
				for result in result_function.results.values():
					self.output.cd()
					#Write result function items to output
					result.Write()

			for meta_result_function in analysis_instance.meta_result_functions:
				#Call meta-result function if we touched first entry of that file
				meta_result_function(self.analysis_instance.pchain.first_entry_files)
				for result in meta_result_function.results.values():
					self.output.cd()
					#Write meta-result function items to output
					result.Write()

		except Exception:
			self.error = 'Exception caught while handling results\n'+traceback.format_exc()
			sys.exit()

		print '{0}% complete, {1} Hz'.format(round(done,2), round(rate,2))	
		print 'Sending output {0}'.format(output_name)


	def cleanup(self):
		if self.output: self.output.Close()
		if self.error: error_file.write(self.error+'\n')
		self.logger_file.flush()
		self.logger_file.close()
		self.error_file.flush()
		self.error_file.close()


if __name__=='__main__':

	import sys
	import argparse
	import atexit

	parser = argparse.ArgumentParser(prog='analyze_singlet.py',description='Useful caller for analyses with single process.')
	parser.add_argument('-t','--textinput',dest='TEXTINPUT',required=True,help='Text file containing input file(s) to analyze.  Separate files by line.')
	parser.add_argument('-m','--module',dest='MODULE',required=True,help='Module containing analysis class.')
	parser.add_argument('-a','--analysis',dest='ANALYSIS',required=True,help='Name of analysis to use.')
	parser.add_argument('-o','--output',dest='OUTPUT',required=True,help='Name to give output ROOT file.')
	parser.add_argument('-l','--logger',dest='LOGGER',required=True,help='Name to give output logger file.')
	parser.add_argument('-z','--error',dest='ERROR',required=True,help='Name to give error logger file.')
	parser.add_argument('-s',type=int,dest='START',required=True,help='Entry to start processing.')
	parser.add_argument('-e',type=int,dest='END',required=True,help='Entry to end processing.')
	parser.add_argument('-n','--tree',dest='TREE',required=True,help='TTree name which contains event information.')
	parser.add_argument('-g','--grl',default=[],dest='GRL',nargs='+',help='Good run list(s) XML file to use.')
	parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')

	args = parser.parse_args()

	singlet = analyze_slice(
		args.MODULE,
		args.ANALYSIS,
		args.TREE,
		args.GRL,
		args.TEXTINPUT,
		args.START,
		args.END,
		args.OUTPUT,
		args.ERROR,
		args.LOGGER,
		args.KEEP,
		)
	atexit.register(singlet.cleanup)
	singlet.initialize()
	singlet.run()
