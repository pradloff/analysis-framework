from PyCool import cool
import argparse
from xml.dom.minidom import parseString
import ROOT
import code
import sys
from multiprocessing import Queue, cpu_count, Process
from Queue import Empty
import time
import traceback

class query():
	def __init__(self):
		self.database_service = cool.DatabaseSvcFactory.databaseService()
		self.databases = {}
		self.folders = {}

	def get_database(self,database_name):
		if database_name not in self.databases:
			self.databases[database_name] = self.database_service.openDatabase(database_name,True)
		return self.databases[database_name]
	
	def get_folder(self,folder_name):
		if folder_name not in self.folders:
			database_name = self.get_database_name(folder_name)
			database = self.get_database(database_name)
			self.folders[folder_name] = database.getFolder(folder_name)
		return self.folders.get(folder_name)
		
	def get_database_name(self,folder_name):
		folder_name = '/'+folder_name if not folder_name.startswith('/') else folder_name
		if folder_name.startswith('/LHC/DCS/'):
			return 'oracle://ATLAS_COOLPROD;schema=ATLAS_COOLOFL_DCS;dbname=COMP200;'
		if folder_name.startswith('/TRIGGER/HLT/') and 'Rates' in folder_name:
			return "oracle://ATLAS_COOLPROD;schema=ATLAS_COOLONL_TRIGGER;dbname=MONP200;"
		return "oracle://ATLAS_COOLPROD;schema=ATLAS_COOLONL_{0};dbname=COMP200;".format(folder_name.split("/")[1])

	def __del__(self):
		for database in self.databases.values(): database.closeDatabase()

def runlb_to_lb(runlb):
	run = runlb>>32
	return runlb-(run<<32)

#instantiate and call to get span of runlbs for a given run, returns high low tuple
class runlb_span_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,run):
		min_runlb = run<<32
		max_runlb = ((run+1)<<32)-1
		folder = self.get_folder('/TRIGGER/LUMI/LBLB')
		channel = cool.ChannelSelection()
		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel)

		max_runlb = min_runlb
		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			current_runlb = current_slice.since()
			min_runlb = min(min_runlb,current_runlb)
			max_runlb = max(max_runlb,current_runlb)

		folder_iterator.close()
		return min_runlb,max_runlb

#instantiate and call to get span of time for a span of runlbs, returns dict of high low nanotimes indexed by runlb
class time_span_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,runlb_span):
		min_runlb,max_runlb = runlb_span
		folder = self.get_folder('/TRIGGER/LUMI/LBLB')
		channel = cool.ChannelSelection()
		time_span = {}
		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			current_runlb = current_slice.since()
			time_span[current_runlb] = (int(current_slice.payloadValue('StartTime')),int(current_slice.payloadValue('EndTime')))

		folder_iterator.close()
		return time_span

#instantiate and call to get luminosity for a span of runlbs, returns dict of luminosity indexed by runlb
class luminosity_span_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,runlb_span):
		min_runlb,max_runlb = runlb_span
		folder = self.get_folder('/TRIGGER/LUMI/LBLESTONL')
		channel = cool.ChannelSelection(0)
		luminosity = {}
		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			current_runlb = current_slice.since()
			luminosity[current_runlb] = float(current_slice.payloadValue('LBAvInstLumi'))

		folder_iterator.close()
		return luminosity

class stable_span_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,runlb_span):
		min_runlb,max_runlb = runlb_span
		folder = self.get_folder('/TDAQ/RunCtrl/DataTakingMode')
		channel = cool.ChannelSelection(0)
		stable = {}
		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			runlb_start = current_slice.since()
			runlb_end = current_slice.until()
			for runlb in range(max(min_runlb,runlb_start),min(max_runlb,runlb_end)):
				stable[runlb] = int(current_slice.payloadValue('ReadyForPhysics'))

		folder_iterator.close()
		return stable

class TriggerInfoSlice():
	def __init__(self,run):
		self.run = run
		self.channel = {}
		self.lower = {}
		self.streams = {}
		self.counter = {}

class TriggerInfo():
	def __init__(self):
		self.channel = {}
		self.lower = {}
		self.streams = {}
		self.counter = {}

	def update(self,trigger_info_slice):
		for name in [
			'channel',
			'lower',
			'streams',
			'counter',
			]:
			if trigger_info_slice.run not in self.__dict__[name]: self.__dict__[name][trigger_info_slice.run] = {}
			self.__dict__[name][trigger_info_slice.run].update(trigger_info_slice.__dict__[name])

class trigger_info_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,run):
		min_runlb = run<<32
		trigger_info_slice = TriggerInfoSlice(run)

		folder = self.get_folder('/TRIGGER/LVL1/Menu')
		channel = cool.ChannelSelection()
		folder_iterator = folder.browseObjects(min_runlb,min_runlb,channel)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			trigger_info_slice.channel[current_slice.payloadValue('ItemName')] = current_slice.channelId()
		folder_iterator.close()

		folder = self.get_folder('/TRIGGER/HLT/Menu')
		channel = cool.ChannelSelection()
		folder_iterator = folder.browseObjects(min_runlb,min_runlb,channel)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			chain_name = current_slice.payloadValue('ChainName')
			trigger_info_slice.channel[chain_name] = current_slice.channelId()
			trigger_info_slice.lower[chain_name] = current_slice.payloadValue('LowerChainName')
			streams = [stream.split(',') for stream in current_slice.payloadValue('StreamInfo').split(';')]
			trigger_info_slice.streams[chain_name] = [stream[1]+'_'+stream[0] for stream in streams]
			trigger_info_slice.counter[chain_name] = int(current_slice.payloadValue('ChainCounter'))

		folder_iterator.close()
		return trigger_info_slice

class trigger_prescale_getter(query):
	def __init__(self):
		query.__init__(self)

	def __call__(self,span,trigger,trigger_info):
		min_runlb,max_runlb = span

		run = min_runlb>>32

		if isinstance(trigger_info,TriggerInfo):
			trigger_l2 = trigger_info.lower[run][trigger]
			trigger_l1 = trigger_info.lower[run][trigger_l2]
			channel_l1 = cool.ChannelSelection(trigger_info.channel[run][trigger_l1])
			channel_l2 = cool.ChannelSelection(2*trigger_info.counter[run][trigger_l2]+1)
			channel_EF = cool.ChannelSelection(2*trigger_info.counter[run][trigger])
		
		else:
			if run!=trigger_info.run: raise KeyError
			trigger_l2 = trigger_info.lower[trigger]
			trigger_l1 = trigger_info.lower[trigger_l2]
			channel_l1 = cool.ChannelSelection(trigger_info.channel[trigger_l1])
			channel_l2 = cool.ChannelSelection(2*trigger_info.counter[trigger_l2]+1)
			channel_EF = cool.ChannelSelection(2*trigger_info.counter[trigger])
		
		prescales = {}

		#level1 trigger prescale
		folder = self.get_folder('/TRIGGER/LVL1/Prescales')

		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel_l1)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			runlb_start = current_slice.since()
			runlb_end = current_slice.until()
			for runlb in range(max(min_runlb,runlb_start),min(max_runlb,runlb_end)):
				prescales[runlb] = float(current_slice.payloadValue('Lvl1Prescale')) 
		folder_iterator.close()

		#level2 trigger prescale
		folder = self.get_folder('/TRIGGER/HLT/Prescales')

		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel_l2)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			runlb_start = current_slice.since()
			runlb_end = current_slice.until()
			for runlb in range(max(min_runlb,runlb_start),min(max_runlb,runlb_end)):
				if runlb not in prescales: continue
				prescales[runlb] *= float(current_slice.payloadValue('Prescale')) 
		folder_iterator.close()

		#EF trigger prescale
		folder = self.get_folder('/TRIGGER/HLT/Prescales')

		folder_iterator = folder.browseObjects(min_runlb,max_runlb,channel_EF)

		while folder_iterator.goToNext():
			current_slice = folder_iterator.currentRef()
			runlb_start = current_slice.since()
			runlb_end = current_slice.until()
			for runlb in range(max(min_runlb,runlb_start),min(max_runlb,runlb_end)):
				if runlb not in prescales: continue
				prescales[runlb] *= float(current_slice.payloadValue('Prescale')) 
		folder_iterator.close()

		return prescales

class GRL(dict):
	def __init__(self):
		pass
	def update(self,grl):
		for run in grl:
			if run not in self: self[run] = []
			self[run]+=grl[run]

def parse_grl(grl_string):
	dom = parseString(grl_string)
	grl = GRL()

	for lb_collection in dom.getElementsByTagName('LumiBlockCollection'):
		run = int(lb_collection.getElementsByTagName('Run')[0].firstChild.data)
		for lb_range in lb_collection.getElementsByTagName('LBRange'):
			start = lb_range.getAttribute('Start')
			end = lb_range.getAttribute('End')
			if run not in grl: grl[run] = []
			grl[run]+= range(int(start),int(end)+1)

						
	return grl
	
def parse_grl_xml(grl_xml):
	with open(grl_xml) as f: return(parse_grl(f.read()))

def get_run_info(trigger,queue_in,queue_out):

	get_runlb_span = runlb_span_getter()	
	get_trigger_info = trigger_info_getter()
	get_trigger_prescale = trigger_prescale_getter()
	get_luminosity_span = luminosity_span_getter()
	get_time_span = time_span_getter()
	try:
		while 1:
			try: run = queue_in.get(False,0.01)
			except Empty:
				time.sleep(0.1)
				continue
			if run is True: break
			span = get_runlb_span(run)
			luminosity = get_luminosity_span(span)
			trigger_info_slice = get_trigger_info(run)
			prescales = get_trigger_prescale(span,trigger,trigger_info_slice)
			times = get_time_span(span)
			queue_out.put((luminosity,prescales,times))
	except Exception:
		print traceback.format_exc()
	finally:
		queue_out.put(True)

if __name__=='__main__':
	parser = argparse.ArgumentParser(prog='cool_tool.py',description='')
	parser.add_argument(dest='TRIGGER')
	parser.add_argument('-x','--xml',dest='GRL',required=True,help='grl xml')
	parser.add_argument('-d',dest='D3PD',default=[],nargs='+',help='d3pds containing luminosity info trees')
	parser.add_argument('-p',dest='PROCESSES',default=1,type=int,help='number of processes to gather info')
	args = parser.parse_args()

	data = None

	if args.D3PD:
		data = GRL()
		c = ROOT.TChain('lumi')
		for d3pd in args.D3PD:
			c.Add(d3pd)
		for entry in range(c.GetEntries()):
			c.GetEntry(entry)
			data.update(parse_grl(str(c.grl)))

	grl = parse_grl_xml(args.GRL)

	queue_put = Queue()
	queue_get = Queue()

	todo = 0
	for run in grl.keys(): 
		queue_put.put(run)
		todo+=1
	for i in range(args.PROCESSES): queue_put.put(True) #Poison pill

	interrupted = False

	processes = [Process(target=get_run_info,args=(args.TRIGGER,queue_put,queue_get,)) for i in range(args.PROCESSES)]
	try:
		for process in processes: process.start()
		print 'Processing {0} runs with {1} processes'.format(todo,args.PROCESSES)
		luminosity = {}
		prescales = {}
		times = {}

		finished = 0
		counter = 0

		while 1:
			try: result = queue_get.get(False,0.01)
			except Empty:
				time.sleep(0.1)
				continue
			if result is True:
				finished+= 1
				if finished==args.PROCESSES: break
				continue
			counter+= 1
			luminosity_,prescales_,times_ = result
			luminosity.update(luminosity_)
			prescales.update(prescales_)
			times.update(times_)
			print '\r{0}/{1} finished'.format(counter,todo)
			sys.stdout.flush()

	except KeyboardInterrupt:
		print 'Interrupted, how rude. GOODBYE'
		interrupted = True
	finally:
		for process in processes: process.terminate()
		for process in processes: process.join()
		if interrupted: sys.exit(1)

	code.interact(local=locals())

def get_count(file_,tree):
	f = ROOT.TFile.Open(file_)
	if not f: return None
	t = getattr(f,tree)
	t.SetBranchStatus('*',0)
	t.SetBranchStatus('mc_channel_number',1)
	t.mc_channel_number
	return t.mc_channel_number,f.cutflow_weighted.GetBinContent(1)

def get_counts(files,tree):
	d = {}
	for file_ in files:
		try:
			mc_channel_number,counts = get_count(file_)
		except Exception:
			continue
		if mc_channel_number not in d: d[mc_channel_number] = 0.
		d[mc_channel_number]+=counts
	return d

def get_cross_sections(cross_section_file):
	d = {}
	with open(cross_section_file) as f:
		for line in f.readlines():
			if line.lstrip().startswith('#'): continue
			mc_channel_number = line.split()[0]
			cross_section = line.split()[1]*line.split()[2]
			d[mc_channel_number] = cross_section
	return d

if __name__=='__main__':

	import sys
	import argparse
	import os

	parser = argparse.ArgumentParser(prog='lumi.py',description='Useful caller for getting MC scaling and lumi info.')
	parser.add_argument(dest='TRIGGER')
	parser.add_argument('-x','--xml',dest='GRL',required=True,help='grl xml')
	parser.add_argument('-d',dest='D3PD',default=[],nargs='+',required=True,help='d3pds containing luminosity info trees')
	parser.add_argument('-m',dest='SKIM',default=[],nargs='+',required=True,help='Directory containing skimmed MC/Data with weighted cross-section info')
	parser.add_argument('-c',dest='XSEC',required=True,help='File containing cross-section information')
	parser.add_argument('-p',dest='PROCESSES',default=1,type=int,help='number of processes to gather info')
	parser.add_argument('-o',dest='OUTPUT',required=True,help='output luminosity/cross-section information')
	parser.add_argument('-t',dest='TREE',required=True,help='Tree containing event info')
	args = parser.parse_args()

	cross_sections = get_cross_sections(args.XSEC)
	counts = get_counts(args.SKIM,args.TREE)

	sys.exit()

	#Get data luminosity
	data = GRL()
	c = ROOT.TChain('lumi')
	for d3pd in args.D3PD:
		c.Add(d3pd)
	for entry in range(c.GetEntries()):
		c.GetEntry(entry)
		data.update(parse_grl(str(c.grl)))

	grl = parse_grl_xml(args.GRL)


	queue_put = Queue()
	queue_get = Queue()

	todo = 0
	for run in grl.keys(): 
		queue_put.put(run)
		todo+=1
	for i in range(args.PROCESSES): queue_put.put(True) #Poison pill

	interrupted = False

	processes = [Process(target=get_run_info,args=(args.TRIGGER,queue_put,queue_get,)) for i in range(args.PROCESSES)]
	try:
		for process in processes: process.start()
		print 'Processing {0} runs with {1} processes'.format(todo,args.PROCESSES)
		luminosity = {}
		prescales = {}
		times = {}

		finished = 0
		counter = 0

		while 1:
			try: result = queue_get.get(False,0.01)
			except Empty:
				time.sleep(0.1)
				continue
			if result is True:
				finished+= 1
				if finished==args.PROCESSES: break
				continue
			counter+= 1
			luminosity_,prescales_,times_ = result
			luminosity.update(luminosity_)
			prescales.update(prescales_)
			times.update(times_)
			print '\r{0}/{1} finished'.format(counter,todo)
			sys.stdout.flush()

	except KeyboardInterrupt:
		print 'Interrupted, how rude. GOODBYE'
		interrupted = True
	finally:
		for process in processes: process.terminate()
		for process in processes: process.join()
		if interrupted: sys.exit(1)

