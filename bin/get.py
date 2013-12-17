#!/usr/bin/env python

def get(
	grid_jsons,
	num_processes=1,
	):

	import os
	import json
	import subprocess
	from time import sleep

	class watcher():

		def __init__(self,child_call):
			self.child_call = child_call
	
		def start(self):
			self.child = subprocess.Popen(self.child_call,shell=True)

		def poll(self):
			exitcode = self.child.poll()
			return exitcode

		def kill(self):
			try: self.child.kill()
			except OSError: pass

	#parse all first because we change directory and finds json problems
	grid_datas = []
	for grid_json in grid_jsons:
		with open(grid_json) as f: grid_datas.append(json.load(f))

	watchers = []

	for grid_data in grid_datas:
		for output_dataset in grid_data['datasets']:
			child_call = 'dq2-get -T 1,20 -f \"*.root\" -H {dataset} {dataset}'.format(
				dataset = output_dataset if output_dataset.endswith('/') else output_dataset+'/',
				)
			watchers.append(watcher(child_call))

	#Preload num_process watchers:
	processes = []
	for i in range(num_processes):
		try: 
			watcher = watchers.pop()
			watcher.start()
			processes.append(watcher)
		except IndexError: break
		
	#Monitor/submit mode
	while 1:
		finished = []
		for process in processes:
			exitcode = process.poll()
			if exitcode is not None:
				if exitcode: print 'Failed {0}'.format(process.child_call)
				else: print 'Finished {0}'.format(process.child_call)
				finished.append(process)
		for process in finished:
			del processes[processes.index(process)]
			try: 
				watcher = watchers.pop()
				watcher.start()
				processes.append(watcher)
			except IndexError: pass
		if not processes: break
		sleep(2)
	

if __name__ == '__main__':

	import argparse

	parser = argparse.ArgumentParser(prog='get.py',description='Get grid files using grid json files.')
	parser.add_argument('-p','--processes',default=1,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument(dest='GRID',required=True,nargs='+',help='Similar to [-t --textinput] except containing datasets on grid.  Organize datasets in json file, indexed by output dataset name.')

	args = parser.parse_args()
		
	get(
		args.GRID,
		num_processes=args.PROCESSES,	
		)
	
