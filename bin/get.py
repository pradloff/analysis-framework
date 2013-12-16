#!/usr/bin/env python

def call_grid(
	grid_jsons,
	num_processes=1,
	):

	import os
	import json
	import subprocess

	class watcher():

		def __init__(self,child_call):
			self.child_call = child_call
	
		def start(self):
			subprocess.Popen(self.child_call.split())

		def poll(self):
			exitcode = self.child.poll()
			if exitcode: error = self.child.communicate()[1]
			else: error = None
			return exitcode,error

		def kill(self):
			try: self.child.kill()
			except OSError: pass

	watchers = []

	for grid_data in grid_datas:
		for output_dataset in grid_data['datasets']:
			child_call = 'dq2-get -f "*root*" {dataset}'.format(
				dataset = output_dataset,
				)
			watchers.append(watcher(child_call))

	processes = []
	
	#Monitor/submit mode
	while 1:
		finished = []
		for i in range(num_processes-len(processes)):
			try: watcher = watchers.pop()
			except IndexError: break
			watcher.start()
			processes.append(watcher)
		for process in processes:
			exitcode,error = process.poll()
			if exitcode is not None:
				if exitcode: print 'Failed in call of \n{0}\nError:\n{1}\n'.format(process.child_call,error)
				else: print 'Finished call of \n{0}\n'.format(process.child_call)
				finished.append(process)
		for process in finished:
			del processes[processes.index(process)]
		sleep(2)

	#Monitor mode
	while processes:
		for process in processes:
			exitcode,error = process.poll()
			if exitcode is not None:
				if exitcode: print 'Failed in call of \n{0}\nError:\n{1}\n'.format(process.child_call,error)
				else: print 'Finished call of \n{0}\n'.format(process.child_call)
				finished.append(process)
		for process in finished:
			del processes[processes.index(process)]
		sleep(2)		
		

if __name__ == '__main__':

	import argparse

	parser = argparse.ArgumentParser(prog='get.py',description='Get grid files using grid json files.')
	parser.add_argument('-p','--processes',default=1,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument('--grid',dest='GRID',required=True,nargs='+',help='Similar to [-t --textinput] except containing datasets on grid.  Organize datasets in json file, indexed by output dataset name.')

	args = parser.parse_args()
		
	get(
		args.GRID,
		num_processes=args.PROCESSES,	
		)
	
