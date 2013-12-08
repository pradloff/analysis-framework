#!/usr/bin/env python

def call_grid(
	module_name,
	analysis_name,
	grid_jsons,
	tree='physics',
	grl= None,
	num_processes=1,
	keep=False,
	merge=False,
	jobsize=1,
	):

	import os
	import atexit
	import shutil
	from time import sleep, time
	import string
	import random
	from common.external import call
	import tarfile
	import json

	#parse all first because we change directory and finds json problems
	grid_datas = []
	for grid_json in grid_jsons:
		with open(grid_json) as f: grid_datas.append(json.load(f))

	while True:
		directory = '/tmp/'+''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
		try: os.mkdir(directory)
		except OSError as error:
			if error.errno != 17: raise
			continue
		break
	print 'Created temporary directory {0}'.format(directory)

	cwd = os.getcwd()
	atexit.register(os.chdir,cwd)
	atexit.register(shutil.rmtree,os.path.abspath(directory))
	os.chdir(directory)

	analysis_home = os.getenv('ANALYSISHOME')
	analysis_framework = os.getenv('ANALYSISFRAMEWORK')

	analysis_constructor = __import__(module_name,globals(),locals(),[analysis_name]).__dict__[analysis_name]

	analysis_instance = analysis_constructor()

	#create tarball of working directory
	print 'Creating tarball'
	tarball = tarfile.open('send.tar.gz','w:gz')

	os.chdir(analysis_framework+'/../')
	tarball.add(os.path.basename(analysis_framework))
	os.chdir(analysis_home+'/../')
	tarball.add(os.path.basename(analysis_home))
	tarball.close()

	os.chdir(directory)

	exclude_sites = []

	for grid_data in grid_datas:

		grl = grid_data.get('GRL')

		grid_command = 'unset tmp; unset tmpdir; source analysis-framework/setup.sh; source {analysis_home}/setup.sh; analyze.py -m {module} -a {analysis} -i \`echo %IN | sed \'s/,/ /g\'\` -o skim.root -p {processes} -n {tree}{keep}{grl}'.format(
			module=module_name,
			analysis=analysis_name,
			tree=tree,
			processes=num_processes,
			analysis_home=os.path.basename(analysis_home),
			keep=' --keep' if keep else '',
			grl = ' -g {0}'.format(' '.join(grl)) if grl else '',
			)
	
		make_command = 'unset tmp; unset tmpdir; source analysis-framework/setup.sh; source {analysis_home}/setup.sh; python {analysis_home}/make_externals.py'.format(
			analysis_home=os.path.basename(analysis_home),
			)

		prun_command = 'prun --bexec="{make_command}" --exec "{grid_command}" --rootVer="5.34.07" --cmtConfig="x86_64-slc5-gcc43-opt" --outputs="skim.root" --inDsTxt=input_datasets.txt --outDS={output_name} --inTarBall=send.tar.gz --nFilesPerJob={jobsize}{exclude_sites} --nGBPerJob=MAX --useContElementBoundary{merge}'

		for output_name,input_datasets in grid_data.get('datasets').items():

			with open('input_datasets.txt','w') as f:
				for input_dataset in input_datasets:
					f.write(input_dataset+'\n')


			final_prun_command = prun_command.format(
				grid_command=grid_command,
				make_command=make_command,
				output_name=output_name,
				merge=' --mergeOutput' if merge else '',
				jobsize=jobsize,
				exclude_sites='--excludedSite='+','.join(exclude_sites) if exclude_sites else '',
				)

			#print final_prun_command
			print call(final_prun_command,verbose=True).strip()

if __name__ == '__main__':

	import argparse

	parser = argparse.ArgumentParser(prog='analyze_grid.py',description='Useful grid caller for analyses.')
	parser.add_argument('-m','--module',default=None,dest='MODULE',required=True,help='Module containing analysis class.')
	parser.add_argument('-a','--analysis',default=None,dest='ANALYSIS',required=True,help='Name of analysis to use.')
	parser.add_argument('-n','--tree',default='physics',dest='TREE',required=True,help='TTree name which contains event information.')
	parser.add_argument('-g','--grl',default=[],dest='GRL',nargs='+',help='Good run list(s) XML file to use.')
	parser.add_argument('-p','--processes',default=1,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')
	parser.add_argument('--grid',dest='GRID',required=True,nargs='+',help='Similar to [-t --textinput] except containing datasets on grid.  Organize datasets in json file, indexed by output dataset name.')
	parser.add_argument('--merge',dest='MERGE',action='store_true',help='Merge output of grid jobs.')
	parser.add_argument('--jobsize',default=1,type=int,dest='JOBSIZE',help='Number of files per job.')

	args = parser.parse_args()
		
	call_grid(
		args.MODULE,
		args.ANALYSIS,
		args.GRID,
		tree=args.TREE,
		grl=args.GRL,
		num_processes=args.PROCESSES,
		keep=args.KEEP,
		merge=args.MERGE,
		jobsize=args.JOBSIZE,		
		)
	
