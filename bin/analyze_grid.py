#!/usr/bin/env python

import os
import sys
from common.analysis import analysis
import traceback
import shutil
from time import sleep, time
from distutils.dir_util import mkpath
import string
import random
from common.external import call
import tarfile
import json

def call_grid(
	module_name,
	analysis_name,
	grid_input,
	tree='physics',
	grl= None,
	num_processes=1,
	keep=False,
	merge=False,
	):

	with open(grid_input) as f: dataset = json.load(f)

	cwd = os.getcwd()

	analysis_home = os.getenv('ANALYSISHOME')
	analysis_framework = os.getenv('ANALYSISFRAMEWORK')

	os.chdir(analysis_home)

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

	analysis_instance = analysis_constructor()

	os.chdir(cwd)

	while True:
		directory = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
		if directory not in os.listdir('.'):
			os.mkdir(directory)
			break
	print 'Created temporary directory {0}'.format(directory)

	os.chdir(directory)

	#shutil.copytree(os.getenv('ANALYSISFRAMEWORK'),'analysis-framework')
	#shutil.copytree(os.getenv('ANALYSISHOME'),'analyses')

	#create tarball of working directory
	print 'Creating tarball'
	tarball = tarfile.open('send.tar.gz','w:gz')
	tarball.add(analysis_framework)
	tarball.add(analysis_home)
	tarball.close()

	grid_command = 'echo %IN | sed \'s/,/\\n/g\' | sed \'s/ //g\' > input.txt; source analysis-framework/setup.sh; source {analysis_home}/setup.sh; analyze.py -m {module} -a {analysis} -t input.txt -o skim.root -p {processes} -n {tree}{keep}{{grl}}'.format(
		module=module_name,
		analysis=analysis_name,
		tree=tree,
		processes=num_processes,
		analysis_home=os.path.basename(analysis_home),
		keep='--keep' if keep else '',
		)
	
	prun_command = 'prun --exec "{final_grid_command}" --rootVer="5.34.07" --cmtConfig="x86_64-slc5-gcc43-opt" --outputs="skim.root" --inDsTxt=input.txt --outDS={output} --inTarBall=send.tar.gz --useContElementBoundary{merge}'

	for output,contents in dataset.items():
		grl = contents.get('GRL')
		final_grid_command = grid_command.format(
			grl = '-g {0}'.format(grl) if grl else '',
			)

		with open('input.txt','w') as f:
			for input_dataset in contents.get('datasets'):
				f.write(input_dataset+'\n')


		final_prun_command = prun_command.format(
			final_grid_command=final_grid_command,
			output=output,
			merge='--mergeOutput' if merge else '',
			)

		print final_prun_command
		#print call(final_prun_command).strip()

#=======================================================================================================
	
if __name__ == '__main__':

	import sys
	import argparse
	import code

	parser = argparse.ArgumentParser(prog='analyze_condor.py',description='Useful grid caller for analyses.')
	parser.add_argument('-m','--module',default=None,dest='MODULE',help='Module containing analysis class.')
	parser.add_argument('-a','--analysis',default=None,dest='ANALYSIS',help='Name of analysis to use.')
	parser.add_argument('-n','--tree',default='physics',dest='TREE',help='TTree name which contains event information.')
	parser.add_argument('-g','--grl',default=None,dest='GRL',help='Good run list XML file to use.')
	parser.add_argument('-p','--processes',default=2,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')
	parser.add_argument('--grid',default=None,dest='GRID',help='Similar to [-t --textinput] except containing datasets on grid.  Organize datasets in json file, indexed by output dataset name.')
	parser.add_argument('--merge',dest='MERGE',action='store_true',help='Merge output of grid jobs.')

	args = parser.parse_args()
		
	allargs = True
	
	if not any([args.GRID]): print 'Must include some form of input [-i, --input], [-t, --textinput], [-d, --dataset], or [-r, --grid]'; allargs = False
	if not args.MODULE: print 'Must include name of module containing analysis [-m, --module]'; allargs = False
	if not args.ANALYSIS: print 'Must include name of analysis [-a, --analysis]'; allargs = False

	if not allargs: sys.exit()

	call_grid(
		args.MODULE,
		args.ANALYSIS,
		args.GRID,
		tree=args.TREE,
		grl=args.GRL,
		num_processes=args.PROCESSES,
		keep=args.KEEP,
		merge=args.MERGE,		
		)
	
