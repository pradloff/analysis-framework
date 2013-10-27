#!/usr/bin/env python

#=======================================================================================================

def analyze(
	__analysisModule__,
	__analysisName__,
	__input__=[],
	__output__='result.root',
	__tree__='physics',
	__skim__=False,
	__GRL__= None,
	__processes__=2,
	__verbose__=False,
	__keep__=False,
	):

	__analysisModule__ = __analysisModule__.replace('/','.')[:-3]
	
	analysis = __import__(__analysisModule__,globals(),locals(),[__analysisName__]).__dict__[__analysisName__](__keep__=__keep__)
	
	analysis.AddInput(*__input__)
	analysis.SetTree(__tree__)
	analysis.SetSkim(__skim__)
	analysis.SetOutput(__output__)
	analysis.SetGRL(__GRL__)
	analysis.SetProcesses(__processes__)
	analysis.SetVerbose(__verbose__)
	
	return analysis()

#=======================================================================================================

import glob
import re
import code

def analyzeDataset (args):

	inputDict = {}
	outputDict = {}


	for __file__ in glob.iglob(args.DATASET.replace('#NAME#','*')):

		m = re.match(args.DATASET.replace('.','\.').replace('*','.*').replace('#NAME#','(.*)'),__file__)
		if not m:
			continue
		stream = m.group(1)
		
		skip = False
		for skipRE in args.SKIP:
			if re.match(skipRE,stream): skip = True
			break
		if skip: continue
						
		if stream not in inputDict: inputDict[stream] = []
		if stream not in outputDict: outputDict[stream] = args.OUTPUT.replace('#NAME#',stream)
		inputDict[stream].append(__file__)
		
	for i,(stream,__input__) in enumerate(sorted(inputDict.items())):
		if i: args.VERBOSE = False
		if os.path.exists(outputDict.get(stream)) and not args.FORCE: continue
		args.OUTPUT = outputDict.get(stream)
		result = callAnalyze(args,__input__)
		if not result: return result
	
	return 1
	
#=======================================================================================================

import json
import os
import subprocess

def call(command):
	result,error=tuple(subprocess.Popen([command], shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate())
	return result,error

def callGrid(args):

	if args.MERGE: merge = '--mergeOutput '
	else: merge = ''
	if args.SKIM: skim = ''
	else: skim = '--noskim '
	if args.KEEP: keep = '--keep '
	else: keep = ''
	if args.TREE: tree = '--tree={tree} '.format(tree=args.TREE)
	else: tree = ''
	
	includes = ''
	foldersToInclude = ['./config']#,'./BDT','./BDT/weights']
	for folder in foldersToInclude:
		for file_ in os.listdir(folder):
			if not includes: includes+= folder+'/'+file_
			else: includes+= ','+folder+'/'+file_
		if not includes: includes+= './MV1_cxx.so'	
		else: includes+= ',./MV1_cxx.so'	

		if not includes: includes+= './MV1_cxx.d'	
		else: includes+= ',./MV1_cxx.d'	

		if not includes: includes+= './MV1.cxx'	
		else: includes+= ',./MV1.cxx'	

	gridCommand = 'echo %IN | sed \'s/,/\\n/g\' | sed \'s/ //g\' > input.txt; source setup.sh; python2.6 bin/analyze.py -m {module} -a {analysis} -t input.txt -o skim.root {skim}{keep}{tree}-v -p {processes}'.format(module=args.MODULE,analysis=args.ANALYSIS,skim=skim,keep=keep,tree=tree,processes=args.PROCESSES)
	#prunCommand = 'prun --exec "{gridCommand}" --noBuild --athenaTag=17.1.4 --outputs="skim.root" --inDsTxt={__input__} --outDS={__output__} --excludeFile "results/*,data/*,limits/*" --extFile "{includes}" --nGBPerJob={ngb} --excludedSite=ANALY_GOEGRID --useContElementBoundary {merge}' # --noSubmit 
	if args.NGB!=-1: prunCommand = 'prun --exec "{gridCommand}" --rootVer="5.34.07" --cmtConfig="x86_64-slc5-gcc43-opt" --outputs="skim.root" --inDsTxt={__input__} --outDS={__output__} --excludeFile "results/*,data/*,limits/*" --extFile "{includes}" --nGBPerJob={ngb} --useContElementBoundary {merge}' # --noSubmit 
	else: prunCommand = 'prun --exec "{gridCommand}" --rootVer="5.34.07" --cmtConfig="x86_64-slc5-gcc43-opt" --outputs="skim.root" --inDsTxt={__input__} --outDS={__output__} --excludeFile "results/*,data/*,limits/*" --extFile "{includes}" --nFilesPerJob={nf} --useContElementBoundary {merge}' # --noSubmit 

	if args.EXPRESS: prunCommand+= ' --express'

	with open(args.GRID) as f: datasetDict = json.load(f)
	
	for __output__,contents in datasetDict.items():
		i = includes
		GRLXML = contents.get('GRLXML')
		gC = gridCommand
		if GRLXML: gC+= ' -g {GRLXML}'.format(GRLXML=GRLXML)
		with open('inDsTxt.txt','w') as f:
			for __input__ in contents.get('datasets'):
				f.write(__input__+'\n')
		if GRLXML: i+= ',./'+GRLXML
		pC = prunCommand.format(gridCommand = gC, __input__='inDsTxt.txt', __output__ = __output__,includes=i,ngb = args.NGB,nf=args.NF,merge=merge)
		print pC
		result,error = call(pC)
		print result
		os.remove('inDsTxt.txt')

#=======================================================================================================

def callAnalyze(args,__input__):
	return analyze(
		args.MODULE,
		args.ANALYSIS,
		__input__,
		__output__=args.OUTPUT,
		__tree__=args.TREE,
		__skim__=args.SKIM,
		__GRL__=args.GRL,
		__processes__=args.PROCESSES,
		__verbose__=args.VERBOSE,
		__keep__=args.KEEP
		)
		
#=======================================================================================================
	
if __name__ == '__main__':

	import sys
	import argparse
	import code
	
	parser = argparse.ArgumentParser(prog='analyze.py',description='Useful caller for analyses.')
	parser.add_argument('-i','--input',default=[],dest='INPUT', nargs='+',help='Input file(s) to analyze.')
	parser.add_argument('-t','--textinput',default=None,dest='TEXTINPUT',help='Text file containing input file(s) to analyze.  Separate files by line.')
	parser.add_argument('-m','--module',default=None,dest='MODULE',help='Module containing analysis class.')
	parser.add_argument('-a','--analysis',default=None,dest='ANALYSIS',help='Name of analysis to use.')
	parser.add_argument('-o','--output',default='result.root',dest='OUTPUT',help='Name to give output ROOT file.')
	#parser.add_argument('-s','--skim',dest='SKIM',action='store_true',help='Includes skimmed TTree in output.')
	parser.add_argument('--noskim',dest='NOSKIM',action='store_true',help='Turns off default skimmed TTree in output.')
	parser.add_argument('-v','--verbose',dest='VERBOSE',action='store_true',help='Sets verbosity to TRUE.')
	parser.add_argument('-f','--force',dest='FORCE',action='store_true',help='Sets force to TRUE.')
	parser.add_argument('-k','--keep',dest='KEEP',action='store_true',help='Sets keep to TRUE.')
	parser.add_argument('-n','--tree',default='physics',dest='TREE',help='TTree name which contains event information.')
	parser.add_argument('-g','--grl',default=None,dest='GRL',help='Good run list XML file to use.')
	parser.add_argument('-p','--processes',default=3,dest='PROCESSES',type=int,help='Number of processes to use.')
	parser.add_argument('--nGBPerJob',default=-1,dest='NGB',type=int,help='Number of GB per grid job.')
	parser.add_argument('--nFilesPerJob',default=1,dest='NF',type=int,help='Number of files per grid job.')
	parser.add_argument('-d','--dataset',default=None,dest='DATASET',help='Unix find style command input to assign file(s) associated with a stream. #NAME# sets output name for stream.')
	parser.add_argument('-r','--grid',default=None,dest='GRID',help='Similar to [-t --textinput] except containing datasets on grid.  Organize datasets in json file, indexed by output dataset name.')
	parser.add_argument('-e','--express',dest='EXPRESS',action='store_true',help='Only used when running on grid, sets express to TRUE.  Useful for test jobs.')
	parser.add_argument('--merge',dest='MERGE',action='store_true',help='Merge output of grid jobs.')
	parser.add_argument('--skip',default=[],dest='SKIP',nargs='+',help='Regular expression to use when skipping streams.  Only used with [-d, --dataset]')
		
	args = parser.parse_args()
		
	allargs = True
	
	if not any([args.INPUT,args.TEXTINPUT,args.DATASET,args.GRID]): print 'Must include some form of input [-i, --input], [-t, --textinput], [-d, --dataset], or [-r, --grid]'; allargs = False
	if not args.MODULE: print 'Must include name of module containing analysis [-m, --module]'; allargs = False
	if not args.ANALYSIS: print 'Must include name of analysis [-a, --analysis]'; allargs = False

	if not allargs: sys.exit()

	args.SKIM = not args.NOSKIM

	if args.GRID:
		callGrid(args)
		sys.exit()

	#--------------Below this line, only executed if grid not used  ----------------------------------------------

	if args.DATASET:
	
		allargs = True
		
		if args.DATASET.count('#NAME#') != 1: print '#NAME# needs to be included once and only once in dataset [-d, --dataset].'; allargs = False
		if not args.OUTPUT.count('#NAME#'): print '#NAME# needs to be included once in output when used in conjunction with datasets [-o, --output].'; allargs = False
		if allargs: analyzeDataset(args)
		
		sys.exit()
	
	#--------------Below this line, only executed if dataset not used  ----------------------------------------------

	__input__ = []
	
	if isinstance(args.INPUT,str): __input__.append(args.INPUT)
	elif isinstance(args.INPUT,list): __input__+= args.INPUT
	
	if args.TEXTINPUT:
		with open(args.TEXTINPUT) as f:
			for line in f.readlines():
				if not line.strip(): continue
				__input__.append(line.strip())
	
	callAnalyze(args,__input__)

	
