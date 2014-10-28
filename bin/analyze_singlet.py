#!/usr/bin/env python

if __name__=='__main__':

	import os
    import sys
    import argparse
    import atexit
    import itertools

    parser = argparse.ArgumentParser(prog='analyze_singlet.py',description='Useful caller for analyses with single process.')
    parser.add_argument('-t','--textinput',dest='TEXTINPUT',required=True,help='Text file containing input file(s) to analyze.  Separate files by line.')
    parser.add_argument('-m','--module',dest='MODULE',required=True,help='Module containing analysis class.')
    parser.add_argument('-a','--analysis',dest='ANALYSIS',required=True,help='Name of analysis to use.')
    parser.add_argument('-o','--output',dest='OUTPUT',required=True,help='Name to give output ROOT file.')
    parser.add_argument('-d',dest='DIRECTORY',default='.',help='Run directory')
    parser.add_argument('-l','--logger',dest='LOGGER',default='',help='Name to give output logger file.')
    parser.add_argument('-z','--error',dest='ERROR',default='',help='Name to give error logger file.')
    parser.add_argument('-s',type=int,dest='START',required=True,help='Entry to start processing.')
    parser.add_argument('-e',type=int,dest='END',required=True,help='Entry to end processing.')
    parser.add_argument('-n','--tree',dest='TREE',required=True,help='TTree name which contains event information.')
    parser.add_argument('-g','--grl',default=[],dest='GRL',nargs='+',help='Good run list(s) XML file to use.')
    parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')
    parser.add_argument('--interactive',default=False,dest='INTERACT',action='store_true',help='Interact with event')

    args = []
    for i,(k,g) in enumerate(itertools.groupby(sys.argv,lambda x:x=='-')):
        g=list(g)
        args += g[1:]
        break

    args = parser.parse_args(args)
    from common.analysis import analyze_slice

	if not os.path.exists(args.DIRECTORY): os.mkdir(args.DIRECTORY)
	os.chdir(args.DIRECTORY)

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
    singlet.run(interactive=args.INTERACT)
