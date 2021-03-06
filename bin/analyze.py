#!/usr/bin/env python
import subprocess,os,time,sys,tempfile

class watcher():
    def __init__(self,directory,child,prefix):
        self.directory = directory
        self.error = '/'.join([directory,'error.txt'])
        self.logger = '/'.join([directory,'log.txt'])
        self.child = child
        self.prefix = prefix

        self.error_file = None
        self.logger_file = None

    def poll(self):
        if all([
            self.error_file is None,
            os.path.exists(self.error)
            ]): self.error_file = open(self.error,'r+')
        if all([
            self.logger_file is None,
            os.path.exists(self.logger)
            ]): self.logger_file = open(self.logger,'r+')

        exitcode = self.child.poll()
        error = ''
        logger = ''

        if self.error_file: error = self.error_file.read()
        if self.logger_file: logger = self.logger_file.read()

        if error: error = self.prefix+error.replace('\n','\n'+' '*len(self.prefix))
        if logger: logger = self.prefix+logger.replace('\n','\n'+' '*len(self.prefix))

        return error,logger,exitcode    

    def kill(self):
        try: self.child.kill()
        except OSError: pass
        if self.error_file: self.error_file.close()
        if self.logger_file: self.logger_file.close()

if __name__ == '__main__':

    import common.commandline
    from common.commandline import parser, get_arg_groups
    import sys
    
    parser = parser(prog="analyze.py")

    parser.add_argument('-m','--module',dest='module',required=True,help='Module containing analysis class.')
    parser.add_argument('-a','--analysis',dest='analysis',required=True,help='Name of analysis to use.')
    parser.add_argument('-p',dest='processes',default=1,type=int,help='Number of processes to use')  
    parser.add_argument('-u',dest='usage',action='store_true',help='Show usage for analysis and functions, then exit')
    
    arg_groups = get_arg_groups()

    args = parser.parse_args(arg_groups[''])

    if args.usage: common.commandline.USAGE = True

    analysis_args = arg_groups.get('analysis',[])
    
    if args.processes>1:
        while True:
            try:
                i = analysis_args.index('-p')
                j = ([index for index in range(i+1,len(analysis_args)) if analysis_args[index].startswith('-') and not analysis_args[index][1].isdigit()]+[None])[0]
                if j is not None: del analysis_args[i:j]
                else: del analysis_args[i:]
            except ValueError: break

    try:
        analysis = __import__(
            args.module,
            globals(),
            locals(),
            [args.analysis]
            ).__dict__[args.analysis]()
    except KeyError: raise ImportError('Could not import analysis "{0}" from module "{1}"'.format(args.analysis,args.module))

    if args.usage: sys.exit(1)

    analysis.setup()

    if args.processes>1:
        #start and monitor processes
        del arg_groups['']
        watchers = []
        exitcodes = []
        finished = []
        
        tempdir = tempfile.mkdtemp()
        try: 
            i = analysis_args.index('-d')
            del analysis_args[i:i+2]
        except ValueError: pass
        finally: analysis_args += ['-d',tempdir]
        for process in range(args.processes):
            try:
                i = analysis_args.index('-p')
                del analysis_args[i:i+3]
            except ValueError: pass
            finally: analysis_args += ['-p',str(process),str(args.processes)]
            run = ' - '.join(['analyze.py -m {0} -a {1}'.format(args.module,args.analysis)]+[
                ' '.join([prog]+[arg for arg in arg_groups[prog]]) for prog in arg_groups
                ])
            #print run
            watchers.append(watcher(
                '/'.join([tempdir,str(process)]),
                subprocess.Popen(run.split()),
                'Process {0}: '.format(process),
                ))
            finished.append(False)
                
        while True:
            try:
                time.sleep(1)
                for process_number,watcher in enumerate(watchers):
                    logger,error,exitcode = watcher.poll()
                    if logger: print logger.strip()
                    if error: print error.strip()
                    if exitcode is not None and not finished[process_number]:
                        if exitcode: print 'Process {0} failed'.format(process_number)
                        else: print 'Process {0} finished successfully'.format(process_number)
                        exitcodes.append(exitcode)
                        finished[process_number]=True
                if all(finished): break

            except KeyboardInterrupt:
                for watcher in watchers:
                    watcher.kill()
                raise

        if any(exitcodes) or not all(finished):
            print 'Abnormal exit in at least one process, terminating'
            sys.exit(1)
            
        for output in analysis.outputs:
            output.merge(['/'.join([tempdir,str(process)]) for process in range(args.processes)])
    #merge results
    else: 
        #run and close analysis
        analysis.run()
        analysis.close()



def run(analysis,start,end):
    pass
    
"""
    import tempfile
    import sys
    from common.functions import parser
    import argparse
    import os
    import itertools
    import textwrap

    #parser = argparse.ArgumentParser(prog='analyze.py',description='Useful caller for analyses.')
    parser = parser(
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent('\n\r-----------'),
        )
    parser.add_argument('-i','--input',default=[],dest='INPUT', nargs='+',help='Input file(s) to analyze.')
    parser.add_argument('-t','--textinput',default=None,dest='TEXTINPUT',help='Text file containing input file(s) to analyze.  Separate files by line.')
    parser.add_argument('-m','--module',dest='MODULE',required=True,help='Module containing analysis class.')
    parser.add_argument('-a','--analysis',dest='ANALYSIS',required=True,help='Name of analysis to use.')
    parser.add_argument('-o','--output',dest='OUTPUT',required=True,help='Name to give output ROOT file.')
    parser.add_argument('--entries',default=None,dest='ENTRIES',help='Number of entries to process.')   
    parser.add_argument('-n','--tree',dest='TREE',required=True,help='TTree name which contains event information.')
    #parser.add_argument('-g','--grl',default=[],dest='GRL',nargs='+',help='Good run list(s) XML file to use.')
    parser.add_argument('-p','--processes',default=1,dest='PROCESSES',type=int,help='Number of processes to use.')
    #parser.add_argument('--keep',default=False,dest='KEEP',action='store_true',help='Keep all branches, default False')

    args = []
    for i,(k,g) in enumerate(itertools.groupby(sys.argv,lambda x:x=='-')):
        g=list(g)
        args += g[1:]
        break

    help = False

    for h in ['-h','--help']:
        try: 
            args.remove(h)
            help=True
        except ValueError:
            pass

    if help: parser.print_help()

    args = parser.parse_args(args)
    #print args

import os
import sys
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
from common.pchain import generate_dictionaries
import shutil
from time import sleep, time
from distutils.dir_util import mkpath
import string
import random
from math import log
import subprocess
import stat
import atexit

def analyze(
    module_name,
    analysis_name,
    files,
    tree,
    #grl,
    num_processes,
    output,
    entries,
    #keep,
    help,
    ):

    class watcher():
        def __init__(self,directory,error,logger,child,prefix):
            self.directory = directory
            self.error = '/'.join([directory,error])
            self.logger = '/'.join([directory,logger])
            self.child = child
            self.prefix = prefix

            #attach these to indicated files
            self.error_file = None
            self.logger_file = None

        def poll(self):
            if all([
                self.error_file is None,
                os.path.exists(self.error)
                ]): self.error_file = open(self.error,'r+')
            if all([
                self.logger_file is None,
                os.path.exists(self.logger)
                ]): self.logger_file = open(self.logger,'r+')

            exitcode = self.child.poll()
            error = ''
            logger = ''

            if self.error_file: error = self.error_file.read()
            if self.logger_file: logger = self.logger_file.read()

            if error: error = self.prefix+error.replace('\n','\n'+' '*len(self.prefix))
            if logger: logger = self.prefix+logger.replace('\n','\n'+' '*len(self.prefix))

            return error,logger,exitcode    

        def kill(self):
            try: self.child.kill()
            except OSError: pass
            if self.error_file: self.error_file.close()
            if self.logger_file: self.logger_file.close()

    full_output = os.path.abspath(output)
    #print full_output
    analysis_constructor = __import__(module_name,globals(),locals(),[analysis_name]).__dict__[analysis_name]

    analysis_instance = analysis_constructor()
    if help: sys.exit(2)

    dictionary_location = generate_dictionaries()

    analysis_instance.tree = tree
    #analysis_instance.grl = grl
    analysis_instance.add_file(*files)
    analysis_instance.setup_chain()
    analysis_instance.set_output(full_output)

    directory = tempfile.mkdtemp()

    while True:
        directory = '/tmp/'+''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(10))
        try: os.mkdir(directory)
        except OSError as error:
            if error.errno != 17: raise
            continue
        break
    print 'Created temporary directory {0}'.format(directory)
    
    os.symlink(dictionary_location, directory+'/dictionaries')
    cwd = os.getcwd()
    atexit.register(os.chdir,cwd)
    atexit.register(shutil.rmtree,os.path.abspath(directory))
    os.chdir(directory)

    if entries is not None:
        entries = min([int(entries),analysis_instance.pchain.get_entries()])
    else: entries = analysis_instance.pchain.get_entries()

    outputs = analysis_instance.outputs
    print 'Creating outputs:{0}'.format([outputs.name for output in outputs])
    #del analysis_instance

    ranges = [[i*(entries/num_processes),(i+1)*(entries/num_processes)] for i in range(num_processes)]
    ranges[-1][-1]+= entries%(num_processes)

    files_text = 'files.txt'
    with open(files_text,'w') as f:
        for file_ in files:
            #print file_
            f.write(file_+'\n')

    #Start children
    print 'Processing {0} entries with {1} processes'.format(entries,num_processes)

    args = []

    for i,(k,g) in enumerate(itertools.groupby(sys.argv,lambda x:x=='-')):
        if not i: continue
        g=list(g)
        args += g[:]

    watchers = []
    
    directories = []
    
    for process_number in range(num_processes):
        if num_processes>1: suffix = '{0:0>{1}}'.format(process_number,int(log(num_processes-1,10))+1)
        else: suffix = ''
        
        directory = os.path.dirname(suffix+'/.')
        directories.append(directory)
        os.mkdir(directory)
    	os.symlink(dictionary_location, directory+'/dictionaries')
    	os.symlink(cwd+'/'+files_text, directory+'/'+files_text)
        start,end = ranges[process_number]
        output = 'result.root'.format(suffix)
        error = 'error.out'.format(suffix)
        logger = 'logger.out'.format(suffix)

        child_call = 'analyze_singlet.py -a {analysis_name} -m {module_name} -n {tree} -s {start} -e {end} -t {files_text} -d {directory} -o {output} -z {error} -l {logger} {args}'.format(
            analysis_name = analysis_name,
            module_name = module_name,
            tree = tree,
            start = start,
            end = end,
            files_text = files_text,
            output = output,
            directory = suffix,
            error = error,
            logger = logger,
            args = ' '.join(args),
            )
        #print child_call
        watchers.append(watcher(suffix,error,logger,subprocess.Popen(child_call.split()),'Process {0}: '.format(process_number)))

    #Monitor
    #results = []
    exitcodes = []
    finished = [False for i in range(num_processes)]
    
    while True:
        try:
            sleep(1)
            for process_number,watcher in enumerate(watchers):
                logger,error,exitcode = watcher.poll()
                if logger: print logger.strip()
                if error: print error.strip()
                if exitcode is not None and not finished[process_number]:
                    if exitcode: print 'Process {0} failed'.format(process_number)
                    else: print 'Process {0} finished successfully'.format(process_number)
                    exitcodes.append(exitcode)
                    #results.append(watcher.result)
                    finished[process_number]=True
            if all(finished): break

        except KeyboardInterrupt:
            for watcher in watchers:
                watcher.kill()
            raise
            #break

    if any(exitcodes) or not all(finished):
        print 'Abnormal exit in at least one process, terminating'
        sys.exit(1)

    for watcher in watchers: watcher.kill()

    os.chdir(cwd)
    mkpath(os.path.dirname(full_output))
    os.chdir(os.path.dirname(full_output))
    
    for output in outputs:
        #print output.name
    	output.merge(directories)
    

if __name__ == '__main__':

    files = []
    
    if args.INPUT:
        files += [file_ if ':' in file_ else os.path.abspath(file_) for file_ in args.INPUT]
    
    if args.TEXTINPUT:
        with open(args.TEXTINPUT) as f:
            for line in f.readlines():
                if not line.strip(): continue
                file_ = line.strip()
                files.append(file_ if ':' in file_ else os.path.abspath(file_))

    if not files:
        print 'No input found, exiting'
        sys.exit(1)
        #exit = True

    analyze(
        args.MODULE,
        args.ANALYSIS,
        files,
        args.TREE,
        #args.GRL,
        args.PROCESSES,
        args.OUTPUT,
        args.ENTRIES,
        #args.KEEP,
        help,
        )
"""
