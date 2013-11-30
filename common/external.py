import ROOT
import os
import subprocess

def check_output(*popenargs, **kwargs):
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output

try: subprocess.check_output
except: subprocess.check_output = check_output

def make_default(package,overwrite=False):
	if os.path.exists('Makefile.RootCore') and not overwrite: return
	with open('Makefile.RootCore','w') as f:
		for line in [
			'PACKAGE          = {0}'.format(package),
			'include $(ROOTCOREDIR)/external/Makefile-common'
			]: f.write(line+'\n')

def call(command,args='',verbose=False):
	try: result = subprocess.check_output([command, args],stderr=subprocess.STDOUT,shell=True)
	except subprocess.CalledProcessError as error:
		if verbose: print error.output
		raise error
	return result

def include(package,files):
	cwd = os.getcwd()
	home = os.getenv('ANALYSISHOME')
	packagePath = '{0}/external/{1}'.format(home,package)
	if not os.path.exists(packagePath):
		os.chdir(cwd)
		raise OSError('package {0} not found in $ANALYSISHOME/external'.format(package))
	os.chdir(packagePath)
	for file_ in files:
		if not os.path.exists(file_):
			raise OSError('file not found: $ANALYSISHOME/{0}/{1}'.format(package,file_))
		ROOT.gROOT.ProcessLine('.L {0}'.format(file_))
	
def load(package,prerequesites=None,verbose=False,clean=False,overwrite=False):
	if not prerequesites: prerequesites = []
	for prerequesite in prerequesites: self.Load(prerequesite)
	if verbose == True: print 'Loading {0}'.format(package)
	cwd = os.getcwd()
	home = os.getenv('ANALYSISHOME')
	packagePath = '{0}/external/{1}'.format(home,package)

	if not os.path.exists(packagePath):
		os.chdir(cwd)
		raise OSError('package {0} not found in $ANALYSISHOME/external'.format(package))

	cmtPath = packagePath+'/cmt'
	if not os.path.exists(cmtPath):
		os.chdir(cwd)
		raise OSError('cmt directory not found in $ANALYSISHOME/external/{0}'.format(package))
	os.chdir(cmtPath)
	
	make_default(package,overwrite=overwrite)
	if clean: call('make -f Makefile.RootCore clean')
	result = call('make -f Makefile.RootCore')
	
	standalonePath = packagePath+'/StandAlone'
	if not os.path.exists(standalonePath):
		os.chdir(cwd)
		raise OSError('StandAlone directory not found in $ANALYSISHOME/external/{0}'.format(package))

	os.chdir(standalonePath)
	ROOT.gSystem.Load(os.path.abspath('lib{0}.so'.format(package)))
	os.chdir(cwd)
