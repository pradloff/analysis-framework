import ROOT
import os

#Services located in external/Root with includes in separate service folder

class externalServiceManager():
	def __init__(self,):
		ROOT.gROOT.ProcessLine('.include {0}/external'.format(os.path.abspath(os.getcwd())))
	def loadExternal(self,cxx,services,namespaces):
		ROOT.gROOT.ProcessLine('.L external/Root/{cxx}'.format(cxx=cxx))
		services_ = dict((name,getattr(ROOT,name)) for name in services)
		namespaces_ = dict((name,getattr(ROOT,name)) for name in namespaces)
		return services_,namespaces_
