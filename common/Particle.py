import ROOT
from copy import copy

#------------------------------------------------------------------------------------------------------------------------------

class Particle():

	def __getattr__(self,name):
		if not (name.startswith('__') and name.endswith('__')):
			try: return getattr(self.__particle__,name)
			except AttributeError: pass
		try: return self.__dict__[name]
		except KeyError: raise AttributeError
	
	def __init__(self,**kwargs):
		self.__properties__ = {}
		self.__properties__.update(kwargs)
		self.__setstate__(kwargs)

	def createParticle(self):
		self.__particle__ = ROOT.TLorentzVector()
	
		if None not in [self.__dict__.get('pt'),self.__dict__.get('eta'),self.__dict__.get('phi'),self.__dict__.get('m')]:
			self.SetPtEtaPhiM( self.pt,self.eta,self.phi,self.m )
		
		elif None not in [self.__dict__.get('pt'),self.__dict__.get('eta'),self.__dict__.get('phi'),self.__dict__.get('e')]:
			self.SetPtEtaPhiE( self.pt,self.eta,self.phi,self.e )
		
		elif None not in [self.__dict__.get('px'),self.__dict__.get('py'),self.__dict__.get('pz'),self.__dict__.get('e')]:
			self.SetPxPyPzE( self.px,self.py,self.pz,self.e )
		
	def __setstate__(self,state):
		self.__dict__.update(state)
		self.createParticle()
		
	def __add__(self,otherParticle):
		return self.__particle__ + otherParticle.__particle__

	
	def __getstate__(self):
		return copy(self.__dict__)
		
	def __str__(self):
		try: return str(self.pdgId)
		except: return 'derrr'


	def __copy__(self):
		newParticle = Particle()
		newParticle.__setstate__(self.__getstate__())
		return newParticle
