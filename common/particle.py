import ROOT
from copy import deepcopy

#------------------------------------------------------------------------------------------------------------------------------

class particle():
	def __init__(self,**kwargs):
		self.__properties__ = {}
		self.__properties__.update(kwargs)
		self.__setstate__(kwargs)

	def set_particle(self,__particle__):
		if not isinstance(__particle__,ROOT.TLorentzVector):
			raise ValueError('particle must be of type TLorentzVector')
		self.__particle__ = __particle__

	def create_particle(self):
		if all([name in self.__properties__ for name in ['pt','eta','phi','e']]): self.set_pt_eta_phi_e(*[self.__properties__[name] for name in ['pt','eta','phi','e']])
		elif all([name in self.__properties__ for name in ['pt','eta','phi','m']]): self.set_pt_eta_phi_m(*[self.__properties__[name] for name in ['pt','eta','phi','m']])
		elif all([name in self.__properties__ for name in ['px','py','pz','e']]): self.set_px_py_pz_e(*[self.__properties__[name] for name in ['px','py','pz','e']])
		else: raise ValueError('particle could not be infered from properties')

	def set_pt_eta_phi_e(self,pt,eta,phi,e):
		if '__particle__' in self.__dict__: self().SetPtEtaPhiE(pt,eta,phi,e)
		else:
			__particle__ = ROOT.TLorentzVector()
			__particle__.SetPtEtaPhiE(pt,eta,phi,e)
			self.set_particle(__particle__)

	def set_pt_eta_phi_m(self,pt,eta,phi,m):
		if '__particle__' in self.__dict__: self().SetPtEtaPhiM(pt,eta,phi,m)
		else:
			__particle__ = ROOT.TLorentzVector()
			__particle__.SetPtEtaPhiM(pt,eta,phi,m)
			self.set_particle(__particle__)

	def set_px_py_pz_e(self,px,py,pz,e):
		if '__particle__' in self.__dict__: self().SetPxPyPzE(px,py,pz,e)
		else:
			__particle__ = ROOT.TLorentzVector()
			__particle__.SetPxPyPzE(px,py,pz,e)
			self.set_particle(__particle__)	

	def __setstate__(self,state):
		self.__dict__.update(state)

	def __getstate__(self):
		return deepcopy(self.__dict__)

	def __copy__(self):
		new_particle = particle()
		new_particle.__setstate__(self.__getstate__())
		return new_particle

	def __call__(self):
		if '__particle__' not in self.__dict__:
			try: self.create_particle()
			except ValueError:
				raise ValueError('particle not set')
		return self.__particle__
