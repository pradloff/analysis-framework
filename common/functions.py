import ROOT
from copy import copy
import inspect

class event_function():
	def __init__(self):
		self.required_branches = []
		self.keep_branches = []
		self.create_branches = {}
	
	def __call__(self,event):
		return

class result_function():
	def __init__(self):
		self.results = {}
	
	def __call__(self,event):
		return

class meta_result_function():
	def __init__(self):
		self.results = {}

	def __call__(self,files):
		return
