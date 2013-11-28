#Patches stdout to pipe to a Queue which will handle logging
class logpatch():
	def __init__(self,queue,prefix,suffix):
		self.queue=queue
		self.prefix = prefix
		self.suffix = suffix
		self.buffer = ''

	def flush(self):
		pass

	def write(self,log):
		if log != '\n':
			self.buffer += log
		else:
			self.queue.put(self.prefix+self.buffer.replace('\n','\n'+' '*len(self.prefix)))
			self.buffer = ''

#Patches stdout to a file
class logpatch_file():
	def __init__(self,output,prefix='',suffix=''):
		self.output = output
		self.prefix = prefix
		self.suffix = suffix
		self.buffer = ''

	def flush(self):
		self.output.flush()

	def close(self):
		self.output.flush()
		self.output.close()

	def write(self,log):
		if log != '\n':
			self.buffer += log
		else:
			self.output.write(self.prefix+self.buffer.replace('\n','\n'+' '*len(self.prefix))+'\n')
			self.flush()
			self.buffer = ''
