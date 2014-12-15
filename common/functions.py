"""
class parser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write('error: {0}\n'.format(message))
        self.print_help()
        sys.exit(2)

class InstantiationError(TypeError):

    def __init__(self, cls, args, kwargs):
        self.msg = 'Failed to initialize {_class}({args}{kwargs})'.format(
            _class = cls.__name__,
            args = ','.join(str(arg) for arg in args),
            kwargs=','.join(str(key)+'='+str(value) for key,value in kwargs.items())
            )

    def __repr__(self):
        return str(self)

    def __str__(self):
        return 'InstantiationError: '+self.msg
"""
class EventBreak(Exception):
    pass
"""
class arg():
    def __init__(self, default, required=False, help=''):
        self.value = default
        self.required = required
        self.type = default if inspect.isclass(default) else type(default)
        self.help=help

    def __call__(self):
        return self.value


#special init for functions to defer their actual instantiation and parse cla
def __init__(self,*args,**kwargs):
    self.__args = args
    self.__kwargs = kwargs

    dyn_parser = parser(
        prog=self.__class__.__name__,
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent('\n\r-----------'),
        )

    argspec = inspect.getargspec(self.__deferred_init__)

    arg_dict = {}
    if argspec.defaults is not None:
        for arg_name,arg_value in [(_arg_name,_arg_value) for _arg_name,_arg_value in zip(argspec.args[-len(argspec.defaults):],argspec.defaults) if isinstance(_arg_value,arg)]:
            arg_dict[arg_name] = arg_value

    if arg_dict:
        cla={}    
        for arg_name,arg_value in arg_dict.items():
            if arg_name in kwargs: arg_value.value = kwargs[arg_name]
            cla[arg_name]=arg_value
            dyn_parser.add_argument(
                '--'+arg_name,
                required=arg_value.required,
                default=arg_value.value,
                dest=arg_name,
                help=arg_value.help,
                type=arg_value.type,
                )

        args = []
        help = False
        for i,(k,g) in enumerate(itertools.groupby(sys.argv,lambda x:x=='-')):
            g=list(g)
            if all([
                not i,
                any([
                    '-h' in g,
                    '--help' in g
                    ]),
                ]): help = True
            if k: continue
            if g[0]!=self.__class__.__name__: continue
            args += g[1:]
            
        for arg_name,arg_value in arg_dict.items():
        	self.__dict__['__kwargs'][arg_name] = arg_value.value

        if help: dyn_parser.print_help()

        else:
            for kw,value in dyn_parser.parse_args(args).__dict__.items():
                self.__dict__['__kwargs'][kw] = value

from common.base import base

class function_meta(type):
    def __init__(cls, name, bases, dct):
        if base not in bases:
            cls.__deferred_init__=cls.__init__
            cls.__init__=__init__
            #print dct
        super(function_meta,cls).__init__(name, bases, dct)
"""
import cPickle
import code

def memoize(func):
    cache = {}
    def wrapper(*args,**kwargs):
        key = cPickle.dumps((args,kwargs))
        if key not in cache:
            result = func(*args,**kwargs)
            cache[key] = result
        return cache[key]
    return wrapper

class memo_meta(type):
    @memoize
    def __call__(cls,*args,**kwargs):
        return super(memo_meta,cls).__call__(*args,**kwargs)

class output_base(object):
    __metaclass__ = memo_meta
    def __init__(self,directory,name):
        self.directory = directory
        self.name = name
        self.path = '/'.join([directory,name])
    def setup(self,directory,stream): pass

class function(object):
    @property
    def analysis(self):
        if hasattr(self,'_analysis'): return self._analysis
        raise RuntimeError('Analysis not set for function {0}'.format(self))

    @analysis.setter
    def analysis(self,analysis):
        self._analysis = analysis

#class function(object):
#	pass
"""
class function(object):
    @property
    def analysis(self):
    	if hasattr(self,'_analysis'): return self._analysis
    	raise RuntimeError('Analysis not set for function {0}'.format(self))
        
	#@analysis.setter
	#def analysis(self,analysis): self._analysis = analysis
"""    
class event_function(function):
    def __init__(self):
        super(event_function,self).__init__()
        self.branches = []
        self.break_exceptions = []

    def setup(self): pass

    def request_branches(self):
        #_branches = []
        _read_branches = []
        for branch in self.branches:
            if 'r' in branch.mode:
                try:
                    #replace stub branch with readable branch
                    branch = self.analysis.pchain.request_branch(branch)
                    _read_branches.append(branch)
                except AttributeError as e:
                    if not 'u' in mode: raise
                    branch.mode = mode.replace('r','')
                    if 'k'  in mode: branch.mode = branch.mode.replace('k','')
       	#self.branches = _branches #all branches
       	self.read_branches = _read_branches #open for reading branches
            
    def __call__(self,event):
        for branch in self.read_branches:
            branch.update(event.__entry__)
            event.__dict__[branch.name] = branch.payload
        #self.analyis.pchain.get_branches(event,[branch for branch in self.branches if 'r' in branch.mode])

class result_function(function):
    def __init__(self):
        super(result_function,self).__init__()
        self.outputs = []
    def setup(self): pass
    #def setup(self):
    #   for output in self.outputs: output.setup()
    #def setup_output(self): 
    #    for output in self.outputs: output.setup(self.analysis.dir,self.analysis.stream)
    def __call__(self,event): pass
    
class meta_result_function(function):
    def setup(self): pass
    def __call__(self,files): pass
