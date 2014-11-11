import ROOT
from copy import copy
import inspect
import argparse
import sys
import textwrap
import itertools
import types

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

class EventBreak(Exception):
    pass

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

import cPickle
import code

def memoize(func):
    cache = {}
    def wrapper(*args,**kwargs):
        key = cPickle.dumps((args,kwargs))
        if key not in cache:
            #print 'storing'
            result = func(*args,**kwargs)
            cache[key] = result
        return cache[key]
    return wrapper

class memo_meta(type):
    @memoize
    def __call__(cls,*args,**kwargs):
        return super(memo_meta,cls).__call__(*args,**kwargs)

class output_base(base):
    __metaclass__ = memo_meta
    def __init__(self,file_name):
        self.file_name = file_name
        self.open()
    def open(self): raise NotImplementedError
    def merge(self,directories): raise NotImplementedError
    def close(self): raise NotImplementedError

#from common.analysis import AnalysisUnlocked

class function(base):
    __metaclass__ = function_meta
    def __init__(self): pass
    def __getattribute__(self,attr):
        if attr not in [
        	'__dict__',
            '__deferred_init__',
            '__class__',
            ]:
            setattr(self.__class__, '__getattribute__', types.MethodType(object.__getattribute__,self))
            self.__deferred_init__(*self.__dict__['__args'],**self.__dict__['__kwargs'])

        return super(function, self).__getattribute__(attr)
        
    def set_analysis(self,analysis):
        self.analysis = analysis
    def get_analysis(self):
        analysis = getattr(self,'analysis',None)
        if analysis is None: raise AnalysisUnlocked()
        
class event_function(function,base):
    def __init__(self):
        self.required_branches = []
        self.keep_branches = []
        self.create_branches = {}
        self.break_exceptions = []
        
    def __call__(self,event):
        return

class result_function(function,base):
    def __init__(self,output):
        #self.results = {}
        self.output = output
        
    def __call__(self,event):
        return

class meta_result_function(function,base):
    def __init__(self,output):
        #self.results = {}
        self.output = output

    def __call__(self,files):
        return
