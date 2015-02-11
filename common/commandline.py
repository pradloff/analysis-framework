import inspect
import argparse
import sys
import itertools
from common.functions import memoize
import code

USAGE=False

@memoize
def get_arg_groups():
    arg_groups = {}
    for i,(k,g) in enumerate(itertools.groupby(sys.argv,lambda x:x==' - ')):
        if k: continue
        g=list(g)
        if i: name = g[0]
        else: name = ''
        if name in arg_groups: arg_groups[name] += g[1:]
        else: arg_groups[name] = g[1:]
    return arg_groups

class parser(argparse.ArgumentParser):
    def __init__(self,**kwargs):
        kwargs['formatter_class']=argparse.ArgumentDefaultsHelpFormatter
        super(parser,self).__init__(
            **kwargs
            )
    def error(self,message):
        sys.stderr.write('error: {0}\n'.format(message))
        self.print_help()
        sys.exit(2)

class CommandLineError(TypeError): pass

class arg():
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs

class commandline(object):

    def __init__(self,identifier,**commandline_args):
        self.identifier = identifier
        self.commandline_args = commandline_args

    def get_args(self):
        try: self.args = get_arg_groups()[self.identifier]
        except KeyError: self.args = ''
        
    def generate_parser(self):
        self.parser = parser(prog=self.identifier)
        for arg_name,arg in self.commandline_args.items():
            #print arg_name
            arg.kwargs['dest'] = arg_name
            self.parser.add_argument(*arg.args,**arg.kwargs)
        #self.parser.add_argument('--usage',action=usage)
    def setup_defaults(self,f,kwargs):
        argspec = inspect.getargspec(f)
        #print argspec
        
        if argspec.defaults is not None: kwargs_spec = dict((arg_name,arg_value) for arg_name,arg_value in zip(argspec.args[-len(argspec.defaults):],argspec.defaults))
        else: kwargs_spec = dict()

        for arg_name in kwargs_spec:
            if arg_name not in kwargs: kwargs[arg_name]=kwargs_spec[arg_name]

        for arg_name in self.commandline_args:
            if arg_name not in kwargs_spec: raise CommandLineError('"{0}" not a kwarg of function {1}'.format(arg_name,f))
            self.commandline_args[arg_name].kwargs['default']=kwargs.get(arg_name,kwargs_spec[arg_name]) 

    def update_args(self,kwargs):
        parsed_args = self.parser.parse_args(self.args).__dict__
        #print parsed_args
        for arg_name in kwargs:
            if arg_name not in parsed_args: continue
            #print arg_name,parsed_args[arg_name]
            kwargs[arg_name] = parsed_args[arg_name]
        #for arg_name,value in self.parser.parse_args(self.args).__dict__.items():
        #    kwargs[arg_name]=value

    def __call__(self,f,*args,**kwargs):
        def wrap(*args,**kwargs):
            self.setup_defaults(f,kwargs)
            self.generate_parser()
            self.get_args()
            self.update_args(kwargs)
            if USAGE: 
                self.parser.print_help()
            return f(*args,**kwargs)
        return wrap

"""
example:

@commandline(
    'x',
    #a=arg(action='store_false',help='help'),
    b=arg(type=int,help='help'),
    c=arg(action='store_true',help='help'),
    )
class x(object):

    def __init__(self,a,b=2,c=False):
        self.a = a
        self.b = b
        self.c = c



a = x(1,b=10)
print a.a,a.b,a.c

"""
