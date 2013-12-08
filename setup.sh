DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

unset TMP
unset TMPDIR

export PATH=$DIR/bin:$PATH
export PYTHONPATH=$DIR/python:$PYTHONPATH
export PYTHONPATH=$DIR:$PYTHONPATH
export ANALYSISFRAMEWORK=$DIR
