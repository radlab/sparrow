#!/bin/sh
cd "`dirname $0`"
PYTHONPATH="$PYTHONPATH:../third_party/boto-2.1.1" python ./probe_ratio.py $@
