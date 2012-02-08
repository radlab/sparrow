#!/bin/sh
cd "`dirname $0`"
PYTHONPATH="$PYTHONPATH:third_party" python parse_logs.py $@

