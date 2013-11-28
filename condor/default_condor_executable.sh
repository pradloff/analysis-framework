#!/bin/bash

echo "Sourcing bash environment"
source /export/home/pradloff/.bashrc
shopt -s expand_aliases
echo "Setting up ATLAS environment"
setupATLAS > /dev/null
echo "Setting python"
localSetupPython > /dev/null
echo "Setting ROOT"
localSetupROOT > /dev/null
echo "Setting up analysis tools"
source {analysis_framework}/setup.sh
source {analysis_home}/setup.sh
echo "Running analysis slice"
python -c "from common.analysis import analyze_slice_condor; analyze_slice_condor('{module_name}','{analysis_name}','{tree}',{grl},'{files}',{keep},{start},{end},'{output_name}',{process_number},'{error_file_name}','{logger_file_name}')"
touch done

