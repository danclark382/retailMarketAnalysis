#!bin/bash

cd ~/projects/python/retailMarketAnalysis;
conda init --all --dry-run --verbose;
python submission_script.py
return_code=$?
i=0
while [ $return_code -ne 1 ]; do
     i=$[$i+1]
     echo "Run Number: $i"
     python submission_script.py
done
exit;