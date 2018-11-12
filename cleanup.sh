#!/bin/bash

# this will cleanup the output of the python scripts 
# and sends all error messages to dev/null 

{
rm *.txt
rm *.xlsx
rm *.zip
} &> /dev/null

echo "All done - txt and xlsx files deleted"
