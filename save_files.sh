#!/bin/bash

# This is a utlity script that tars the key files for later reference
# and zips the files that I want to download for maniplulation on 
# my desktop

DESTDIR="backups/"

FILENAME=files-$(date +%Y%m%d)-$(date +%H%M%S).tar
TOGO=package-$(date +%Y%m%d)-$(date +%H%M%S).zip
YEAR=$(date +%Y)

tar -czvf $DESTDIR$FILENAME *.csv *.db *.xlsx
zip $TOGO *.xlsx *.db 
