#!/bin/bash
# shell script to export sqlite3 data into a csv
#https://stackoverflow.com/questions/31175018/run-command-line-sqlite3-query-and-exit
#

SOURCEDIR="databases/"
DESTDIR="products/"
INNAME="2020_caseload_to_4_14.db"
OUTNAME=export-$(date +%Y%m%d)-$(date +%H%M%S).csv

OUTFORMAT=".mode csv"
HEADERS=".headers on"
SEPARATOR=".separator ,"
OUTPUT=".output $OUTNAME"
QUERY="SELECT * FROM Visit_Table;"

sqlite3 $INNAME "$HEADERS" "$SEPARATOR" "$OUTFORMAT" "$OUTPUT" "$QUERY" ".exit" 
