import sqlite3
from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes


'''
Hello.  I am the deliverator.  I am here to create routes
this script will run through a l2f export file that has been cleaned up
after iterative address fixing passes and quality control via the
address_parser... script and careful digesting of logs.
It will find the geocoordinates of each hh and create a datastructure of 
Delivery_Household's to feed to the Delivery_Routes class for routing

TO DO: refactor basket_sorting... to separate out the log to db stuff
into a separte route db class
also add an iter method to the Delivery_Routes Class so we can cycle through
it and drop the routes into a DB
'''
