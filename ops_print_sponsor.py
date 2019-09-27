#!/usr/bin/python3.6

'''
Hello.  I am the deliverator.  I am here to create routes!
this script will run through a l2f export file that has been cleaned up
after iterative address fixing passes and quality control via the
address_parser... script and careful digesting of logs.

This script open the csv and strips out the households line by line
It attempts to gather key bits of information such as is it the right
kind of visit?  Is a service being provided by another party? Has it been
routed previously and thus, should be ignored?

If it passes these tests it is added to a Delivery_Household_Collection
or equivalent data type and then it...
iterate through the collection of households to create routes
log the routes in the database in a separate table

the database will then yeild the materials necessary to make the route cards
and should be more portable
sponsor report files are also created out of the appropriate datatypes
everyone wins!

It is also a complete mess and needs some refactoring love

'''


import sqlite3
from collections import namedtuple
from collections import defaultdict
import logging
from datetime import datetime

from r_config import configuration

from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes
from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection
'''
from address_parser_and_geocoder import Coordinates
from address_parser_and_geocoder import SQLdatabase
from address_parser_and_geocoder import AddressParser

from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser
from db_data_models import Person
from db_parse_functions import itr_joiner

from kw_neighbourhoods import Neighbourhoods
'''
from delivery_card_creator import Delivery_Slips
from delivery_binder import Binder_Sheet
from delivery_binder import Office_Sheet

from sponsor_reports import Report_File

# objects
from ops_print_routes import Service_Database
from ops_print_routes import Service_Database_Manager
# functions
from ops_print_routes import package_applicant





# INITIALIZE CONFIGURATION FILE
conf = configuration.return_r_config()
target = conf.get_target # source file
db_src, _, outputs = conf.get_folders()
_, session = conf.get_meta()

# NAMED TUPLES
Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

# LOGGING
ops_logger = logging.getLogger('ops')
ops_logger.setLevel(logging.INFO)
ops_logger_formatter = logging.Formatter('%(message)s')
ops_log_file_handler = logging.FileHandler('Logging/ops.log')
ops_log_file_handler.setFormatter(ops_logger_formatter)
ops_logger.addHandler(ops_log_file_handler)
ops_logger.info('Running new session {}'.format(datetime.now()))

def get_hh_from_sponsor_table(database):
    
    rdbm = Service_Database_Manager.get_service_db()
    rdbm.initialize_connections()

    dhc = Delivery_Household_Collection()

    sponsor_families = rdbm.return_sponsor_hh('rdb')
    # tuples of (fid, food sponsor, gift_sponsor)
    for sfam in sponsor_families:
        fid, f_sponsor, g_sponsor, sort_date = sfam
        main_applicant = rdbm.get_main_applicant('rdb', fid)[0]
        gan, gat = rdbm.return_sa_info_pack('rdb', 'sa', fid)
        
        hh_package, _ = package_applicant(main_applicant, gan, None, None)
        dhc.add_household(*hh_package)
        dhc.add_sponsors(fid, f_sponsor, g_sponsor)
    
        fam = rdbm.get_family_members('rdb', fid)
        if fam:
            dhc.add_hh_family(fid, fam)
        if all((gan, gat)):
            dhc.add_sa_app_number(fid, gan)
            dhc.add_sa_app_time(fid, gat)

    return dhc

def write_sponsor_reports(delivery_households):

    b_now = f'{datetime.now()}'
    b_head = f'{outputs}{session}'

    s_report = {'Salvation Army': Report_File(f'{b_head}_Salvation Army.xlsx'),
                'DOON': Report_File(f'{b_head}_DOON.xlsx'),
                'Sertoma': Report_File(f'{b_head}_Sertoma.xlsx'),
                'REITZEL': Report_File(f'{b_head}_REITZEL.xlsx')}

    for house in delivery_households:
        rt = house.return_route()
        fid, rn, rl, nhood =  rt
        summ = house.return_summary() # the HH info (name, address etc.)
        fam = house.family_members    
        
        sap, fs, gs = house.return_sponsor_package()
        spg = [fs, gs]
        
        groups = [x for x in spg if len(x) > 2]


        for g in groups: 
            age_cut = {'Salvation Army': 16, 
                   'DOON': 18,
                   'Sertoma': 12,
                   'REITZEL': 18}
            aco = age_cut.get(g, 18)
            s_report[g].add_household(summ, fam, age_cutoff=aco) # sponsor report
    for x in s_report.keys():
        s_report[x].close_worksheet()



route_database = Route_Database(f'{db_src}{session}rdb.db')
dh = get_hh_from_sponsor_table(route_database)
write_sponsor_reports(dh)
