#!/usr/bin/python3.6

'''
Hello, I am the sponsor report printer

This script makes use of the ops_print_routes
Service_Database_Manager object to interface with 
the route/service database and the database of sa
gift appointment time slots and date time strings

It iterates throught the service database and instantiates
Delivery_Household_Collections

it then iterates through those objects to add entries
to a report file household by household

because salvation army may have blank appiontment slots
for the sa report some logic is needed to insert blank entries
so that the report product can accomodate
last minute pickups that are scheduled (or rescheduled) in
it is important to note that if there are appointment numbers
outside of the established range.  They will appear at the start
of the file with False in the App # and Time cells

'''


import sqlite3
import sys
from collections import namedtuple
from collections import defaultdict
import logging
from datetime import datetime

from r_config import configuration
from file_iface import Menu

from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection

from sponsor_reports import Report_File

# objects
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
ops_logger.info('Printing Sponsor Reports')

def get_hh_from_sponsor_table(database,criteria=None):
    
    rdbm = Service_Database_Manager.get_service_db()
    rdbm.initialize_connections()

    dhc = Delivery_Household_Collection()

    sponsor_families = rdbm.return_sponsor_hh('rdb',crit=criteria)
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
    
    return dhc, rdbm

def write_sponsor_reports(delivery_households, r_dbs):
    '''
    iterates through delivery_households which is a
    Delivery_Household_Collection

    and databases which is the Service_Database_Manager object
    instantiated by the get_hh_from_sponsor_table function

    the database is needed to pull out day time strings for the 
    empty appointment slots so blank entry templates will be inserted
    in sequence when the sa report is printed.

    '''
    dbs = r_dbs
    b_now = f'{datetime.now()}'[:16] # slice to the time but not to the ms
    b_head = f'{outputs}{session}'

    s_report = {'Salvation Army': Report_File(f'{b_head}_Salvation Army_{b_now}.xlsx'),
                'DOON': Report_File(f'{b_head}_DOON_{b_now}.xlsx'),
                'Sertoma': Report_File(f'{b_head}_Sertoma_{b_now}.xlsx'),
                'REITZEL': Report_File(f'{b_head}_REITZEL_{b_now}.xlsx')}

    running_sa_count = 0 # used to keep track of the current sa appointment
                         # this and the offset that the current sa app
                         # are used to keep track of how many blank app
                         # templates to write into the final report
    null_a = ('' for x in range(13))
    null_fam = ((None, None, None, None, None, None, None, None, None, None),)

    visit_sum = namedtuple('visit_sum', 'applicant, fname, lname, size, phone, email,\
                               address, address2, city, postal, diet, sa_app_num, \
                           sms_target')
    null_app = visit_sum(*null_a)

    for house in delivery_households.army_iter():
        rt = house.return_route()
        fid, rn, rl, nhood =  rt
        summ = house.return_summary() # the HH info (name, address etc.)
        fam = house.family_members    
        
        sap, fs, gs = house.return_sponsor_package()
        saa, sat = house.get_sa_day_time() # application # application time
        spg = [fs, gs]
        
        groups = [x for x in spg if len(x) > 2]

        ops_logger.debug(f'operating on {fid} SAP = {sap}, fs = {fs} gs = {gs} saa = {saa} sat = {sat}')
        
        for g in groups: 
            age_cut = {'Salvation Army': 16, 
                   'DOON': 18,
                   'Sertoma': 12,
                   'REITZEL': 18}
            aco = age_cut.get(g, 18)
            if not g == 'Salvation Army':
                s_report[g].add_household(summ, fam, age_cutoff=aco) # sponsor report
            else: 
                # this logic is for determining if there is a need to 
                # print a blank entry for the SA to keep the spacing 
                # and order of appointments
                if running_sa_count == 0:
                    ops_logger.debug(f'running count = 0')
                    ops_logger.debug(f'saa = {saa} sap = {sap}')
                    running_sa_count = sap 
                    s_report[g].add_household(summ, fam, age_cutoff=15,
                                              app_pack=(saa, sat))

                else:
                    dif_count = sap - running_sa_count
                    ops_logger.debug(f'dif_count= {dif_count}')
                    ops_logger.debug(f'saa = {saa} sap = {sap}')
                    if dif_count == 1:
                        s_report[g].add_household(summ, fam, age_cutoff=15,
                                              app_pack=(saa, sat))
                        running_sa_count += 1
                    else:
                        app_num = running_sa_count
                        for x in range(1, dif_count):
                            blank_num = app_num + x
                            blank_time =  dbs.return_sa_app_time('sa',
                                                                 blank_num)
                            s_report[g].add_household(null_app,
                                                    null_fam, 
                                                    age_cutoff=15,
                                                    app_pack =  (blank_num,
                                                              blank_time))
                        
                        s_report[g].add_household(summ, fam, age_cutoff=15,
                                              app_pack=(saa, sat))
                        running_sa_count = sap

    for x in s_report.keys():
        s_report[x].close_worksheet()
    dbs.close_all()


sponsor_date = None
menu = Menu()

input_all = input('do you want to print all? Enter y or n  ')
if input_all == 'y':
    print('printing all of them!')
elif input_all == 'n':
    print('enter a date YYYY-MM-DD')
    sponsor_date = menu.prompt_input('sponsor')
else:
    print('invalid input!')
    sys.exit(1)


route_database = Route_Database(f'{db_src}{session}rdb.db')
dh, dbs = get_hh_from_sponsor_table(route_database, criteria=sponsor_date)
write_sponsor_reports(dh,dbs)
