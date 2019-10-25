#!/usr/bin/python3.6

'''
this script processess a l2f export pointed to in the
setup yaml file, or via the command line
it identifies service requests, sorts the appropriate HH's 
into routes and logs all routes and service requests
into the route database

it presumes that the file has been fed into the 
address parsing/geocoding script, the address
for each HH is in the correct format and exists in the 
address database.  If this script cannot resolve
the address properly, the HH will not be registered
for services/routed

The primary objects this script deals with are 
Delivery_Household
Delivery_Routes
Route_Database
Delivery_Household_Collection

It encapulsates the process into a series of function calls
that modify where appropriate Delivery_Household's via
the Delivery_Household_Collection
and Manages Database insertions via the Route_Database object

The function calls go in this sequence

parse_and_sort_file(export_file, address_dbase, k_w, delivery_households)
sort_routes(route_database, delivery_households, routes)
log_routes_to_database(route_database, delivery_households)


'''
import sqlite3
import argparse
import logging
from collections import namedtuple
from datetime import datetime
import timeit
import sys

from r_config import configuration

from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes
from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection

from address_parser_and_geocoder import SQLdatabase
from address_parser_and_geocoder import AddressParser

from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser
from db_data_models import Person
from db_parse_functions import itr_joiner

from kw_neighbourhoods import Neighbourhoods

from delivery_card_creator import Delivery_Slips
from delivery_binder import Binder_Sheet
from delivery_binder import Office_Sheet

from sponsor_reports import Report_File

from file_iface import Menu



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

nr_logger = logging.getLogger('nr') # not routed
nr_logger.setLevel(logging.INFO)
nr_logger_formatter = logging.Formatter('%(message)s')
nr_log_file_handler = logging.FileHandler('Logging/nr.log')
nr_log_file_handler.setFormatter(ops_logger_formatter)
nr_logger.addHandler(nr_log_file_handler)
nr_logger.info('Running new session {}'.format(datetime.now()))

add_log = logging.getLogger('add') # address related
add_log.setLevel(logging.INFO)
add_log_formatter = logging.Formatter('%(message)s')
add_log_file_handler = logging.FileHandler('Logging/add.log')
add_log_file_handler.setFormatter(add_log_formatter)
add_log.addHandler(add_log_file_handler)
add_log.info('Running new session {}'.format(datetime.now()))




###### PARSE THE SOURCE FILE LINE BY LINE ######

# open the source file and parse the households out of it
# it determines hamper type and if there is a sponsor or sa appointment
# and if it has previously been routed 
# it then stores the main applicant info, address etc. as well as family 
# member details for use later if needed.

def registration_check(line):
    '''
    operates on a line and returns
    
    if a VLO is a viable visit it will set some flags in it 
    and return it
    otherwise it will return False

    '''
    line_object = Visit_Line_Object(line,fnames.ID, december_flag = True)

    # FOOD
    is_xmas = line_object.is_christmas_hamper() # is christmas hamper
    # SPONSOR
    sponsored, food_sponsor, gift_sponsor = line_object.is_sponsored_hamper()
    # SA - set sms and app num if SA and return 3 tuple for unpacking
    # must use is_army() method to set SA related attributes
    with_sa, sms_target, sa_app_num = line_object.is_army() 
    
    # extract summary from the visit line 
    summary = line_object.get_HH_summary() # a named tuple
    applicant = summary.applicant 
    is_routed = route_database.prev_routed(applicant)
    
    flags = (applicant, is_xmas, is_routed, with_sa)

    if (is_xmas and not is_routed) or (sponsored or with_sa):
        return (line_object, flags)
    else:
        return (False, flags)

def check_address(line_object):
    '''
    attempts address parse and returns simple address
    or logs errors
    '''
    summary = line_object.get_HH_summary()
    applicant = summary.applicant
    address = summary.address
    try:
        simple_address, _ = address_parser.parse(address, applicant)
        return simple_address

    except Exception as a_err:
        add_log.error(f'{applicant} raised {a_err} from {address}')
        nr_logger.error(f'{applicant} raised an error during address parse')
        return False

def sort_types(line_object, simple_address, address_database, kw,\
               delivery_households, flags):
    '''
    examines the hh and attempts to sort them into the correct data structure
    by service request type
    it adds hh to the delivery_households
    and where necessary modifies the sponsored_households structure

    '''
    summary = line_object.get_HH_summary()
    address = summary.address
    pos_code = summary.postal
    city = summary.city
    family_size = summary.size
    applicant, is_xmas, is_routed, with_sa = flags
    sa_app = summary.sa_app_num    
    
    # t/f, (Reitzel/Doon), (sertoma/sa) 
    sponsored, food_sponsor, gift_sponsor = line_object.is_sponsored_hamper()
    type_flags = (is_xmas, food_sponsor, gift_sponsor, sa_app)

    if not any((sponsored, food_sponsor, gift_sponsor, is_xmas, with_sa)):
        return (False, type_flags)

    try:
        crds = address_dbase.get_coordinates(simple_address, city)   
        lt, lg = crds.lat, crds.lng
        if all([lt, lg]): # if there is a previously geocoded address
            # insert base information needed to build a route and card
            n_hood = k_w.find_in_shapes(lt, lg) # find neighbourhood
            add_log.info(f'{applicant} is in this neighbourhood: {n_hood}')
                # create a HH object and insert the summary we need to build 
                # a route (lt, lg)a route card(summary). 
                # and later a route summary (n_hood)
            hh_added = False
            if not food_sponsor and is_xmas: # if not sponsored by doon or reitzel
                delivery_households.add_household(applicant, None, 
                                                  family_size,
                                                  lt, lg, summary, 
                                                  n_hood,
                                                 food=True)

                add_log.info('{} has lat {} lng {}'.format(applicant, 
                                                               lt, lg))
                hh_added = True
            if any((food_sponsor,gift_sponsor)):
                # sponsors should be held in a dictionary
                # record the sponsor in the datastructure
                # and then later we will create reports based
                # off the data for each sponsor
                ops_logger.info(f'added {applicant} to food sponsor {food_sponsor} {gift_sponsor}')
                
                if not hh_added: 
                    delivery_households.add_household(applicant, None, 
                                                  family_size,
                                                  lt, lg, summary, 
                                                  n_hood)
                    hh_added = True
                delivery_households.add_sponsors(applicant, food_sponsor, gift_sponsor)
            if  all((with_sa, sa_app)):
                ops_logger.info(f'added {applicant} to SA bc with_sa {with_sa} sa app: {sa_app}')
                if not hh_added:
                    delivery_households.add_household(applicant,
                                                         None,
                                                         family_size,
                                                         lt, lg,
                                                         summary,
                                                         n_hood)
                delivery_households.add_sa_app_number(applicant, sa_app)

            # AND FINALLY...
            return (True, type_flags)


        else: # if we have not geocoded the address
            # we need to raise and exception.  It is better to 
            # run the geocoding script first and dealing with potential errors
            raise ValueError(f'{address} has not been geocoded! Run the gc \
                             script 1st')
    except Exception as errr:
        nr_logger.error(f'{applicant} has raised {errr} and was not added to\
                        delivery households')
        # add individual details for each family member to the d_h object
        # if present.
        return (False, type_flags)

def check_family(line_object, type_flags, delivery_households):
    '''
    adds family details to delivery_households
    and or sponsored_households if necessary
    '''
    summary = line_object.get_HH_summary()
    applicant = summary.applicant
    #is_xmas, food_sponsor, gift_sponsor, sa_app = type_flags

    try:
        if line_object.has_family():
            family = line_object.get_family_members(fnames) # returns [tuples]
            ops_logger.info(family)
            
            # ID, fname, lname, dob, age, gender, ethno, disability
            delivery_households.add_hh_family(applicant, family)
            ops_logger.info('{} has family and they have been stored in dhh object'.format(applicant))
    
    except Exception as oops:
        nr_logger.error('{} has family, but they were not stored in dhh object, due to {}'.format(applicant, oops))


def parse_and_sort_file(export_file, address_database, kw, delivery_households):
    '''
    takes an export_file object
    and iterates throught the lines of the file
    attempting to 
    -instantiate a Visit_Line_Object
    -derive a simplified address and find geocoordinates from db
    -sort the line into different services and stores hh data
    in the appropriate data structures

    '''
    for line in export_file:
        line_object, flags = registration_check(line)

        if line_object:
            simple_address = check_address(line_object)
            if simple_address:
                typed, type_flags = sort_types(line_object, simple_address,address_database, kw, delivery_households,flags)
                if typed:
                    check_family(line_object, type_flags, delivery_households)
        else:
            applicant, is_xmas, is_routed, with_sa = flags
            nr_logger.info(f'{applicant} is not ours? is xmas:{is_xmas} \
                           route:{is_routed} sa:{with_sa}')

def sort_routes(route_database, delivery_households):
    '''
    works through the delivery households and sorts them into routes
    if they have not been previously routed.
    finds a starting route number and then
    calls the sort_method of the database on the delivery_household_collection
    '''

    # Configure the max number of boxes and
    # the starting route number and pass in the route database
    # for checking existing file numbers to see if they have been
    # previously routed
    routes = Delivery_Routes(route_database, 7, 1)

    tic = timeit.default_timer()
    print(f'ROUTE SORTING BEGIN: {str(datetime.now())}')

    starting_rn = route_database.return_last_rn() # find last rn
    routes.start_count = int(starting_rn) + 1 # reset rn to resume from last route
    routes.sort_method(delivery_households) # start sorting
    toc = timeit.default_timer()
    print(f'ROUTES SORTED: it took {toc-tic} seconds')
    print(f'                 ...or {(toc-tic)/60} minutes')

def family_to_db(house, route_database):
    '''
    takes a Delivery_Household and a Route_Database
    and adds main applicant and family info to the database

    this is a generic function that can follow logic
    to determine if the HH has requested a service
    and if so, will add the family details to the database

    '''
    applicant, rn, rl, n_hd = house.return_route()
    # get the summary from the HH object
    # created by the visit_line
    summ = house.return_summary()  
    # parse out the summary data
    fname = summ.fname
    lname = summ.lname
    email = summ.email
    phone = itr_joiner(summ.phone)
    address = summ.address
    add2 = summ.address2
    city = summ.city
    family_size = summ.size
    diet = summ.diet
    sms_target = summ.sms_target
    postal = summ.postal
    # add household to the summary data
    app_tupe = (applicant,
                fname,
                lname,
                family_size,
                phone,
                email,
                address,
                add2,
                city,
                postal,
                diet,
                n_hd,
                sms_target,)
    ops_logger.info(f'{app_tupe}')
    # ADD MAIN APPLICANT AND HOUSEHOLD INFO
    # ADDRESS ET AL TO THE APPLICANTS TABLE  
    if not route_database.fam_prev_entered(applicant):
        route_database.add_family(app_tupe) 
        ops_logger.info('{} has been logged to applicants db'.format(applicant))
    else:
        ops_logger.error('{} has prev. been logged to applicants db'.format(applicant)) 
    # need to do a check in the database to verify that they have not been
    # added already
    if house.family_members:
        for person in house.family_members:
            pid = person[0]
            if not route_database.fam_member_prev_entered(pid):
                person_o = Person(person)
                # insert ID, Fname, Lname, Age
                base = person_o.get_base_profile()
                route_database.add_family_member(applicant,base)
                ops_logger.info('added {} to family db table'.format(person))
            else:
                ops_logger.info('{} already exists in family table'.format(pid))



def parse_sponsor_db_return(f_sponsor, g_sponsor, route_database, applicant):
    '''
    looks in the database and attempts to make sense
    of what is already there relative to the source file
    '''
    data_base_return = route_database.prev_sponsor(applicant)
    gift_flag = False
    food_flag = False

    if data_base_return:
        _, fs, gs, sd = data_base_return
        # is there new information that we need to overwrite 
        # or add to?
        if f_sponsor and fs:
            # there is a return from the database and there is a food
            # sponsor
            ops_logger.info(f'{applicant} has {f_sponsor} database {fs}')
            if f_sponsor != fs:
                # if the f_sponsor from teh source file and the
                # food sponsor from the database are not the same
                # over ride the database return b/c we need to make a report
                # with new information
                food_flag = True

        if g_sponsor and gs:
            ops_logger.info(f'{applicant} has {g_sponsor} db has {gs}')
            if g_sponsor != gs:
                gift_flag = True

    return (food_flag, gift_flag)
    
def log_sponsors_to_database(household, route_database):
    '''
    operates on the household
    and compares any previously logged sponsorships with those in the file
    if the file contains different sponsor information it overwrites it in 
    the database, otherewise it logs the sponsor data to the database
    the logic for making that decision is in the database method

    '''
    sa_app_num, f_sponsor, g_sponsor = household.return_sponsor_package()
    if any((sa_app_num, f_sponsor, g_sponsor)):
        ops_logger.info(f'working on sponsored HH: {household}')
        #rt = household.return_route() # use this to get the file number
        summ = household.return_summary() # the HH info (name, address etc.)
        applicant = summ.applicant
        #sa_app_num = summ.sa_app_num
        fam = household.family_members
        # find the sponsors in the file
        # now look in the database - do we have a new sponsor?
        # have things changed, or have we not logged this before?

        new_providers = parse_sponsor_db_return(f_sponsor, g_sponsor,\
                                         route_database, applicant)
        ops_logger.info(f'{applicant} with sa {g_sponsor != False} with # {sa_app_num} ')
        ops_logger.info(f'f_spon {f_sponsor} g_spon {g_sponsor}')
        if all(((g_sponsor != 'Salvation Army'), sa_app_num)):
            ops_logger.info('SPONSOR ERROR: {applicant}')
        # add MAIN APPLICANT to database
        ops_logger.info(f'attempting to write {applicant} with family {fam}')
        route_database.add_sponsor(applicant, f_sponsor, g_sponsor,\
                                   new=new_providers)
        # add FAMILY to database
        family_to_db(household, route_database)

        # ADD SALVATION GIFT APP to database
        if all((sa_app_num, (g_sponsor == 'Salvation Army'))):
            sql_error = route_database.add_sa_appointment(applicant, sa_app_num)
            if not sql_error:
                family_to_db(household, route_database)
            else:
                ops_logger.info(f'ERROR: {applicant} has SA app collision\
                                on {sa_app_num}') 


def log_routes_to_database(route_database, delivery_households):
    '''
    takes the data structures holding hh route and sponsor data
    and logs them into the route database
    the logic is based on the idea that only routes will
    have a rn, rl so the is_route boolian test should keep
    hh that are not routes out of the route table
    
    and the log_sponsors... function will let any non sponsor HH
    fall through, or log them to the db if they are reg. for
    sponsor and/or sa services

    '''
    # populate the database with summary and route data
    for house in delivery_households:
        applicant, rn, rl, n_hd = house.return_route()
        
        # get the summary from the HH object
        # created by the visit_line
        
        ops_logger.info(f'trying to log to route table {house.return_summary()}')
        is_route = all((rn, rl))
    # ADD ROUTE INFORMATION TO DATABASE
        if not route_database.prev_routed(applicant) and is_route:
            route_database.add_route(applicant, rn, rl)
            # add family!  if necessary
            family_to_db(house, route_database)

            ops_logger.info(f'{applicant} has been added to rt db')
        else:
            ops_logger.error(f'{applicant} is either not a route, or already\
                             has been logged') 

        sa_app_num, f_sponsor, g_sponsor = house.return_sponsor_package()

        # LOG SPONSOR DETAILS TO DATABASE
        log_sponsors_to_database(house, route_database)




# CONFIGURATION SETUP
conf = configuration.return_r_config()
target = conf.get_target() # source file
db_src, _, outputs = conf.get_folders()
_, session = conf.get_meta()


# MENU INPUT
menu = Menu(base_path='sources/' )
menu.get_file_list()
s_target = menu.handle_input(menu.prompt_input('files'))

confirm = input(f'''1. Use default {target}\n2. Use choice {s_target}\n3. Exit\n ''')

if str(confirm) == '1':
    print(f'using: {confirm}') 
elif str(confirm) == '2':
    target = s_target
    print(f'using: {target}')
else:
    print(f'exiting. Input was: {confirm}')
    sys.exit(0)


# CONFIG AND SETUP of objects 
address_parser = AddressParser() # I strip out extraneous junk from address strings

address_dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
address_dbase.connect_to(f'{db_src}Address.db', create=True) # testing = atest.db

route_database = Route_Database(f'{db_src}{session}rdb.db')

fnames = Field_Names(target) # I am header names
export_file = Export_File_Parser(target, fnames) # I open a csv 
export_file.open_file()

# delivery and sponsor households go into this object
delivery_households = Delivery_Household_Collection()

k_w = Neighbourhoods(r'City of Waterloo and Kitchener Planning district Geometry.json')
k_w.extract_shapes() # get shapes ready to test points


### FUNCTION CALLS ###
### open, parse lines, sort into services, sort routes, log routes and sponsors
### to database
parse_and_sort_file(export_file, address_dbase, k_w, delivery_households)
sort_routes(route_database, delivery_households)
log_routes_to_database(route_database, delivery_households)

# close databases
route_database.close_db()
address_dbase.close_db()


