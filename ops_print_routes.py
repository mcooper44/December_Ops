#!/usr/bin/python3.6

'''
This script iterates through a database provisioned by
the ops_sort script and rebuilds HH objects and a 
Delivery_Household_Collection and then writes delivery
slips and other products that are essential to the 
effort of getting services out to everyone

'''


import sqlite3
from collections import namedtuple
import logging
from datetime import datetime

from r_config import configuration

from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes
from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection

from address_parser_and_geocoder import Coordinates
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

# INITIALIZE CONFIGURATION FILE
conf = configuration.return_r_config()
target = conf.get_target() # source file
db_src, _, outputs = conf.get_folders()
_, session = conf.get_meta()


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




class Service_Database:
    def __init__(self, path_name):
        self.path_name = path_name
        self.conn = None
        self.cur = None

     
    def connect(self, first_time=False, strings=None):
        '''
        establishes connection to database stored in the .path_name attribute
        if the first_time flag is set
        it will attempt to create tables with strings contained in a list or
        tuple held in the 'strings' parameter
        '''
        
        try:
            self.conn = sqlite3.connect(self.path_name)
            self.cur = self.conn.cursor()
            print(f'Database.connect: database connection open to {self.path_name}')

            if first_time == True and any(strings):
                for string in strings:
                    self.cur.execute(string)
                    #print(f'executing: {string}')
                    self.conn.commit()
            elif first_time == True and not any(strings):
                print('no strings provided to provision tables')

        except Exception as e:
            print('could not establish connection to database')
            raise e
    

    def lookup(self, target, table, row, paramater):
        '''
        SELECT {file_id} FROM {routes} WHERE {route_num}{ BETWEEN 50 AND 130}
        SELECT {*} FROM {applicants} WHERE {file_id}{=123456}
        
        http://www.sqlitetutorial.net/sqlite-between/    
        '''
        ex_string = f'SELECT {target} FROM {table} WHERE {row}{parameter}'
        self.cur.execute(ex_string)
        rows = self.cur.fetchall()
        return rows

    def update(self, string, tple):
        '''
        executes sql string with values tple
        to update a table
        '''
        if all((string,tple)):
            try:
                self.cur.execute(string, tple)
                return True
            except Exception as error:
                return error
        else:
            print('input for SQL update operation is none')
            return False

    def lookup_string(self, string, tple, echo=False):
        '''
        executes sql string with values tple
        if tple != None asuming it a fetchone scenario
        if tple = None, assuming it is a fetchall scenario
        '''
        rows = None
        if tple:
            try:
                self.cur.execute(string, tple) 
                rows = self.cur.fetchone()
            except:
                print(f'could not lookup {tple} with {string}')
        else:
            self.cur.execute(string)
            rows = self.cur.fetchall()
        
        if echo and not rows: print('WARNING: NO DATABASE RESULT')

        return rows

    def close(self):
        '''
        closes the db connection

        '''
        name = self.path_name
        try:
            self.conn.close()
            print(f'connection to {name} closed')
        except:
            print('could not close connection')


class Service_Database_Manager:
    '''
    Interface for the Database class
    contains methods for making specific calls to the database

    dbm = database_manager({'client_db': 'sms_client_test.db', 
                                'sa': 'sa_2019_appointments.db'})
    dbm.initialize_connections() # instantiate db objects

    '''


    def __init__(self, data_bases):
        self.db_path_dict = data_bases # dict of {db_name: path_string}
        self.db_struct = {}

    def initialize_connections(self):
        for db_name in self.db_path_dict.keys():
            path_string = self.db_path_dict.get(db_name, None)
            self.db_struct[db_name] = Service_Database(path_string)
            self.db_struct[db_name].connect()

    @classmethod
    def get_service_db(cls):
        return cls({'rdb': 'databases/2019_testing_rdb.db',
                   'sa' : 'databases/sa_2019_appointments.db'})

    def close_all(self):
        '''
        iterates through the databases being managed and closes them
        '''
        for db_name in self.db_struct.keys():
            self.db_struct[db_name].close()

    def insert_to(self, db_name, d_struct):
        '''
        insert data structre d_struct to database db_name
        param d_struct is a dictionary as per doc string described
        on insert method of the database class

        'table name': ([(values_to_write,)], # list of tuple(s)
                       '(?, [...])') # string tuple with a ? for each 
                                     # value to insert
        '''
        self.db_struct[db_name].insert(d_struct)

    def return_app_num(self, database, file_id):
        '''
        looks up and returns app_num from the gift table
        '''
        ls = f'SELECT app_num FROM gift_table WHERE file_id={file_id}'
        return self.db_struct[database].lookup_string(ls)
    
    def return_sa_info_pack(self, rdb, app_db, fid):
        '''
        for a given file id, what is the appointment number and time string
        for the hh salvation army appointment?

        return a 2 tuple with app num, app time
        or (None, False)
        '''
        
        try:
            ls1 = f'SELECT app_num FROM gift_table WHERE file_id={fid}'
            num = self.db_struct[rdb].lookup_string(ls1, None)[0][0]
            ls2 = f'SELECT day,time FROM Appointments WHERE ID={num}'
            a_day, a_time = self.db_struct[app_db].lookup_string(ls2, None)[0]
            time_str = f'{a_day} {a_time}'
            return (num, time_str)
        except:
            return (False, False)

    def return_sa_app_time(self, app_db, num):
        '''
        gets a string representing the Day and Time of a given
        salvation army appointment number (num)

        '''
        ls = f'SELECT day, time FROM Appointments WHERE ID={num}'
        a_day, a_time = self.db_struct[app_db].lookup_string(ls, None)[0]
        return f'{a_day} {a_time}'

    def get_main_applicant(self, database, file_id):
        '''
        for a given file_id return the route info
        for that hh
        
        returns a person from the applicants table
        for instantiation of a Delivery_Household
        returns
        file_id, f_name, l_name, family_size, phone, email, address_1,
        address_2, city, diet, neighbourhood, sms_target
        '''


        ls = f'''SELECT * FROM applicants WHERE file_id={file_id}'''
        
        return self.db_struct[database].lookup_string(ls, None)

    def get_family_members(self, database, file_id):
        '''
        returns tuples of family members from the family table
        for a given main applicant
        (file id f name, l name, age
        '''
        ls = f'''SELECT client_id, fname, lname, age, gender FROM family WHERE main_applicant={file_id}'''
        
        return self.db_struct[database].lookup_string(ls, None)

    def return_app_range(self, database, low, high):
        '''
        return a range of gift appointments between the low and high values
        used to lookup file_id's that have upcoming gift appointments
        returns 2 tuples of file_id, app_num
        '''
        ls = f'''SELECT gift_table.file_id, app_num, sms_target
                 FROM gift_table 
                 INNER JOIN applicants ON gift_table.file_id =
                 applicants.file_id WHERE 
                 app_num <= {high} AND app_num >= {low} AND message_sent = 0
                 AND applicants.sms_target IS NOT NULL'''
        return self.db_struct[database].lookup_string(ls, None)
    
    def return_route_range(self, database, low, high):
        '''
        return a range of file_id's that have route numbers between parameter
        low and high - used to find households that have an upcoming delivery
        returns 2 tuples of file_id, route_num
        '''
        ls = f'''SELECT file_id, route_number, route_letter FROM 
                 routes 
                 WHERE route_number >= {low} AND 
                 route_number <= {high}'''
        return self.db_struct[database].lookup_string(ls, None)

    def return_route_num(self, database, file_id):
        ls = 'SELECT route_number FROM routes WHERE file_id=?'
        return self.db_struct[database].lookup_string(ls, (file_id,))

    def return_file_ids(self, database):
        '''
        returns tuples of (file_id, cell_num)
        from the applicants table of the client_database
        for all the Households that have a valid NOT NULL cell_num value
        typical usage would be dbm.return_file_ids('client_database.db')
        or in testing scenarios .return_file_ids('sms_client_test.db')

        '''
        ls = '''SELECT file_id, cell_num FROM applicants 
                WHERE cell_num IS NOT NULL'''
        return self.db_struct[database].lookup_string(ls, None)
    
    def return_app_date(self, database, app_num, echo=False):
        '''
        returns the day and time as a tuple from the database when
        supplied a app_num
        '''
        ls = 'SELECT day, time from Appointments where ID=?'
        day_time =  self.db_struct[database].lookup_string(ls, (app_num,))
        if echo: print(f'found {day_time}')
        return day_time

    def return_sponsor_hh(self, database):
        ls = '''SELECT file_id, food_sponsor, gift_sponsor, sorting_date FROM sponsor'''
        return self.db_struct[database].lookup_string(ls, None)


def package_applicant(main, sa, rn, rl):
    '''
    rebuilds the datastructures that are needed to by the route
    and ops binder printing functions with data from the database

    this function rebuilds the structure needed to instantiate a 
    Delivery_Household via a Delivery_Household_Collection
    .add_household() call

    and a route summary object

    '''
    # use a named tuple to alias all the fields
    ma = namedtuple('add_h', 'f, fn, ln, fs, ph, em,\
                       a1, a2, ct, po, di, ne, sms')
    # summary a la. VLO.get_HH_summary()
    visit_sum = namedtuple('visit_sum', 'applicant, fname, lname, \
                           size, phone, email, address, address2, city, \
                           postal, diet, sa_app_num, sms_target')
    if main:
        a = ma(*main)
        if not sa: sa = None
        summary = visit_sum(a.f, a.fn, a.ln, a.fs, a.ph, a.em, a.a1, a.a2, a.ct,
                        a.po, a.di, sa, a.sms)

        package = (a.f, None, a.fs, None, None, summary, a.ne, a.po, rn,
                   rl,True, True)
        rt_sum_package = (a.f, a.fs, a.di, rl, a.a1, a.ne)
        return package, rt_sum_package

    else:
        raise ValueError('There is no main applicant for package_applicant!')

def get_hh_from_database(database, r_start=1, r_end=900):
    '''
    This function gets routes out of the database
    and structures them into a delivery_household_collection
    and returns that structure

    '''
    rdbm = Service_Database_Manager.get_service_db() 
    rdbm.initialize_connections()

    dhc = Delivery_Household_Collection()

    logged_routes = rdbm.return_route_range('rdb', r_start, r_end)

    for drt in logged_routes: # for delivery route...
    #print(applicant)
        fid, rn, rl = drt

        main_applicant = rdbm.get_main_applicant('rdb', fid)[0]
        # get gift appointment number (gan) gift app time (gat)
        # or False, False
        gan, gat = rdbm.return_sa_info_pack('rdb', 'sa', fid)

        hh_package, rt_sp  = package_applicant(main_applicant, gan, rn, rl)
        dhc.add_household(*hh_package)
        dhc.setup_rt_summary(rn)

        dhc.add_to_route_summary(rn, rt_sp)
        
        fam = rdbm.get_family_members('rdb', fid)
        if fam:
            dhc.add_hh_family(fid, fam)

        if all((gan, gat)):
            dhc.add_sa_app_number(fid, gan)
            dhc.add_sa_app_time(fid, gat)
    
    rdbm.close_all()
    return dhc

def write_delivery_tools(delivery_households, start_rt=1):

    b_now = f'{datetime.now()}'
    b_head = f'{outputs}{session}'
    slips = Delivery_Slips(f'{outputs}{session}_delivery_slips_{datetime.now()}.xlsx') # source file for the households
    route_binder = Binder_Sheet(f'{b_head}_route_binder_{b_now}.xlsx')  
    ops_ref = Office_Sheet(f'{b_head}_operations_reference_{b_now}.xlsx')

    current_rt = (start_rt -1)  # to keep track of the need to print 
                                    # a summary card or not
    rt_written_set = set()

    #OPEN WORKSHEETS


    for house in delivery_households.route_iter():
        rt = house.return_route()
        fid, rn, rl, nhood =  rt
        summ = house.return_summary() # the HH info (name, address etc.)
        fam = house.family_members    

        ops_ref.add_line(rt, summ, fam) # delivery binder
        ops_logger.info(f'Added {fid} in route {rn} to ops reference')
        # the following two lines need to be pulled out into a separate 
        # pipeline - we need a hh structure for true sponsored households
        #s_report.add_household(summ, fam) # sponsor report
           
        rt_str = rn # because the route number is an int 
        # decide if now is the right time to insert a route summay on the stack
        rt_card_summary = delivery_households.route_summaries.get(rt_str, None) 
        
        if rn > current_rt: # if we have the next route number
            if rt_card_summary: # and the summary is there
                # add a summary card because we are at the start of a new route
                slips.add_route_summary_card(rt_card_summary)
                current_rt += 1

                route_binder.add_route(rt_card_summary) # add an entry to the route
                                                        # binder as well
                ops_logger.info(f'route {rn} added to stack and route_binder')
            else:
                ops_logger.error(f'missed a route card summary for rn {rn} {rl}')
        slips.add_household(rt, summ) # adds another card to the file
        ops_logger.info(f'{fid} in rt {rn} {rl} added to card stack')

    # CLOSE WORKSHEETS
    slips.close_worksheet()
    route_binder.close_worksheet()
    ops_ref.close_worksheet() 

route_database = Route_Database(f'{db_src}{session}rdb.db')

dh = get_hh_from_database(route_database)
write_delivery_tools(dh)

