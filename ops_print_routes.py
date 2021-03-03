#!/usr/bin/python3.6

'''
This script iterates through a database provisioned by
the ops_sort script and rebuilds HH objects and a 
Delivery_Household_Collection and then writes delivery
slips and other products that are essential to the 
effort of getting services out to everyone
it prompts the user for input and utilizes that input 
to print the correct range
'''

import logging
import sys
import sqlite3
from collections import namedtuple
from datetime import datetime

from r_config import configuration
from file_iface import Menu

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

SERVICE_TABLE_KEYS = {'KW Salvation Army': 'Appointments',
                        'Salvation Army - Cambridge': 'CSA'}

# INITIALIZE CONFIGURATION FILE
conf = configuration.return_r_config()
target = conf.get_target() # source file
db_src, _, outputs = conf.get_folders()
_, session = conf.get_meta()
db_dictionary = conf.get_bases() # {rdb: file, sa: file}

if not all((target, db_src, session)):
    raise Exception('NO CONFIG FILE LOADED')
    sys.exit(1)


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
    '''
    this is a  model of the route database
    and a database in general....
    '''
    
    
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
            if echo: print(rows) 
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
        '''
        standard files that will be used and are located in the
        databases folder

        keys are 'rdb' for the route database
                 'sa' for the app # day time mapping

        '''
        return cls(db_dictionary)

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
    
    def return_sa_info_pack(self, rdb, app_db, fid, provider, echo=False):
        '''
        for a given file id, what is the appointment number and time string
        for the hh salvation army appointment?

        return a 2 tuple with app num, app time
        or (None, False)
        '''
        table_select = SERVICE_TABLE_KEYS.get(provider, None)
        num = False
        time_str = False
        if table_select:
            if provider:
                try:
                    ls1 = f'''SELECT app_num FROM gift_table WHERE file_id={fid} and
                    provider="{provider}"'''
                    num = self.db_struct[rdb].lookup_string(ls1, None)[0][0]
                except Exception as e1:
                    if echo: print(f'{fid} caused {e1}')
            
            else:
                try:
                    ls1a = f'''SELECT app_num FROM gift_table WHERE
                    file_id={fid}'''
                    num = self.db_struct[rdb].lookup_string(ls1a, None)[0][0]
                except Exception as e1a:
                    if echo: print(f'{fid} caused {e1a}')
            
            try:
                ls2 = f'SELECT day,time FROM {table_select} WHERE ID={num}'
                a_day, a_time = self.db_struct[app_db].lookup_string(ls2, None)[0]
                time_str = f'{a_day} at {a_time}'
            except Exception as e2:
                if echo: print(f'ops_print_routes: {fid} caused {e2}')
            return (num, time_str)
        else:
            #print(f'{fid} failed at table select')
            #y = input('pausing in print routes')
            return False, False

    def return_sa_app_time(self, app_db, num, provider):
        '''
        gets a string representing the Day and Time of a given
        salvation army appointment number (num)

        '''
        table_select = SERVICE_TABLE_KEYS.get(provider, 'None')
        ls = f'SELECT day, time FROM {table_select} WHERE ID={num}'
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
    
    def return_gifts_not_food(self, database):
        '''
        returns file_id of households who did not register for food
        and only exist in the gift table
        '''
        ls = f'''SELECT file_id FROM gift_table WHERE file_id NOT IN (SELECT
            file_id FROM routes)'''
        return self.db_struct[database].lookup_string(ls, None)

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

    def return_sponsor_hh(self, database, crit=None):
        '''
        param crit is a date in the format of YYYY-MM-DD
        wrapped in quotes
        '''
        if not crit:
            ls = '''SELECT file_id, food_sponsor, gift_sponsor,
            voucher_sponsor, turkey_sponsor, sorting_date FROM sponsor'''
            return self.db_struct[database].lookup_string(ls, None)
        elif crit:
            ls2 = f'''SELECT file_id, food_sponsor, gift_sponsor,
            voucher_sponsor, turkey_sponsor, sorting_date                    
                    FROM sponsor WHERE date(sorting_date) = date("{crit}")'''
            return self.db_struct[database].lookup_string(ls2, None)
   
    def return_sponsors(self, database, fid):
        ls = f'''SELECT food_sponsor, gift_sponsor, voucher_sponsor, turkey_sponsor 
        FROM sponsor WHERE file_id = {fid}'''
        return self.db_struct[database].lookup_string(ls, None)

    def return_pickup_table(self, database):
        '''
        returns the values from the pickup table in the route database
        this table holds the pickup zone and pickup number for the household
        representing a household headed by file_id
        '''
        ls = '''SELECT file_id, pu_zone, pu_num, message_sent FROM pickup_table'''
        return self.db_struct[database].lookup_string(ls, None)

    def return_pu_package(self, database, fid):
        '''
        returns pu_zone and number frome the pickup table where fid has an
        entry
        '''
        ls = f'SELECT pu_zone, pu_num FROM pickup_table WHERE file_id = {fid}'
        try:
            
            retrn = self.db_struct[database].lookup_string(ls, None)
            zn, zn_num  = retrn[0]
            return zn, zn_num
        except Exception as e:
            print(f'return_pu_package for {fid} failed with {e}')
            return False, False
    
    def return_pu_time(self, database, app_number):
        '''
        returns the time from the applications database for the corresponding
        app_number
        '''
        ls = f'SELECT day, time from Zones WHERE ID={app_number}'
        try:
            retrn = self.db_struct[database].lookup_string(ls, None)
            day, time = retrn[0]
            return day, time
        except:
            return False, False

    def return_geo_points(self, add_tuple, database='address'):
        '''
        param database is the Address database
        param add tuple is a ('123 Main Street', 'City') combo
        '''

        street, city = add_tuple
        ls = f'SELECT lat, lng FROM address WHERE source_street = "{street}" AND\
        source_city = "{city}"'
        return self.db_struct[database].lookup_string(ls, None)
    
    def return_comp_services(self, database):
        '''
        returns the service table joined to key datapoints from applicants and
        routes
        '''
        ls = '''SELECT s.file_id, s.food_sponsor, s.gift_sponsor,
        s.voucher_sponsor, s.turkey_sponsor, s.sorting_date,
        a.family_size, a.diet, a.address_1, a.city, a.neighbourhoood, 
        r.file_id, r.route_number, r.route_letter
        FROM
            sponsor as s
        LEFT JOIN
            applicants as a
        ON
            s.file_id = a.file_id
        LEFT JOIN
            routes as r
        ON
            s.file_id = r.file_id
        ORDER BY
            r.route_number,
            r.route_letter'''
        return self.db_struct[database].lookup_string(ls, None)

def package_applicant(main, sa, rn, rl, die=None):
    '''
    rebuilds the datastructures that are needed to by the route
    and ops binder printing functions with data from the database

    this function rebuilds the structure needed to instantiate a 
    Delivery_Household via a Delivery_Household_Collection
    .add_household() call

    and a route summary object

    '''
    # use a named tuple to alias all the fields
    #file, firstn, lastn familysize, phone, email,
    #address line 1, address line 2, city, postal code, diet, hood, sms_number
    ma = namedtuple('add_h', 'f, fn, ln, fs, ph, em,\
                       a1, a2, ct, po, di, ne, sms')


    # summary a la. VLO.get_HH_summary()
    visit_sum = namedtuple('visit_sum', 'applicant, fname, lname, \
                           size, phone, email, address, address2, city, \
                           postal, diet, sa_app_num, sms_target')
    lat = None
    lng = None

    if main:
        a = None
        if not die:
            a = ma(*main)
        else:
            a = ma(main[0], main[1], main[2], main[3], main[4], main[5],\
                   main[6], main[7], main[8], main[9], die, main[11],\
                   main[12])

        if not sa: sa = None
        summary = visit_sum(a.f, a.fn, a.ln, a.fs, a.ph, a.em, a.a1, a.a2, a.ct,
                        a.po, a.di, sa, a.sms)

        package = (a.f, None, a.fs, lat, lng, summary, a.ne, a.po, rn,
                   rl,True, True)
        
        rt_sum_package = (a.f, a.fs, a.di, rl, a.a1, a.ne)
        return package, rt_sum_package

    else:
        raise ValueError('There is no main applicant for package_applicant!')

def reformat_diet(sponsor_tuple, diet_str):
        '''
        this is a dirty hack - to over ride diet with services in a really 
        bad way - but great things come at the 11th hour no?
        '''
        t_mod = ''
        service_pk = []

        if ('vegan' in diet_str) or ('vegetarian' in diet_str):
            t_mod = 'Vegetarian '
        if 'halal' in diet_str:
            t_mod = 'halal '
        
        _, _, v, t = sponsor_tuple[0] 
        if v and len(v) > 1:
            service_pk.append('voucher')
        if t and len(t) > 1:
            service_pk.append(f'{t_mod}turkey')

        return ','.join(service_pk)

def get_hh_from_database(database, r_start=1, r_end=900,gift_prov='KW Salvation Army'):
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
        
        # get geo points from address database
        # main_applicant is a tuple from the database call
        # position 6 and 8 are address line 1 string
        # and City string
        l1_city = (main_applicant[6], main_applicant[8])
        gp = rdbm.return_geo_points(l1_city, 'address')
        
        diet = main_applicant[10]
        
        gan, gat = rdbm.return_sa_info_pack('rdb', 'sa', fid,gift_prov)
        sponsors = rdbm.return_sponsors('rdb', fid)

        new_diet = reformat_diet(sponsors, diet)

        hh_package, rt_sp = package_applicant(main_applicant, gan, rn, rl,\
                                              die=new_diet)
        #(file_id, hh_id, family_size, lat, lng, summary, hood, postal, rn, rl,
        # food, null_g)
        
        dhc.add_household(*hh_package)
        dhc.setup_rt_summary(rn)

        dhc.add_to_route_summary(rn, rt_sp)
        dhc.add_sponsors(fid, *sponsors)

        fam = rdbm.get_family_members('rdb', fid)
        if fam:
            dhc.add_hh_family(fid, fam)

        if all((gan, gat)):
            dhc.add_sa_app_number(fid, gan, gift_prov)
            dhc.add_sa_app_time(fid, gat)
    
    rdbm.close_all()
    return dhc

def write_delivery_tools(delivery_households, start_rt=1):

    b_now = f'{datetime.now()}'[:16] # cut off the miliseconds
    b_head = f'{outputs}{session}'
    slips = Delivery_Slips(f'{outputs}{session}_delivery_slips_{b_now}.xlsx') # source file for the households
    route_binder = Binder_Sheet(f'{b_head}_route_binder_{b_now}.xlsx')  
    ops_ref = Office_Sheet(f'{b_head}_operations_reference_{b_now}.xlsx')

    current_rt = (int(start_rt) -1)  # to keep track of the need to print 
                                    # a summary card or not
    rt_written_set = set()

    #OPEN WORKSHEETS


    for house in delivery_households.route_iter():
        rt = house.return_route()
        fid, rn, rl, nhood =  rt
        summ = house.return_summary() # the HH info (name, address etc.)
        fam = house.family_members    
        # T/F voucher, turkey requested
        vt_request_tuple = house.return_delivery_service_pack()

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

def input_check(inp):
    '''
    validates user input for the route ranges
    and returns True if inp is a number or False
    if it is something else
    '''
    try:
        x = int(inp)
        if isinstance(x, int):
            return True
        else:
            return False
    except:
        return False

def val_start_end(start, end):
    '''
    prompts the user to input the start and end range of the routes
    to print and validates the input, exiting if it is invalid
    or returning the start, end range if valid

    '''
    
    # check that numbers were input
    if not all((input_check(start), input_check(end))):
        print(f'Invalid input {start} {end}')
        sys.exit(1)
    # validate that we have a valid range
    if int(end) < int(start):
        print('We cannot run backwards!')
        print(f'end point {end} < {start}')
        sys.exit(1)

    return start, end

def main():
    '''
    prompts the user to input a starting route number
    and an ending number
    validates input
    asks for confirmation
    and then preps route cards, delivery binder
    and operation summary files
    '''

    print('### TIME TO PRINT SOME ROUTES ###')

    route_database = Route_Database(f'{db_src}{session}rdb.db')
    last_number = route_database.return_last_rn()

    if last_number == 0:
        raise ValueError(f'THERE ARE NO ROUTES IN {db_src}{session}rdb.db ')
    else:
        print(f'There are currently {last_number} routes in the database')

    menu = Menu()
    start_p, end_p = menu.prompt_input('s_routes'), menu.prompt_input('e_routes')
    start_r, end_r = val_start_end(start_p, end_p)
        
    conf_all = input(f'Please confirm (y/n) print of routes {start_r} to {end_r}')

    if conf_all.lower() == 'n':
        print('exiting...')
        sys.exit(0)
    elif conf_all.lower() == 'y':
        dh = get_hh_from_database(route_database, r_start=start_r, r_end=end_r)
        write_delivery_tools(dh, start_rt=start_r)

        print('...Route Printing Process Complete...')
    else:
        print(f'invalid input {conf_all}') 
        raise ValueError('input either y or n')

if __name__ == '__main__':
    main()
