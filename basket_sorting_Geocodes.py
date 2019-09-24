'''
Contains classes and methods for parsing household data into routes

The Delivery_Household_Collection() provides methods to organize and
interrogate Delivery_Households() and contains Route_Summary() objects 
that are used by classes elsewhere to create Route_Binder() entries etc.

The Delivery_Routes() class accepts a Delivery_Household_Collection() 
and iterates through the Delivery_Households() it contains to build routes,
it then labels each household with its route number and letter designation 
by using methods in the Delivery_Household_Collection() class

The Route_Database() class provides methods to insert, reference and manage
household, family member and route data in a SQLite3 database

'''

from collections import namedtuple
from collections import defaultdict
from collections import Counter
from math import radians, cos, sin, asin, sqrt
from operator import attrgetter
import csv
import sqlite3
import logging
import datetime

logging.basicConfig(filename='Logging/route_sorting.log',level=logging.INFO)
logging.info('Running new session {}'.format(datetime.datetime.now()))

sort_log = logging.getLogger('sort')
sort_log.setLevel(logging.INFO)
sort_log_frmt = logging.Formatter('%(message)s')
sort_log_file_handler = logging.FileHandler('Logging/sort.log')
sort_log_file_handler.setFormatter(sort_log_frmt)
sort_log.addHandler(sort_log_file_handler)
sort_log.info('Running new session: {}'.format(datetime.datetime.now()))

Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')



def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Thanks Stack Overflow!

    This function is called in the sort_method() of the Delivery_Routes() 
    class 

    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km


class Route_Summary():
    '''
    A central collection of relevant information that is helpful
    on the dock: streets, applicants, mapping of diet to hh etc. 

    It needs to be added to iteratively

    This object is used by the classes that print the delivery
    binder as well as the route summary cards that sit at the head of 
    the route card stacks

    It is held in the Delivery_Household_Collection() in a dictionary
    keyed to the Route number.
    
    It can be used to distribute data into places where a summary
    of a route is helpful for preparation and distribution

    '''

    def __init__(self, rn):
        self.route = rn # route number
        self.streets = set() # set of streets for quick summary
        self.neighbourhood = [] # the City Neighbourhood(s) the route hits
        self.applicant_list = [] # list of file ids
        self.sizes = [] # list of family sizes used 
        self.letter_map = {} # Mapping of route letter to family size and diet
        self.boxes = Counter() # Family size Counter
        self.letters = [] 
        self.street_list = [] # for an ordered summary of streets
                              # if .streets is used duplicated addresses
                              # will be dropped and there may be a mismatch
                              # when printing columns such as:  
                              # | streets | household info |
    
    def add_household_summary(self, sum_tp):
        '''
        adds household data to the data structure as some other method
        iterates through a range of households that have been sorted 
        into a route
        
        This method is called in the Delivery_Household_Collection
        Class by the .add_to_route_summary() method

        '''
        fid, family_size, diet, letter, street, hood = sum_tp
        
        self.letters.append(letter)
        self.applicant_list.append(fid)
        self.sizes.append(family_size)
        self.street_list.append(street)
        self.letter_map[fid] = 'Box: {} Family: {} Diet: {}'.format(letter,
                                                               family_size,
                                                               diet)
        self.streets.add(street)
        if hood:
            self.neighbourhood.append(hood)

        self.boxes.update([str(family_size)]) # add as list not just str

class Route_Database():
    '''
    This is a SQL database of households and contains the following tables
    1. 'applicants' = base data encapsulated by a delivery card
    2. 'family' = family member data keyed off a main applicant file id 
    3. 'routes' = file id for a main applicant and their route, route letter 
    4. 'sponsor' = file id for main applicant, food_sponsor, gift_sponsor
    5. 'gift_table' = file_id for main applicant, sa_app_num
    It is the central database that will recieve routes, reproduce them
    and return route specific information when needed by other classes
    and methods
    including information about sponsors and gift appointments since there is a
    very high degree of overlap between households requesting those services
    '''

    def __init__(self, path_name):
        self.path_name = path_name
        self.conn = None
        self.cur = None
        self.summary_array = {} # where we will stash summary objects
        if self.path_name:
            self.conn = sqlite3.connect(path_name)
            self.cur = self.conn.cursor()
            # ROUTE TABLE
            self.cur.execute('''CREATE TABLE IF NOT EXISTS routes (file_id INT
                             UNIQUE, route_number INT, route_letter TEXT,
                             sorting_date timestamp, message_sent INT)''')
            self.conn.commit()
            # APPLICANTS
            self.cur.execute('''CREATE TABLE IF NOT EXISTS applicants (file_id
                             INT UNIQUE, f_name TEXT, l_name TEXT, family_size INT, phone TEXT,
                             email TEXT, address_1 TEXT, address_2 TEXT, city
                             TEXT, postal TEXT, diet TEXT, neighbourhood TEXT, 
                             sms_target TEXT )''')
            self.conn.commit()
            # FAMILY MEMBERS
            self.cur.execute('''CREATE TABLE IF NOT EXISTS family
                             (main_applicant INT, client_id INT UNIQUE, fname TEXT,
                             lname TEXT, age INT)''')
            # SPONSOR TABLE
            self.cur.execute('''CREATE TABLE IF NOT EXISTS sponsor (file_id INT
                             NOT NULL UNIQUE, food_sponsor TEXT, gift_sponsor TEXT)''')
            
            # GIFT TABLE
            self.cur.execute('''CREATE TABLE IF NOT EXISTS gift_table (file_id
                             INT NOT NULL UNIQUE, app_num INT, message_sent INT)''')

            self.conn.commit()

    def add_route(self, file_id, rn, rl):
        '''
        logs a route in the database 'routes' table

        takes a file id, route number and route letter, wraps them in a tuple
        and inserts them into the table

        the current date is also added for auditing purposes and 
        for potential rollback scenarios
        '''
        dt = datetime.date.today()
        db_tple = (file_id, rn, rl, dt, 0)
        self.cur.execute("INSERT OR IGNORE INTO routes VALUES (?, ?, ?, ?, ?)", db_tple)
        self.conn.commit()  

    def add_sponsor(self, file_id, food_sponsor, gift_sponsor,\
                    new=(False,False)):
        '''
        logs a main applicant file id in the database with their assigned sponsors

        takes a file id, food provider and gift provider wraps them in a tuple
        and inserts them into the table

        if there are new service providers this method will update the value
        in the table where appropriate
        '''
        
        db_tple = (file_id, food_sponsor, gift_sponsor)
        new_food, new_gift = new
        if not any(new): # service providers
            self.cur.execute("INSERT OR IGNORE INTO sponsor VALUES (?, ?, ?)",\
                             db_tple)
            self.conn.commit()
        elif all(new):
            self.cur.execute("UPDATE sponsor SET food_sponsor=?,gift_sponsor=?\
                             WHERE file_id=?", db_tple)
            self.conn.commit()
        elif new_food and not new_gift:
            self.cur.execute("UPDATE sponsor SET food_sponsor=? WHERE\
                             file_id=?", (food_sponsor, file_id,))
            self.conn.commit()
        elif new_gift and not new_food:
            self.cur.execute("UPDATE sponsor SET gift_sponsor=? WHERE\
                             file_id=?", (gift_sponsor, file_id,))
            self.conn.commit()
    
    def add_sa_appointment(self, file_id, app_num):
        '''
        logs a salvation army gift appointment number to the database
        takes the file_id, app_num and wraps them in a tuple
        and inserts them into the 'gift_table'
        but if there is a duplicated values
        it will bounce back with an error that will be caught and returned 
        in a 2 tuple (True, error_string) if
        the insertion attempt has no errors it will return a 2 tuple 
        of (False, None) for no error and no error retrn
        '''
        try:
            db_tple = (file_id, app_num, 0)
            self.cur.execute("INSERT INTO gift_table VALUES (?, ?, ?)",
                         db_tple)
            self.conn.commit()
            return (False, None)
        except Exception as sqlerror:
            return (True, sqlerror)

    def add_family(self, family_tple):
        '''
        this adds a household to the 'applicants' table
        
        it accepts a tuple in the following order:
        (applicant name, address, contact info etc)
        '''

        self.cur.execute('''INSERT OR IGNORE INTO applicants VALUES 
                         (?,?,?,?,?,?,?,?,?,?,?,?,?)''',family_tple)
        self.conn.commit()

    def add_family_member(self, app_id, person):
        '''
        adds a family member to the 'family' table 
        
        it expects to recieve a tuple created by the 
        Person.get_base_profile() method in the following format
        (file id, fname, lname, age)

        '''

        five_tuple = (app_id, person[0], person[1], person[2], person[3])
        self.cur.execute("INSERT OR IGNORE INTO family VALUES (?,?,?,?,?)", five_tuple)
        self.conn.commit()


    def prev_routed(self, applicant):
        '''
        double checks to see if this household has been routed
        before by looking for a file id in the 'routes' table
        '''

        self.cur.execute("SELECT * FROM routes WHERE file_id=?", (applicant,))
        if self.cur.fetchone():
            return True
        else:
            return False
    
    def prev_sponsor(self, applicant):
        '''
        looks in the sponsor table to see if the household has been sponsored
        previously and returns either the tuple  or False
        '''
        self.cur.execute("SELECT * FROM sponsor WHERE file_id=?", (applicant,))
        sponsor_details = self.cur.fetchone()
        if not sponsor_details:
            return False
        else:
            return sponsor_details

    def fam_prev_entered(self, applicant):
        '''
        returns True if the household has been logged in the
        'applicants' table or False

        applicant = a main applicant file ID

        '''
        self.cur.execute("SELECT * FROM applicants WHERE file_id=?",(applicant,))
        if self.cur.fetchone():
            return True
        else:
            return False

    def fam_member_prev_entered(self, person):
        '''
        looks in the 'family' table to see if a family member has been
        previously entered.

        person = the file id of a family member

        returns True if the family member has been logged in 
        the database family table
        or False if they have not.
        '''

        self.cur.execute("SELECT * FROM family WHERE client_id=?",(person,))
        if self.cur.fetchone():
            return True
        else:
            return False

    def return_last_rn(self):
        '''
        returns the last route number in the database so that we
        can resume the numbering sequence as households are added
        and to avoid route number collision between runs
        '''
        self.cur.execute("SELECT MAX(route_number) FROM routes LIMIT 1")
        last_rn = self.cur.fetchone()
        
        if last_rn[0]:
            return last_rn[0]
        else:
            return 0
   
    def __iter__(self):
        '''
        returns a 2 tuple combining data from two database tables for each 
        household that has been logged.
       
        The 2 tuple contains the result from the 'applicants' table and the
        'routes' table
        '''
        self.cur.execute("SELECT * FROM routes SORT BY route_number")
        rts = self.cur.fetchall()
        for hh_route in rts:
            fid = hh_route[0]
            self.cur.execute("SELECT * FROM applicants WHERE file_id=?",(fid,))
            household = self.cur.fetchone()
            package = (household, hh_route)
            yield package

    def close_db(self):
        '''
        closes db connection

        '''
        self.conn.close()
        print('db connection closed')

def prep_geolocation(lat, lng, give_null=False):
    '''
    this is a kludge to deal with the scenario
    where we are extracting routes from the database
    and creating slips, reports
    but we no longer need geolocation data.

    param give_null is passed in through the Delivery_Household
    null_geo parameter.

    if we are no longer at the point where we care about
    sorting, and just need a Delivery_Household
    to print cards null_geo=True should be used.
    if only 2018 me knew what 2019 would need and spent some
    time doing proper architecture
    '''

    if not give_null:
        return Geolocation(float(lat), float(lng))
    elif give_null:
        return (None, None)


class Delivery_Household():
    '''
    a collection of datapoints needed to assemble a delivery route
    and the methods to output and organize all the little bits
    
    It is interfaced with and controlled by the Delivery_Household_Collection() 
    class which has various methods of putting data into a DH() and getting 
    it to report on what it contains. 

    It provides the data needed sort routes, and eventually add to the various
    files that are needed for operations in the warehouse
    
    A key source of household information that the DH() contains is located in
    the summary parameter which is a named tupled created by the
    Visit_Line_Object class get_HH_summary() method

    '''

    def __init__(self, file_id, hh_id, family_size, lat, lng, summary, hood,
                 postal=None, rn=None, rl=None, null_geo=False):
        self.main_app_ID = file_id
        self.household_ID = hh_id
        self.hh_size = family_size
        # used by the Delivery_Routes().sort_method()
        self.geo_tuple = prep_geolocation(lat, lng, null_geo) 
        self.route_number = rn
        self.route_letter = rl
        self.neighbourhood = hood
        self.postal = summary.postal # typically not used but may be of interest to
                             # partners
        self.summary = summary # route card data with address et al. 
                               # created by the visit line object .get_HH_summary()
        self.family_members = None # family members in tuples
        self.sa_app_num = False
        self.food_sponsor = False
        self.gift_sponsor = False
        self.sa_time = False

    def return_sponsor_package(self):
        '''
        returns a 3 tuples of app num, food sponsor, gift sponsor
        default values are False
        '''
        return (self.sa_app_num, self.food_sponsor, self.gift_sponsor)
    
    def set_sa_status(self, sa_app_num):
        '''
        adds an appointment number to the sa_app_num attribute
        and sets gift sponsor to 'Salvation Army'
        '''
        self.sa_app_num = sa_app_num
        self.gift_sponsor = 'Salvation Army'

    def set_sa_time(self, sa_time):
        '''
        sets the sa_time attribute to equal param sa_time
        this will be a date and time string i.e. Dec 1 at 1:00pm
        '''
        self.sa_time = sa_time

    def set_sponsors(self, food, gift):
        if food:
            self.food_sponsor = food
        if gift:
            self.gift_sponsor = gift

    def return_hh(self):
        '''
        returns the input values needed to sort a route


        '''
        lat, lng = self.geo_tuple
        return (self.main_app_ID, 
                self.household_ID, 
                self.hh_size, 
                lat, 
                lng,
                self.route_number,
                self.route_letter)

    def add_routing(self, number, letter):
        '''
        a method to add a route number and letter to the household
        '''
        self.route_number = int(number)
        self.route_letter = letter

    def add_family_members(self, family_tuples):
        '''
        takes a collection of family tuples created by the Visit_Line_Object
        class .get_family_members() method
        these can be turned into Person() objects later if needed to write 
        that data back to different files
        '''
        self.family_members = family_tuples

    def routed(self):
        '''
        returns True or False if it has a route number and letter designation

        this method is used by the Delivery_Routes.sort_method() and is 
        called by the Delivery_Household_Collection() class through its 
        .has_been_routed() method.
        '''
        return all([self.route_number, self.route_letter])
    
    def return_route(self):
        '''
        returns a tuple of (file id, routing number, letter, neighbourhood)

        This method is useful at various points when iterating over
        Delivery_Household() objects for database inserts, and for using logic
        to decide if it is necessary to add a route card to the card stack or
        insert a summary and route binder entry.
        '''
        return (self.main_app_ID, self.route_number, self.route_letter, self.neighbourhood)
    
    def return_summary(self):
        '''
        #### For an individual route card ####
        Returns a summary of the Household for creating an individual 
        route card
        
        returns the HH summary.  Data needed to put on the route card like
        name, address, etc. in the form of a named tuple with labels
        'applicant, fname, lname, size, phone, email, address, 
        address2, city, postal, diet, sa_app_num, sms_target'
        this named tuple is created by the .get_HH_summary() method 
        in the Visit_Line_Object() class found in db_data_models
        '''
        return self.summary

    def return_card_summary(self):
        '''
        #### For adding summary data to the Route_Summary() for use
        on the binder etc. ####

        returns (fid, family_size, diet, letter, street, hood)
        for use in the card summary object that will help create
        a summary of all the households in the route
        and go to the head of the stack of route cards

        it is called by the label_route()  method in the 
        Delivery_Household_Collection() class to provide input to the
        Route_Summary() classes .add_household_summary() method 

        '''

        return (self.main_app_ID, 
                self.hh_size, 
                self.summary.diet, 
                self.route_letter, 
                self.summary.address,
                self.neighbourhood)
    
    def __str__(self):
        return f'{self.summary} {self.family_members}' 

class Delivery_Household_Collection():
    '''
    A way to manage a collection of Delivery_Household() objects

    This class is used to supply the Delivery_Routes() class with Households to
    sort into routes and a way of interfacing with the Delivery_Routes() 
    either inserting information like route numbers and letters or asking the 
    objects to report on what info they contain to provide inputs to the various 
    classes and methods used in the pipeline 
    '''

    def __init__(self):
        self.hh_dict = {} # this is the collection of Delivery_Households()
                          # keyed to file_id
        self.fids_routed = set()
        self.route_summaries = {} # summarized routes rn: summary_objects
                                  # these are the collections of information
                                  # that are used to generate the summary
                                  # cards that sit at the head of the route
                                  # in the card stack
        self.delivery_targets = [] # list of HH that are registered
                                   # for delivery

    def add_household(self, file_id, hh_id, family_size, lat, lng, summary,
                      hood, postal=None, rn=None,
                      rl=None,food=False,null_g=False):
        '''
        add a Delivery_Household() object to the .hh_dict attribute
        of this class
        param: food - is a toggle to add the fild_id to an internal list of 
        households that are going to be delivered to
        the route iterator will yield households that are on that list only
        and avoid situations where sponsored households are routed
        when they are not to be setup for that service
        param: null_g trips the no geotuples flag in the Delivery_Household
        '''
        self.hh_dict[file_id] = Delivery_Household(file_id, 
                                                   hh_id, 
                                                   family_size, 
                                                   lat, 
                                                   lng, 
                                                   summary, 
                                                   hood, 
                                                   postal, 
                                                   rn, 
                                                   rl,
                                                   null_geo=null_g)
        if food:
            self.delivery_targets.append(file_id)

    def add_hh_family(self, applicant, familytples):
        '''
        adds tuples of the family members to a Delivery_Household
        using its add_family_members() method
        '''
        #print('trying to add family to key {}'.format(applicant))
        if self.hh_dict.get(applicant, False):

            self.hh_dict[applicant].add_family_members(familytples)
            #print('added family members to {}'.format(applicant))
        else:
            print('Attempting to add {applicant} family members \
                  but {applicant} is not in the \
                  Delivery_Household_Collection. .add_hh_family \
                  has failed')
    
    def add_to_route_summary(self, rn, r_summary):
        '''
        adds a household to a Route_Summary() object
        by calling the Route_Summary.add_household_summary() method
        the Route_Summary is 
        an object that will be used to create a summary
        card to put at the head of a route stack
        
        r_summary is tuple (fid, family_size, diet, letter, street, hood)
        '''
        
        self.route_summaries[rn].add_household_summary(r_summary)

    def get_HH_set(self):
        '''
        returns a set of file id's
        '''
        return set(self.hh_dict.keys())
    
    def has_been_routed(self, fid):
        '''
        calls the .routed() method of the Delivery_Household() contained
        in the self.hh_dict dictionary which returns either True or False
        '''
        return self.hh_dict[fid].routed()

    def setup_rt_summary(self, rn):
        '''
        when pulling data out of the route database to print
        cards et al.
        this method is needed to instantiat a Route_Summary() object
        that existing routes can be added to
        so summary cards can be extracted and printed
        if the batch script ops_route_creator is being used
        the label_route method does this in the process of labelling 
        the routes that it has created. 
        '''
        if rn not in self.route_summaries.keys():
            self.route_summaries[rn] = Route_Summary(rn)


    def label_route(self, route_key, route):
        '''
        Takes a route number and collection of file id's in that route
        created by the .sort_method() of the Delivery_Routes() class 
        and then labels the Delivery Household() objects that correspond to 
        the file ids contained in the route parameter which is just a 
        containter of file id's 
        ***RIP 2017 as teh year without a G***
        In the process of doing so, it also creates a Route_Summary() class
        object and stores it in the .route_summaries attribute which is a
        dictionary keyed off route numbers


        route_key = route number
        route = container of fid's 
        
        The Route_Summary() takes a tuple
        (fid, family_size, diet, letter, street, hood)

        '''
        # zip letter strings 'A', 'B' etc with File Id's so we have a
        # mapping that we can use to assign letters to each HH
        r_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'] 
        letter_map = zip(route, r_letters) # [(fid1, 'A'), (fid2, 'B')]

        #create a Route_Summary() object
        self.route_summaries[route_key] = Route_Summary(route_key)

        # unzip the file id and letter combos
        for fid_lttr in letter_map:
            fid, lttr = fid_lttr
            # and assign the route number and sub route (the letter) to the 
            # Delivery_Household() that corresponds to the file id and that is 
            # stored in the .hh_dict attribute of this class
            
            # add a route number to Delivery_Household() keyed to main app fid
            self.hh_dict[fid].add_routing(route_key, lttr)
            
            # extract a summary of that newly routed Delivery_Household()
            rt_hh = self.hh_dict[fid].return_card_summary()
            self.route_summaries[route_key].add_household_summary(rt_hh)

    def get_size(self, fid):
        '''
        returns the family size of a Delivery_Household in the .hh_dict
        attribute
        '''
        return self.hh_dict[fid].hh_size

    def get_summary(self, fid):
        '''
        gets the summary data needed to print a delivery card
        name,file id, address, phone, diet etc.
        '''
        return self.hh_dict[fid].return_summary()


    def get_sponsors(self, fid):
        '''
        returns a 3 tuple of sa_app_num, food sponsor, gift sponsor
        '''
        return self.hh_dict[fid].return_sponsor_package()


    def add_sa_app_number(self, fid, app_num):
        '''
        for hh with main app  fid, add app_num to 
        the sa_app_num attribute and set
        self.gift_sponsor = 'Salvation Army'
        '''
        self.hh_dict[fid].set_sa_status(app_num)
    
    def add_sa_app_time(self, fid, sa_time):
        '''
        calls teh .set_sa_time method of the HH and inserts 
        a pickup time retrieved from the database
        '''
        self.hh_dict[fid].set_sa_time(sa_time)

    def add_sponsors(self, fid, food=False, gift=False):
        '''
        for hh with main app fid, add food and or
        gift sponsors to the .food_sponsor or .gift_sponsor
        attributes
        '''
        
        self.hh_dict[fid].set_sponsors(food, gift)

    def __iter__(self):
        '''
        An iterator that yields Delivery_Households() containted in the
        .hh_dict attribute
        '''
        
        for hh in self.hh_dict:
            yield self.hh_dict[hh]

    def route_iter(self):
        '''
        yields an iterator made from the .hh_dict attribute but that is sorted
        by route_number and then route_letter.
        
        This iterator is useful for adding households to the route slip stack,
        and route summary in the Delivery_Slips(), Binder_Sheet(), 
        Office_Sheet(), Report_File() classes through their various methods
        '''

        for hh in (sorted(self.hh_dict.values(),
                          key=attrgetter('route_number','route_letter'))):
            yield hh

    def delivery_iter(self):
        '''
        yields an iterator made up of all the hh that have registered for
        delivery and need to be routed.  This should exclude hh that
        have registered for food from a sponsor group

        '''
        for hh in self.delivery_targets:
            yield self.hh_dict[hh]

    def __str__(self):
        return f'{self.hh_dict}'

class Delivery_Routes():
    '''
    Provides .sort_method() as a way of finding households that live within a
    geographic proximity to eachother and labelling them into routes

    it has two parameters in its __init__() method 
    max_boxes = the maximum number of boxes desired in a route
    start_count = the starting number to label routes.  Can be over ridden to
    maintain continunity of numbering when adding routes later.

    Route data will be held in a database or the
    Delivery_Households_Collection() class and fed in as a batch job


    '''    

    def __init__(self, max_boxes = 7, start_count = 1):
        self.max_boxes = max_boxes # max number of boxes per/route
        self.start_count = start_count # what we start counting routes at

    def sort_method(self, households):
        '''
        Uses a brute force method to sort households into geographically
        proximate piles and labels them with a route number and letter

        Takes a data structure described by the Delivery_Household_Collection() 
        class and iterates through the households taking the first one 
        off the stack and calculating it's distance to all of the other
        households in the datastructure

        it then uses methods in the DHC class to label the route

        '''
        
        box_mask = {'0' : 1, '1' : 1, '2' : 1, '3' : 1, '4' : 1, '5': 2, 
                    '6' : 2, '7' : 2, '8' : 3, '9' : 3, '10': 3, '11': 3, 
                    '12': 4, '13':4, '14':4, '15':4, '16':4, '17':4, 
                    '18':4 }
        max_box_count = self.max_boxes
        route_counter = self.start_count
        #routes = {} # labeled routes and the families they contain
        assigned = set() # container to add hh that have been assigned
        print('starting sort_method')
        # for key in dictionary of households in the
        # Delivery_Households_Collection class...
        for applicant in households.delivery_iter():
            h1_lat, h1_long = applicant.geo_tuple # a tuple of (lat, lng)
            applicant_route = [] # the working container that we will then add to the route dictionary
            size = str(applicant.hh_size) # turn the size into a string so we can...
            boxes = box_mask[size] # start building the route with the household
            app_file_id = applicant.main_app_ID
            if not applicant.routed():
                # add them to list of assigned HH to avoid adding them again
                #assigned.add(applicant) 
                # start by adding the household we are starting with to the container for this route
                applicant_route.append(app_file_id)
                # build a container to add {calculated distances: households} to
                # this will allow us to make a sorted list of the shortest distances and the HH
                # that are at that distance            
                distance_hh_dictionary = defaultdict(list)                                                                                                             
                # ITERATE THROUGH THE HOUSEHOLDS AND CALCULATE DISTANCES FROM THE CHOSEN STARTING HH
                for HH in households.delivery_iter(): # iterate through the keys to find the distances of remaining households                    
                    if HH.main_app_ID != app_file_id: # if this is a new household
                        if not HH.routed(): # and we have not already used them in a route
                            ident = HH.main_app_ID # their file number
                            # TO DO - clarify how to access households in this
                            # block.  Should we iterate through objects or file
                            # ids and then grab the object?
                            h2_lat, h2_long = HH.geo_tuple # their lat,long
                            # caculated the distance between the two households
                            distance_between = haversine(h1_long, h1_lat, h2_long, h2_lat) # returns float distance in KM                        
                            d_key = str(distance_between) # convert to string so we can use it as a dictionary key
                            distance_hh_dictionary[d_key].append(ident) # update dictionary of distances: HH identifier
                # now we have calculated all the distances from Route #X A to all of the other households in the caseload
                # sort a list of all the distances so we can skim the shortest off
                distances = sorted([float(k) for k in distance_hh_dictionary.keys()])
                # NOW WE WILL ITERATE THROUGH THE DISTANCES AND TRY AND PUT A ROUTE TOGETHER
                for float_value in distances: # for distance in sorted listed of distances
                    key = str(float_value) # convert the float to a string so we can use it in the distance : families at that distance dictionary
                    # now we need to iterate through the list of HH at this distance.
                    for fam in distance_hh_dictionary[key]: # for the individual or family in the list of households at this distance
                        if not households.has_been_routed(fam): # if we haven't sorted them into a route yet
                            fam_size = households.get_size(fam) # determine family size
                            box_num = box_mask[fam_size] # determine number of boxes
                            # do math to determine if we can add them to the route
                            # then if there are still more families at this distance we need to pop the one we just added
                            # and evaluate the next one and so on down the chain until we max out box count per route or exaust
                            # remaining households
                            if box_num + boxes <= max_box_count: # if we added this family to the pile and they didn't add too many boxes
                                boxes += box_num
                                assigned.add(fam) # add them to the assigned list
                                applicant_route.append(fam) # add them to the route
            
            if applicant_route:
                sort_log.info('we have iterated and made a route! It is {}'.format(applicant_route))
                r_key = str(route_counter)
                # if we record what route each HH is in do we need a separate
                # data structure of routes in this class?  We can just iterate
                # through the Delivery_Household_Collection and strip out the
                # necessary information
                
                # this step records the routes on the households
                households.label_route(r_key, applicant_route)
                route_counter += 1
