'''
Provides classes and methods for interacting with a l2f export file
and extracting major datapoints for use in the December Ops pipeline
or to reimplement l2f in a localized SQL database for long term 
data and caseload projects 

It allows access to the following objects 
    file 
    line 
    field names
    person
    household
    visit
'''

import db_parse_functions as parse_functions
import csv
import sys
from collections import Counter, defaultdict, namedtuple
import phonenumbers as pn


person_ethno_profile = namedtuple('person_ethno_profile', 'visible_minority, first_nations, metis, inuit, NA, undisclosed')
self_identifies_profile = namedtuple('self_identifies_profile', 'disabled, less_than_ten, other, NA, undisclosed')
visit_tuple_structure = namedtuple('visit_tuple_structure', 'date, main_applicant_id, visit_object')
a_person = namedtuple('a_person', 'ID, Lname, Fname, DOB, Age, Gender, Ethnicity, SelfIdent')

PROVIDERS = {'Emergency Food Hamper Program - House of Friendship': 1,
             'Chandler Mowat Community Centre - House of Friendship': 2,
             'Kingsdale Community Centre - House of Friendship': 3,
             'Courtland Shelley Community Centre - House of Friendship': 4,
             'Sunnydale Community Centre - House of Friendship': 5,
             'Victoria Hills Community Centre': 6,
             'Centreville Chicopee Community Centre': 7,
             'Forest Heights Community Centre': 8}

AGENTS_HOF = ['House of Friendship']
SERVICE_AGENTS_HOF = "House of Friendship Delivery,HoF Zone 1,HoF Zone 2,HoF Zone \
3,HoF Zone 4,HoF Zone 5,HoF Zone 6,HoF Zone 7,HoF Zone 8,HoF Zone 9".split(',')
PICKUP_AGENTS_HOF = SERVICE_AGENTS_HOF[1:]
SERVICE_AGENTS_SVDP = "Blessed Sacrament Church,Our Lady of Lourdes,\
St. Agnes Catholic Church,St. Aloyious,St. Anne's Catholic Church,\
St. Anthony Daniel Catholic Church,St. Francis of Assisi Church,\
St. John's Church,St. Joseph's Church,St. Louis SSVP,St. Mark's Catholic\
 Church,St. Mary's Catholic Church,St. Michael's Catholic Church,\
St. Teresa's Church".split(',')
SERVICE_AGENTS_CAM = "Salvation Army - Cambridge,Cambridge \
Firefighters,Cambridge Self-Help Food Bank".split(',')
SERVICE_AGENTS_GIFTS = 'KW Salvation Army,SPONSOR - SERTOMA,SPONSOR - REITZEL'.split(',')
SERVICE_AGENTS_RURAL = "Wilmot Family Resource Centre,Woolwich Community\
Services".split(',')
SERVICE_AGENTS_SPONSOR = 'SPONSOR - DOON,SPONSOR - REITZEL'.split(',')
SERVICE_AGENTS_KWSA = 'KW Salvation Army'

SERVICE_LIST = (SERVICE_AGENTS_HOF,
                SERVICE_AGENTS_SVDP,
                SERVICE_AGENTS_CAM,
                SERVICE_AGENTS_GIFTS,
                SERVICE_AGENTS_RURAL,
                SERVICE_AGENTS_SPONSOR)

TURKEY_PROVIDERS = SERVICE_AGENTS_HOF + SERVICE_AGENTS_SPONSOR

class Field_Names():
    '''
    Provides a way of interacting with data in the export by using the heading
    labels

    Provides a way to configure the field name indexes for the file and avoids having to 
    make changes to the order of export and change lots of numbers in the different objects
    manually.  i.e. 'Client Date of Birth' = index x, 'Client First Name' =
    index Y
    '''

    def __init__(self, config_file):
        self.config_file= config_file # path to the config file
        self.ID = dict() # a dictionary of field name : index number as int()
        self.file_headers = None

        with open(self.config_file) as f:
            name_ID_reader = csv.reader(f)
            self.file_headers = next(name_ID_reader, None)
            for col_header in self.file_headers:
                col_header_index = self.file_headers.index(col_header)
                self.ID[col_header] = col_header_index

    def init_index_dict(self):
        '''
        this method creates a dictionary using the config_file that can be used to extract
        data points from the line and provides an easier way to keep track of where the
        varioius fields are.
        param config_file should be the file
        '''
        # this is method depreciated
        pass
    
    def return_family_subslice_len(self):
        '''
        returns the size of the chunk that each family members takes up
        at the end of the file.  This is useful to know when clipping family
        members out of the Visit_Line and avoids a scenario where you need to
        open the each file and manually determine how long the family member
        subslices are and enter that value somewhere
        '''
        return self.ID['HH Mem 2- ID'] - self.ID['HH Mem 1- ID']
    
    def return_fam_header_indexes(self):
        '''
        returns a dictionary that can be used to parse out the datapoints
        from a slice of the visit line containing family details.
        This will help automate the search for the relevant info
        and not require manual calibration of parsing functions to 
        use the correct index
        '''
        fam_hd = {'ID' : 0,
                'lname': None,
                'fname': None,
                'dob': None,
                'age': None,
                'gender': None,
                'ethnicity': None,
                'self_ident': None,
                'relationship': None,
                'immigration': None
                 }

        targets = ['HH Mem 1- Last Name','HH Mem 1- First Name',
                   'HH Mem 1- Date of Birth', 'HH Mem 1- Age',
                   'HH Mem 1- Gender','HH Mem 1- Ethnicities',
                   'HH Mem 1- Self-Identifies As',
                   'HH Mem 1- Relationship to Main Client',
                   'HH Mem 1- Immigration']

        reference = self.file_headers.index('HH Mem 1- ID')
        d_t = list(fam_hd.keys())
        dx = 0
 
        for k in d_t[1:]:
            try:
                fam_hd[k] = self.file_headers.index(targets[dx]) - reference
                dx += 1
            except:
                dx += 1
        
        return fam_hd

class Service_Request():
    '''
    models the service request fields that separate each request with a comma
    and bracket the |service provider with bars|

    This localizes the logic for parsing services and providers in the 
    request and simplifies things when providers increase in number
    '''
    def __init__(self, service_str):
        self.original = service_str
        self.services = service_str.split(',')
        self.num_bookings = len(self.services)
        self.providers = []
        self.requests = []
        self.service_lookup = defaultdict(set) # keyed provider: request
        self.service_dict = {} # keyed request: provider
        self.blank = True

        if any(self.services):
            for service in self.services:
                # remove the last | and then split on the remaining one 
                request, provider = service[:-1].split('|') 
                self.requests.append(request)
                self.providers.append(provider)
                self.service_dict[request] = provider
                self.service_lookup[provider].add(request)
                self.blank = False
        #print(f'SERVICE REQUEST: {self.service_dict} {self.service_lookup}')

    def lookup_request_provider(self, request):
        '''
        uses the service_dict attribute to find the service provider
        for a specific request
        '''
        return self.service_dict.get(request, False)

    def confirm_agents(self, agents, get_service=False):
        '''
        confirms if the hh has made a request from one of the various
        clusters of service providers
        param agents is a iterable container of service provider strings
        it should ideally only be used to test against one pool of 
        service providers at a time
        if param: get_service=True
        it will return a tuple of providers as a set and services as a list
        '''
        providers = False
        service = None
       
        providers = set(agents).intersection(set(self.service_lookup.keys()))
        if get_service:
            service_list = []
            for service in providers:
                service_list += list(self.service_lookup[service])
            return providers, service

        else:
            return providers
    
    def confirm_services(self, service_list):
        '''
        iterates through service_list
        and if the service is in the self.requests attribute
        return True
        otherwise default to False
        '''
        for service in service_list:
            if service in self.requests:
                return True
        return False


    def lookup(self, lookup):
        '''
        Takes param lookup which is a Service provider name
        as a str and returns the set of service requested
        or False
        '''
        return self.service_lookup.get(lookup, False)
    
    def return_lookup(self):
        '''
        Returns the structure of {Provider: Service}
        '''
        return self.service_lookup
    
    def __str__(self):
        '''
        returns the orignal string if needed
        '''
        return self.orignal

class Person():
    '''
    A person is a part of a household, they have a profile of personal information
    and a relationship to a main applicants and an implied relationship to other 
    household members
    
    param: person_summary is a uple created by the get_family_members() method of
    the Visit_Line_Object when it attempts to parse the line and extract family
    details
    
    '''    

    def __init__(self, person_summary):
        self.person_ID = person_summary[0] # HH Mem X - ID
        self.person_Fname = person_summary[2] # HH Mem X - Fname 
        self.person_Lname = person_summary[1] # HH Mem X - Lname
        self.person_DOB = person_summary[3] # HH Mem X - Date of Birth
        self.person_Age = person_summary[4] # HH Mem X - Age
        self.person_Gender = person_summary[5] # HH Mem X - Gender
        self.person_Ethnicity = person_summary[6] # HH Mem X - Ethno
        self.person_Idenifies_As = person_summary[7] # HH Mem X - Disable etc.
        self.person_Relationship = person_summary[8]
        self.person_immigration_date = person_summary[9] # immigration date 
        self.person_HH_membership = [] # a list of HH they have been a part of
        self.HH_Identities = defaultdict(list) # a dictionary of HHIDs and the relationship tag they have in that HH

    def person_description_string(self):
        '''
        returns a string of name, birthday and gender identification for the person coded in the object
        '''
        i_am_string = 'My name is {} {}.\nMy birthday is {}\nand I identify as a {}'.format(self.person_Fname,
                                                                                           self.person_Lname,
                                                                                           self.person_DOB,
                                                                                           self.person_Gender)
        return i_am_string
    
    def __str__(self):
        return '{} {} {}'.format(self.person_ID, self.person_Fname, self.person_Lname)

    def __repr__(self):
        return 'Person: {} {} {}'.format(self.person_ID, self.person_Fname, self.person_Lname)
    
    def is_adult(self, Age = 18):
        '''
        return True if 18 years or older
        or False if not
        provide a different value for parameter Age for testing different ages

        This method is helpful in the December Ops pipeline for sorting
        household members into appropriate adult/child buckets for the
        sponsor reports
        '''
        return int(self.person_Age) >= Age

    def get_base_profile(self):
        '''
        returns a tuple of (ID, Fname, Lname, Age, Gender)

        this method is used for inserting family member information in a
        Route_Database() object coded in basket sorting_Geocodes.py
        via the .add_family_member() method
        
        it is called in december ops in the sort_routes, and print scripts
        '''
        return (self.person_ID, self.person_Fname, self.person_Lname,
                self.person_Age, self.person_Gender)

    def Get_Self_Ident_Profile_Tuple(self):
        '''
        returns a named tuple with boolian values for each of the different categories coded in the database
        'self_identifies_profile', 'disabled, less_than_ten, other, NA, undisclosed' 
        '''
        no = False
        undisco = False
        disabled = False
        immigrant = False
        other = False
        if 'none' in self.person_Idenifies_As:
            no = True
        if 'undisclosed' in self.person_Idenifies_As:
            undisco = True
        if 'person_with_disability' in self.person_Idenifies_As:
            disabled = True
        if 'new_immigrant' in self.person_Idenifies_As:
            immigrant = True
        if 'other' in self.person_Idenifies_As:
            other = True
        bool_self_ident_profile = self_identifies_profile(disabled, immigrant, other, no, undisco)
        return bool_self_ident_profile

    def Get_Ethno_Profile_Tuple(self):
        '''
        returns a named tuple of with values True or False for the different categories
        'person_ethno_profile', 'visible_minority, first_nations, metis, inuit, NA, undisclosed'
        '''
        not_applic = False
        undisco = False
        visi_minor = False
        first_nat = False
        metis = False
        inuit = False
        if 'not_applicable' in self.person_Ethnicity:
            not_applic = True
        if 'undisclosed' in self.person_Ethnicity:
            undisco = True
        if 'visible_minority' in self.person_Ethnicity:
            visi_minor = True
        if 'first_nations' in self.person_Ethnicity:
            first_nat = True
        if 'metis' in self.person_Ethnicity:
            metis = True
        if 'inuit' in self.person_Ethnicity:
            inuit = True
        bool_ethno_profile = person_ethno_profile(visi_minor, first_nat, metis, inuit, not_applic, undisco)
        return bool_ethno_profile
    
    def add_HH_visit(self, HHID):
        '''
        registers that this person has visited as part of a HH
        '''
        self.person_HH_membership.append(HHID)
    
    def add_HH_relationship(self, HHID, relationship):
        '''
        registers are HHID and a list of relationships they have been tagged with while a member of that HHID
        '''
        self.HH_Identities[HHID].append(relationship)

    def return_count_of_HH_visits(self):
        '''
        returns a Counter of how many visits this Person has made in one or more
        Households
        '''
        return Counter(self.person_HH_membership)
    
    def return_relationships(self):
        '''
        returns all of the different relationship roles that this person has 
        been tagged with
        '''
        return self.HH_Identities.values()

class Household():
    '''
    A Household has an ID, 
    contains people, and their relationships, 
    has visit dates and a total of all visits
    has dietary conditions, sources of income
    referrals provided and ethnicities as well 
    as geocoordinates if those can be derived.
    '''
    
    def __init__(self, Household_ID):
        self.Household_ID = Household_ID
        self.Visits = {} # a dictionary of visit objects keyed off of the seq. visit_id
        self.Member_Roles = defaultdict(list) # TO DO: this will be some sort of datastructure Adults : [], Children : [] etc.
        self.Member_Set = set()
        self.coordinates = []
        self.neighbourhood = []              

    def add_visit(self,HH_Visit_Number_Sequence_ID, Visit_Object):
        '''
        register a visit in the dictionary keyed to the sequential ID
        '''
        self.Visits[HH_Visit_Number_Sequence_ID] = Visit_Object

    def add_members(self, fmember_ID_list):
        '''
        adds family members to the set of all members
        '''
        for person in fmember_ID_list:
            self.Member_Set.add(person)
    
    def add_HH_role(self, relationship_object):
        '''
        captures the different roles that individuals play in the household
        as the households change over time and members enter, leave or are born
        into households
        the relationship_object should be a dictionary created of Person_ID : 'role'
        '''
        for person_id_num in relationship_object.keys():
            relationship = relationship_object[person_id_num]
            self.Member_Roles[relationship].append(person_id_num)

    def add_geolocation(self, lat, lng, v_date):
        '''
        adds a tuple of (visit date, (lat, lng)) to the 
        self.coordinates list. lat, lng are floats
        '''
        points = (lat, lng)
        self.coordinates.append((v_date, points))

    def add_neighbourhood(self, neighbourhood_name, v_date):
        '''
        adds a tuple of (visit date, neighbourhood) to the 
        self.neighbourhood list.  neighbourhood is a string
        '''
        self.neighbourhood.append((v_date, neighbourhood_name))

    def __str__(self):
        return '{} has {} members and {} visits'.format(self.Household_ID, len(self.Member_Set), len(self.Visits))

    def __repr__(self):
        return 'Household {} has the following members {}'.format(self.Household_ID, self.Member_Set)

class Visit_Line_Object():
    '''
    takes a line from the csv export 
    a visit is made up of people, organized in a household
    a visit happens on a specific date and describes key services

    The visit line object should extract this information and provides methods
    for structuring it, so that person, HH and Visit objects can be created
    
    it also has a flag to indicate if there are xmas features to extract
    and provides methods to do so if needed
    :param: visit_line = a line from a csv that contains information about a visit
    :param: fnamedict = a dictionary of headernames: index number - it is used
    to reference where data points are on the visit_line
    :param: december_flag = a marker to toggle looking for Christmas related
    headers

    '''    
    
    def __init__(self, visit_line, fnamedict, december_flag = False): # line, dict of field name indexes, is Xmas?
        self.vline = visit_line
        self.visit_Date = None 
        self.main_applicant_ID = visit_line[fnamedict['Client ID']] # Main Applicant ID
        self.main_applicant_Fname = None
        self.main_applicant_Lname = None
        self.main_applicant_DOB = None
        self.main_applicant_Age = visit_line[fnamedict['Client Age']] # Main Applicant Age
        self.main_applicant_Gender = None # Main Applicant Gender
        self.main_applicant_Phone = None # Main Applicant Phone Numbers
        self.main_applicant_Email = None
        self.main_applicant_Ethnicity = None
        self.main_applicant_Self_Identity = None
        self.languages = None
        self.immigration_date = None
        self.delivery = None
        self.household_primary_SOI = None # Client Primary Source of Income
        self.visit_Address = None 
        self.visit_Address_Line2 = None
        self.visit_City = visit_line[fnamedict['City']]
        self.visit_Postal_Code = None
        self.lat = None # latitude
        self.lng = None # longitude
        self.housing_type = None # Housing type string
        self.visit_Household_ID = None  # Household ID - the unique file number used to identify households
        self.visit_household_Size = visit_line[fnamedict['Household Size']] # The Number of people included in the visit
        self.visit_household_Diet = None # Dietary Conditions in a readable form
        self.visit_food_hamper_type = None 
        self.visit_Family_Slice = None
        self.visit_Agency = None # organization that provided services
        self.first_visit = None
        self.HH_main_applicant_profile = None
        self.HH_family_members_profile = None
        self.visit_Referral = None # Referrals Provided
        self.quantity = None
        self.foods_provided = None # Normal is food prov. Xmas is foods prov. fml
        self.items_provided = Service_Request(visit_line[fnamedict['Items Provided']])
        self.xmas_ID = None
        self.xmas_notes = None
        self.xmas_application_site = None
        self.ex_reference = None # holds the sms number if recorded
        # set by methods
        self.sa_status = None # has SA appointment?
        self.sa_app_num = None # SA appointment number - coded between ##x##
        self.sms_target = None # the phone number to send sms messages to
        self.hof_zone = None # Zone label 
        self.hof_pu_num = None # Zone pickup number - coded between $$x$$
        self.item_req = {'Voucher': False, 'Gifts': False}
        self.food_req = {'Delivery Christmas Hamper': False, 
                         'Turkey': False, 'Pickup Christmas Hamper': False}
        self.delivery_h = False # Designates a delivery hamper
        self.f_sponsor = None # a list
        self.g_sponsor = None # a list
        if fnamedict.get('Visit Date', False):
            self.visit_Date = visit_line[fnamedict['Visit Date']]

        if fnamedict.get('Food Provided', False):
            self.foods_provided = Service_Request(visit_line[fnamedict['Food Provided']])
        elif fnamedict.get('Foods Provided', False):
            self.foods_provided = Service_Request(visit_line[fnamedict['Foods Provided']])
        if fnamedict.get('Dietary Considerations', False):
            self.visit_household_Diet = parse_functions.diet_parser(visit_line[fnamedict['Dietary Considerations']]) # Dietary Conditions in a readable form
        if fnamedict.get('Client Ethnicities', False):
            self.main_applicant_Ethnicity = visit_line[fnamedict['Client Ethnicities']]
        if fnamedict.get('Household Languages', False):
            self.languages = visit_line[fnamedict['Household Languages']]
        if fnamedict.get('Client Immigration', False):
            self.immigration_date = visit_line[fnamedict['Client Immigration']]
        if fnamedict.get('Delivery', False):
            self.delivery = visit_line[fnamedict['Delivery']]

        if fnamedict.get('Quantity', False):
            self.visit_food_hamper_type = parse_functions.hamper_type_parser(int(fnamedict['Quantity'])) # Quantity of food parsed to be Food or Baby 3 = hamper 1 = baby hamper
            self.quantity = visit_line[fnamedict['Quantity']]
        #if fnamedict.get('Visit Date', False):
        #    self.visit_Date = visit_line[fnamedict['Visit Date']] # Visit Date
        if fnamedict.get('Client First Name', False):
            self.main_applicant_Fname = visit_line[fnamedict['Client First Name']] # Main Applicant First Name
        if fnamedict.get('Client Last Name', False):
            self.main_applicant_Lname = visit_line[fnamedict['Client Last Name']] # Main Applicant Last Name
        if fnamedict.get('Client Date of Birth', False):
            self.main_applicant_DOB = visit_line[fnamedict['Client Date of Birth']] # Main Applicant Date of Birth
        if fnamedict.get('Client Self-Identifies As', False):
            self.main_applicant_Self_Identity = visit_line[fnamedict['Client Self-Identifies As']] 
        if fnamedict.get('Client First Food Bank Visit-Date', False):
            self.first_visit = visit_line[fnamedict['Client First Food Bank Visit-Date']]
        # ADDRESS RELATED SWITCHES
        if fnamedict.get('Address', False): # used by the L2F Services Export
            self.visit_Address = visit_line[fnamedict['Address']] 
        if fnamedict.get('Street', False): # used by the normal L2F Export
            self.visit_Address = visit_line[fnamedict['Street']]
        if fnamedict.get('Latitude', False):
            self.lat = visit_line[fnamedict['Latitude']]
        if fnamedict.get('Longitude', False):
            self.lng = visit_line[fnamedict['Longitude']]
        if fnamedict.get('Housing Type', False):
            self.housing_type = visit_line[fnamedict['Housing Type']]
        if fnamedict.get('Postal Code', False):
            self.visit_Postal_Code = visit_line[fnamedict['Postal Code']]
        if fnamedict.get('Line 2', False):
            self.visit_Address_Line2 = visit_line[fnamedict['Line 2']]
        elif not fnamedict.get('Line 2', False): 
            # if the l1 and l2 are comma
            # separated then we can split
            # it on the comma, for some reason the xmas export 
            # concatenates the two address lines
            try:
                split_address = self.visit_Address.split(',')
                if len(split_address) == 2:
                    l1, l2 = split_address
                    self.visit_Address_Line2 = l2
                    self.visit_Address = l1
                else:
                    self.visit_Address = split_address[0]
                    self.visit_Address_Line2 = ''.join(split_address[1:])
            except:
                pass

        if fnamedict.get('Client Gender', False):
            self.main_applicant_Gender = visit_line[fnamedict['Client Gender']] # Main Applicant Gender
        if fnamedict.get('Client Phone Numbers', False):
            self.main_applicant_Phone = visit_line[fnamedict['Client Phone Numbers']].split(',') # Main Applicant Phone Numbers
        if fnamedict.get('Household Primary Income Source',False):
            self.household_primary_SOI = visit_line[fnamedict['Household Primary Income Source']]
        if fnamedict.get('Household ID', False):
            self.visit_Household_ID = str(visit_line[fnamedict['Household ID']])
        if fnamedict.get('Referrals Provided', False):
            self.visit_Referral = visit_line[fnamedict['Referrals Provided']] # Referrals Provided
        if fnamedict.get('HH Mem 1- ID', False):
            self.visit_Family_Slice = visit_line[fnamedict['HH Mem 1- ID']:]
        if fnamedict.get('Visited Agency', False):
            self.visit_Agency = visit_line[fnamedict['Visited Agency']]
        if fnamedict.get('Client Email Addresses', False):
            self.main_applicant_Email = visit_line[fnamedict['Client Email Addresses']]

        if december_flag: # enforce items and food lines in export existing
            self.xmas_ID = visit_line[fnamedict['Request ID']]
            self.xmas_notes = visit_line[fnamedict['Notes Recorded']]
            self.xmas_application_site = visit_line[fnamedict['Requesting Agency']]
            self.ex_reference = visit_line[fnamedict['External Reference']]


    # CASELOAD DATA TABLE RETURN FUNCTIONS
    def visit_table(self):
        '''
        returns data for insertion to the Visit_Table
        of the caseload database
        it depends on the PROVIDERS dictionary defined at the
        head of the file.  If the provider is not present, 
        it will pass on the value that has been extracted from
        the file.
        '''
        provider_code = PROVIDERS.get(self.visit_Agency, self.visit_Agency)
        return (self.visit_Household_ID, self.visit_Date, 
                provider_code, self.main_applicant_ID)

    def visit_address_table(self):
        '''
        returns data for insertion into the visit_Address_Table
        '''
        return (self.visit_Address, self.visit_Address_Line2, self.visit_City,
                self.visit_Postal_Code, self.housing_type)

    def visit_services(self):
        '''
        returns data for insertion into the Visit_Services
        '''
        return (self.quantity, self.foods_provided, self.items_provided,
                self.delivery, self.visit_Referral)

    def household_demo_table(self):
        return (self.visit_household_Size, self.languages,
                self.visit_household_Diet, self.housing_type,
               self.household_primary_SOI)

    def visit_coordinates_table(self):
        return (self.lat, self.lng, 0)

    def ma_person_table(self):
        '''
        returns values for inserting the main applicant into the Person_Table
        '''
        return (self.main_applicant_ID, self.main_applicant_Lname,
                self.main_applicant_Fname, self.main_applicant_DOB,
                self.main_applicant_Age, self.main_applicant_Gender,
                self.main_applicant_Ethnicity, self.main_applicant_Self_Identity, 
                self.immigration_date,  self.first_visit)

    @staticmethod
    def return_hvt_and_people(fam_tuples):
        '''
        takes the output of the .get_family_members() method
        and parses it for use in the caseload database
        returning two collections of tuples
        (((visit_services),...), ((person table),...))

        '''
        visit_services = []
        person_table = []
        if any(fam_tuples):

            for fam_member in fam_tuples:
                fm_id = fam_member[0]
                ln = fam_member[2] # ln and fn can swap?
                fn = fam_member[1] # double check!
                dob = fam_member[3]
                age = fam_member[4]
                gen = fam_member[5]
                eth = fam_member[6]
                ide = fam_member[7]
                rel = fam_member[8]
                imm = fam_member[9] #imm date

                vs_t = (fm_id, rel)
                pt_t = (fm_id, ln, fn, dob, age, gen, eth, ide, imm, '')
                visit_services.append(vs_t)
                person_table.append(pt_t)
        
        return tuple(v for v in visit_services), tuple(p for p in person_table)

    @staticmethod
    def get_special_string(notes_string, char_string= '##'):
        '''
        looks for the char_string in the notes_string
        and if it finds it, returns the value that the char_string
        brackets i.e if ##123## is in the notes string
        it will return 123

        if the char_string is not in the notes_string it will return 
        False
        or if something that is not an int is nestled between the marks
        then it will return false
        '''
        # if '##' in '##123##'
        if char_string in notes_string:
            # _, _, 123## = notes_string.partition...
            _, _, val_str_char_str = notes_string.partition(char_string)
            # 123, _, _ = ...
            val_str, _, _ = val_str_char_str.partition(char_string)
            
            try: 
                return int(val_str)
            except:
                return False
        else:
            return False

    @staticmethod
    def get_sms_target(sms_string):
        '''
        takes the string that likely contains a cell phone number and attempts 
        to parse and format it correctly for use in the sms message pipeline
        
        this method uses the phonenumbers library
        https://github.com/daviddrysdale/python-phonenumbers

        and should return a properly formated string '+15195555555' for use in
        the sms pipeline or False
        
        this method is used by the is_army() method to set the .sms_target
        attribute

        '''
        #if sms_string[0] != '1': sms_string = f'{1}{sms_string}'

        if sms_string:
            try:
                # imported phonenumbers library as pn
                n =  pn.parse(sms_string, 'US')
                if pn.is_valid_number(n):
                    # returns a formatted string
                    return pn.format_number(n, pn.PhoneNumberFormat.E164)
                else:
                    return False
            except Exception as sms_fail:
                if sms_string: print(f'invalid sms_input: {sms_string} = {sms_fail}')
                return False
        else:
            return None
    
    def get_address(self):
        '''
        returns the address for the visit formatted as a tuple address, city, postal code
        or returns a 3 tuple of False
        '''
        address_3tuple = (self.visit_Address, self.visit_City, self.visit_Postal_Code)
        if any(address_3tuple):
            return address_3tuple
        else:
            return (False, False, False)

    def get_coordinates(self):
        '''
        returns a tuple of (lat, lng) 
        or False if coordinates 
        were not provided
        '''
        coordinates = (self.lat, self.lng)
        if all(coordinates):
            return coordinates
        else:
            return False

    def get_HH_summary(self):
        '''
        returns a named tuple summary of the household for use in the route sorting 
        pipeline. this summary is inserted into the household object in the
        basket_sorting script and provides access to the key bits of data that we need 
        to construct a route card
        (applicant, fname, lname, fam size, phone, email, address1, address2,
        city, diet, sa_app, sms_target)

        it is input as a paramter for the instatiation of a Delivery_Household
        class in the basket_sorting_G.. file.  It can be used to populate the
        fields for a delivery card
        '''

        visit_sum = namedtuple('visit_sum', 'applicant, fname, lname, size, phone, email,\
                               address, address2, city, postal, diet, sa_app_num, sms_target')
        return visit_sum(self.main_applicant_ID,
                         self.main_applicant_Fname,
                         self.main_applicant_Lname,
                         self.visit_household_Size,
                         self.main_applicant_Phone,
                         self.main_applicant_Email,
                         self.visit_Address,
                         self.visit_Address_Line2,
                         self.visit_City, 
                         self.visit_Postal_Code,
                         self.visit_household_Diet,
                         self.sa_app_num,
                         self.sms_target)

    def get_hh_id_number(self):
        '''
        returns hh id field number
        '''
        return self.visit_Household_ID
    
    def get_applicant_ID(self):
        '''
        returns main applicant ID
        '''
        return self.main_applicant_ID
    
    def get_visit_date(self):
        '''
        returns the visit date
        '''
        return self.visit_Date

    def get_applicant_identity(self):
        '''
        returns a tuple of (Ethnicities, self-identifies as)
        '''
        return (self.main_applicant_Ethnicity, self.main_applicant_Self_Identity)

    def get_main_applicant(self):
        '''
        returns a tuple of information in the order necessary to setup a Person()
        '''
        return  (self.main_applicant_ID, 
                self.main_applicant_Lname, 
                self.main_applicant_Fname,
                self.main_applicant_DOB,
                self.main_applicant_Age,
                self.main_applicant_Gender,
                self.main_applicant_Ethnicity,
                self.main_applicant_Self_Identity,
                self.immigration_date)

    def get_family_members(self, header_object):
        '''
        return a list of 
        family members sliced into tuples formatted to create Person() objects        
        :param: header_object is a Field_Names object and we use
        the .return_family_sublice_len() method to find how long the 
        family member chunks are. This is important to know so we can
        correctly cut the line into the correct size lengths and then slot the
        data into tuples
        '''
        sub_slice_len = header_object.return_family_subslice_len()
        # create a dictionary of index numbers for each of the data points
        h_d = header_object.return_fam_header_indexes()

        family_members = parse_functions.create_list_of_family_members_as_tuples(self.visit_Family_Slice,
                                                                sub_slice_len, h_d)
        # 0 ID, 1 Lname, 2 Fname, 3 DOB, 4 Age, 5 Gender, 6 ethnicity, 7 identity,
        # 8 relationship, 9 immigration date
        return family_members 

    def get_add_city_app(self):
        '''
        returns tuple of (address, city, main_applicant)
        '''
        #print('{} {} {}'.format(self.visit_Address, self.visit_City, self.main_applicant_ID))
        return (self.visit_Address, self.visit_City, self.main_applicant_ID)

    def get_household_type(self, relationship_collection):
        '''
        returns a guess as to the household type
        single person
        single parent etc.
        :param: relationshiop_collection is a tuple of relationships extracted from the
        .get_family_members_list() method
        '''
        household_classification = parse_functions.household_classifier(relationship_collection)
        return household_classification 

    def is_hamper(self):
        '''
        returns the type of hamper - Baby (False) or Food (True)
        when the class is initialized, a helper function figures this out
        '''
        return self.visit_food_hamper_type # True or False
    
    def is_christmas_hamper(self, pu_list=PICKUP_AGENTS_HOF, 
                            service_list=SERVICE_AGENTS_HOF):
        '''
        Christmas_Hamper denotes a service offered by J Cramer's 
        home team rather than a 3rd party or sponsor. 
        This method performs a check to see if this is a Christmas Hamper
        by calling on methods of the Service_Agent object held 
        at the self.foods_provided attribute
        returns True or False
        if True, it will attempt to set the sms_target attribute
        and flip the toggles that identify this as either a delivery
        or a pickup hamper
        it is used in the logic coded in the christmas ops pipeline 
        to determine what to do with the line
        '''
        is_hof = False 
        provider = [] 
        food = False
        item = False
        if not self.foods_provided.blank:
            hof_food = set(service_list).intersection(set(self.foods_provided.providers))
            #print(f'hof_food: {hof_food}')
            if hof_food:
                food = True
                is_hof = True 
                provider += list(hof_food)
        if not self.items_provided.blank:
            hof_items = set(service_list).intersection(set(self.items_provided.providers))
            #print(f'hof_items: {hof_items}')
            if hof_items:
                item = True
                is_hof = True
                provider += list(hof_items)
        #print(f'is_hof: {is_hof}')
        if is_hof:
            # set sms number if present
            sms = Visit_Line_Object.get_sms_target(self.ex_reference)
            if sms:
                self.sms_target = sms
            # determine service type Delivery or Pickup
            pu_intersect = set(provider).intersection(set(pu_list))
            if pu_intersect:
                
                self.hof_zone = list(pu_intersect)[0] # will be HoF Zone 1 etc. 
                self.hof_pu_num = Visit_Line_Object.get_special_string(self.xmas_notes, '$$')
                #print(f'zoned! {self.hof_zone}, {self.hof_pu_num}')

            if 'House of Friendship Delivery' in provider: 
                self.delivery_h = True
            if food:
                for p in provider:
                    foods = self.foods_provided.lookup(p)
                    for f in foods:
                        if f: 
                            self.food_req[f] = p
            if item:
                for p in provider:
                    items = self.items_provided.lookup(p)
                    for i in items:
                        if i: 
                            self.item_req[i] = p 
        #print(f'*ln929 is_hof= db_data {self.hof_zone} {self.item_req} {self.food_req}')
        return is_hof

    def is_sponsored_hamper(self, food_sponsors=SERVICE_AGENTS_SPONSOR, 
                            toy_sponsors=SERVICE_AGENTS_GIFTS):
        '''
        performs a check to see if this a Sponsored Hamper
        by calling Service_Request methods held at the .foods_provided
        and .items_provided attributes

        returns a tuple:
            True, food_sponsor, toy_sponsor
            False, None, None
        '''
        food_sponsor = set(self.foods_provided.providers).\
                intersection(set(food_sponsors))
        gift_sponsor = set(self.items_provided.providers).\
                intersection(set(toy_sponsors))
        #print(f'ln954 food sponsor: {food_sponsor} gift sponsor: {gift_sponsor}')
        if food_sponsor or gift_sponsor:
            fs = None
            gs = None
            if any(food_sponsor):
                fs = list(food_sponsor)
                self.f_sponsor = fs
                for s in fs:
                    foods = self.foods_provided.lookup(s)
                    for f in foods:
                        if f:
                            self.food_req[f] = s
            if any(gift_sponsor):
                gs = list(gift_sponsor)
                self.g_sponsor = gs
                for s in gs:
                    gifts = self.items_provided.lookup(s)
                    for g in gifts:
                        if g:
                            self.item_req[g] = s
            #print('')
            #print(f'*ln965* is sponsored {self.hof_zone} {self.item_req} {self.food_req}')
            #print('')
            return (True, fs, gs)
        else:
            return (False, None, None)

    def is_army(self, army_label='KW Salvation Army'):
        '''
        looks to see if there has been a Salvation Army appointment
        booked, attempts to collect a sms_number and
        an appointment number, returning a three tuple of those values
        (sa_status, sms_target, app_number)
        all values have defaults of False so if one of the values is missing
        they will pass through.
        The three tuples should be able to pass an all() test at the point
        of use for it go forward into the sms pipleline
        
        the values created by this method are used by the Route_Database 
        class add_sa_appointment method to insert the app_num
        into the gift_table

        '''
        if army_label in self.items_provided.providers:
            self.sa_status = True
            self.sa_app_num = Visit_Line_Object.get_special_string(self.xmas_notes)
            if all((self.sa_status, self.sa_app_num)):

                sms = Visit_Line_Object.get_sms_target(self.ex_reference)
                if sms:
                    self.sms_target = sms
        #print(f'*ln995* is_army {self.sa_status} {self.sms_target} {self.sa_app_num}')
        return (self.sa_status, self.sms_target, self.sa_app_num)

    def get_services_dictionary(self):
        '''
        returns a dictionary of the attributs set by the three 
        methods
        is_army
        is_chrismtas_hamper
        is_sponsored_hamper

        '''
        return {'hof_zone': self.hof_zone, 'hof_pu_num': self.hof_pu_num,
             'item_req': self.item_req, 'food_req':self.food_req,
             'delivery_h': self.delivery_h, 'f_sponsor': self.f_sponsor,
             'g_sponsor': self.g_sponsor}

    def has_family(self):
        '''
        returns boolian value indicating False for single people, True for families
        '''
        
        return int(self.visit_household_Size) > 1

    def __str__(self):
        '''
        returns basic information about the visit
        visit date
        main applicant id
        '''
        return '{} {}'.format(self.visit_Date, 
                              self.main_applicant_ID)

class Visit():
    '''
    Contains data related to a visit : 
    a date, a main applicant, family_members, household_id, address, agency (if provided)
    most often we will only have an export from one agency, but sometimes it is possible
    to pool data from multiple service providers
    '''
    def __init__(self, vnumber, date, main_applicant, family_members, householdID, address, agency=None):
       self.vnumber = vnumber
       self.vdate = date
       self.main_applicant = main_applicant
       self.family_members = family_members
       self.householdID = householdID
       self.address = address
       self.service_provider = agency

    def get_address(self):
        '''
        returns the tuple of (street address, city, pcode)
        '''
        return self.address

    def get_city(self):
        '''
        returns the city only from the address tuple collection
        '''
        return self.address[1]
    
    def get_postalcode(self):
        return self.address[2]

    def get_visit_month(self):
        pass

    def get_family_size(self):
        '''
        returns the family size as an int based on adding len(familymembers) + 1 
        '''
        return len(self.family_members) + 1
    
    def get_people_served(self):
        '''
        returns a tuple of the file ids of the people served
        '''
        pass

    def __str__(self):
        return '{} had main applicant {} and was provided by {}'.format(self.vnumber, 
                                                                     self.main_applicant, 
                                                                     self.service_provider)

    def __repr__(self):
        return 'Visit #{} Had applicant: {}'.format(self.vnumber, self.main_applicant)

class Export_File_Parser():
    '''
    a file object for the L2F export. This will be the single method for interacting 
    with the export and getting the information we need out of it

    it has methods to: 
        open a csv file
        and provides a way to iterate over it and create visit line objects
        or can 
        parse the line into visits, households and people
        and feed the data into a database

    file_path = path to csv file
    header_names = a Field_Names object

    '''   
    
    # DICTIONARIES TO STORE OBJECTS
    Person_Table = dict()
    Household_Table = dict()
    Visit_Table = dict()
    
    def __init__(self, file_path, header_names, start_counter_at = 1):
        self.path = file_path
        self.file_object = None # csv reader object set by open_file method
        self.headers = header_names.ID # dictionary from a Field_Names object
        self.header_object = header_names
        self.line_counter = start_counter_at # the index for the visits 1 = visit one
        self.visit_structure = None # some sort of container yet to be determined
        self.summary_profile_object = None                
    
    def __iter__(self):
        for line in self.file_object:
            yield line

    def open_file(self):
        '''
        opens the csv file and sets the file_object variable 
        to be the file minus headers
        '''
        csv_file = open(self.path, newline='')
        visit_reader = csv.reader(csv_file)
        next(visit_reader, None) # skip headers
        print('File Open.')
        self.file_object = visit_reader               

    def parse_visits(self):
        '''
        opens the file and parses it into visits
        and sub objects
        adds things to the visit_structure Export_File Class variable
        '''
        if self.file_object:
            for visit_line in self.file_object:
                # extract people in visit
                # create person objects for them
                # extract HH_ID
                # make a Household object
                # assign a visit to that Household
                line_number = str(self.line_counter)
                line_object = Visit_Line_Object(visit_line, self.headers)
                vdate = line_object.get_visit_date()
                mapp = None # main applicant
                famapp = None # family memmbers
                people_in_visit = [] # list of tuples
                household_id_number = None
                visit_address = None
                people_in_visit_id_list = []
                household_object = None
                visit_object = None
                
                if line_object.is_hamper():
                    mapp = line_object.get_main_applicant()
                    people_in_visit.append(mapp)                     
                    visit_address = line_object.get_address()
                    household_id_number = line_object.get_hh_id_number()
                if line_object.has_family():
                    famapp = line_object.get_family_members(self.header_object)
                    people_in_visit.extend(famapp)

                for individual in people_in_visit:
                    file_id_number = str(individual[0])
                    people_in_visit_id_list.append(file_id_number)
                    if file_id_number not in Export_File_Parser.Person_Table.keys():
                        Export_File_Parser.Person_Table[file_id_number] = Person(individual)
                        # need some code to map out relationships                                        

                    Export_File_Parser.Person_Table[file_id_number].add_HH_visit(household_id_number)

                if household_id_number not in Export_File_Parser.Household_Table.keys():
                    household_object = Household(household_id_number)
                    Export_File_Parser.Household_Table[household_id_number] = household_object
                
                if line_number not in Export_File_Parser.Visit_Table.keys():
                    visit_object = Visit(line_number, vdate, mapp, famapp, household_id_number, visit_address)
                    Export_File_Parser.Visit_Table[line_number] = visit_object

                household_object.add_members(people_in_visit_id_list)
                household_object.add_visit(line_number, visit_object)

                self.line_counter +=1               

                
        else:
            print('The file {} has not been opened yet.'.format(self.path))



if __name__ == "__main__":
    fnames = Field_Names('header_config.csv') # open header config file
    fnames.init_index_dict() # build the dictionary of header name : index
    L2F_2017 = Export_File_Parser('test_export.csv',fnames) 
    L2F_2017.open_file() # open the file
    L2F_2017.parse_visits() # parse the visits into the different objects
    print(L2F_2017.Person_Table)
    print(L2F_2017.Household_Table)
    print(L2F_2017.Visit_Table)
