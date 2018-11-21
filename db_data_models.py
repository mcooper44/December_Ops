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
import db_tuple_collection as tuple_collection
import csv
from collections import Counter, defaultdict, namedtuple

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
                'relationship': None
                 }

        targets = ['HH Mem 1- Last Name','HH Mem 1- First Name',
                   'HH Mem 1- Date of Birth', 'HH Mem 1- Age',
                   'HH Mem 1- Gender','HH Mem 1- Ethnicities',
                   'HH Mem 1- Self-Identifies As','HH Mem 1- Relationship']

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

class Person():
    '''
    A person is a part of a household, they have a profile of personal information
    and a relationship to a main applicants and an implied relationship to other 
    household members
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
        household members into appropriate adult/child buckets
        '''
        return int(self.person_Age) >= Age

    def get_base_profile(self):
        '''
        returns a tuple of (ID, Fname, Lname, Age)

        this method is used for inserting family member information in a
        Route_Database() object coded in basket sorting_Geocodes.py
        
        it is called in december_ops
        '''
        return (self.person_ID, self.person_Fname, self.person_Lname,
                self.person_Age)

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
        bool_self_ident_profile = tuple_collection.self_identifies_profile(disabled, immigrant, other, no, undisco)
        return bool_self_ident_profile

    def Get_Ethno_Profile_Tuple(self):
        '''
        returns a named tuple of with values True or False for the different categories
        'person_ethno_profile', 'visible_minority, first_nations, metis, inuit, NA, undisclosed'
        '''
        nat_applic = False
        undisco = False
        visi_minor = False
        first_nat = False
        metis = False
        inuit = False
        if 'not_applicable' in self.person_Ethnicity:
            nat_applic = True
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
        bool_ethno_profile = tuple_collection.person_ethno_profile(visi_minor, first_nat, metis, inuit, nat_applic, undisco)
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
    :param: visit_line = a line from a csv
    :param: fnamedict = a dictionary of headernames: index number - it is used
    to reference where data points are on the visit_line
    :param: december_flag = a marker to toggle looking for Christmas related
    headers

    '''    
    
    def __init__(self, visit_line, fnamedict, december_flag = False): # line, dict of field name indexes, is Xmas?
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
        self.household_primary_SOI = None # Client Primary Source of Income
        self.visit_Address = visit_line[fnamedict['Address']]
        self.visit_Address_Line2 = None
        self.visit_City = visit_line[fnamedict['City']]
        self.visit_Postal_Code = None
        self.visit_Household_ID = None  # Household ID - the unique file number used to identify households
        self.visit_household_Size = visit_line[fnamedict['Household Size']] # The Number of people included in the visit
        self.visit_household_Diet = None # Dietary Conditions in a readable form
        self.visit_food_hamper_type = None 
        self.visit_Referral = None # Referrals Provided
        self.visit_Family_Slice = None
        self.visit_Agency = None # organization that provided services
        self.HH_main_applicant_profile = None
        self.HH_family_members_profile = None
        self.food = None
        self.xmas_ID = None
        self.xmas_food_provided = None
        self.xmas_items_provided = None
        self.xmas_notes = None
        self.xmas_application_site = None
       
        if fnamedict.get('Dietary Considerations', False):
            self.visit_household_Diet = parse_functions.diet_parser(visit_line[fnamedict['Dietary Considerations']]) # Dietary Conditions in a readable form
        if fnamedict.get('Client Ethnicities', False):
            self.main_applicant_Ethnicity = visit_line[fnamedict['Client Ethnicities']]
        if fnamedict.get('Client Self-Identifies As', False):
            self.main_applicant_Self_Identity = visit_line[fnamedict['Client Self-Identifies As']] 
        if fnamedict.get('Quantity', False):
            self.visit_household_Diet = parse_functions.hamper_type_parser(int(fnamedict['Quantity'])) # Quantity of food parsed to be Food or Baby 3 = hamper 1 = baby hamper
        if fnamedict.get('Visit Date', False):
            self.visit_Date = visit_line[fnamedict['Visit Date']] # Visit Date
        if fnamedict.get('Client First Name', False):
            self.main_applicant_Fname = visit_line[fnamedict['Client First Name']] # Main Applicant First Name
        if fnamedict.get('Client Last Name', False):
            self.main_applicant_Lname = visit_line[fnamedict['Client Last Name']] # Main Applicant Last Name
        if fnamedict.get('Client Date of Birth', False):
            self.main_applicant_DOB = visit_line[fnamedict['Client Date of Birth']] # Main Applicant Date of Birth
        if fnamedict.get('Line 2', False):
            self.visit_Address_Line2 = visit_line[fnamedict['Line 2']]
        elif not fnamedict.get('Line 2', False): 
            # if the l1 and l2 are comma
            # separated then we can split
            # it on the comma
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
        if visit_line[fnamedict['Client Phone Numbers']]:
            self.main_applicant_Phone = visit_line[fnamedict['Client Phone Numbers']].split(',') # Main Applicant Phone Numbers
        if fnamedict.get('Client Primary Income Source',False):
            self.household_primary_SOI = visit_line[fnamedict['Client Primary Income Source']]
        if fnamedict.get('Postal Code', False):
            self.visit_Postal_Code = visit_line[fnamedict['Postal Code']]
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
        if fnamedict.get('Foods Provided', False):
            self.food = visit_line[fnamedict['Foods Provided']]

        if december_flag:
            self.xmas_ID = visit_line[fnamedict['Request ID']]
            self.xmas_food_provided = visit_line[fnamedict['Foods Provided']]
            self.xmas_items_provided = visit_line[fnamedict['Items Provided']]
            self.xmas_notes = visit_line[fnamedict['Notes Recorded']]
            self.xmas_application_site = visit_line[fnamedict['Requesting Agency']]

    def get_address(self):
        '''
        returns the address for the visit formatted as a tuple address, city, postal code
        '''
        address_3tuple = (self.visit_Address, self.visit_City, self.visit_Postal_Code)
        if any(address_3tuple):
            return address_3tuple
        else:
            return (False, False, False)

    def get_HH_summary(self):
        '''
        returns a named tuple summary of the household for use in the route sorting 
        pipeline. this summary is inserted into the household object in the
        basket_sorting script and provides access to the key bits of data that we need 
        to construct a route card
        (applicant, fname, lname, fam size, phone, email, address1, address2,
        city, diet)
        '''
        visit_sum = namedtuple('visit_sum', 'applicant, fname, lname, size, phone, email,\
                               address, address2, city, postal, diet')
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
                         self.visit_household_Diet)

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

    def has_family(self):
        '''
        returns boolian value indicating False for single people, True for families
        '''
        
        return int(self.visit_household_Size) > 1

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
                self.main_applicant_Self_Identity)

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
        h_d = header_object.return_fam_header_indexes()
        family_members = parse_functions.create_list_of_family_members_as_tuples(self.visit_Family_Slice,
                                                                sub_slice_len,
                                                                h_d)
        return family_members

    def is_hamper(self):
        '''
        returns the type of hamper - Baby (False) or Food (True)
        when the class is initialized, a helper function figures this out
        '''
        return self.visit_food_hamper_type # True or False
    
    def is_christmas_hamper(self):
        '''
        performs a check to see if this is a Christmas Export
        return True if so, or False

        it is used in the logic coded in the christmas ops pipeline 
        to determine what to do with the line
        '''
        if self.food:
            return 'Christmas Hamper|Emergency Hampers' in self.food

    def is_sponsored_hamper(self, 
                            food_sponsors=('DOON', 'REITZEL'), 
                            toy_sponsors=('Sertoma',)):
        '''
        performs a check to see if this a Sponsored Hamper
        returns a tuple:
            True, food_sponsor, toy_sponsor
            False, None, None
        '''
        food, toys = self.food, self.xmas_items_provided
        sponsored, food_provider, toy_provider = False, None, None
        if any([food, toys]):
            for sponsor in food_sponsors:
                if sponsor in food:
                    sponsored = True
                    food_provider = sponsor
            for sponsor in toy_sponsors:
                if sponsor in toys:
                    sponsored = True
                    toy_provider = sponsor
        return (sponsored, food_provider, toy_provider)

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

    def set_summary_of_visits(self):
        '''
        creates a statiscal summary of the visits in the file
        # of Hampers by month
        # of HH by month
        # of HH for duration
        # of people served by month
        # of people served for duration
        and stores it in the self.summary_profile_object
        '''
        pass
    
    def get_summary_of_visits(self):
        '''
        returns the summary_of_visits data structure
        '''
        if self.summary_profile_object:
            return self.summary_profile_object
        else:
            print('there is no summary.  Use the set_summary_of_visits method to generate one.')
            return None


if __name__ == "__main__":
    fnames = Field_Names('header_config.csv') # open header config file
    fnames.init_index_dict() # build the dictionary of header name : index
    L2F_2017 = Export_File_Parser('test_export.csv',fnames) 
    L2F_2017.open_file() # open the file
    L2F_2017.parse_visits() # parse the visits into the different objects
    print(L2F_2017.Person_Table)
    print(L2F_2017.Household_Table)
    print(L2F_2017.Visit_Table)
