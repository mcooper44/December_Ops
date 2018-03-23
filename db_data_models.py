import db_parse_functions as parse_functions
import db_tuple_collection as tuple_collection
import csv
from collections import Counter, defaultdict

class Field_Names():
    '''
    Provides a way to configure the field name indexes for the file and avoids having to 
    make changes to the order of export and change lots of numbers in the different objects
    '''
    def __init__(self, config_file):
        self.config_file= config_file # path to the config file
        self.ID = dict() # a dictionary of field name : index number as int()

    def init_index_dict(self):
        '''
        this method creates a dictionary using the config_file that can be used to extract
        data points from the line and provides an easier way to keep track of where the
        varioius fields are.
        the config file should be a csv of: 
        field name, 0
        field name, 1
        field name, 2...
        '''
        with open(self.config_file) as f:
            name_ID_reader = csv.reader(f)
            for row in name_ID_reader:
                self.ID[row[0]] = int(row[1])

class Person():
    '''
    A person is a part of a household, they have a profile of personal information
    and a relationship to a main applicants and an implied relationship to other 
    household members
    '''    

    def __init__(self, person_summary):
        self.person_ID = person_summary[0] # HH Mem X - ID
        self.person_Fname = person_summary[3] # HH Mem X - Fname 
        self.person_Lname = person_summary[2] # HH Mem X - Lname
        self.person_DOB = person_summary[4] # HH Mem X - Date of Birth
        self.person_Age = person_summary[5] # HH Mem X - Age
        self.person_Gender = person_summary[6] # HH Mem X - Gender
        self.person_Ethnicity = person_summary[7] # HH Mem X - Ethno
        self.person_Idenifies_As = person_summary[8] # HH Mem X - Disable etc.
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
    referrals provided and ethnicities
    '''
    
    def __init__(self, Household_ID):
        self.Household_ID = Household_ID
        self.Visits = {} # a dictionary of visit objects keyed off of the seq. visit_id
        self.Member_Roles = defaultdict(list) # TO DO: this will be some sort of datastructure Adults : [], Children : [] etc.
        self.Member_Set = set()
        

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
        
class Visit_Line_Object():
    '''
    takes a line from the csv export 
    a visit is made up of people, organinzed in a household
    a visit happens on a specific date and describes key services

    The visit line object should extract this information and provides methods
    for structuring it, so that person, HH and Visit objects can be created

    '''    

    def __init__(self, visit_line, fnamedict): # line, dict of field name indexes
        self.visit_Date = visit_line[fnamedict['Visit Date']] # Visit Date
        self.main_applicant_ID = visit_line[fnamedict['Client ID']] # Main Applicant ID
        self.main_applicant_Fname = visit_line[fnamedict['Client First Name']] # Main Applicant First Name
        self.main_applicant_Lname = visit_line[fnamedict['Client Last Name']] # Main Applicant Last Name
        self.main_applicant_DOB = visit_line[fnamedict['Client Date of Birth']] # Main Applicant Date of Birth
        self.main_applicant_Age = visit_line[fnamedict['Client Age']] # Main Applicant Age
        self.main_applicant_Gender = visit_line[fnamedict['Client Gender']] # Main Applicant Gender
        self.main_applicant_Phone = visit_line[fnamedict['Client Phone Numbers']].split(',') # Main Applicant Phone Numbers
        self.main_applicant_Ethnicity = visit_line[fnamedict['Client Ethnicities']]
        self.main_applicant_Self_Identity = visit_line[fnamedict['Client Self-Identifies As']] 
        self.household_primary_SOI = visit_line[fnamedict['Client Primary Income Source']] # Client Primary Source of Income
        self.visit_Address = visit_line[fnamedict['Address']]
        self.visit_City = visit_line[fnamedict['City']]
        self.visit_Postal_Code = visit_line[fnamedict['Postal Code']]
        self.visit_Household_ID = str(visit_line[fnamedict['Household ID']]) # Household ID - the unique file number used to identify households
        self.visit_household_Size = visit_line[fnamedict['Household Size']] # The Number of people included in the visit
        self.visit_household_Diet = parse_functions.diet_parser(visit_line[fnamedict['Dietary Considerations']]) # Dietary Conditions in a readable form
        self.visit_food_hamper_type = parse_functions.hamper_type_parser(int(fnamedict['Quantity'])) # Quantity of food parsed to be Food or Baby 3 = hamper 1 = baby hamper
        self.visit_Referral = visit_line[fnamedict['Referrals Provided']] # Referrals Provided
        self.visit_Family_Slice = visit_line[fnamedict['HH Mem 1- ID']:]
        self.HH_main_applicant_profile = None
        self.HH_family_members_profile = None      

    def get_address(self):
        '''
        returns the address for the visit formatted as a tuple address, city, postal code
        '''
        return (self.visit_Address, self.visit_City, self.visit_Postal_Code)

    def get_hh_id_number(self):
        '''
        returns hh id field number
        '''
        return self.visit_Household_ID

    def get_visit_date(self):
        '''
        returns the visit date
        '''
        return self.visit_Date

    def has_family(self):
        '''
        returns boolian value indicating False for single people, True for families
        '''
        if self.visit_household_Size == 1:
            return False
        else:
            return True

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
            
    def get_family_members(self):
        '''
        return a list of 
        family members sliced into tuples formatted to create Person() objects        
        
        '''
        tuple_list_of_family_members = parse_functions.create_list_of_family_members_as_tuples(self.visit_Family_Slice)
        
        return tuple_list_of_family_members

    def is_hamper(self):
        '''
        returns the type of hamper - Baby (False) or Food (True)
        when the class is initialized, a helper function figures this out
        '''
        return self.visit_food_hamper_type # True or False

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
    a date, a main applicant, family_members, household_id, a type
    '''
    def __init__(self, vnumber, date, main_applicant, family_members, householdID, address):
       self.vnumber = vnumber
       self.vdate = date
       self.main_applicant = main_applicant
       self.family_members = family_members
       self.householdID = householdID
       self.address = address

class Export_File():
    '''
    a file object for the L2F export. This will be the single method for interacting 
    with the export and getting the information we need out of it

    it has methods to: 
        open a csv
        parses the visits into visits, households and people
        feeds the data into the database

    '''   
    Person_Table = dict()
    Household_Table = dict()
    Visit_Table = dict()
    
    def __init__(self, file_path, header_names, start_counter_at = 1):
        self.path = file_path
        self.file_object = None
        self.headers = header_names
        self.line_counter = start_counter_at
        self.visit_structure = None # some sort of container yet to be determined
        self.summary_profile_object = None                
    
    def open_file(self):
        '''
        opens the csv file and sets the file_object variable 
        to be the file minus headers
        '''
        try:
            with open(self.path, newline='') as csv_file:
                visit_reader = csv.reader(csv_file)
                next(visit_reader, None) # skip headers
                print('File Open.')
                self.file_object = visit_reader               

        except:
            print('...Error in opening csv file...')

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
                mapp = None
                famapp = None
                people_in_visit = [] # list of tuples
                household_id_number = None
                visit_address = None
                people_in_visit_id_list = []
                household_object = None
                visit_object = None
                
                if line_object.is_hamper():
                    mapp = line_object.get_main_applicant
                    people_in_visit.append(mapp)                     
                    visit_address = line_object.get_address()
                    household_id_number = line_object.get_hh_id_number()
                if line_object.has_family():
                    famapp = line_object.get_family_members()
                    people_in_visit.extend(famapp)

                for individual in people_in_visit:
                    file_id_number = str(individual[0])
                    people_in_visit_id_list.append(file_id_number)
                    if file_id_number not in Export_File.Person_Table.keys():
                        Export_File.Person_Table[file_id_number] = Person(individual)
                        # need some code to map out relationships                                        

                    Export_File.Person_Table[file_id_number].add_HH_visit(household_id_number)

                if household_id_number not in Export_File.Household_Table.keys():
                    household_object = Household(household_id_number)
                    Export_File.Household_Table[household_id_number] = household_object
                
                if line_number not in Export_File.Visit_Table.keys():
                    visit_object = Visit(line_number, vdate, mapp, famapp, household_id_number, visit_address)
                    Export_File.Visit_Table[line_number] = visit_object

                household_object.add_members(people_in_visit_id_list)
                household_object.add_visit(line_number, visit_object)

                self.line_counter +=1               

                
        else:
            print('The file {} has not been opened yet.'.format(self.path))

    def log_visits_to_DB(self):
        '''
        This function will log the visit to the database.
        Perhaps this should get moved somewhere else?
        '''
        pass

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
    fnames = Field_Names('header_config.csv')
    fnames.init_index_dict()
    L2F_2017 = Export_File('Dummy_File.csv',fnames)
    L2F_2017.open_file()
    L2F_2017.parse_visits()

