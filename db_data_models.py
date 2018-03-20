import parse_functions
import tuple_collection
import csv

class Person():
    '''
    A person is a part of a household, they have a profile of personal information
    and a relationship to a main applicant
    '''    
    all_persons_set = set() # a set of all id's as integers

    def __init__(self, person_summary):
        self.person_ID = person_summary[0] # HH Mem X - ID
        self.person_Fname = person_summary[3] # HH Mem X - Fname 
        self.person_Lname = person_summary[2] # HH Mem X - Lname
        self.person_DOB = person_summary[4] # HH Mem X - Date of Birth
        self.person_Age = person_summary[5] # HH Mem X - Age
        self.person_Gender = person_summary[6] # HH Mem X - Gender
        self.person_Ethnicity = person_summary[7] # HH Mem X - Ethno
        self.person_Idenifies_As = person_summary[8] # HH Mem X - Disable etc.
        self.person_Relationship_tag = person_summary[10] # HH Mem X - spouse etc.
        self.person_HH_membership = person_summary[42] # Household ID
        Person.all_persons_set.add(int(self.person_ID))

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
    
    def return_relationship_identity(self):
        '''
        a function that will attempt to determine the relationship this person has to other people in the household
        and return a tuple of (relationship_identity, ID_of_person_they_have_that_relationships_with)  
        i.e. (spouse, 123456) perhaps in relationship to a household?
        '''
        pass

class Household():
    '''
    A Household has an ID, 
    contains people, and their relationships, 
    has visit dates and a total of all visits
    has dietary conditions, sources of income
    referrals provided and ethnicities
    '''
    
    def __init__(self, Household_ID, Household_Main_Applicant, Household_Members):
        self.Household_ID = Household_ID
        self.Household_Main_Applicant = Household_Main_Applicant
        self.Household_Members = Household_Members # list of file ID's
        self.Household_Relationships = None # TO DO: this will be some sort of datastructure 
        
        

class Visit_Line_Object():
    '''
    takes a line from the csv export 
    a visit contains a household, visit date and hamper type
    it has a number of methods to 
    aggregate household data
    create or add information to households

    '''    
    visit_range_household_IDs= set() # set of all the HH ID's as integers

    def __init__(self, visit_line):
        self.visit_Date = visit_line[0] # Visit Date
        self.main_applicant_ID = visit_line[1] # Main Applicant ID
        self.main_applicant_Fname = visit_line[4] # Main Applicant First Name
        self.main_applicant_Lname = visit_line[3] # Main Applicant Last Name
        self.main_applicant_DOB = visit_line[5] # Main Applicant Date of Birth
        self.main_applicant_Age = visit_line[6] # Main Applicant Age
        self.main_applicant_Gender = visit_line[7] # Main Applicant Gender
        self.main_applicant_Phone = visit_line[9].split(',') # Main Applicant Phone Numbers
        self.main_applicant_Ident_Status = visit_line[10:13] # Marital status, Ethnicities, Self-identifies as
        self.household_primary_SOI = visit_line[14] # Client Primary Source of Income
        self.visit_Address = visit_line[38]
        self.visit_City = visit_line[39]
        self.visit_Postal_Code = visit_line[40]
        self.visit_Household_ID = int(visit_line[42]) # Household ID - the unique file number used to identify households
        self.visit_household_Size = visit_line[43] # The Number of people included in the visit
        self.visit_household_Diet = parse_functions.diet_parser(visit_line[45]) # Dietary Conditions in a readable form
        self.visit_food_hamper_type = parse_functions.hamper_type_parser(int(visit_line[46])) # Quantity of food parsed to be Food or Baby 3 = hamper 1 = baby hamper
        self.visit_Referral = visit_line[50] # Referrals Provided
        self.visit_Family_Slice = visit_line[51:]
        self.HH_main_applicant_profile = None
        self.HH_family_members_profile = None       
    
    def get_main_applicant(self):
        '''
        returns a tuple of information in the order necessary to setup a Person()
        '''
        return (self.main_applicant_ID, 
                self.main_applicant_Lname, 
                self.main_applicant_Fname,
                self.main_applicant_DOB,
                self.main_applicant_Age,
                self.main_applicant_Gender,
                self.main_applicant_Ident_Status[1],
                self.main_applicant_Ident_Status[2],
                'main applicant',
                self.visit_Household_ID)
            
    def get_family_members_list(self):
        '''
        return a list of 
        family members sliced into tuples formatted to create Person() objects        
        
        '''
        tuple_list_of_family_members = parse_functions.create_list_of_family_members_as_tuples(self.visit_Family_Slice, 
                                                                                               self.visit_Household_ID)
        
        return tuple_list_of_family_members

    def get_hamper_type(self):
        '''
        returns the type of hamper - Baby or Food
        when the class is initialized, a helper function figures this out
        '''
        return self.visit_food_hamper_type

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
    pass
    
class Export_File():
    '''
    a file object for the L2F export. This will be the single method for interacting 
    with the export and getting the information we need out of it

    it has methods to: 
        open a csv
        parses the visits into visits, households and people
        feeds the data into the database

    '''   
    def __init__(self, file_path, start_counter_at = 1):
        self.path = file_path
        self.file_object = None
        self.visit_counter = start_counter_at
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
        visit_count = self.visit_counter # start counting visits at 1 by default
        if self.file_object:
            for _line in self.file_object:
                visit_object = Visit_Line_Object(_line)
                # do stuff with the Visit() like use it to build the datastructure
                _main = visit_object.get_main_applicant()
                _family = visit_object.get_family_members_list()
                _family_ids = [x[0] for x in _family]
                relationships = tuple([x[10] for x in _family])
                _hh_type = visit_object.get_household_type(relationships)

                
                
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


