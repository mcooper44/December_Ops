def hamper_type_parser(days_of_food):
    '''
    takes the days of food provided by L2F and attempts to guess at the hamper type
    typically 1 = baby hamper + no food, and 3 = full food hamper
    '''
    return days_of_food > 1
        
def diet_parser(diet_string):
    '''
    this function returns the string of redundant dietary conditions
    minus the redundant conditions
    '''
    diet_conditions = set(diet_string.split(',')) # a unique list of conditions split on the commas
    if 'Other' in diet_conditions:
        diet_conditions.remove('Other') # we don't want this because it has zero information

    return ', '.join(diet_conditions)

def extract_families_into_list_slices(l,n):
    '''
    takes a list slice of family member information from a visit and produces a generator
    that yields list slices of the distinct family member.
    l = a visit in the export sliced to the family details section i.e. visit[4:]
    n = the len of the chunk of of the visit that has a distinct family members details

    i.e. a slice = [12345,'smith','matt',45678, 'smith','erin'...]
    this function will create a generator that yields [12345,'smith','matt'], [45678, 'smith','erin']

    the generator can then be used to further extract family data from the sub lists    
    '''
    for i in range(0,len(l), n):
        yield l[i:i +n]


def create_list_of_family_members_as_tuples(family_range_frm_visit, len_of_family_sub_slice, h_d):
    '''    
    :param: family_range_frm_visit = list slice of family member details
    This functions filters the family and returns a list of tuples that
    can then be turned into Person objects
    :param: len_of_family_sub_slice = number generated by Field_Names 
    that indicates the length of the family member details on the visit line
    :param: h_d = a dictionary created by the Field_Names object to locate
    the indexes of where the key bits of info are located.  This avoids
    having to manually calibrate where items are and allows the use of 
    exports that have super.  It is made up of header: index value 
    pairs this function uses to find the values 
    '''
    # create a nested list of family member chunks via the extract_families function
    nested_list_of_family_data = [x for x in extract_families_into_list_slices(family_range_frm_visit, len_of_family_sub_slice)]    
    
    list_of_family_member_tuples = [] # where we will hold on to the tuples
    # checks to see that the slice of family data is the right length and 
    # avoid index errors
    it_is_a_full_slice = lambda x : len(x) == len_of_family_sub_slice # return True if len x == 11
 
    for family_member in nested_list_of_family_data:
        if it_is_a_full_slice(family_member): # if it is a full list then proceed 
    
            f_set = [] # container for building the slice
            for k in h_d.keys(): # for key in dictionary of header: index
                if h_d.get(k, False) or h_d.get(k, False) == 0: # see if there is a value or None
                    f_set.append(family_member[h_d[k]]) # if so append the datapoint in the slice
                else:
                    # otherwise swap None in because that header was not used
                    f_set.append(None) 
        
        if any(f_set):
            list_of_family_member_tuples.append(tuple(f_set)) # add the tuple 
    
    # ID, Lname, Fname, DOB, Age, Gender, Ethnicity, Identity
    return list_of_family_member_tuples # return the list of tuples


def any_one_in(tup_1, tup_2):
    '''
    takes 2 tuples and checks to see if any elements from tup_1 is in tup_2
    this function is used by the household_classifier() function
    there is probably a better way to do this, but it's Friday afternoon and 
    *effort*
    '''
    value_flag = False
    for x in tup_1:
        if x in tup_2:
            value_flag = True
    return value_flag

def household_classifier(tuple_of_relationships):
    '''
    takes a tuple_of_relationships and returns a guess at the type of relationship
    '''
    guess = 'unknown' # default is 'unknown' in case my logic sucks - i still don't break things

    potential_ = {'partners' : ('boyfriend_girlfriend', 'common_law_partner', 'spouse'),
                  'kids' : ('child',),
                  'mom_dad' : ('parent',),
                  'brother_sister' : ('sibling',),
                  'gran_grandad' : ('grandparent',),
                  'grandchild' : ('grandchild',),
                  'unknowable' : ('roommate', 'undisclosed', 'friend','other','other_relative')
                  }

    relationship_map = {'partner' : False,
                        'kids' : False,
                        'room_mates' : False,
                        'grandchildren' : False,
                        'parent' : False,
                        'grand_parent' : False,
                        'unclear' : False,
                        'siblings' : False,
                        'both_parents' : False
                        }
    # filter the relationships using some logic to set the relationship_map bool values
    if any_one_in(potential_['partners'], tuple_of_relationships):
        relationship_map['partner'] = True
    if any_one_in(potential_['kids'], tuple_of_relationships):
        relationship_map['kids'] = True
    if any_one_in(potential_['mom_dad'], tuple_of_relationships):
        relationship_map['parent'] = True
    if any_one_in(potential_['brother_sister'], tuple_of_relationships):
        relationship_map['siblings'] = True
    if any_one_in(potential_['gran_grandad'], tuple_of_relationships):
        relationship_map['grand_parent'] = True
    if any_one_in(potential_['grandchild'], tuple_of_relationships):
        relationship_map['grandchildren'] = True
    if any_one_in(potential_['unkowable'], tuple_of_relationships):
        relationship_map['unclear'] = True
    
    # now use some logic to parse the household type depending on the presences 
    # of certain bool values
    if relationship_map['kids']:
        if relationship_map['partner']:
            guess = 'two_parent_household'
        if relationship_map['parent'] or relationship_map['grand_parent']:
            guess = 'intergenerational_single_parent_household'
        else:
            guess = 'single_parent'
    
    elif relationship_map['partner'] and not relationship_map['kids']:
        guess = 'two_adult_household_no_kids'

    elif not any(relationship_map.values()):
        guess = 'single_person_household'

    return guess
