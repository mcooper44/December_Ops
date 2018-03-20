

def hamper_type_parser(days_of_food):
    '''
    takes the days of food provided by L2F and attempts to guess at the hamper type
    typically 1 = baby hamper + no food, and 3 = full food hamper
    '''
    if days_of_food == 1:    
        return 'Baby Hamper'
    else:
        return 'Food Hamper'

def diet_parser(diet_string):
    '''
    this function returns the string of redundant dietary conditions
    minus the redundant conditions
    '''
    diet = str() # what will be returned
    diet_conditions = set(diet_string.split(',')) # a unique list of conditions split on the commas
    if 'Other' in diet_conditions:
        diet_conditions.remove('Other') # we don't want this because it has zero information

    for condition in diet_conditions: # for each issue/preference
        if condition: # if it's not blank
            diet = diet + condition + ', ' # add it to the string that will be returned

    return diet[:-2].strip()  # return the string minus the trailing comma and extra spaces

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


def create_list_of_family_members_as_tuples(family_range_frm_visit, hh_identity_num, len_of_family_sub_slice=11):
    '''    
    family_range_frm_visit = list slice of family member details
    This functions filters the family and returns a list of tuples that
    can then be turned into Person objects
    it filters out any incomplete slices that may contain blank strings
    '''
    # create a nested list of family member chunks via the extract_families function
    nested_list_of_family_data = [x for x in extract_families_into_list_slices(family_range_frm_visit, len_of_family_sub_slice)]    
    # then we need to ensure that the chunks are the right shape
    # so create a container  
    list_of_family_member_tuples = []
    # and iterate through the lists
    it_is_a_full_slice = lambda x : len(x) == 11 # return True if len x == 11
    for family_member in nested_list_of_family_data:
        #print(family_member)
        if it_is_a_full_slice(family_member): # if it is a full list then proceed 
            # us a tool to filter out and format the family member info in the order of a Person()
            family_slice_lambda = lambda x : (x[0], x[3], x[2], x[4], x[5], x[6], x[7], x[8], x[10], hh_identity_num)
            # and add it to the container in the order of a Person()           
            list_of_family_member_tuples.append(family_slice_lambda(family_member))
    # finally return the list of family member tuples.  They can now be turned into person
    # objects when we need to do that
    return list_of_family_member_tuples

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