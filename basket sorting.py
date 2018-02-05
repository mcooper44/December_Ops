from random import randint
from collections import namedtuple
from collections import defaultdict

Client = namedtuple('Client', 'size location')
box_mask = {'1' : 1,
            '2' : 1,
            '3' : 1,
            '4' : 1,
            '5': 2,
            '6' : 2,
            '7' : 3
            }


# populate a list of named tuples with random sizes and distance markers
# wrapped in a dictionary
def generate_families():
    households = []
    hh_dictionary = {}
    count_object = 1
    
    for i in range(1, 501, 1):
        family = randint(1,7)
        distance = randint(1,75)        
        households.append(Client(family, distance))

    for HH in households:
        label = str(count_object)
        hh_dictionary[label] = HH
        count_object += 1

    return hh_dictionary
    


def sort_method(households):
    '''
    takes the dictionary created by generate_familes and returns a list of sorted
    households who are in proximity to eachother
    we are just going to brute force this because why the fuck not!
    go through the entire list and create a sorted list of all the distances
    and then skim off as many of the shortest distance HH as will fill up to the
    max # of boxes
    '''
    max_box_count = 8
    route_counter = 1
    routes = {} # labeled routes and teh families they contain
    assigned = [] # container to add hh that have been assigned
    
    for applicant in households: # for key in dictionary of households
        print('for applicant {} with size {} and distance {} we will start the iterations'.format(applicant,
                                                                                                  households[applicant].size,
                                                                                                  households[applicant].location))
        applicant_route = [] # the working container that we will then add to the route dictionary
        size = str(households[applicant].size) # turn the size into a string so we can...
        boxes = box_mask[size] # start building the route with the household
        
        if applicant in assigned:
            print('we have done this one already')
            pass
        else:
            print('Applicant {} has not been added to a route yet'.format(applicant))
            
            assigned.append(applicant)
            print('we will add them to the list, which currently has {} households in it'.format(len(assigned)))
            applicant_route.append(applicant) # start by adding the household we are starting with
            
            
            
            distance_hh_dictionary = defaultdict(list) # build a container to add {calculated distances: households} to
                                                       # this will allow us to make a sorted list of the shortest distances and the HH
                                                       # that are at that distance            
            print('We will start building a dictionary of distances and the HH that live at them')
            # ITERATE THROUGH THE HOUSEHOLDS AND CALCULATE DISTANCES FROM THE CHOSEN STARTING HH
            for HH in households.keys(): # iterate through the keys to find the distances of remaining households
                print('HH under consideration for addition to the route: {}'.format(HH))
                if HH == applicant:
                    print('passing! you cannot add the starting HH to the list twice')
                    
                if HH != applicant:
                    if HH not in assigned:
                        print('this household has not been added a route')
                        ident = HH # their file number
                        dist = str(households[HH].location) # their distance
                        print('they have a distance of: {} and ident of {}'.format(dist, ident))
                        distance_hh_dictionary[dist].append(ident) # update dictionary of distances: HH identifier
                        print('we have added them to the dictionary')

            distances = sorted([int(k) for k in distance_hh_dictionary.keys()]) # sort a list of all the distances so we can skim the shortest off
            print('we have calculated {} distances'.format(len(distances)))
            for ke in distances:
                ske = str(ke)
                print('households: {} are at {} distance'.format(distance_hh_dictionary[ske], ke))
                # so some distances will have a number of households, so lets print them out
            
            # NOW WE WILL ITERATE THROUGH THE DISTANCES AND TRY AND PUT A ROUTE TOGETHER
            for int_value in distances: # for distance in sorted listed of distances
                
                key = str(int_value) # convert the int to a string so we can use it in the distance : families at that distance dictionary
                print('looking at distance {} it contains {}'.format(key, distance_hh_dictionary[key]))
                for fam in distance_hh_dictionary[key]: # for the individual or family in the list of households at this distance
                    
                    if fam not in assigned: # if we haven't sorted them into a route yet
                        print('looking at family {}'.format(fam))
                        fam_size = str(households[fam].size) # determine family size
                        print('family {} has size {}'.format(fam, fam_size))
                        box_num = box_mask[fam_size] # determine number of boxes
                        print('they will have {} boxes made up for them'.format(box_num))
                        # do math to determine if we can add them to the route
                        # then if there are still more families at this distance we need to pop the one we just added
                        # and evaluate the next one and so on down the chain until we max out box count per route or exaust
                        # remaining households
                        if box_num + boxes <= max_box_count: # if we added this family to the pile and they didn't add too many boxes
                            print('adding them to the route will not add too many boxes, so lets add them')
                            boxes += box_num
                            print('we are currently at {} boxes and have {} spaces remaining'.format(boxes, max_box_count - boxes))
                            assigned.append(fam) # add them to the assigned list
                            applicant_route.append(fam) # add them to the route
        
        if applicant_route:
            print('we have iterated and made a route! It is {}'.format(applicant_route))
            r_key = str(route_counter)
            routes[r_key] = applicant_route
            route_counter += 1
    
    return routes
    

applicants = generate_families()
test = sort_method(applicants)

for x in test.keys():
    print('route {} contains {}'.format(x, test[x]))


for x in applicants.keys():
    peeps = str(applicants[x].size)
    print('family {} contains {} people which is {} boxes'.format(x, applicants[x].size, box_mask[peeps]))

for k in test.keys():
    total_b = 0
    locations = []
    for fam_ in test[k]:
        size_ = applicants[fam_].size
        local = applicants[fam_].location
        boxes_ = box_mask[str(size_)]
        total_b += boxes_
        locations.append(local)
    print('{} has {} boxes and this range of locations {}'.format(k, total_b, locations))

























