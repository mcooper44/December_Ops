import testing_class as tc 
import basket_sorting_Geocodes as bsg 

target_number = 1500 # this is the number of test households we want to create

test_cd = tc.Test_Codes(target_number) 
test_hh = tc.Test_Households(target_number) 
test_cd.generate_coordinates() # generate ramdom sample of geocodes
test_hh.generate_list_of_households() # generate random sample of file id, hh id, family size

# zip them into a nice neat little package
coords_hhlds = list(zip(test_hh.return_households_list(), test_cd.return_coordinates())) 

hh_dictionary = {} # this will be a dictionary of file id : Delivery_Household objects

for x in coords_hhlds:
    fid =  x[0][0]
    hhid = x[0][1]
    fs = x[0][2]
    lat = x[1][0]
    lng = x[1][1]

    hh_dictionary[str(fid)] = bsg.Delivery_Household(fid, hhid, fs, lat, lng)

print('there are {} keys in the dictionary'.format(len(hh_dictionary.keys()))

test2018 = bsg.Delivery_Routes(7, 1) # initialize Delivery_Routes() object
test2018.set_hh_data_structure(hh_dictionary) # give it the dictionary
test2018.get_status() # check status 
test2018.sort_method() # sort the routes
test2018set = test2018.get_route_collection() # pull the routes out 

# print the routes
for x in test2018set:
    print('{} {}'.format(x, test2018set[x]))
    z = [hh_dictionary[y].geo_tuple for y in test2018set[x]]
    print(z)


