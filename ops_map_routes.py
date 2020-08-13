#!/usr/bin/python3.6
from datetime import datetime
import sys

import folium
from folium.plugins import MarkerCluster

# extracts Delivery_Household_Collection
from basket_sorting_Geocodes import Route_Database
from collections import namedtuple
from ops_print_routes import Service_Database_Manager
from address_parser_and_geocoder import full_address_parser

D_FOLDER = 'databases/'
R_SOURCE = f'{D_FOLDER}2019_production_rdb.db'
DEF_LOC =(43.465837, -80.482006) # default start location
OUT_DIR = 'products/'
DEF_MAP_NAME =f'{OUT_DIR}Route_Map_{datetime.now().strftime("%m-%d %H %M %S")}.html' 

def simplify_address(add_string):
    '''
    strips out some of the extra junk around the address
    so that the call to the address database doesn't return false 
    negative results
    '''
    try:
        _, _, parsed_address = full_address_parser(add_string, '111111')
        simple_address, _ = parsed_address
        return simple_address
    except KeyboardInterrupt:
        sys.exit(1)
    except:
        return None

def get_hh_data(drt, rdbm):
    '''
    param: drt is a delivery route summary tuple (fid, rn, rl)
    param: rdbm is a route database manager class
    uses info from drt to ping the databases (route, address) and 
    returns tuple of key data summarizing a route
    fid, fam size, diet, lat, lng, neighbourhood, rn, rl, gift app num, gift
    appt time

    '''
    fid, rn, rl = drt

    main_applicant = rdbm.get_main_applicant('rdb', fid)[0]
    # main_applicant is a tuple from the database call
    # position 6 and 8 are address line 1 string
    # and City string
    # 0 file_id, 1 f_name, 2 l_name, 3 family_size, 4 phone, 5 email, 6 address_1,
    # 7 address_2, 8 city, 9 postal, 10 diet, 11 neighbourhood, 12 sms_target

    hh_dat = namedtuple('hh_dat', 
        'main_app_ID, family_size, diet, lat, lng, neighbourhood, rn, rl, gan, gat ')
    street_str = simplify_address(main_applicant[6])

    l1_city = (street_str, main_applicant[8])
    try:
        lat, lng = rdbm.return_geo_points(l1_city, 'address')[0]
        gan, gat = rdbm.return_sa_info_pack('rdb', 'sa', fid)
        hh_data = hh_dat(fid, main_applicant[3], main_applicant[10], lat, lng, main_applicant[11], rn, rl, gan, gat)
    
        return hh_data
    except Exception as e:
        print(f'{fid} raised {e} when attempting to derive lat, lng')
        return False

def merge_food_and_gifts(routes, gifts):
    '''
    takes the 3 tuples from the routes database call
    and the one tuple and fills in some fields in the gifts
    and concatenates things so that everything will work properly
    in later function calls that expect a 3 tuple of (fid, rn, rl)
    '''
    gifts_2 = [(x[0], None, None) for x in gifts]
    return routes + gifts_2

def get_hh_data_from_database(rdbm, r_start=1, r_end=900):
    '''
    This function gets routes out of the database
    and structures them into a delivery_household_collection
    and returns that structure

    '''

    # get (file_id, rn, rl) in rt number range
    logged_routes = rdbm.return_route_range('rdb', r_start, r_end)
    # get (file_id,) from gift table where file_id NOT IN routes
    logged_gifts = rdbm.return_gifts_not_food('rdb')     
    all_services = merge_food_and_gifts(logged_routes, logged_gifts)
    return all_services

def get_html_str(hh_dat):
    '''
    :param: hh_dat is a named tuple generated by the get_hh_data() function
    returns a html string for populating a folium popup
    '''
    info_string = f"""
           <html>
           <p>Main Applicant: {hh_dat.main_app_ID}<br />
            Family Size: {hh_dat.family_size}<br />
            Diet: {hh_dat.diet}<br />
            Neighbourhood: {hh_dat.neighbourhood}<br />
            Route: {hh_dat.rn} {hh_dat.rl}<br />
            Gift Appointment: {hh_dat.gan}<br />
            Gift Appointment Time: {hh_dat.gat}<br />

            </p>
            </html>"""

    return info_string

def make_map_MarkerCluster(locations, labels, sloc=DEF_LOC):
    '''
    param: locations = list of geo_code tuples
    param: labels = list of popup folium.Popup(iframe) objects to pair with the
    locations.  used by the make_route_map() function
    param: sloc = start location for the map to focus on
    '''
    hh_map = folium.Map(location=sloc, zoom_start=12)
    hh_map.add_child(MarkerCluster(locations=locations, popups=labels))
    hh_map.save('2019_Christmas_Caseload.html')

def make_map_custom_MarkerCluster(m, fname=DEF_MAP_NAME):
    '''
    param: m = folium.map(location, zoom_start) object
    param: fname = the file name
    '''
    
    m.save(fname)


def add_custom_marker(loctn, pop_up, icon, mark):
    '''
    param: loctn - tuple of (lat, long) floats
    param: pop_up - a string or iframe object
    param: icon: tuple of ('color', 'icon-name')
    param: mark - a marker_cluster object 
    '''
    colour, icn = icon
    folium.Marker(
        location=loctn,
        popup=pop_up,
        icon=folium.Icon(color=colour, icon=icn),).add_to(mark)

def get_rrange(ln):
    '''
    takes param ln as int and prompts user to enter 
    a range of routes to extract from the database
    returning a 2 tuple
    or quitting 
    '''
    
    print(f'There are {ln} routes in the database')
    print('If you want to map all press enter at the first prompt or "x" to quit')
    start_rt = input('Enter Start of range  ')
    if not start_rt:
        return 1, ln
    elif start_rt:
        end_rt = input('Enter End of range   ')
        return int(start_rt), int(end_rt)
    else:
        print('exiting...')
        sys.exit(1)

def get_delivery_HH_D(dbase_src, f_or_g=False):
    '''
    param: dbase_src = path to a database folder
    param: f_or_g is a flag to signify that 
    either only the gift exclusive requests are needed
    or if food and gifts are need
    returns delivery_households and an initialized 
    route database manager object

    '''
    route_database = Route_Database(dbase_src)
        
    # get delivery household collection
    rdbm = Service_Database_Manager.get_service_db() 
    rdbm.initialize_connections()
    
    if f_or_g != 'gifts':
        ln = route_database.return_last_rn()
    
        r_start, r_end = get_rrange(ln)
    
        dh = get_hh_data_from_database(rdbm, r_start=r_start, r_end=r_end)
        print('returning hamper routes')
        return dh, rdbm
    
    elif f_or_g == 'gifts':
        print('returning gifts only')
        logged_gifts = rdbm.return_gifts_not_food('rdb')
        fmted_gifts = merge_food_and_gifts([], logged_gifts)
        return fmted_gifts, rdbm

def parse_and_return_icon_tpl(hh,s_filter=None):
    '''
    parse the named tuple containing the following .fields
    main_app_ID, family_size, diet, lat, lng, neighbourhood, rn, rl, gan, gat ')
    and return a tuple to be used to generate an icon to be added to a 
    custom cluster_marker map
    for reference:
    https://stackoverflow.com/questions/53721079/python-folium-icon-list


    '''
    lookup = {'foodANDgifts': ('red', 'gift'),
              'food': ('green', 'cutlery'),
              'gifts': ('green', 'gift')}
    if not s_filter:
        if all((hh.rn, hh.gan)):
            return lookup.get('foodANDgifts')
        elif hh.rn:
            return lookup.get('food')
        elif hh.gan:
            return lookup.get('gifts')
        else:
            return None
    elif s_filter:
        try:
            actual_s = parse_and_return_icon_tpl(hh)
            service = lookup.get(s_filter, False)
            if actual_s == service:
                return service
            else:
                return False
        except:
            raise('please choose "foodANDgifts", "food", "gifts" as a filter')

def make_custom_route_map(dbase_src=R_SOURCE, s_filter=None):
    '''
    param dbase_src is a path to a route database
    this function opens that database 
    opens a Service_Database_Manager with the standardized paths

    and makes a Marker Cluster Folium Map with
    Popup(iframes) with key summary stats
            Main Applicant: 
            Family Size:
            Diet:
            Neighbourhood: 
            Route: 
            Gift Appointment: 
            Gift Appointment Time: 

    param s_filter is a str that can be used to generate a map of one specific
    service request available keys are: ("foodANDgifts", "food", "gifts")
    '''
    dh, rdbm = get_delivery_HH_D(dbase_src, s_filter)
    m = folium.Map(location=DEF_LOC, zoom_start=5)
    
    marker_cluster = MarkerCluster().add_to(m)

    for household in dh: # for Delivery_Household in route iterator
        try:
            hh = get_hh_data(household, rdbm) # returns named tuple
        
            ht_str = get_html_str(hh)
            iframe = folium.IFrame(html=ht_str, width=250, height=150)
            ifrm_popup = folium.Popup(iframe, max_width=2650)
        
            i_con = parse_and_return_icon_tpl(hh, s_filter)
        
            location = (hh.lat, hh.lng)
        
            if i_con:
                add_custom_marker(location, ifrm_popup, i_con, marker_cluster)
        except Exception as occurance:
            print(f'{household} raised {occurance}')

    # Save Map
    make_map_custom_MarkerCluster(m)
    # close databases
    rdbm.close_all()

def make_route_map(dbase_src=R_SOURCE):
    '''
    param dbase_src is a path to a route database

    this function opens that database 
    opens a Service_Database_Manager with the standardized paths

    and makes a Marker Cluster Folium Map with
    Popup(iframes) with key summary stats
            Main Applicant: 
            Family Size:
            Diet:
            Neighbourhood: 
            Route: 
            Gift Appointment: 
            Gift Appointment Time: 

    '''

    dh, rdbm = get_delivery_HH_D(dbase_src)
    locations = []
    labels = []

    for household in dh: # for Delivery_Household in route iterator
        hh = get_hh_data(household, rdbm)
        if hh: 
            ht_str = get_html_str(hh)
            iframe = folium.IFrame(html=ht_str, width=250, height=150)
            popup = folium.Popup(iframe, max_width=2650)
            location = (hh.lat, hh.lng)
            labels.append(popup)
            locations.append(location)
        else:
            print(f'could not derive hh from {household}')

    make_map_MarkerCluster(locations, labels)

    rdbm.close_all()


def main():
    choice1 = input('Choose Map Type:\n1. Marker Cluster\n2. Custom Icon MarkerCluster\n')
    if choice1 == '1':
        make_route_map()
    elif choice1 == '2':
        choice2 = input('1. Default Icons\n2. Filter Services\n')
        if choice2 == '1':
            make_custom_route_map()
        elif choice2 == '2':
            choice3 = input('1. foodANDgifts\n2. food\n3. gifts\n')
            if choice3 in ['1','2','3']:
                service_lookup = {'1': 'foodANDgifts', 
                                  '2': 'food', 
                                  '3': 'gifts'}
                service_filter = service_lookup.get(choice3, None)
                make_custom_route_map(s_filter=service_filter)
            else:
                print('Invalid input.\nExiting...')
                sys.exit(1)
    else:
        print('invalid input - please choose 1 or 2.\nExiting...')
        sys.exit(1)

if __name__ == '__main__':
    main() 
