#!/usr/bin/python3.6

import folium
from folium.plugins import MarkerCluster

# extracts Delivery_Household_Collection
from basket_sorting_Geocodes import Route_Database
from collections import namedtuple
from ops_print_routes import Service_Database_Manager
from address_parser_and_geocoder import full_address_parser

D_FOLDER = 'databases/'
R_SOURCE = f'{D_FOLDER}2019_production_rdb.db'

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
    pings the databases and 

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

    lat, lng = rdbm.return_geo_points(l1_city, 'address')[0]
    gan, gat = rdbm.return_sa_info_pack('rdb', 'sa', fid)
    hh_data = hh_dat(fid, main_applicant[3], main_applicant[10], lat, lng, main_applicant[11], rn, rl, gan, gat)
    
    return hh_data

def get_hh_data_from_database(rdbm, r_start=1, r_end=900):
    '''
    This function gets routes out of the database
    and structures them into a delivery_household_collection
    and returns that structure

    '''

    # get (file_id, rn, rl) in rt number range
    logged_routes = rdbm.return_route_range('rdb', r_start, r_end)

    return logged_routes

def get_html_str(hh_dat):

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

def make_map(locations, labels):
    sloc = (43.465837, -80.482006)
    hh_map = folium.Map(location=sloc, zoom_start=12)
    hh_map.add_child(MarkerCluster(locations=locations, popups=labels))
    hh_map.save('2019_Christmas_Caseload.html')
    
def main():
    route_database = Route_Database(R_SOURCE)
    ln = route_database.return_last_rn()
    #ln = 5 
    # get delivery household collection

    rdbm = Service_Database_Manager.get_service_db() 
    rdbm.initialize_connections()
    
    dh = get_hh_data_from_database(rdbm, r_start=1, r_end=ln)
    
    locations = []
    labels = []

    for household in dh: # for Delivery_Household in route iterator
        hh = get_hh_data(household, rdbm)
        
        ht_str = get_html_str(hh)
        iframe = folium.IFrame(html=ht_str, width=250, height=150)
        popup = folium.Popup(iframe, max_width=2650)
        location = (hh.lat, hh.lng)
        labels.append(popup)
        locations.append(location)
    
    make_map(locations, labels)

    rdbm.close_all()


if __name__ == '__main__':
    main() 
