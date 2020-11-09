import csv

from confirmation_pdf import Letter_Text
from confirmation_pdf import Confirmation_Letter
from confirmation_pdf import A_SELECT

from create_codes import return_bars
from create_codes import file_set

from basket_sorting_Geocodes import Route_Database

from ops_print_sponsor import get_hh_from_sponsor_table

from r_config import configuration

import sys
from collections import namedtuple

ALL_CODES = file_set()

SERVICE_PACK = namedtuple('SERVICE_PACK', 'sa_app_num, food_sponsor, gift_sponsor, voucher_sponsor, turkey_sponsor')

DEL_SELECT = {'House of Friendship Delivery': 'on December 5',
              'SPONSOR - REITZEL': 'on December 18',
              'SPONSOR - DOON' : 'on December 19',
              'Cambridge Delivery': 'in December'}

DEL_PROV = ['House Of Friendship', 'Doon Pioneer Park Christmas Miracle',
            'Possibilities International','House of Friendship Delivery',
           'SPONSOR - REITZEL', 'SPONSOR - DOON',
           'Cambridge Delivery', 'Cambridge Self Help Food Bank']
            
ALL_IN_ONE = ['Possibilities International', 
              'SPONSOR - REITZEL', 
              'Keller Williams Realty']

# INITIALIZE CONFIGURATION FILE
conf = configuration.return_r_config()
target = conf.get_target # source file
db_src, _, outputs = conf.get_folders()
_, session = conf.get_meta()


def write_label(file_date, label_list, ftype='labels'):
    '''
    since we are printing letters we will need mailing label info
    this function accepts the date run that we are using
    because it is the label that will be used to link this file with the pdf file
    and represents a run of letters that are being produced
    as well as a list for the name, address, address line 2, city and postal
    code of the recipient 
    and writes them to a csv file
    '''
    with open(f'{file_date}_{ftype}.csv', mode='a') as label_file:
        label_writer = csv.writer(label_file, delimiter=',')
        label_writer.writerow(label_list)

def purge_brackets(address_line_2):
    '''
    a little helper that relies on buzz code and other delivery relevent data
    included on the second line of the address being between two ()
    and strips that out, since to deliver our letter, the postal delivery
    person does not need to know their buzz number
    '''
    if address_line_2:
        return address_line_2.split('(')[0]
    else:
        return address_line_2


def check_dates(database, date_string):
    '''
    prompts for a date string to lookup in the database
    and then returns the sponsor table at that date range
    if it is valid
    if an invalid date is provided, it provides a list of valid dates to choose
    from
    '''
    vals, n_reg = database.return_count_spon_at_date(date_string)
    if vals:
        print(f'vals: {vals} n_reg: {n_reg}')
        confirmation = input(f'there are {n_reg} registered.  Proceed?  Y/N  ')
        if confirmation.lower() == 'y':
            dh_collection, db_manager = get_hh_from_sponsor_table(route_database, criteria=date_string)
            return date_string, dh_collection, db_manager
        else:
            print('aborting')
            sys.exit(0)
    else:
        check = input('there are no results at that date. look at available? Y/N  ')
        if check.lower() == 'y':
            lookup_table = {}
            for k, v in enumerate(n_reg):
                print(f'{k}:  {v[0]}')
                lookup_table[k] = v
                selection = input('which date do you want to use? (pick a number) ')
                choice = lookup_table.get(selection, 'Invalid')
                if choice == 'Invalid':
                    print('Invalid choice')
                    sys.exit(0)
                else:
                    check_dates(database, choice)
        else:
            print('exiting...')
            sys.exit(0)

SERVICE_TEMPLATE = {'file_id': None, 
                    'name': None, 
                    'service': None, 
                    'service_prov': None,
                    'food_service': None, 
                    'when' : None, 
                    'email': None,
                    'phone': None, 
                    'food_pu_loc': None, 
                    'food_pu_date': None,
                    'food_del_date': None, 
                    'gift_pu_loc': None, 
                    'gift_pu_date': None,
                    'all_in_one': None}

def compose_string(service_string):
    '''
    takes a list of services and builds a string that can
    be inserted into a sentence
    '''
    punct_lookup = {'turkey': 'a ',
                    'gift card': 'a '}
    services = None
    if len(service_string) == 1:
        services = service_string[0]
    if len(service_string) == 2:
        services = ' and '.join(service_string)
    if len(service_string) == 3:
        mark1 = punct_lookup.get(service_string[2],'')
        services = f'{service_string[0]}, {service_string[1]} and {mark1}{service_string[2]}'
    if len(service_string) == 4:
        mark2 = punct_lookup.get(service_string[3], '')
        services = f'{service_string[0]}, {service_string[1]}, {service_string[2]} and {mark2}{service_string[3]}'
    return services

def make_service_provider_strings(food, gift, vouch, turkey):
    '''
    takes the service providers for the 4 services
    and builds a string that describes what people have signed up for
    the foods they have requested to get for insertion into the letter text
    and a list of providers to insert into the confirmation letter describing
    who will be provider their services
    '''
    #print(f'{food}, {gift}, {vouch}, {turkey}') 
    no_zero = lambda x: x if len(x) > 1 else ''
    # DATABASE RETURNS 0 FOR EMPTY COLUMNS
    food = A_SELECT.get(food, '')
    gift = A_SELECT.get(gift, '')
    vouch = A_SELECT.get(vouch, '')
    turkey = A_SELECT.get(turkey, '')

    #print(f'{food}, {gift}, {vouch}, {turkey}') 
    #pause = input('pausing')
    # containers
    service_string = []
    provider_set = set()
    food_service = []

    # deliverables
    services = None
    food_services = None
    provider = None

    #print(f'food: {food} gift: {gift} vouch: {vouch} turkey: {turkey}')
    #go = input('paused  ')
    # FOOD
    if food: 
        if food in DEL_PROV:
            service_string.append('delivery of food')
            food_service.append('delivery of food')
        else:
            service_string.append('food')
            food_service.append('food')
        provider_set.add(food)
    # VOUCHER
    if vouch: 
        service_string.append('gift card for a local food store')
        provider_set.add(vouch)
        food_service.append('gift card')
    # TURKEY
    if turkey:
        service_string.append('turkey')
        provider_set.add(turkey)
        food_service.append('turkey')
    # GIFTS
    if gift: 
        service_string.append('gifts for your children')
        provider_set.add(gift)
    
    services = compose_string(service_string)
    food_services = compose_string(food_service)
    
    #if not services:
    #    raise Exception('services are blank!')
    provider = ' and '.join([x for x in provider_set if x])

    return services, food_services, provider, len(provider_set)

#SERVICE_PACK = namedtuple('SERVICE_PACK', 'sa_app_num, food_sponsor, gift_sponsor, voucher_sponsor, turkey_sponsor')
if __name__ == '__main__':
    print('#### CONFIRMATION LETTER PRINTER ####')
    input_date = input('Please enter session date (YYYY-MM-DD):  ')
    print(f'attempting {input_date}')
    route_database = Route_Database(f'{db_src}{session}rdb.db')
    the_date, dhc, dbm = check_dates(route_database, input_date)
    if the_date:
        pdf_file1 = Confirmation_Letter(the_date)
        
        #write mailing label headers
        mlh = ['name', 'address1','address2', 'city', 
               'province', 'postal code']
        
        merge_headers = ['fid', 'fname', 'lname', 'email', 
                         'header', 'line1', 'line2','footer', 
                         'gift','turkey','voucher']
        
        write_label(the_date, mlh)
        write_label(the_date, merge_headers, ftype='merge')

        for hh in dhc.key_iter():
            s_d = SERVICE_TEMPLATE
            package = SERVICE_PACK(*hh.return_sponsor_package())
            family_size = hh.hh_size

            #print(f'package: {package}')

            # HOF ZONE (HoF Zone 1, INT, time
            hof_zone, hof_num, hof_time = hh.get_zone_package()
            # HOF ZONE date for pickup
            zone_date = hh.get_zone_date()
            #print(f'zone package: {hof_zone} {hof_num} {hof_time}')
            # SALVATION ARMY
            sa_num, sa_time = hh.get_sa_day_time()

            # HOUSEHOLD SUMMARY named tuple
            # named tuple with the following attributes
            # applicant, fname, lname, size, phone, email,
            # address, address2, city, postal, diet, sa_app_num, sms_target
            h_summary = hh.return_summary()

            # CREATE STRINGS TO DESCRIBE PICKUP TIME AND DATE
            zone_pu_string = None
            gift_pu_string = None

            if all((zone_date, hof_time)):
                zone_pu_string = f'{zone_date} at {hof_time}'
            if all((sa_num, sa_time)):
                gift_pu_string = f'{sa_num} on {sa_time}'
            # FULL NAME STRING
            full_name = f'{h_summary.fname} {h_summary.lname}'
            # CREATE STRINGS FOR THE HEADER
            # TO DESCRIBE FOOD
            # TO LIST SERVICE PROVIDER
            service, food_service, service_prov, n_of_p  =\
            make_service_provider_strings(package.food_sponsor,\
                                         package.gift_sponsor,\
                                         package.voucher_sponsor,\
                                         package.turkey_sponsor)
            # SERVICE CLUSTERS
            food_package = [x for x in [package.food_sponsor, package.voucher_sponsor,
                            package.turkey_sponsor] if len(x) > 2]
            
            del_key = None
            
            for fp in food_package:
                if fp in DEL_PROV:
                    del_key = fp
                    break

            # WRITE INFO TO CSV FILE FOR PRINTING MAILING LABELS LATER
            l2 = purge_brackets(h_summary.address2)
            label_info = [f'{h_summary.fname} {h_summary.lname}     ({n_of_p})',
                          h_summary.address, l2,
                          h_summary.city, 'Ontario', hh.postal]

            write_label(the_date, label_info)

            # GET RID OF '0' RESPONSES FROM DATABASE 
            gift_package = [x for x in [package.gift_sponsor] if len(x) > 2]

            all_in = any([True for x in package if x in ALL_IN_ONE])
            
            provider_number = len(set([x for x in [package.food_sponsor,
                                      package.gift_sponsor,
                                      package.voucher_sponsor,
                                      package.turkey_sponsor] if len(x) > 1]))
            food_set = set(food_package)
            gift_set = set(gift_package)

            has_food = any(food_set)
            has_gifts = any(gift_set)
            
            # generate barcode image
            barcode = return_bars(str(h_summary.applicant), ALL_CODES)
            
            # STACK THE RICKETY DATASTRUCTURE WITH JUNK
            s_d['barcode'] = barcode
            s_d['file_id'] = h_summary.applicant
            s_d['family_size'] = family_size
            s_d['name'] = full_name
            s_d['service'] = service # x, y, and z
            s_d['service_prov'] = service_prov # from a, b and c
            s_d['phone'] = h_summary.phone
            s_d['email'] = h_summary.email
            s_d['food_pu_loc'] = package.food_sponsor
            s_d['food_pu_date'] = zone_pu_string 
            s_d['hof_pu_num'] = hof_num
            s_d['del_f_prov'] = del_key
            s_d['food_del_date'] = DEL_SELECT.get(del_key, None) 
            s_d['gift_pu_loc'] = package.gift_sponsor
            s_d['gift_pu_date'] = gift_pu_string
            s_d['food_service'] = food_service
            s_d['all_in_one'] = all_in # gifts and food from same provider
            s_d['has_food'] = has_food
            s_d['has_gifts'] = has_gifts
            s_d['provider_number'] = provider_number
            s_d['food_set'] = food_set
            s_d['gift_set'] = gift_set
            s_d['food_prov'] = package.food_sponsor
            s_d['gift_sponsor'] = package.gift_sponsor
            s_d['turkey_sponsor'] = package.turkey_sponsor
            s_d['voucher_sponsor'] = package.voucher_sponsor
            


            pdf_file2 = Confirmation_Letter(f'letters/{h_summary.applicant}') 
            service_text = Letter_Text(s_d)
            


            pdf_file1.parse_text(f'{h_summary.applicant}', service_text)

            if h_summary.email:
                merge_tags1 = [h_summary.applicant, 
                             h_summary.fname,
                             h_summary.lname, 
                             h_summary.email]
                
                merge_tags2 = service_text.get_merge_values() 
                
                merge_tags3 = [package.gift_sponsor, 
                             package.turkey_sponsor,
                             package.voucher_sponsor]
                
                all_tags = merge_tags1 + merge_tags2 + merge_tags3

                # write to csv file here
                write_label(the_date, all_tags, ftype='merge')
            if n_of_p == 2:
                pdf_file1.parse_text(f'{h_summary.applicant}', service_text)
            
            pdf_file2.parse_text(f'{h_summary.applicant}', service_text)
            pdf_file2.write()
       




        pdf_file1.write()


