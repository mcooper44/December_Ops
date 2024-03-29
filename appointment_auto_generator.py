#!/usr/bin/python3.6

"""
This will create an excel file that can be used to print out sheets we will use to sign up
people for appointments and coordinate services across a number of different
service providers 



"""
            
import string
import csv
import xlsxwriter #http://xlsxwriter.readthedocs.io/format.html
import re
from collections import defaultdict
from collections import namedtuple
from operator import itemgetter # http://stackoverflow.com/questions/4174941/how-to-sort-a-list-of-lists-by-a-specific-index-of-the-inner-list
import sqlite3

from r_config import configuration

Visit = namedtuple('Visit', 'slot_number, day, time') 

# CONFIGURATION FILE AND GLOBAL VARIABLES
conf_file = configuration.return_r_config() # use class method to instantiate
_, sesh = conf_file.get_meta() # 'year_prod/testing_'

# the Salvation Army schedules are set and do not vary
# they can be coded into the configuration file.
# the other providers and sites will have radically different
# schedules and variability. This requires configuration.  If there is
# a smaller number of providers (3) they can be coded into the configuration
# file

# KW SALVATION ARMY
FILE_OUTPUT = f'products/SA_{sesh}gift_app_times.xlsx'
dy, tms, mlt, dmlt = conf_file.get_sa_app_package()
sa_db = conf_file.get_bases()['sa']

# CAMBRIDGE SALVATION ARMY
csa_output = f'products/CSA_{sesh}gift_app_times.xlsx'
csa_dy, csa_tms, csa_mlt, csa_dmlt = conf_file.get_csa_app_package()

# THE ZONES!  THESE ARE NON GIFT PICKUP LOCATIONS 
zone_output = f'products/ZN_{sesh}pickup_app_times.xlsx'
z_dy, z_tms, z_mlt, z_dmlt = conf_file.get_zone_app_package()

# LABELS 
c_dy = ['Cambridge Firefighters',
        'Dec 9', 'Dec 10', 'Dec 11', 
        'Dec 12', 'Dec 14', 'Dec 15', 
        'Dec 16', 'Dec 17', 
        'Dec 18']
cam_dy = ['Monday Dec 7', 'Tuesday Dec 8', 'Wednesday Dec 9', 
          'Thursday Dec 10', 'Friday Dec 11']

cshfb_dy = ['Monday Dec 14', 'Tuesday Dec 15', 'Wednesday Dec 16', 
            'Thursday Dec 17']

# TIME LABELS
c_tms = ['10:00','10:10','10:20','10:30','10:40',
         '10:50','11:00','11:10','11:20','11:30','11:40',
         '11:50', '12:30', '12:40', '12:50', '1:00', '1:10',
         '1:20',  '1:30', '1:40','1:50','2:00','2:10','2:20',
         '2:30', '2:40', '2:50', '3:00', '3:10', '3:20']

cam_tms = ['9:00', '9:10', '9:20', '9:30', '9:40', '9:50', 
             '10:00', '10:10', '10:20', '10:30', '10:40', '10:50',
             '11:00', '11:10', '11:20', '11:30', '11:40', '11:50',
             '12:40', '12:50',
             '1:00', '1:10', '1:20', '1:30', '1:40', '1:50',
             '2:00', '2:10', '2:20', '2:30', '2:40', '2:50',
             '3:00', '3:10', '3:20', '3:30', '3:40', '3:50']

cshfb_tms = ['9:00', '9:10', '9:20', '9:30', '9:40', '9:50', 
             '10:00', '10:10', '10:20', '10:30', '10:40', '10:50',
             '11:00', '11:10', '11:20']

# HOW MANY PEOPLE PER TIME SLOT, HOW MANY TIME SLOTS PER DAY
c_mlt = [(15, 30), # Cambridge Firefighters
        (12, 30), # Legion 
         (15, 30), # St. Marks
         (10, 30), # Blessed
         (10, 30), # St. Anthony
         (10, 30), # St. Francis
         (10, 30), # Our Lady
         (10, 30), # First Mennonite
         (10, 30), # WPA
         (10, 30) # Kingsdale
        ]

cam_mlt = [(8, 38),
            (8, 38),
            (8, 38),
            (8, 38),
            (8, 38)
        ]

cshfb_mlt = [(7,15),
              (7,15),
              (7,15),
              (7,15)]

# HOW MANY SERVICE EVENTS PER SESSION
c_dmlt = [450, 360, 450, 300, 300, 300, 300, 300, 300, 300] 
cam_dmlt = [304, 304, 304, 304, 304]
cshfb_dmlt = [105, 105, 105, 105]

def create_time(day=dy, times=tms, multipliers=mlt, day_multp=dmlt,ap=1):
    '''
    This function returns a list of named 'Visit' tuples to insert into the booking sheet
    with .slot_number .day .time
    they are stacked in sequence
    '''
    #time_slots = [x for x in range(1, 2899)]
    # 9:30-4:40 time period with no breaks doing 13 every 20 minutes
    # or alternating blocks of 6 and 7 appointments booked every
    # 10 minutes works out to
    # ((45 apps/hour * 7 hours)+7) * 9 days +1 (b/c of how range works)
    # is 2899 appointment time slots available

    times_list = []
    time_strings = []
    day_strings = []
    
    r_times = times[::-1]
    time_counter = len(times)
    time_index = len(times) -1
    
    while time_counter:
        for x in multipliers: # block of apps during time block
            number = x
            time = r_times[time_index]
            while number:
                time_strings.append(time)
                number -= 1
            time_index -= 1
            time_counter -= 1

    time_strings = time_strings * len(day)
    
    day_counter = len(day)
    day_index = 0

    while day_counter:
        for x in day_multp:
            number = x
            the_day = day[day_index]
            while number:
                day_strings.append(the_day)
                number -=1
            day_counter -=1
            day_index += 1

    app_number = ap
    for day_time in zip(day_strings, time_strings):
        vt = Visit(str(app_number), day_time[0], day_time[1])
        times_list.append(vt)
        app_number += 1
    
    return times_list

def create_database(time_list, dbase_name=sa_db, table='Appointments'):
    '''
    Takes a time_list generated by the create_time function and generates 
    a SQL database with the appointment number as the primary key and the 
    date and time as values
    '''
    # kw salvation army
    INIT_STRING = """CREATE TABLE IF NOT EXISTS Appointments (ID INTEGER PRIMARY KEY, day TEXT, time TEXT)"""
    # cambridge salvation army
    INIT_CSA = '''CREATE TABLE IF NOT EXISTS CSA (ID INTEGER PRIMARY KEY, day TEXT, time TEXT)'''
    # pickup zones
    INIT_ZONES = '''CREATE TABLE IF NOT EXISTS Zones (ID INTEGER PRIMARY KEY, day TEXT, time TEXT)'''
    # operation strings
    INSERT_STRING = f"INSERT INTO {table} (ID, day, time) VALUES (?,?,?)"
    FETCH_STRING = "SELECT (day, time) FROM Appointments WHERE ID=?"
    # connect to db
    con = sqlite3.connect(dbase_name)
    cursor = con.cursor()
    # create Appointments
    cursor.execute(INIT_STRING)
    con.commit()
    # create Zone_Time
    cursor.execute(INIT_ZONES)
    con.commit()
    # create Cambridge SA table
    cursor.execute(INIT_CSA)
    con.commit()

    for ntuple in time_list:
        payload = (int(ntuple.slot_number), ntuple.day, ntuple.time)
        #print(payload)
        cursor.execute(INSERT_STRING, payload)
        con.commit()
    
    con.close()

def write_gift_sheet(gift_times, provider='SA', zone_max=None,\
                     zone_ident=None, fname=None):
    '''
    This will output a series of appointment sheets that we can fill in 
    with gift appointments     
    '''
    file_output = FILE_OUTPUT
    if fname:
        file_output = fname

    spot_counter = 0 # how many times have we printed a line to a page

    header_off_set = 8 # how far to push the header out from the previous header
    header_counter = 0 # have we printed a header?      
    
    off_set = 1 # how many spaces between pages
    spot_counter = 0 # how many times have we printed a line to a page

    row_count = 0 # for keeping track of rows for formatting purposes
    
    l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23] # reference positions
    
    workbook = xlsxwriter.Workbook(file_output) # create a workbook
    bold = workbook.add_format({'bold': True}) # setup the ability to bold things

    worksheet = workbook.add_worksheet('Booking Sheet') # add worksheet _booking sheet_
    worksheet.set_margins(0.1,0.1,.9,.1) # Left,Right,Top,Bottom margins set
    worksheet.set_landscape() # set page landscape format so we can put more material on the page

#  http://xlsxwriter.readthedocs.io/example_headers_footers.html
    service_booker = {'ZONES': 'HoF Pickup', 
                      'SA': 'Salvation Army', 
                      'SA1':'Salvation Army',
                      'FIRE': 'Cambridge Fire Fighters'}.get(provider, 'UNKNOWN!') 
    header_title_date = f'&L&"Courier New,Bold"               {service_booker} Appointment Sheet              Page: &P of &N'
    worksheet.set_header(header_title_date)
    
    # http://xlsxwriter.readthedocs.io/worksheet.html
    if provider == 'SA' or provider == 'SA1':
        worksheet.set_column(0, 0, 5.29)  # A
        worksheet.set_column(1, 1, 6) #     B
        worksheet.set_column(2, 2, 4.86) #  C
        worksheet.set_column(3, 3, 27) #    D
        worksheet.set_column(4, 4, 14) #    E
        worksheet.set_column(5, 5, 19) #    F
        worksheet.set_column(6, 6, 5) #     G
        worksheet.set_column(7, 7, 12) #    H
        worksheet.set_column(8, 8, 12) #    I
        worksheet.set_column(9, 9, 20) #    J
    if provider == 'ZONES':
        worksheet.set_column(0, 0, 6)  # A- App #
        worksheet.set_column(1, 1, 6) #     B- Day
        worksheet.set_column(2, 2, 5) #  C- Time
        worksheet.set_column(3, 3, 7) #    D- Zone
        worksheet.set_column(4, 4, 27) #    E- Name
        worksheet.set_column(5, 5, 14) #    F- File #
        worksheet.set_column(6, 6, 19) #     G- Phone #
        worksheet.set_column(7, 7, 5) #    H- Gifts
        worksheet.set_column(8, 8, 8) #     I- Voucher
        worksheet.set_column(9, 9, 6) #     J-Turkey
        worksheet.set_column(10, 10,20) #   K- checked



    OPTIONS = {'SA1': {1 : 'App #', # for written sign ups
                    2 : 'Day',
                    3 : 'Time',
                    4 : 'Name (First + Last)',
                    5 : 'File #',
                    6 : ' ',
                    7 : '# of kids',
                    8 : 'Girls Ages',
                    9 : 'Boys Ages',
                    10 : 'Signature'},
               'SA': {1 : 'App #', # for 2018/2019 style
                    2 : 'Day',
                    3 : 'Time',
                    4 : 'Name (First + Last)',
                    5 : 'File #',
                    6 : 'Phone #',
                    7 : 'Gifts',
                    8 : 'Voucher',
                    9 : 'Turkey',
                    10 : 'Checked by:'},
               'ZONES': {1 : 'App #', # for 2020 hellzone
                    2 : 'Day',
                    3 : 'Time',
                    4 : 'Zone',
                    5 : 'Name',
                    6 : 'File #',
                    7 : 'Phone #',
                    8 : 'Gifts',
                    9 : 'Voucher',
                    10 : 'Turkey',
                    11 : 'Checked By:'}}

    column_heads = OPTIONS[provider]
    zone_number = 1
    zone_counter = 1

    for slot in gift_times: # for each timeslot
        
        if header_counter == 0: # if we are starting a new page write the headers
            worksheet.set_row(row_count, 66, bold)
            worksheet.write('A{}'.format(l_n[1]), column_heads[1])
            worksheet.write('B{}'.format(l_n[1]), column_heads[2])
            worksheet.write('C{}'.format(l_n[1]), column_heads[3])
            worksheet.write('D{}'.format(l_n[1]), column_heads[4])
            worksheet.write('E{}'.format(l_n[1]), column_heads[5])
            worksheet.write('F{}'.format(l_n[1]), column_heads[6])
            worksheet.write('G{}'.format(l_n[1]), column_heads[7])
            worksheet.write('H{}'.format(l_n[1]), column_heads[8])
            worksheet.write('I{}'.format(l_n[1]), column_heads[9])
            worksheet.write('J{}'.format(l_n[1]), column_heads[10])
            if zone_max:
                worksheet.write(f'K{l_n[1]}', column_heads[11])
            
            header_counter = 1
            spot_counter += 1
            row_count +=1
            worksheet.set_row(row_count, 68)

        #print(slot.slot_number, slot.day, slot.time)
        worksheet.write('A' + str(l_n[2]), slot.slot_number) # write appointment slot number i.e. 1
        worksheet.write('B' + str(l_n[2]), slot.day) # write day i.e Dec 4 
        worksheet.write('C' + str(l_n[2]), slot.time) # write time 9am
        if zone_max:
            # if writing zones as a batch
            # with uniform characteristics
            # this will insert the string and increment
            # the Zone number when tipping over the end
            # of the day it is writing and moving into the next zone
            zone_string = f'Zone {zone_number}'
            worksheet.write(f'D{l_n[2]}', zone_string)
            if zone_counter % zone_max == 0:
                zone_counter += 1
                zone_number += 1
            else:
                zone_counter += 1
        if zone_ident:
            # otherwise if doing custom zones
            # send the number down and it will be written
            zone_string = f'Zone {zone_ident}'
            worksheet.write(f'D{l_n[2]}', zone_string)

        worksheet.set_row(row_count, 68)
        row_count += 1
        spot_counter += 1 
        

        if spot_counter == 8: # adjust things for a new page
            off_set += 1
            # use a full set of numbers in sequence to make it easier to add items later
            l_n[1] += header_off_set # push the header to write on the next page
            l_n[2] += off_set # 
            l_n[3] += off_set
            l_n[4] += off_set
            l_n[5] += off_set
            l_n[6] += off_set
            l_n[7] += off_set
            l_n[8] += off_set
            l_n[9] += off_set
            l_n[10] += off_set
            l_n[11] += off_set
            l_n[12] += off_set
            l_n[13] += off_set
            l_n[14] += off_set
            l_n[15] += off_set
            l_n[17] += off_set
            l_n[18] += off_set
            l_n[19] += off_set
            l_n[20] += off_set

            spot_counter = 0
            off_set -= 1
            header_counter = 0
            
        else: # space every cell out by the standard measure
            # use a full set of numbers in sequence to make it easier to add items later
            l_n[2] += off_set # 
            l_n[3] += off_set
            l_n[4] += off_set
            l_n[5] += off_set
            l_n[6] += off_set
            l_n[7] += off_set
            l_n[8] += off_set
            l_n[9] += off_set
            l_n[10] += off_set
            l_n[11] += off_set
            l_n[12] += off_set
            l_n[13] += off_set
            l_n[14] += off_set
            l_n[15] += off_set
            l_n[17] += off_set
            l_n[18] += off_set
            l_n[19] += off_set
            l_n[20] += off_set
    
    workbook.close()




def create_custom_time(d, t, mul, dm, app):
    '''
    wraps around create_time
    takes a day, time list
    creates a time multiplier list
    takes a day multiplier
    and app number to start counting at
    and returns a time list
    '''
    m, n = mul
    mult_list = [m] * n
    return create_time(d, t, mult_list, dm, app)


# WRITING FUNCTION CALLS
if __name__ == '__main__':
    '''
    a_time = create_time()
    write_gift_sheet(a_time)
    print('Salvation Appointment Sheets Output')
    create_database(a_time)
    print('KW Salvation Army table populated')
    csa_time = create_time(day=csa_dy, times=csa_tms, multipliers=csa_mlt, day_multp=csa_dmlt)
    write_gift_sheet(csa_time, provider='SA')
    print('Cambridge Salvation Army Appointment Sheets Output')
    create_database(csa_time, table='CSA') 
    print('Cambridge SA database table populated')
    '''

    ###################################################################
    # This loop prints out separate files for the different zones but #
    # still numbers them sequentially and labels them                 #
    ###################################################################
    
    #time_master = 1 # the appointment number to start counting at
    '''
    for d in range(len(c_dy)):
        print(c_dy[d])
        zone = d + 1
        file_n = f'products/p2020_{c_dy[d]}_ZN_{zone}_{sesh}.xlsx'
        day_list = create_custom_time([c_dy[d]], c_tms, c_mlt[d],\
                                      [c_dmlt[d]],time_master)
        write_gift_sheet(day_list, provider='ZONES',zone_ident=zone,fname=file_n)
        time_master += c_dmlt[d] # increment by number of visits in the day
        create_database(day_list, table='Zones')
    '''

    time_master = 3361
    for d in range(len(cam_dy)):
        print(cam_dy[d])
        zone = d + 1
        file_n = f'products/p2020_c{cam_dy[d]}_CZN_{zone}_{sesh}.xlsx'
        day_list = create_custom_time([cam_dy[d]], cam_tms, cam_mlt[d],\
                                      [cam_dmlt[d]], time_master)
        write_gift_sheet(day_list, provider='ZONES', zone_ident=zone,fname=file_n)
        time_master += cam_dmlt[d]
        create_database(day_list, table='Zones')

    
    for d in range(len(cshfb_dy)):
        print(cshfb_dy[d])
        zone = d + 1
        file_n = f'products/p2020_c{cshfb_dy[d]}_CZN_{zone}_{sesh}.xlsx'
        day_list = create_custom_time([cshfb_dy[d]], cshfb_tms, cshfb_mlt[d],\
                                      [cshfb_dmlt[d]], time_master)
        write_gift_sheet(day_list, provider='ZONES', zone_ident=zone,fname=file_n)
        time_master += cshfb_dmlt[d]
        create_database(day_list, table='Zones')

