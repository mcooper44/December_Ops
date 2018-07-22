#!/usr/bin/python3.6

"""
This will create an excel file that can be used to print out sheets we will use to sign up
people for gift appointments

"""
            
import string
import random
import csv
import xlsxwriter
#http://xlsxwriter.readthedocs.io/format.html
import re
from collections import defaultdict
from collections import namedtuple
from operator import itemgetter # http://stackoverflow.com/questions/4174941/how-to-sort-a-list-of-lists-by-a-specific-index-of-the-inner-list

file_output = 'gift sign up 2018 - print test.xlsx'

Visit = namedtuple('Visit', 'slot_number, day, time') 

def create_time():
    '''
    This function returns a list of strings to insert into the booking sheet
    '''
    time_slots = [x for x in range(1, 2648)] # 39 apps/hour * 8 hours * 9 days +1 b/c of how range works

    day = ['Dec 6', #  0 - Wednesday - FIRST DAY
            'Dec 7', #  1- Thursday
            'Dec 8', #  2 - Friday
            'Dec 13', # 3 - Wednesday, Dec 11-12 are the unstructured days
            'Dec 14', # 4 - Thursday
            'Dec 15', # 5 - Friday
            'Dec 18', # 6 - Monday
            'Dec 19', # 7 - Tuesday
            'Dec 20', #  8 - Wednesday - LAST DAY!
            'Dec 21'
            ]

    hour_blocks = ['10', # 0
             '11', # 1
             '12', # 2
             '1',  # 3
             '2',  # 4
             '4',  # 5
             '5',  # 6
             '6',  # 7
             '7'   # 8
             ]

    minute_blocks = [':00', # 0 (6)
                    ':10', # 1 (7) 13
                    ':20', # 2 (6) 18
                    ':30', # 3 (7)
                    ':40', # 4 (6)
                    ':50'  # 5 (7)
                    ]


    times_list = []

    day_index = 0
    hour_index = 0
    minute_index = 0
    app_count = 0

    
    for slot in time_slots:
        time_string = '{}{}'.format(hour_blocks[hour_index], minute_blocks[minute_index])
        slot_string = '{}{}'.format('#',str(slot))
        time_tuple = Visit(slot_string, day[day_index], time_string)
        
        times_list.append(time_tuple)

        app_count += 1

        if app_count == 6: # 0
            minute_index += 1 
        if app_count == 13: # 1
            minute_index += 1
        if app_count == 19: # 2
            minute_index += 1

        if hour_index == 4:
            if minute_index == 3:
                hour_index += 1
                minute_index = 0
                app_count = 0


        if hour_index == 8:
            if minute_index == 3:
                hour_index = 0
                minute_index = 0
                day_index += 1


        if app_count == 26: # 3
            minute_index += 1
        if app_count == 33: # 4
            minute_index += 1           
        if app_count == 39:# 5
            minute_index = 0
            app_count = 0
            hour_index += 1

    return times_list

def write_gift_sheet():
    '''
    This will output a series of appointment sheets that we can fill in with gift appointments     
    '''

    gift_times = create_time() # a list of all the time slot strings for all of the gift times 1 Dec 6 at 10:00

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
    header_title_date = '&L&"Courier New,Bold"               Salvation Army Appointment Sheet              Page: &P of &N'
    worksheet.set_header(header_title_date)
    
    # http://xlsxwriter.readthedocs.io/worksheet.html
    worksheet.set_column(0, 0, 5.29)  # App #
    worksheet.set_column(1, 1, 6) # Day
    worksheet.set_column(2, 2, 4.86) # Time
    worksheet.set_column(3, 3, 27) # Name
    worksheet.set_column(4, 4, 14) # File #
    worksheet.set_column(5, 5, 19) # Phone #
    worksheet.set_column(6, 6, 5) # of kids
    worksheet.set_column(7, 7, 12) # Girls Ages
    worksheet.set_column(8, 8, 12) # Boys Ages
    worksheet.set_column(9, 9, 20) # Signature
    
    # headers on the columns as a dictionary 
    column_heads = {1 : 'App #',
                    2 : 'Day',
                    3 : 'Time',
                    4 : 'Name (First + Last)',
                    5 : 'File #',
                    6 : 'Phone #',
                    7 : '# of kids',
                    8 : 'Girls Ages',
                    9 : 'Boys Ages',
                    10 : 'Signature' 
                 
                    }
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
            
            header_counter = 1
            spot_counter += 1
            row_count +=1
            worksheet.set_row(row_count, 68)

        
        worksheet.write('A' + str(l_n[2]), slot.slot_number) # write appointment slot number i.e. 1
        worksheet.write('B' + str(l_n[2]), slot.day) # write appointment slot number i.e. 1
        worksheet.write('C' + str(l_n[2]), slot.time) # write appointment slot number i.e. 1
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

# WRITING FUNCTION CALLS

write_gift_sheet()
print('Appointment Sheets Output')



