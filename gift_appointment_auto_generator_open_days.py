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

file_output = 'gift sign up 2017 - unstructured.xlsx'

Visit = namedtuple('Visit', 'slot_number, day, time') 

def create_time():
    '''
    This function returns a list of strings to insert into the booking sheet
    '''
    time_slots = [x for x in range(1, 547)] # 39 apps/hour * 8 hours * 2 days +1 b/c of how range works

    day = 'Dec 10 or 11'          
    time = '10-2:00 or 4-6:30'
    times_list = [] # containter for times
    for slot in time_slots:
        time_string = '{}'.format(time)
        slot_string = '{}{}'.format('Open ',str(slot))
        time_tuple = Visit(slot_string, day, time_string)
        
        times_list.append(time_tuple)
        
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
    
    bold = workbook.add_format({'bold': True,
                                'text_wrap': True})
                                # setup the ability to bold things
    wrap = workbook.add_format({'text_wrap': True, 
                                'center_across': True})

    worksheet = workbook.add_worksheet('Booking Sheet') # add worksheet _booking sheet_
    worksheet.set_margins(0.1,0.1,.9,.1) # Left,Right,Top,Bottom margins set
    worksheet.set_landscape() # set page landscape format so we can put more material on the page
    #  http://xlsxwriter.readthedocs.io/example_headers_footers.html
    header_title_date = '&L&"Courier New,Bold"               Salvation Army Appointment Sheet              Page: &P of &N'
    worksheet.set_header(header_title_date)
    
    # http://xlsxwriter.readthedocs.io/worksheet.html
    worksheet.set_column(0, 0, 8.57) #
    worksheet.set_column(1, 1, 6) # Day
    worksheet.set_column(2, 2, 27) # Name
    worksheet.set_column(3, 3, 14) # File #
    worksheet.set_column(4, 4, 19) # Phone #
    worksheet.set_column(5, 5, 8) # of kids
    worksheet.set_column(6, 6, 12) # Girls Ages
    worksheet.set_column(7, 7, 12) # Boys Ages
    worksheet.set_column(8, 8, 20) # Signature
    
    # headers on the columns as a dictionary 
    column_heads = {1 : 'App #',
                    2 : 'Day',
                    3 : 'Name (First + Last)',
                    4 : 'File #',
                    5 : 'Phone #',
                    6 : '# of kids',
                    7 : 'Girls Ages',
                    8 : 'Boys Ages',
                    9 : 'Signature'  
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
            worksheet.write('I{}'.format(l_n[1]), 'Signature')
            
            header_counter = 1
            spot_counter += 1
            row_count +=1
            # https://xlsxwriter.readthedocs.io/worksheet.html#set_row
            worksheet.set_row(row_count, 68)

        
        worksheet.write('A' + str(l_n[2]), slot.slot_number,wrap) # write appointment slot number i.e. 1
        worksheet.write('B' + str(l_n[2]), slot.day, wrap) # write appointment slot number i.e. 1
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



