"""
Contains two classses for adding sheets to the delivery binder
and for creating a master list of all routes and the 
household details of each route

Binder_Sheet() creates an xlsx workbook and had a method to
add a structured route summary to it block by block

Office_Sheet() creates an xlsx workbook and has a method to
add a household summary for each HH within a route
It essentially rebuilds the source CSV with route information
and a minimized set of household and household member info

"""

import string
import xlsxwriter
from db_parse_functions import itr_joiner

class Binder_Sheet():
    '''
    This class manages adding routes to the route binder

    It takes summary data about the route and prints it into the book
    to assist with coordinating deliveries

    '''

    def __init__(self, book_name):
        self.book_name = book_name
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.cell_format_size = None
        self.route_cell_size = None
        self.wrap = None
        self.off_set = 8 # how many lines is the block we are writing out
        self.spot_counter = 0
        self.title_flag = True
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]
        
        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('route_binder')
            self.worksheet.set_margins(0.2, 0.2, 0.75, 0.75) # L,R,T,B
            self.worksheet.set_header('', {'margin': 0.29}) # header margin
            self.worksheet.set_footer('', {'margin': 0.29}) # footer margin
            self.worksheet.set_column(0, 0, 5) # col A
            self.worksheet.set_column(1, 1, 17) # col B
            self.worksheet.set_column(2, 2, 8) # col C
            self.worksheet.set_column(3, 3, 8.15) # col D
            self.worksheet.set_column(4, 4, 28) # col E
            self.worksheet.set_column(5, 10, 10) # col F:K
            
            self.cell_format_size = self.workbook.add_format()
            self.cell_format_size.set_font_size(9) # for writing the address list

            self.route_cell_size = self.workbook.add_format()
            self.route_cell_size.set_font_size(24)
            
            self.wrap = self.workbook.add_format()
            self.wrap.set_text_wrap()

            self.worksheet.set_landscape()

    def add_route(self, summary):
        '''
        distributes information from the summary and writes it into the
        sheet in a structured way

        The summary is a Route_Summary object created by and stored in 
        a Delivery_Household_Collection class from the 
        basket_sorting_Geocodes.py script

        '''

        rn = summary.route # route number
        street_list= summary.street_list # list of streets
        letter_map = summary.letters # container holding 'Box: A Family: 2 Diet:'

        package = summary.get_service_dict()
        
        self.worksheet.merge_range('A{}:A{}'.format(self.l_n[2],
                                                    self.l_n[8]),'')
        self.worksheet.merge_range('B{}:B{}'.format(self.l_n[2],
                                                    self.l_n[8]),'')
        self.worksheet.merge_range('C{}:C{}'.format(self.l_n[2], self.l_n[8]),rn, self.route_cell_size)
        if self.title_flag:
            self.worksheet.write('A{}'.format(self.l_n[1]), 'DATE', self.wrap)
            self.worksheet.write('B{}'.format(self.l_n[1]), 'DRIVER NAME', self.wrap)
            self.worksheet.write('C{}'.format(self.l_n[1]), 'ROUTE NUMBER', self.wrap)
            self.worksheet.write('D{}'.format(self.l_n[1]), 'ROUTE LETTER', self.wrap)
            self.worksheet.write('E{}'.format(self.l_n[1]), 'ROUTE DETAILS', self.wrap)
            self.worksheet.write('F{}'.format(self.l_n[1]), 'DELIVERED', self.wrap)
            self.worksheet.write('G{}'.format(self.l_n[1]), 'RETURNED', self.wrap)
            self.worksheet.write('H{}'.format(self.l_n[1]), 'RE-ROUTED WITH', self.wrap)
            self.worksheet.write('I{}'.format(self.l_n[1]), 'RE-ROUTED WITH', self.wrap)
            self.worksheet.write('J{}'.format(self.l_n[1]), 'RE-ROUTED WITH', self.wrap)
            self.worksheet.write('K{}'.format(self.l_n[1]), 'RE-ROUTED WITH', self.wrap)
            self.title_flag = False
        # write route number
        # self.worksheet.write('C{}'.format(l_n[2]), rn, self.route_cell_size)

        rl_c = self.l_n[2] # Route Letter counter
        for family in sorted(package.keys()): # for box, size, diet in ...
            
            rl = package[family].letter
            self.worksheet.write('D{}'.format(rl_c), rl)
            rl_c +=1

        s_c = self.l_n[2] # street counter
        for family in sorted(package.keys()):
            street = package[family].street
            self.worksheet.write('E{}'.format(s_c), street, self.cell_format_size)
            s_c +=1

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.title_flag = True
            self.off_set += 0 # change the offset to start on the next page
    
            for x in range(1,15):
                self.l_n[x] += self.off_set
            
            self.spot_counter = 0 # start counting from zero
            self.off_set -= 0 # reset the offset and procede as normal
            
        else: # use the existing offset because we haven't reached the end of the page
            for x in range(1,15):
                self.l_n[x] += self.off_set

    
    def close_worksheet(self):
        self.workbook.close()
        print('workbook {} closed'.format(self.book_name))

class Office_Sheet():
    '''
    Creates an xlsx file with a listing of the household details of the
    applicant.  This is key for the operations centre to connect callers with
    details about the route their box was sent on.
    By cross referencing fid's from L2F we can determine what the route
    is for the hh calling in that cold week
    '''
    
    def __init__(self, book_name):
        self.book_name = book_name
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.title_flag = True # should a title be written?
        self.l_n = 1 # line counter
        self.letters = None
        self.headers = ['Route Number', 'Route Letter', 'File ID',
                        'F. Name', 'L. Name', 'Address', 'City', 
                        'Phone', 'Family Size']
        self.fheaders = [] # something to hold the headers in
        self.cells = None # something to hold the letter designators for
                          # the workbook
        
        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('ops_reference')
            # build the header titles
            # We can dynamically generate the needed range of them by joining
            # cell locations AA1 with the header strings
            # 1. First generate the cell locations by joining letters with 
            #    Numbers
            a = ['{}{}'.format(x,y) for x in string.ascii_uppercase for y in \
                 string.ascii_uppercase] # AA..ZZ
            b = [x for x in string.ascii_uppercase] # A..Z
            self.cells = b + a # A ... ZZ
            
            # 2. Generate the family member headers by joining headers with
            #    Family Mem etc. with numbers
            frange = range(1, 16)
            for num in frange:
                for head in self.headers[2:5]:
                    self.headers.append('Family Mem {}- {}'.format(num, head))
    
    def add_line(self, route, summary, family):    
        '''
        Add an entry to the ops sheet so staff can find details
        about routes when people are calling

        lists route #, letter, name of main app and family members
        in the HH

        route is (fid, rn, rl) from the Delivery_Household() .return_route()
        method
        
        summary is the named tuple used to build the route cards generated by
        the Delivery_Household() .return_summary() method

        family is the family member summary held in the 
        Delivery_Household() .family_members attribute
        (ID, Lname, Fname, DOB, Age, Gender, Ethnicity, Identity)
        the first three items of which are written to the sheet
        '''
        
        _, rn, rl, _ = route

        if self.title_flag:
            # write the titles
            l_h = list(zip(self.cells, self.headers))
            for tp in l_h:
                l, h = tp
                self.worksheet.write('{}1'.format(l), h)
            self.title_flag = False
            self.l_n += 1

        self.worksheet.write('A{}'.format(self.l_n), rn)
        self.worksheet.write('B{}'.format(self.l_n), rl)
        self.worksheet.write('C{}'.format(self.l_n), summary.applicant) # ID number
        self.worksheet.write('D{}'.format(self.l_n), summary.fname)    #  first name
        self.worksheet.write('E{}'.format(self.l_n), summary.lname)    # last name
        self.worksheet.write('F{}'.format(self.l_n), summary.address) # address 
        self.worksheet.write('G{}'.format(self.l_n), summary.city) # city 
        self.worksheet.write('H{}'.format(self.l_n), 'None') 
        self.worksheet.write('I{}'.format(self.l_n), summary.size)  #hh size
        # markers to march across the line and write family data
        start = 9
        middle = 10
        end = 11
        if family: # if family is present
            for mem in family: # for tuple in collection
                fmid, ln, fn  = mem[:3] # use the first three fid, ln, fn
                idh = self.cells[start] # identify the Letter index
                fnh = self.cells[middle]
                lnh = self.cells[end]
            
                self.worksheet.write('{}{}'.format(idh, self.l_n), fmid)
                self.worksheet.write('{}{}'.format(fnh, self.l_n), fn)
                self.worksheet.write('{}{}'.format(lnh, self.l_n), ln)
                start += 3 # move forward for the next slice
                middle += 3
                end += 3
        self.l_n +=1 # increment for writing the next line

    def close_worksheet(self):
        '''
        Closes the workbook file and ends the write process
        '''
        
        self.workbook.close()
        print('workbook {} closed'.format(self.book_name))
