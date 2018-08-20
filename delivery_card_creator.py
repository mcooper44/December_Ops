"""
This file contains a class and methods to add delivery summaries or delivery
cards to an xlsx file.


"""
import xlsxwriter
from db_parse_functions import itr_joiner

class Delivery_Slips():
    '''
    Creates a xlsx file and adds structured household data to it to create
    delivery cards.

    It builds in a summary card at the head of each route stack and then
    inserts sorted route data after it, adjusting spacing to compensate for
    spacing issues that come up from page transitions


    '''

    def __init__(self, book_name):
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.cell_format_size = None
        self.off_set = 14
        self.spot_counter = 0
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]

        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('food')
            self.worksheet.set_margins(0.25, 0.25, 0.15, 0.15)
            self.worksheet.set_column(0, 0, 14.5) # col A
            self.worksheet.set_column(2, 2, 16.5) # col C
            self.worksheet.set_column(3, 3, 6) # col D
            self.worksheet.set_column(8, 8, 4) # col I
            self.worksheet.set_column(9, 9, 2) # col J
            self.cell_format_size = self.workbook.add_format()
            self.cell_format_size.set_font_size(8) # for writing the address list

    def add_household(self, route, summary):
        '''
        Adds a household to the route card stack in the xlsx file 
        

        route (file ID, Route Number, Route Letter) is from the
        Delivery_Household() class .return_route() method
        
        summary is the named tuple held in the Delivery_Household() class
        and returned by the .return_summary() method. It is orginally created 
        by the visit line object and contains the following 
        (applicant, fname,lname,size,phone,email,address,address2,city,diet)
        '''
        
        _, rn, rl = route # file_id, rn, rl

        diet = summary.diet
        phone = itr_joiner(summary.phone)
        
        # write client info to worksheet
        self.worksheet.write('A{}'.format(self.l_n[1]), '# of Persons:')
        self.worksheet.write('B{}'.format(self.l_n[1]), summary.size)  #hh size
        self.worksheet.write('C{}'.format(self.l_n[1]), summary.fname)    #  first name
        self.worksheet.write('D{}'.format(self.l_n[1]), summary.lname)    # last name
        self.worksheet.write('C{}'.format(self.l_n[5]), 'PHONE: {}'.format(phone)) # write phone
                                                                # number str 
        self.worksheet.write('C{}'.format(self.l_n[2]), summary.address) # address
        self.worksheet.write('C{}'.format(self.l_n[3]), summary.address2)
        self.worksheet.write('A{}'.format(self.l_n[3]), 'CHRISTMAS ID#') 
        self.worksheet.write('B{}'.format(self.l_n[3]), summary.applicant) # ID number
        self.worksheet.write('C{}'.format(self.l_n[4]), summary.city) # city
        self.worksheet.write('D{}'.format(self.l_n[4]), '') # postal code
        self.worksheet.write('A{}'.format(self.l_n[6]), 'SPECIAL DIET: {}'.format(diet))
        self.worksheet.write('H{}'.format(self.l_n[1]), 'ROUTE:')
        self.worksheet.write('I{}'.format(self.l_n[1]), rn)
        self.worksheet.write('J{}'.format(self.l_n[1]), rl)
        at_home_str = 'AT HOME:    YES  /  NO               AT HOME:    YES  /  NO               AT HOME:    YES  /  NO'
 
        # Driving related text strings
        self.worksheet.write('A{}'.format(self.l_n[7]),
                             'DRIVER(S):     ATTEMPT 1_________________ ATTEMPT 2: _________________ ATTEMPT 3:_________________ ')
        self.worksheet.write('B{}'.format(self.l_n[8]), at_home_str)
        self.worksheet.write('B{}'.format(self.l_n[9]), 'LEFT HAMPER:    YES  / NO      LEFT HAMPER:    YES  /  NO       LEFT HAMPER:    YES  /  NO')
        self.worksheet.write('A{}'.format(self.l_n[10]),'LEFT HAMPER WITH: _________________________________')
        self.worksheet.write('F{}'.format(self.l_n[10]),'NEW ADDRESS: ___________________________ ' )

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.off_set -= 4 # change the offset to start on the next page
            for x in range(1, 15):
                self.l_n[x] += self.off_set
            
            self.spot_counter = 0 # start counting from zero
            self.off_set += 4 # reset the offset and procede as normal
            
        else: # use the existing offset because we haven't reached the end 
              # of the page
            for x in range(1, 15):
                self.l_n[x] += self.off_set
    
    def add_route_summary_card(self, summary):
        '''
        Adds a route summary card to the stack
        This method needs to be called before writing an actual route card to
        the stack and provides an 'at a glance' summary of the route with
        information needed by the warehouse team and routing desk team.
        
        summary is a Route_Summary() class held in the
        Delivery_Household_Collection() class .route_summaries attribute
        which is a dictionary keyed off the route number

        '''
        
        rn = summary.route
        box_count = summary.boxes # a Counter()
        # write the Counter() into a string to summarize hh's size
        # in a readable way for the warehouse team to assemble the 
        # route
        family_count = ['{} household(s) of {}'.format(box_count[x], x) \
                                    for x in sorted(box_count.keys())]
        street_set = summary.streets
        hood = itr_joiner(summary.neighbourhood)
        #applicant_list = summary.applicant_list
        #size_counter = summary.sizes
        letter_map = summary.letter_map # 'Box: A Family: 3 Diet: Halal
        
        # write client info to worksheet
        self.worksheet.write('A{}'.format(self.l_n[1]), 
                             'Route: {}'.format(rn))
        self.worksheet.write('A{}'.format(self.l_n[2]), 
                             'Neighbourhood(s):{}'.format(str(hood))) 
        self.worksheet.write('A{}'.format(self.l_n[3]), 
                             'STREETS:') # Title
        self.worksheet.write('C{}'.format(self.l_n[3]), 
                             'HOUSEHOLDS:') # Title
        self.worksheet.write('H{}'.format(self.l_n[3]), 
                             'SUMMARY OF BOXES') # Title

        s_c = self.l_n[4]
        for street in street_set: # writes delivery streets
            self.worksheet.write('A{}'.format(s_c), street, 
                                 self.cell_format_size)
            s_c +=1
        
        lmap = self.l_n[4]
        for hh_sum in letter_map: # writes 'Box A Family: 2 Diet: Diabetic'
            self.worksheet.write('C{}'.format(lmap), letter_map[hh_sum])
            lmap += 1
        
        b_f = self.l_n[4]
        for size_bc in family_count:# writes '1 household(s) of 1' etc
            self.worksheet.write('H{}'.format(b_f), '{}'.format(size_bc))
            b_f += 1

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.off_set -= 4 # change the offset to start on the next page
            for x in range(1, 15):
                self.l_n[x] += self.off_set

            self.spot_counter = 0 # start counting from zero
            self.off_set += 4 # reset the offset and procede as normal
            
        else: # use the existing offset because we haven't reached the end of the page
            for x in range(1,15):
                self.l_n[x] += self.off_set

    def close_worksheet(self):
        self.workbook.close()
        print('workbook closed')


