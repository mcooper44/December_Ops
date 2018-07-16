"""
This file opens a database of households and routes and generates delivery
slips for printing.
"""
import xlsxwriter
from db_parse_functions import itr_joiner
#http://xlsxwriter.readthedocs.io/format.html


class Delivery_Slips():
    '''
    This is a monster, but it works? 
    Initialize key points of information and formatting
    Then cycle through the sorted routes and insert them into the 
    correct cells, then move them down, and adjust for the weirdness
    that happens with page transitions.

    '''
    def __init__(self, book_name):
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.cell_format_size = None
        self.off_set = 14
        self.spot_counter = 0
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]

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
        file_id, rn, rl = route

        diet = summary.diet
        phone = itr_joiner(summary.phone)
        # cell locations 
        hh_str = 'A{}'.format(self.l_n[1]) # string hh size
        hh_size = 'B{}'.format(self.l_n[1]) # actual hh size
        f_name = 'C{}'.format(self.l_n[1]) # first name cell location
        l_name = 'D{}'.format(self.l_n[1]) # l name cell location
        ph_num = 'C{}'.format(self.l_n[5]) # phone number cell location
        address = 'C{}'.format(self.l_n[2]) # street address
        addressl2 = 'C{}'.format(self.l_n[3]) # line 2 unit number et al
        ID_num_str = 'A{}'.format(self.l_n[3]) # string of Request ID 
        ID_num = 'B{}'.format(self.l_n[3]) # actual ID number
        city = 'C{}'.format(self.l_n[4])
        postal_code = 'D{}'.format(self.l_n[4])
        diet_str = 'A{}'.format(self.l_n[6])
        #diet_actual = 'B{}'.format(self.l_n[5])
        driver_str = 'A{}'.format(self.l_n[8])# driver name string
        route_str = 'H{}'.format(self.l_n[1]) # Route string
        route_num = 'I{}'.format(self.l_n[1]) # Actual route number
        route_let = 'J{}'.format(self.l_n[1]) # Actual Route Letter


        at_home_str = 'A{}'.format(self.l_n[9])
        at_yes_str = 'B{}'.format(self.l_n[9])
        new_add_str = 'E{}'.format(self.l_n[9])
        left_hamper = 'A{}'.format(self.l_n[10])
        left_w_y_str = 'B{}'.format(self.l_n[10])
        left_with_str = 'E{}'.format(self.l_n[10])
        
        # write client info to worksheet
        self.worksheet.write(hh_str, '# of Persons:')
        self.worksheet.write(hh_size, summary.size)  #hh size
        self.worksheet.write(f_name, summary.fname)    #  first name
        self.worksheet.write(l_name, summary.lname)    # last name
        self.worksheet.write(ph_num, 'PHONE: {}'.format(phone)) # write the first phone number we have
        self.worksheet.write(address, summary.address) # address
        self.worksheet.write(ID_num_str, 'CHRISTMAS ID#') 
        self.worksheet.write(ID_num, summary.applicant) # ID number
        self.worksheet.write(city, summary.city) # city
        self.worksheet.write(postal_code, '_') #postal code
        self.worksheet.write(diet_str, 'SPECIAL DIET: {}'.format(diet))
        #self.worksheet.write(diet_actual, diet) # diet issues/preferences
        self.worksheet.write(route_str, 'ROUTE:')
        self.worksheet.write(route_num, rn)
        self.worksheet.write(route_let, rl)

        # Driving related text strings
        self.worksheet.write(driver_str, 'DRIVER NAME(s): ATTEMPT 1_________________ ATTEMPT 2: _________________ ATTEMPT 3:_________________')
        self.worksheet.write(at_home_str, 'AT HOME:')
        self.worksheet.write(at_yes_str, 'YES   /   NO ')
        self.worksheet.write(new_add_str, 'NEW ADDRESS: __________________________________')            
        self.worksheet.write(left_hamper, 'LEFT HAMPER:')
        self.worksheet.write(left_w_y_str, 'YES   /   NO')
        self.worksheet.write(left_with_str, 'WITH: __________________________________________')

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.off_set -= 4 # change the offset to start on the next page
            
            self.l_n[1] += self.off_set
            self.l_n[2] += self.off_set
            self.l_n[3] += self.off_set
            self.l_n[4] += self.off_set
            self.l_n[5] += self.off_set
            self.l_n[6] += self.off_set
            self.l_n[7] += self.off_set
            self.l_n[8] += self.off_set
            self.l_n[9] += self.off_set
            self.l_n[10] += self.off_set
            self.l_n[11] += self.off_set
            self.l_n[12] += self.off_set
            self.l_n[13] += self.off_set
            self.l_n[14] += self.off_set
            
            self.spot_counter = 0 # start counting from zero
            self.off_set += 4 # reset the offset and procede as normal
            
        else: # use the existing offset because we haven't reached the end of the page
                        
            self.l_n[1] += self.off_set
            self.l_n[2] += self.off_set
            self.l_n[3] += self.off_set
            self.l_n[4] += self.off_set
            self.l_n[5] += self.off_set
            self.l_n[6] += self.off_set
            self.l_n[7] += self.off_set
            self.l_n[8] += self.off_set
            self.l_n[9] += self.off_set
            self.l_n[10] += self.off_set
            self.l_n[11] += self.off_set
            self.l_n[12] += self.off_set
            self.l_n[13] += self.off_set
            self.l_n[14] += self.off_set 
    
    def add_route_summary_card(self, summary):
        '''
        Adds a route summary card to the stack
        call this method before writing an actual route card
        to the stack
        '''
        rn = summary.route
        box_count = summary.boxes
        family_count = ['{} household(s) of {}'.format(box_count[x], x) for x in \
                                  box_count]
        street_set = summary.streets
        hood = itr_joiner(summary.neighbourhood)
        applicant_list = summary.applicant_list
        size_counter = summary.sizes
        letter_map = summary.letter_map # 'Box: A Family: 3 Diet: Halal
        
        # locations written as strings
        rn_loc = 'A{}'.format(self.l_n[1]) # route number
        hood_loc = 'A{}'.format(self.l_n[2]) # neighbourhood
        # locations with titles and separate string writes
        street_title_loc = 'A{}'.format(self.l_n[3]) # STREETS: 
        family_title_loc = 'C{}'.format(self.l_n[3]) # FAMILIES
        box_summary_loc = 'H{}'.format(self.l_n[3]) # BOX SUMMARY

        # write client info to worksheet
        self.worksheet.write(rn_loc, 'Route: {}'.format(rn))
        self.worksheet.write(hood_loc, 'Neighbourhood(s):{}'.format(str(hood)))
        self.worksheet.write(street_title_loc, 'STREETS:')
        self.worksheet.write(family_title_loc, 'HOUSEHOLDS:')
        self.worksheet.write(box_summary_loc, 'SUMMARY OF BOXES')

        s_c = self.l_n[4]
        for street in street_set:
            self.worksheet.write('A{}'.format(s_c), street, self.cell_format_size)
            s_c +=1
        
        lmap = self.l_n[4]
        for hh_sum in letter_map:
            self.worksheet.write('C{}'.format(lmap), letter_map[hh_sum])
            lmap += 1
        
        b_f = self.l_n[4]
        for size_bc in family_count:
            self.worksheet.write('H{}'.format(b_f), '{}'.format(size_bc))
            b_f += 1

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.off_set -= 4 # change the offset to start on the next page
            
            self.l_n[1] += self.off_set
            self.l_n[2] += self.off_set
            self.l_n[3] += self.off_set
            self.l_n[4] += self.off_set
            self.l_n[5] += self.off_set
            self.l_n[6] += self.off_set
            self.l_n[7] += self.off_set
            self.l_n[8] += self.off_set
            self.l_n[9] += self.off_set
            self.l_n[10] += self.off_set
            self.l_n[11] += self.off_set
            self.l_n[12] += self.off_set
            self.l_n[13] += self.off_set
            self.l_n[14] += self.off_set


            self.spot_counter = 0 # start counting from zero
            self.off_set += 4 # reset the offset and procede as normal
            
        else: # use the existing offset because we haven't reached the end of the page
                        
            self.l_n[1] += self.off_set
            self.l_n[2] += self.off_set
            self.l_n[3] += self.off_set
            self.l_n[4] += self.off_set
            self.l_n[5] += self.off_set
            self.l_n[6] += self.off_set
            self.l_n[7] += self.off_set
            self.l_n[8] += self.off_set
            self.l_n[9] += self.off_set
            self.l_n[10] += self.off_set
            self.l_n[11] += self.off_set
            self.l_n[12] += self.off_set
            self.l_n[13] += self.off_set
            self.l_n[14] += self.off_set


    def close_worksheet(self):
        self.workbook.close()
        print('workbook closed')


