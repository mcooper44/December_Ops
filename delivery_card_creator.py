"""
This file opens a database of households and routes and generates delivery
slips for printing.
"""
import xlsxwriter
#http://xlsxwriter.readthedocs.io/format.html

# HELPER FUNCTIONS

def diet_parser(diet_string):
    '''
    this function returns the string of redundant dietary conditions
    minus the redundant conditions
    '''
    diet = ''
    diet_conditions = set(diet_string.split(',')) # a unique list of conditions split on the commas
    if 'Other' in diet_conditions:
        diet_conditions.remove('Other') # we don't want this because it has zero information
    
    for condition in diet_conditions:
        if condition != '': # if it's not blank
            diet += ', '.join(condition)
    return diet

class Delivery_Slips():
    def __init__(self, book_name):
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.off_set = 14
        self.spot_counter = 0
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10]

        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('food')
            self.worksheet.set_margins(0.25, 0.25, 0.15, 0.15)
        
    def add_household(self, route, summary):
        file_id, rn, rl = route

        diet = diet_parser(summary.diet) # reformat diet to cut out redundancy
        phone = str(summary.phone) # whatever is in the field - not the formatted number
        # cell locations 
        hh_str = 'A{}'.format(self.l_n[1]) # string hh size
        hh_size = 'B{}'.format(self.l_n[1]) # actual hh size
        f_name = 'C{}'.format(self.l_n[1]) # first name cell location
        l_name = 'D{}'.format(self.l_n[1]) # l name cell location
        ph_num = 'A{}'.format(self.l_n[5]) # phone number cell location
        address = 'C{}'.format(self.l_n[2]) # street address
        addressl2 = 'C{}'.format(self.l_n[3]) # line 2 unit number et al
        ID_num_str = 'A{}'.format(self.l_n[3]) # string of Request ID 
        ID_num = 'B{}'.format(self.l_n[3]) # actual ID number
        city = 'C{}'.format(self.l_n[4])
        postal_code = 'D{}'.format(self.l_n[4])
        diet_str = 'A{}'.format(self.l_n[6])
        diet_actual = 'B{}'.format(self.l_n[5])
        driver_str = 'A{}'.format(self.l_n[8])# driver name string
        route_str = 'H{}'.format(self.l_n[1]) # Route string
        route_num = 'I{}'.format(self.l_n[1]) # Actual route number
        route_let = 'J{}'.format(self.l_n[1]) # Actual Route Letter


        at_home_str = 'A{}'.format(self.l_n[9])
        at_yes_str = 'B{}'.format(self.l_n[9])
        #at_no_str = 'C{}'.format(self.l_n[9])
        new_add_str = 'E{}'.format(self.l_n[9])
        left_hamper = 'A{}'.format(self.l_n[10])
        left_w_y_str = 'B{}'.format(self.l_n[10])
        left_w_n_str = 'C{}'.format(self.l_n[10])
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
        self.worksheet.write(diet_actual, diet) # diet issues/preferences
        self.worksheet.write(route_str, 'ROUTE:')
        self.worksheet.write(route_num, rn)
        self.worksheet.write(route_let, rl)

        # Driving related text strings
        self.worksheet.write(driver_str, 'DRIVER NAME:_______________________________________________________________________')
        self.worksheet.write(at_home_str, 'AT HOME:')
        self.worksheet.write(at_yes_str, 'YES / NO ')
        #self.worksheet.write(at_no_str, 'NO')
        self.worksheet.write(new_add_str, 'NEW ADDRESS: __________________________________')            
        self.worksheet.write(left_hamper, 'LEFT HAMPER:')
        self.worksheet.write(left_w_y_str, 'YES / NO')
        self.worksheet.write(left_w_n_str, 'NO')
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
    
    def close_worksheet(self):
        self.workbook.close()
        print('workbook closed')
