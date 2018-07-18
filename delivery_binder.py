"""
This script adds routes to the delivery
route desk binder for coordinating drivers and routes
in the cold cold month of December
"""
import xlsxwriter
from db_parse_functions import itr_joiner
#http://xlsxwriter.readthedocs.io/format.html


class Binder_Sheet():
    '''
    This class manages adding routes to the route binder

    '''
    def __init__(self, book_name):
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.cell_format_size = None
        self.route_cell_size = None
        self.wrap = None
        self.off_set = 8 # was 14
        self.spot_counter = 0
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
        
        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('route_binder')
            self.worksheet.set_margins(0.1, 0.1, 2, 0.25) # L,R,T,B
            self.worksheet.set_column(0, 0, 6.5) # col A
            self.worksheet.set_column(1, 1, 17) # col B
            self.worksheet.set_column(2, 2, 9) # col C
            self.worksheet.set_column(3, 3, 8.15) # col D
            self.worksheet.set_column(4, 4, 32) # col E
            self.worksheet.set_column(5, 10, 10) # col F:K
            
            self.cell_format_size = self.workbook.add_format()
            self.cell_format_size.set_font_size(9) # for writing the address list

            self.route_cell_size = self.workbook.add_format()
            self.route_cell_size.set_font_size(36)
            
            self.wrap = self.workbook.add_format()
            self.wrap.set_text_wrap()

            self.worksheet.set_landscape()

    def add_route(self, summary):
        rn = summary.route # route number
        street_set = summary.streets # set of streets
        letter_map = summary.letters # container holding 'Box: A Family: 2 Diet:'

        self.worksheet.merge_range('A{}:A{}'.format(self.l_n[2],
                                                    self.l_n[8]),'')
        self.worksheet.merge_range('B{}:B{}'.format(self.l_n[2],
                                                    self.l_n[8]),'')
        self.worksheet.merge_range('C{}:C{}'.format(self.l_n[2],
                                                    self.l_n[8]),rn, self.route_cell_size)
        
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
        # write route number
        # self.worksheet.write('C{}'.format(l_n[2]), rn, self.route_cell_size)

        rl_c = self.l_n[2] # Route Letter counter
        for letter in letter_map: # for box, size, diet in ...
            rl = letter
            self.worksheet.write('D{}'.format(rl_c), rl)
            rl_c +=1

        s_c = self.l_n[2] # street counter
        for street in street_set:
            self.worksheet.write('E{}'.format(s_c), street, self.cell_format_size)
            s_c +=1

        self.spot_counter += 1 # move the spot up one
        
        if self.spot_counter == 4: # if there are four spots on the page
            self.off_set -= 8 # change the offset to start on the next page
            
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
            self.off_set += 8 # reset the offset and procede as normal
            
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


