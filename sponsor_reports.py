"""
This provides classes and methods to write the reports that partners need
to conduct their branch of the operations
"""
import xlsxwriter
from db_data_models import Person

def extract_special_number(notes_string, double_symbol='##'):
    '''
    this function extracts the gift appointment number or turkey voucher number
    in the double ## or $$ signs in the notes section
    ## = gift appointment number
    $$ = turkey voucher code number
    '''
    # to clear out the carriage returns
    # https://stackoverflow.com/questions/3739909/how-to-strip-all-whitespace-from-string
    #compressed_string = ''.join(notes_string.split())
    if double_symbol in notes_string:

        trash1, trash2, number_str_hashsigns = notes_string.partition(double_symbol)
        number_str, trash3, trash4 = number_str_hashsigns.partition(double_symbol)
        return number_str
    else:
        return False

# MAIN FILE WRITING FUNCTIONS
class Report_File():
    '''
    This class provides methods to write summary data into a report that 
    partners can use to move forward with their work
    '''
    def __init__(self, book_name):
        self.name = book_name
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet1 = None # printable sponsor report
        self.worksheet2 = None # list of kids
        self.bold = None
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] 
        self.off_set = 16
        self.spot_counter = 0
        self.clc = 1 # 'Child List Counter' used to keep track of what line
                     # we are writing to
        if self.workbook:
            self.worksheet1 = self.workbook.add_worksheet('Sponsor_Report')
            self.worksheet2 = self.workbook.add_worksheet('Child_Report')
            self.worksheet1.set_margins(0.25,0.25,0.15,0.15)
            self.bold = self.workbook.add_format({'bold': True})
    

    def add_household(self, summary, family, age_cutoff = 18): 
        '''
        Adds a formatted summary to the xlsx file for the household
        that lists address, dietary issues, and family member details
        summary = the base summary for writing delivery slips
        family = family tuples
        age_cutoff = default age divider to separate kids who get services
        and 'adults' or people who are too old depending on service provider
        '''
        #gift_appointment = extract_special_number(request[11], double_symbol='##')
        
        #sort household members into adults and kids
        adults = []
        kids = []
        applicant = Person((summary.applicant, 
                            summary.lname,
                            summary.fname,
                            None,
                            None,
                            None,
                            None,
                            None))
        adults.append(applicant)
        if family:
            for family_member in family:
                po = Person(family_member)
                if po.is_adult(Age = age_cutoff):
                    adults.append(po)
                else:
                    kids.append(po)

        # client info
        self.worksheet1.write('A{}'.format(self.l_n[1]), 
                        'CHRISTMAS ID: {}'.format(summary.applicant),
                        self.bold)
        self.worksheet1.write('A{}'.format(self.l_n[2]), 
                         'Family Size: {}'.format(summary.size))
        self.worksheet1.write('C{}'.format(self.l_n[1]), summary.address)
        self.worksheet1.write('C{}'.format(self.l_n[2]), summary.address2)
        self.worksheet1.write('C{}'.format(self.l_n[3]), '{}, {}'.format(summary.city,
                                                                   summary.postal))
        # title strings
        self.worksheet1.write('A{}'.format(self.l_n[5]), 'ADULTS', self.bold)
        self.worksheet1.write('D{}'.format(self.l_n[5]), 'CHILDREN', self.bold)
        self.worksheet1.write('F{}'.format(self.l_n[5]), 'AGE', self.bold)
        self.worksheet1.write('G{}'.format(self.l_n[5]), 'GENDER', self.bold)

        a_c_i = self.l_n[7] # adult cell index

        for a in adults:
            self.worksheet1.write('A{}'.format(a_c_i), '{} {}'.format(a.person_Fname,
                                                                a.person_Lname))                            
            a_c_i += 1

        # now the kids names...
        k_c_i = self.l_n[6] # kids cell index
                            # for keeping track of writing kids info on the 
                            # summary card
        # write 
        for  k in kids:
            self.worksheet1.write('D{}'.format(k_c_i),
                                   k.person_Fname)
            self.worksheet1.write('F{}'.format(k_c_i),
                                   k.person_Age)
            self.worksheet1.write('G{}'.format(k_c_i),
                                   k.person_Gender)
            # write to the second sheet
            self.worksheet2.write('A{}'.format(self.clc),
                             '{}'.format(k.person_Fname))
            self.worksheet2.write('B{}'.format(self.clc),
                             '{}'.format(k.person_Lname))
            self.worksheet2.write('C{}'.format(self.clc),
                             '{}'.format(k.person_Age))
            self.worksheet2.write('D{}'.format(self.clc),
                             '{}'.format(k.person_Gender))
            self.worksheet2.write('E{}'.format(self.clc),
                             '{}'.format(summary.applicant))
                             

            self.clc +=1 # increment the line count for the second sheet
            k_c_i += 1 # increment the sub line counter for the main sheet

        self.spot_counter += 1 # move the spot up one

        if self.spot_counter == 3: # if there are three spots on the page
            self.off_set += 4 # change the offset to start on the next page
            for x in range(1, 11):
                self.l_n[x] += self.off_set

            self.spot_counter = 0 # start counting from zero
            self.off_set -= 4 # reset the offset and procede as normal

        else: # use the existing offset because we haven't reached the end of the page
            for x in range(1, 11):
                self.l_n[x] += self.off_set

    def close_worksheet(self):
        '''
        close the workbooks and call it a day
        '''
        self.workbook.close()
        print('workbook {} closed'.format(self.name))
