'''
This script is used to parse the google form csv that people fill out
to apply.

It creates a csv file with all the data formatted for easy reading
and for the intake team to process.

'''

import csv
from collections import namedtuple
from datetime import datetime
import xlsxwriter

SOURCE_FILE = 'sources/2020 Christmas Bureau Sign Up.csv'
PERSON = namedtuple('person', 'f_name, l_name, bday, relationship')
FORMAT_STRING =  "%Y/%m/%d %H:%M:%S"

class g_form_response:
    def __init__(self, line):
        self.stamp = line[0]
        self.tstamp = line[0].split(' ')[:2]
        self.date = datetime.strptime(f'{self.tstamp[0]} {self.tstamp[1]}', FORMAT_STRING)
        self.user = line[1]
        self.address = {'city': line[6], 'housing_type': line[7],
                        'street_address': line[8], 'app_unit_num': line[11],
                        'buzzer': line[12]}
        self.emails = (line[13], line[14])
        self.phone = {'Primary_Number': line[16], 'can_text': line[17],
                      'Secondary': line[18]}
        self.soi = line[19]
        self.family_size = line[20]
        self.primary_app = None
        self.family = []
        self.ten_plus = None
        self.gifts = {'have kids': line[228], 'want gifts': line[229]}
        self.items = line[230]
        self.diet = line[231]
        self.car = line[232]
        self.gift_ask = line[233]

        if self.family_size.lower() == "1 Person (I'm applying as a single person)".lower():
            self.primary_app = PERSON(line[21], line[22], line[23], 'Primary')
        if self.family_size.lower() == '2 people'.lower():
            self.primary_app = PERSON(line[24], line[25], line[26], 'Primary')
            self.family.append(PERSON(line[27], line[28], line[29], line[30]))
        if self.family_size == '3 People':
            self.primary_app = PERSON(line[31], line[32], line[33], 'Primary')
            self.family.append(PERSON(line[34], line[35], line[36], line[37]))
            self.family.append(PERSON(line[38], line[39], line[40], line[41]))
        if self.family_size == '4 People':
            self.primary_app = PERSON(line[42], line[43], line[44], 'Primary')
            self.family.append(PERSON(line[45], line[46], line[47], line[48]))
            self.family.append(PERSON(line[49], line[50], line[51], line[52]))
            self.family.append(PERSON(line[53], line[54], line[55], line[56]))
        if self.family_size.lower() == '5 people'.lower():
            self.primary_app = PERSON(line[57], line[58], line[59], 'Primary')
            self.family.append(PERSON(line[60], line[61], line[62], line[63]))
            self.family.append(PERSON(line[64], line[65], line[66], line[67]))
            self.family.append(PERSON(line[68], line[69], line[70], line[71]))
            self.family.append(PERSON(line[72], line[73], line[74], line[75]))
        if self.family_size == '6 People':
            self.primary_app = PERSON(line[76], line[77], line[78], 'Primary')
            self.family.append(PERSON(line[79], line[80], line[81], line[82]))
            self.family.append(PERSON(line[83], line[84], line[85], line[86]))
            self.family.append(PERSON(line[87], line[88], line[89], line[90]))
            self.family.append(PERSON(line[91], line[92], line[93], line[94]))
            self.family.append(PERSON(line[95], line[96], line[97], line[98]))
        if self.family_size == '7 People':
            self.primary_app = PERSON(line[99], line[100], line[101], 'Primary')
            self.family.append(PERSON(line[102], line[103], line[104], line[105]))
            self.family.append(PERSON(line[106], line[107], line[108], line[109]))
            self.family.append(PERSON(line[110], line[111], line[112], line[113]))
            self.family.append(PERSON(line[114], line[115], line[116], line[117]))
            self.family.append(PERSON(line[118], line[119], line[120], line[121]))
            self.family.append(PERSON(line[122], line[123], line[124], line[125]))
        if self.family_size == '8 People':
            self.primary_app = PERSON(line[126], line[127], line[128], 'Primary')
            self.family.append(PERSON(line[129], line[130], line[131], line[132]))
            self.family.append(PERSON(line[133], line[134], line[135], line[136]))
            self.family.append(PERSON(line[137], line[138], line[139], line[140]))
            self.family.append(PERSON(line[141], line[142], line[143], line[144]))
            self.family.append(PERSON(line[145], line[146], line[147], line[148]))
            self.family.append(PERSON(line[149], line[150], line[151], line[152]))
            self.family.append(PERSON(line[153], line[154], line[155], line[156]))
        if self.family_size == '9 People':
            self.primary_app = PERSON(line[157], line[158], line[159], 'Primary')
            self.family.append(PERSON(line[160], line[161], line[162], line[163]))
            self.family.append(PERSON(line[164], line[165], line[166], line[167]))
            self.family.append(PERSON(line[168], line[169], line[170], line[171]))
            self.family.append(PERSON(line[172], line[173], line[174], line[175]))
            self.family.append(PERSON(line[176], line[177], line[178], line[179]))
            self.family.append(PERSON(line[180], line[181], line[182], line[183]))
            self.family.append(PERSON(line[184], line[185], line[186], line[187]))
            self.family.append(PERSON(line[188], line[189], line[190], line[191]))
        if self.family_size == '10 or more people':
            self.primary_app = PERSON(line[192], line[193], line[194], 'Primary')
            self.family.append(PERSON(line[195], line[196], line[197], line[198]))
            self.family.append(PERSON(line[199], line[200], line[201], line[202]))
            self.family.append(PERSON(line[203], line[204], line[205], line[206]))
            self.family.append(PERSON(line[207], line[208], line[209], line[210]))
            self.family.append(PERSON(line[211], line[212], line[213], line[214]))
            self.family.append(PERSON(line[215], line[216], line[217], line[218]))
            self.family.append(PERSON(line[219], line[220], line[221], line[222]))
            self.family.append(PERSON(line[223], line[224], line[225], line[226]))
            self.ten_plus = line[227]




class g_summary:
    def __init__(self, book_name):
        self.book_name = book_name
        self.workbook = xlsxwriter.Workbook(book_name)
        self.worksheet = None
        self.l_n = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36]
        self.off_set = 47
        self.counter = 1
        if self.workbook:
            self.worksheet = self.workbook.add_worksheet('summary')

    def add_response(self, g):
        self.worksheet.write(f'H{self.l_n[1]}', f'{self.counter}') 
        self.worksheet.write(f'E{self.l_n[2]}', 'Review Completed by:')
        
        self.worksheet.write(f'A{self.l_n[1]}', f'Timestamp:')
        self.worksheet.write(f'B{self.l_n[1]}', f'{g.stamp}')
        
        self.worksheet.write(f'A{self.l_n[2]}', f'First Name:')
        self.worksheet.write(f'B{self.l_n[2]}', f'{g.primary_app.f_name}')
        
        self.worksheet.write(f'A{self.l_n[3]}', f'Last Name:')
        self.worksheet.write(f'B{self.l_n[3]}', f'{g.primary_app.l_name}')

        self.worksheet.write(f'A{self.l_n[4]}', f'Bday:')
        self.worksheet.write(f'B{self.l_n[4]}', f'{g.primary_app.bday}')

        self.worksheet.write(f'A{self.l_n[5]}', f'Address:')
        self.worksheet.write(f'B{self.l_n[5]}', f'{g.address["street_address"]}')

        self.worksheet.write(f'A{self.l_n[6]}', f'Unit Number:')
        self.worksheet.write(f'B{self.l_n[6]}', f'{g.address["app_unit_num"]}')

        self.worksheet.write(f'A{self.l_n[7]}', f'Buzzer:')
        self.worksheet.write(f'B{self.l_n[7]}', f'{g.address["buzzer"]}')

        self.worksheet.write(f'A{self.l_n[8]}', f'City:')
        self.worksheet.write(f'B{self.l_n[8]}', f'{g.address["city"]}')
        
        self.worksheet.write(f'A{self.l_n[9]}', f'Housing Type:')
        self.worksheet.write(f'B{self.l_n[9]}', f'{g.address["housing_type"][:35]}')

        self.worksheet.write(f'A{self.l_n[10]}', f'email:')
        self.worksheet.write(f'B{self.l_n[10]}', f'{g.user}')

        self.worksheet.write(f'C{self.l_n[10]}', f'{g.emails[0]}')
        self.worksheet.write(f'E{self.l_n[10]}', f'{g.emails[1]}')

        self.worksheet.write(f'A{self.l_n[11]}', f'Primary Phone:')
        self.worksheet.write(f'B{self.l_n[11]}', f'{g.phone["Primary_Number"]}')

        self.worksheet.write(f'A{self.l_n[12]}', f'Can Text?: {g.phone["can_text"]}')
        
        self.worksheet.write(f'A{self.l_n[13]}', f'Secondary Phone:')
        self.worksheet.write(f'B{self.l_n[13]}', f'{g.phone["Secondary"]}')

        self.worksheet.write(f'A{self.l_n[14]}', 'Income Source:')
        self.worksheet.write(f'B{self.l_n[14]}', f'{g.soi}')

        self.worksheet.write(f'A{self.l_n[15]}', f'special diet:')
        self.worksheet.write(f'B{self.l_n[15]}', f'{g.diet}')
        
        self.worksheet.write(f'A{self.l_n[16]}', 'Do you have a car?:')
        self.worksheet.write(f'B{self.l_n[16]}', f'{g.car}')

        self.worksheet.write(f'A{self.l_n[17]}', f'Family Size:')
        self.worksheet.write(f'B{self.l_n[17]}', f'{g.family_size}')

        self.worksheet.write(f'A{self.l_n[18]}', f'Has Kids:')
        self.worksheet.write(f'B{self.l_n[18]}', f'{g.gifts["have kids"]}')

        self.worksheet.write(f'A{self.l_n[19]}', f'Wants gifts:')
        self.worksheet.write(f'B{self.l_n[19]}', f'{g.gifts["want gifts"]}')

        self.worksheet.write(f'A{self.l_n[20]}', f'Food Requests:')
        self.worksheet.write(f'B{self.l_n[20]}', f'{g.items}')

        self.worksheet.write(f'B{self.l_n[22]}', 'F. Name')
        self.worksheet.write(f'C{self.l_n[22]}', 'L. Name')
        self.worksheet.write(f'D{self.l_n[22]}', 'Birthday')
        self.worksheet.write(f'E{self.l_n[22]}', 'relationship')
        
        if g.address['city'] == 'Cambridge' and g.gifts['want gifts']:
            self.worksheet.write(f'A{self.l_n[34]}', 'gift request:')
            self.worksheet.write(f'B{self.l_n[34]}', f'[{g.gift_ask}]')

        f_c = self.l_n[23]
        for p in g.family:
            self.worksheet.write(f'B{f_c}', p.f_name)
            self.worksheet.write(f'C{f_c}', p.l_name)
            self.worksheet.write(f'D{f_c}', p.bday)
            self.worksheet.write(f'E{f_c}', p.relationship)
            f_c += 1
        
        # reset the offset for the next page
        for n in range(len(self.l_n)):
            self.l_n[n] += self.off_set
        self.counter +=1

    def close_worksheet(self):
        self.workbook.close()
        print('file closed')

def parse_csv(csv_f, xl_file, stamp):
    '''
    opens a csv, yeah
    '''
    entry_count = 1
    with open(csv_f) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        for ln in csv_reader:
            request = g_form_response(ln)
            if request.date >= stamp:
                xl_file.add_response(request)
                print(f'request {entry_count} at {request.date} added')
                entry_count += 1

if __name__ == '__main__':
    print('### printing google application summary ###')
    nm = input('please enter root file name  \n')
    dt1 = input('enter date in format YYYY/MM/DD  \n')
    tm1 = input('enter time stamp you want to start with in 24 hour fomat HH:SS:MS  \n')
    stamp = datetime.strptime(f'{dt1} {tm1}', FORMAT_STRING)
    file_nm = f'{nm}.xlsx'
    
    xl_file = g_summary(file_nm)

    parse_csv(SOURCE_FILE, xl_file, stamp)
    xl_file.close_worksheet()

