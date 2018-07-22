#!/usr/bin/python3.6

from collections import defaultdict
import os
import csv

def extract_code(log_string):

    # to clear out the carriage returns
    # https://stackoverflow.com/questions/3739909/how-to-strip-all-whitespace-from-string
    #compressed_string = ''.join(notes_string.split())
    if '##' in log_string:

        _, _, number_str_hashsigns = log_string.partition('##')
        number_str, _, _ = number_str_hashsigns.partition('##')
        return number_str
    else:
        return False

def parse_log_string(log_string, log_code):
    applicant = None
    log_output = None
    flag = None
    split_string = log_string.split()
    if log_code == '10': ##10## {} has errors in address {} and/or city {}'.format(applicant, s_add, s_city)
        applicant = split_string[1]
        log_output = 'double check address format and that the city is correct'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '11' or log_code == '14': ##11## {} flag for out of bounds'.format(applicant)
        applicant = split_string[1]
        log_output = 'city field is not Kitchener or Waterloo. Double check that it is valid.'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '12': ##12## {} returned invalid city {} on google result'.format(applicant, flags)
        applicant = split_string[1]
        log_output = 'the address may not be in Kitchener or Waterloo'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '13': ##13## {} returned valid city, but source: {} does not match google: {}
        applicant = split_string[1]
        log_output = 'The city needs to be updated. It is in KW but the city needs to be swapped from Kitchener to Waterloo or vice versa'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '15': ##15## {} source city {} returned invalid google city {}
        applicant = split_string[1]
        log_output = 'google says this file has an address outside of KW. Please verify'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '20': ##20## {} Returned a mismatch. Following Errors are True Name Type E = {} Dir Type E = {} Eval Flag = {}'.format(applicant, o, t, th)
        applicant = split_string[1]
        name_type = split_string[13]
        dir_type = split_string[18]
        eval_type = split_string[22]
        log_output = ''
        if name_type == 'True':
            log_output = '{} {}'.format(log_output,'Check street address. Do they live on 123 Main Street, but in L2F it says 123 Main Avenue etc?')
        if dir_type == 'True':
            log_output = '{} {}'.format(log_output, 'Check that the street direction is correct.')
        if eval_type == 'True':
            log_output = '{} {}'.format(log_output, 'Verify that the address is correctly formatted.')
        flag = 'bad'    
        return (applicant, log_output, flag)
    if log_code == '21': ##21## {} is missing a unit number on address {}'.format(applicant, flags))
        applicant = split_string[1]
        log_output = 'address is missing a number'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '25': ##25## {} returned a error flag.  Follow up with the address {}'.format(applicant, flags)
        applicant = split_string[1]
        log_output = 'Verify that the address is correctly formatted.'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '30': ##30## {} is missing unit {} Direction {} PostType {}'.format(applicant,u,d,p)
        applicant = split_string[1]
        mu = split_string[5]
        md = split_string[7]
        mpt = split_string[9]
        log_output = ''
        if mu == 'True':
            log_output = '{} {}'.format(log_output,'The address is a multiunit building and is probably missing an appartment number.')
        if md == 'True':
            log_output = '{} {}'.format(log_output,'The address is missing a direction like North, South, East, West.')
        if mpt == 'True':
            log_output = '{} {}'.format(log_output,'The address is missing Ave, Street, Rd etc.')
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '71': ##71## Could not derive Street Address from {}'.format(address)
        applicant = split_string[7]
        log_output = 'Double check that the address is formatted correctly.'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '72':
        applicant = split_string[3]
        log_output = 'Double check that the address is formatted correctly.'
        flag = 'bad'
    if log_code == '74': ##74## Blank Field Error from {}'.format(address)
        applicant = split_string[5]
        log_output = 'Is there an address? Or is the address cell blank?'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '777': ##777## {} was previously coded with errors'.format(applicant)
        applicant = split_string[1]
        log_output = 'was previously coded with errors'
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '80': # good result  ##80## Parsed {} with result {}'.format(address, flags)
        applicant = split_string[2]
        log_output = 'good_address'
        flag = 'good'
        return (applicant, log_output, flag)
    if log_code == '400': ##400##  Try Block in returnGeocoderResult raised Exception {} from {}'.format(boo, address)
        applicant = split_string[9]
        log_output = 'when trying to geocode {} recieved {}'.format(applicant, split_string[8])
        flag = 'address'
        return (applicant, log_output, flag)
    if log_code == '401': ##401## Result is None with status {} on {}'.format(result.status, address) # try again
        applicant = split_string[8]
        log_output = 'got None and status: {}'.format(split_string[6])
        flag = 'address'
        return (applicant, log_output, flag)
    if log_code == '402': ##402## {} yeilded {}'.format(address,result.status)) 'OVER_QUERY_LIMIT'
        applicant = 'bad_google_result'
        log_output = 'OVER_Q_LIMIT'
        flag = 'address'
        return (applicant, log_output, flag)
    if log_code == '403': ##403## Result is not OK or OVER with {} at {}'.format(result.status, address) # some major error with the api?
        applicant = 'bad_google_result'
        log_output = log_string
        flag = 'address'
        return (applicant, log_output, flag)
    if log_code == '405': ##405## Attempting to find geocodes in google_table but not present. Check address on line {}
        applicant = 'source_file'
        log_output = 'check line {}'.format(split_string[-1])
        flag = 'bad'
        return (applicant, log_output, flag)
    if log_code == '500': # good result ##500## {} is {}'.format(address, result.status)
        applicant = 'good_google_result'
        log_output = log_string
        flag = 'good'
        return (applicant, log_output, flag)
    else:
        return (False, False, False)

def return_log_output(log_string):
    log_code = extract_code(log_string)
    if log_code:
        applicant, log_output, flag = parse_log_string(log_string, log_code)
        return (applicant, log_output, flag)
    else:
        return (False, False, False)

class Log_Files():
    def __init__(self, path):
        self.path = path
        self.files =[]
        self.bad_results = defaultdict(set)
        self.good_results = defaultdict(set)
        self.address_problems = defaultdict(set)

    def discover_logs(self):
        
        for each_file in os.listdir(self.path):
            if each_file.endswith(".log"):
                log_path = os.path.join(self.path, each_file)
                self.files.append(log_path)

    def parse_logs(self):
        for file_path in self.files:
            with open(file_path) as log_file:
                for line in log_file:
                    print(line)
                    applicant, log_output, flag = return_log_output(line)
                    if flag == 'bad':
                        self.bad_results[applicant].add(log_output)
                    if flag == 'good':
                        self.good_results[applicant].add(log_output)
                    if flag == 'address':
                        self.address_problems[applicant].add(log_output)
                    if not flag:
                        pass
    
    def write_output_to_file(self, log_type='bad', output_file_name ='log.csv' ):
       '''
        write the output to a file for follow up
       this method needs to be rewritten to be more generic
       '''
       output_file = open(output_file_name, 'w')
       writer = csv.writer(output_file)
       if log_type == 'bad':
           for applicant in self.bad_results.keys():
               output_string = [applicant]
               log_strings = self.bad_results[applicant]
               for issue in log_strings:
                   output_string = output_string + [issue]
                   writer.writerow(output_string)

if __name__ == '__main__':
    muh_logs = Log_Files('Logging')
    muh_logs.discover_logs()
    print(muh_logs.files)
    muh_logs.parse_logs()
    muh_logs.write_output_to_file()
    print('done')
