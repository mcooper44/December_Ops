from collections import defaultdict
import os

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
    if log_code == '10': ##10## {} has errors in address {} and/or city {}'.format(applicant, s_add, s_city)
        return (applicant, log_output, flag)
    if log_code == '11': ##11## {} flag for out of bounds'.format(applicant)
        return (applicant, log_output, flag)
    if log_code == '12': ##12## {} returned invalid city {} on google result'.format(applicant, flags)
        return (applicant, log_output, flag)
    if log_code == '13': ##13## {} returned valid city, but source: {} does not match google: {}
        return (applicant, log_output, flag)
    if log_code == '14': ##14## {} has an invalid source City: {}
        return (applicant, log_output, flag)
    if log_code == '15': ##15## {} source city {} returned invalid google city {}
        return (applicant, log_output, flag)
    if log_code == '20': ##20## {} Returned a mismatch. Following Errors are True Name Type E = {} Dir Type E = {} Eval Flag = {}'.format(applicant, o, t, th)
        return (applicant, log_output, flag)
    if log_code == '21': ##21## {} is missing a unit number on address {}'.format(applicant, flags))
        return (applicant, log_output, flag)
    if log_code == '25': ##25## {} returned a error flag.  Follow up with the address {}'.format(applicant, flags)
        return (applicant, log_output, flag)
    if log_code == '30': ##30## {} is missing unit {} Direction {} PostType {}'.format(applicant,u,d,p)
        return (applicant, log_output, flag)
    if log_code == '71': ##71## Could not derive Street Address from {}'.format(address)
        return (applicant, log_output, flag)
    if log_code == '72': ##72## RepeatedLabelError from {}'.format(address)
        return (applicant, log_output, flag)
    if log_code == '74': ##74## Blank Field Error from {}'.format(address)
        return (applicant, log_output, flag)
    if log_code == '80': # good result  ##80## Parsed {} with result {}'.format(address, flags)
        return (applicant, log_output, flag)
    if log_code == '400': ##400##  Try Block in returnGeocoderResult raised Exception {} from {}'.format(boo, address)
        return (applicant, log_output, flag)
    if log_code == '401': ##401## Result is None with status {} on {}'.format(result.status, address) # try again
        return (applicant, log_output, flag)
    if log_code == '402': ##402## {} yeilded {}'.format(address,result.status)) 'OVER_QUERY_LIMIT'
        return (applicant, log_output, flag)
    if log_code == '403': ##403## Result is not OK or OVER with {} at {}'.format(result.status, address) # some major error with the api?
        return (applicant, log_output, flag)
    if log_code == '405': ##405## Attempting to find geocodes in google_table but not present. Check address on line {}
        return (applicant, log_output, flag)
    if log_code == '500': # good result ##500## {} is {}'.format(address, result.status)
        return (applicant, log_output, flag)

def return_log_output(log_string):
    log_code = extract_code(log_string)
    if log_code:
        applicant, log_output, flag = parse_log_string(log_string, log_code)
        return (applicant, log_output, flag)

class Log_Files():
    def __init__(self, path):
        self.path = path
        self.files =[]
        self.bad_results = defaultdict(set)
        self.good_results = defaultdict(set)

    def discover_logs(self):
        
        for each_file in os.listdir(self.path):
            if each_file.endswith(".log"):
                log_path = os.path.join(self.path, each_file)
                self.files.append(log_path)

    def parse_logs(self):
        for file_path in self.files:
            with open(file_path) as log_file:
                for line in log_file:
                    applicant, log_output, flag = return_log_output(line)
                    if flag == 'bad':
                        self.bad_results[applicant].add(log_output)
                    if flag == 'good':
                        self.good_results[applicant.add(log_output)]
                    if not flag:
                        pass



if __name__ == '__main__':
    muh_logs = Log_Files('Logging')
    muh_logs.discover_logs()
    print(muh_logs.files)