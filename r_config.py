'''
A way to access the different configuration options via
way of a class that returns itself with the standard yaml
config file via the configuration.return_r_config() class method

'''

import yaml

class configuration:
    '''
    Load the data in the configuration file into an object
    and make the data points in the file accessble via
    class attributes and methods

    classmethod return_r_config() returns an instance
    of the class with the 'setup.yml' file that should
    be available by default in the folder

    '''

    def __init__(self, config_file, echo=True):
        self.config = config_file 
        self.whoami = None
        self.session = None
        self.db_src = None
        self.inputs = None
        self.outputs = None
        self.g_api_key = None
        self.target = None
        self.rdb = None
        self.sa_db = None
        self.add_db = None
        self.sa_day = None
        self.sa_times = None
        self.sa_multipliers = None
        self.sa_day_mult = None
        self.zn_times = None
        self.zn_multipliers = None
        self.zn_day_mult = None


        if self.config:
            try:
                if echo: print(f'trying to open {self.config}')
                f = self.config
                with open(f, 'r') as ymlfile:
                    yfile = yaml.load(ymlfile)
                    self.whoami = yfile['whoami']
                    self.session = yfile['session']
                    self.db_src = yfile['db_src']
                    self.inputs = yfile['inputs']
                    self.outputs = yfile['outputs']
                    self.g_api_key = yfile['api_key']
                    self.target = yfile['target']
                    self.rdb = yfile['rdb_nm']
                    self.sa_db = yfile['sadb_nm']
                    self.add_db = yfile['add_db_nm']
                    # KW Salvation Army
                    self.sa_day = yfile['day']
                    self.sa_times = yfile['times']
                    self.sa_multipliers = yfile['multipliers']
                    self.sa_day_mult = yfile['day_mult']
                    # zones
                    self.zn_day = yfile['z_day']
                    self.zn_times = yfile['z_times']
                    self.zn_multipliers = yfile['z_multipliers']
                    self.zn_day_mult = yfile['z_day_mult']
                    # Cambridge SA
                    self.csa_day = yfile['csa_day']
                    self.csa_times = yfile['csa_times']
                    self.csa_multipliers = yfile['csa_multipliers']
                    self.csa_day_mult = yfile['csa_day_mult']
                    if echo: print(f'loaded {self.whoami} {self.session}')
            except:
                print('failed to open config file')

    @classmethod
    def return_r_config(cls):
        '''
        returns an instance of the class with config file
        'setup.yml'
        '''
        return cls('setup.yml')
    
    def get_folders(self):
        '''
        returns folder names for 
            -databases
            -inputs
            -outputs
        '''
        return (self.db_src, self.inputs, self.outputs)

    def get_g_creds(self):
        '''
        return the google api key string
        '''
        return self.g_api_key

    def get_meta(self):
        '''
        returns label for (config file, session)
        '''
        return self.whoami, self.session

    def get_target(self):
        '''
        return file name of input file
        '''
        return f'{self.inputs}{self.target}'
    
    def get_bases(self):
        '''
        returns a dictionary of databases and path to them
        for use in the various database interfaces
        '''
        return {'rdb': f'{self.db_src}{self.rdb}',
                'sa': f'{self.db_src}{self.sa_db}',
                'address': f'{self.db_src}{self.add_db}'
               }
    
    def set_target(self, new_target):
        '''
        sets a new file target for processing
        '''
        self.target = new_target

    def get_sa_app_package(self):
        '''
        returns a 4 tuple of the four lists
        day, times, multipliers, day_mult
        that are needed to print out the SA app sheets
        and provision the SA appointment database
        '''
        return (self.sa_day, self.sa_times, 
                self.sa_multipliers, self.sa_day_mult)

    def get_zone_app_package(self):
        '''
        returns a 4 tuple of the four lists that are needed to print out
        the pickup zone app sheets and provision the pickup database
        '''
        return (self.zn_day, self.zn_times, self.zn_multipliers,
                 self.zn_day_mult)
    
    def get_csa_app_package(self):
        '''
        returns a 4 tuple of the four lists that are needed to print out
        the pickup information for the cambridge salvation army and provision
        the CSA table 
        '''
        return (self.csa_day, self.csa_times, self.csa_multipliers,
                self.csa_day_mult)

