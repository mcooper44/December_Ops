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

    def __init__(self, config_file):
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

        if self.config:
            try:
                print(f'trying to open {self.config}')
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
                    print(f'loaded {self.whoami} {self.session}')
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
                'sa': f'{self.db_src}{self.sa_db}'}
    
    
    def set_target(self, new_target):
        '''
        sets a new file target for processing
        '''
        self.target = new_target


