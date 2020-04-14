import sqlite3

# SCHEMA USED TO CREATE THE DATABASE
STRUCTURE = ('CREATE TABLE Visit_Table (Visit_Number_Key INTEGER ' \
             + 'NOT NULL PRIMARY KEY AUTOINCREMENT, hh_id INT, visit_date TEXT,' \
             + 'service_provider_code INT, primary_applicant INT)',
             'CREATE TABLE Visit_Address_Table ' \
             + '(Visit_Number_Key INT, address_l1 TEXT, address_l2 TEXT,' \
             +'city TEXT, postal TEXT, housing_type TEXT, FOREIGN KEY '
             + '(Visit_Number_Key) REFERENCES Visit_Table(Visit_Number_Key))',
             'CREATE TABLE Visit_Services (Visit_Number_Key '\
             +'INT, quantity INT, food_provided TEXT, items_provided TEXT,'\
             + 'delivery TEXT, referrals TEXT, FOREIGN KEY(Visit_Number_Key)'\
             + ' REFERENCES Visit_Table (Visit_Number_Key))',
             'CREATE TABLE Household_Visit_Table ' \
             + '(Visit_Number_Key INT, person_fid INT, relationship TEXT,'\
             + 'FOREIGN KEY (Visit_Number_Key) REFERENCES Visit_Table'\
             + '(Visit_Number_Key))',
             'CREATE TABLE Household_Demo_Table'\
             + '(Visit_Number_Key INT, hh_size INT, hh_lang TEXT, hh_diet TEXT, ' \
             + 'hh_housing_type TEXT,hh_primary_SOI TEXT,'\
             + ' FOREIGN KEY (Visit_Number_Key) REFERENCES Visit_Table'\
             + '(Visit_Number_Key))',
             'CREATE TABLE Visit_Coordinates_Table ' \
             +'(Visit_Number_Key INT, lat FLOAT, long FLOAT, verified INT,'\
             + 'FOREIGN KEY (Visit_Number_Key) REFERENCES Visit_Table'\
             +' (Visit_Number_Key))',
             'CREATE TABLE Person_Table (Person_fid INTEGER NOT NULL UNIQUE, ' \
             +'fname TEXT, lname TEXT, bday TEXT, age INT, gender TEXT, '\
             +' ethnicity TEXT, identitiy TEXT, immigation TEXT, fvisit TEXT)',
             'CREATE TABLE Service_Provider_Table ' \
             + '(Service_Provider_Code INT, service_name TEXT)')

# PATH OF THE DATABASE TO BE USED
CL_DB = 'databases/caseload.db'

class Database():
    '''
    for interacting with databases
    path_name is the location and name of the databse to connect with
    or create

    connect() method establishes a database and/or connection to one
    

    '''
    def __init__(self, path_name):
        self.path_name = path_name
        self.conn = None
        self.cur = None


    def connect(self, first_time=False, strings=None):
        '''
        establishes connection to database stored in the .path_name attribute
        if the first_time flag is set
        it will attempt to create tables with strings contained in a list or
        tuple held in the 'strings' parameter
        '''
        
        try:
            self.conn = sqlite3.connect(self.path_name)
            self.cur = self.conn.cursor()
            print(f'Database.connect: database connection open to {self.path_name}')

            if first_time == True and any(strings):
                for string in strings:
                    self.cur.execute(string)
                    print(f'executing: {string}')
                    self.conn.commit()
            elif first_time == True and not any(strings):
                print('no strings provided to provision tables')

        except Exception as e:
            print('could not establish connection to database')
            print(e)

    def insert(self, data_struct, echo=False):
        '''
        takes a dictionary 'data_struct' of 
        'table name': ([(values_to_write,)], # list of tuple(s)
                       '(?, [...])') # string tuple with a ? for each 
                                     # to insert

        and iterates through the data structure inserting values
        into the tables used as keys in dictionary
        '''
        for table_name in data_struct.keys():
            # lookup the values to insert i.e [(1,2,3),(4,5,6)]
            # and a tuple representing the number of columns
            # i.e. '(?, ?, ?)'
            lst_of_tples, flds = data_struct.get(table_name, (None,None))
            wr_str = f'INSERT OR IGNORE INTO {table_name} VALUES {flds}'
            if all((lst_of_tples,flds)):
                for tple_to_insert in lst_of_tples:
                    if echo: print(f'writing {tple_to_insert} into {table_name}')
                    self.cur.execute(wr_str, tple_to_insert)
                    self.conn.commit()
    
    def o2m_insert(self, visit, many):
        '''
        handles the one to many table insert for the database

        visit = string to insert to Visit_Table for example
        that will have a primary autoincrement key that will be
        needed to insert into the typles in the many variable
        as foreign keys
        many = Visit_Address_Table, Visit_Services, 
        Household_Demo_Table, Visit_Coordinate_Table
        
        '''
        self.cur.execute(*visit)

        pk = self.cur.lastrowid
        
        for s in many:
            if any((s)):

                try:
                    s1, s2 = s
                    self.cur.execute(s1,(pk,*s2))
                    self.conn.commit()
                except Exception as insert_fail:
                    print(f'{s1} with values {s2}')
                    print('yielded...')
                    print(insert_fail)
                

    def lookup(self, target, table, row, paramater):
        '''
        SELECT {file_id} FROM {routes} WHERE {route_number}{ BETWEEN 50 AND 130}
        SELECT {*} FROM {applicants} WHERE {file_id}{=123456}
        
        http://www.sqlitetutorial.net/sqlite-between/    
        '''
        ex_string = f'SELECT {target} FROM {table} WHERE {row}{parameter}'
        self.cur.execute(ex_string)
        rows = self.cur.fetchall()
        return rows

    def update(self, string, tple):
        '''
        executes sql string with values tple
        to update a table
        '''
        if all((string,tple)):
            try:
                self.cur.execute(string, tple)
                self.conn.commit()
                return True
            except Exception as error:
                return error
        else:
            print('input for SQL update operation is none')
            return False

    def lookup_string(self, string, tple):
        '''
        executes sql string with values tple
        if tple != None asuming it a fetchone scenario
        if tple = None, assuming it is a fetchall scenario
        '''
        rows = None
        if tple:
            try:
                self.cur.execute(string, tple) 
                rows = self.cur.fetchone()
            except:
                print(f'could not lookup {tple} with {string}')
        else:
            self.cur.execute(string)
            rows = self.cur.fetchall()
        
        if not rows: print('WARNING: NO DATABASE RESULT')

        return rows

    def close(self):
        '''
        closes the db connection

        '''
        name = self.path_name
        try:
            self.conn.close()
            print(f'connection to {name} closed')
        except:
            print('could not close connection')

class caseload_manager:
    '''
    Interface for the Database class
    contains methods for making specific calls to the database
    
    :param: data_bases is a dictionary of labels and corresponding
    paths i.e.
    dbm = database_manager({'2019': '2019_caseload.db', 
                            '2018': '2018_caseload.db'})

    pass the dictionary to the class and the call the .initialize_connections()
    method to iterate through the dictionaries and connect to each of the
    databases
    dbm.initialize_connections() # instantiate db objects

    '''

    def __init__(self, data_bases):
        self.db_path_dict = data_bases # dict of {db_name: path_string}
        self.db_struct = {}

    def initialize_connections(self):
        for db_name in self.db_path_dict.keys():
            path_string = self.db_path_dict.get(db_name, None)
            self.db_struct[db_name] = Database(path_string)
            self.db_struct[db_name].connect()

    def close_all(self):
        '''
        iterates through the databases being managed and closes them
        '''
        for db_name in self.db_struct.keys():
            self.db_struct[db_name].close()

    def insert_to(self, db_name, d_struct):
        '''
        insert data structre d_struct to database db_name
        param d_struct is a dictionary as per doc string described
        on insert method of the database class

        'table name': ([(values_to_write,)], # list of tuple(s)
                       '(?, [...])') # string tuple with a ? for each 
                                     # value to insert

        '''
        self.db_struct[db_name].insert(d_struct)

    def insert_visit(self, database, tples):
        '''
        insert to the 7 tables of the database a visit
        param: database is the key label of the database to use
        param: tples is a data structure of tuples that will be
        used sequentially to insert into the database

        vt = Visit_Table values
        vat = Visit_Address_Table
        vs = Visit_Services
        hvt = Household_Visit_Table
        hdt = Household_Demo_Table
        vct = Visit_Coordinates_Table
        pt = Person_Table - a list of tuples corresponding to each person
             in the hh
        
        '''
        vt, vat, vs, hvt, hdt, vct, pt = tples

        VISIT = (f"INSERT INTO Visit_Table VALUES(NULL, ?, ?, ?,?)",vt)
        
                   
        ADDRESS = (f"""INSERT INTO Visit_Address_Table VALUES(?,?,?,?,?,?)""",vat)

        
        VISIT_SERVICES = (f"""INSERT INTO Visit_Services VALUES (?,?,?,?,?,?)""",
                         vs)
        HH_VT = []
        for hh_ in hvt:
            p_fid, ship = hh_
            HH_VT.append(("""INSERT INTO Household_Visit_Table VALUES (?, ?, ?)""",hh_))

        HH_DEMO = ("""INSERT INTO Household_Demo_Table VALUES (?,?,?,?,?,?)""",hdt)
        
        VISIT_COORD = ("INSERT INTO Visit_Coordinates_Table VALUES (?, ?, ?, ?)",vct)

        SONS = []
        person_t_str = "(?,?,?,?,?,?,?,?,?,?)"
        for son in pt:
            SONS.append(son)

        MANY = (ADDRESS, VISIT_SERVICES,*HH_VT, HH_DEMO, VISIT_COORD)
        self.db_struct[database].o2m_insert(VISIT, MANY)
        self.db_struct[database].insert({'Person_Table':
                                         (SONS,person_t_str)},echo=False)

if __name__ == '__main__':
    # SETUP A NEW DATABASE
    caseload = Database(CL_DB)
    caseload.connect(first_time=True, strings=STRUCTURE)
    '''
    # TEST EXISTING DATABASE WITH the database_manager
    test_visit1 = ((123, '1/2/2020', 1, 999),
                   ('main', 'None', 'city', 'n2n1n1','rental'),
                   (3, 'None','diapers','no','None'),
                   ((999,'spouse'),(888,'spouse')),
                   (2,'arabic','halal','rental','EI'),
                   (1.1,2.2,0),
                   ((999,'mr','smith','1/1/1900',
                     '120','male','inuit','None','NA','1/1/1980'),
                    (888,'ms', 'smith','1/1/1970',
                     50,'female','None','None','none','0'))

                  )

    test_visit2 = ((456, 'yesterday', 2, 777),
                   ('avenue', 'None', 'city', 'n2n1n1','rental'),
                   (3, 'None','None','yes','None'),
                   ((777,'None'),),
                   (1,'french','diabetic','social_housing','OAP'),
                   (1.1,2.2,0),
                   ((777,'mr','doe','1/1/1900', 
                     '120','male','none','none','none','1/1/1911'),)
                  )

    dbm = caseload_manager({'test': CL_DB})
    dbm.initialize_connections()
    dbm.insert_visit('test', test_visit1)
    dbm.insert_visit('test', test_visit2)

    dbm.close_all()
    '''

