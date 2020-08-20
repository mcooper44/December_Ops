import sqlite3
from collections import Counter
from collections import defaultdict
from datetime import datetime

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

class Case_Visit:
    '''
    models the visit
    labelled by Visit_Number_Key
    '''
    def __init__(self, VNK, hh_id, visit_date, service_provider_code,
                 primary_applicant):
        self.VNK = VNK
        self.hh_id = hh_id
        self.v_d = datetime.strptime(visit_date, '%Y-%m-%d')
        self.spc = service_provider_code
        self.p_a = primary_applicant
        self.family = None
        self.location = None

    def add_family(self, family):
        # includes primary applicant
        self.family = family

    def add_location(self, location):
        self.location = location

class Case:
    '''
    models visits that a household has over time
    and provides methods to extract household level
    stats.  
    it is labeled by hh_id
    '''
    def __init__(self):
        self.visits = {}

    def add_visit(self, vnk, v):
        self.visits[vnk] = Case_Visit(*v)
    
    def insert_family(self, vnk, family):
        self.visits[vnk].add_family(family)

    def insert_location(self, vnk, location):
        self.visits[vnk].add_location(location)

    def get_monthly_visits(self, month=None):
        if not month:
            return Counter([v.v_d.month for v in self.visits.values()])

    def perform_Case_audit(self):
        pass

class Case_Collection:
    '''
    holds a range of Case()'s which model a household it's composition and
    their visits across a specific time frame
    provides methods to aggregate Case level stats
    '''
    def __init__(self):
        self.hh_struct = {}

    def add(self, v, vnk, hhid):
        self.hh_struct[hhid] = Case()
        self.hh_struct[hhid].add_visit(vnk, v)
    
    def update_family(self, hhid, vnk, family):
        self.hh_struct[hhid].insert_family(vnk, family)

    def update_location(self, hhid, vnk, location):
        self.hh_struct[hhid].insert_location(vnk, location)
    
    def perform_Case_Collection_audit(self):
        pass

class Caseload_View:
    '''
    a range of visits between a start_date and end_date
    manages the database and the model of households created by making calls to
    it
    '''
    def __init__(self, start_date, end_date, caseload_db):
        self.path = caseload_db
        self.start = start_date # 'YYYY-MM-DD'
        self.end = end_date
        self.db_handler = None
        self.hh_range = None
        self.case_collection = Case_Collection()
        self.report_struct = None

        if caseload_db:
            self.db_handler = Database(self.path)
            self.db_handler.connect()
            sql_rangeofhh = f'''SELECT * FROM Visit_Table WHERE
            date(visit_date) 
            BETWEEN date("{self.start}") AND date("{self.end}")'''
            try:
                self.hh_range = self.db_handler.lookup_string(sql_rangeofhh, tple=None)
                for v in self.hh_range: 
                    vnk, hhid, vd, pc, pa = v 
                    # set case
                    self.case_collection.add(v, vnk, hhid)
                    sql_hh_vt = f'''SELECT Household_Visit_Table.Visit_Number_Key, 
                                           Household_Visit_Table.person_fid, 
                                           Household_Visit_Table.relationship,
                                           Person_Table.bday,
                                           Person_Table.gender
                                    FROM 
                                        Household_Visit_Table 
                                    INNER JOIN 
                                        Person_Table 
                                    ON Household_Visit_Table.Person_fid =
                                        Person_Table.Person_fid
                                    WHERE
                                    Household_Visit_Table.Visit_Number_key =
                                    {vnk} 
                                    UNION
                                    SELECT "{vnk}" as Visit_Number_Key,
                                        person_fid,
                                        "Main_Applicant" as relationship,
                                        bday,
                                        gender
                                    FROM
                                        Person_Table
                                    WHERE
                                        person_fid={pa}'''
                    family = self.db_handler.lookup_string(sql_hh_vt,
                                                           tple=None)
                    # set family
                    self.case_collection.update_family(hhid, vnk, family)
                    sql_location = f'''SELECT lat, long FROM
                    Visit_Coordinates_Table WHERE Visit_Number_Key = {vnk}'''
                    location = self.db_handler.lookup_string(sql_location,
                                                             tple=None)
                    # set location
                    self.case_collection.update_location(hhid, vnk, location)

            except Exception as db_call_fail:
                raise db_call_fail
        
    def close_db(self):
        self.db_handler.close()

    def create_report_struct(self):
        start_y = datetime.strptime(self.start, '%Y-%m-%d')
        end_y =   datetime.strptime(self.end, '%Y-%m-%d')
        uniq_years = list(range(start_y, end_y + 1))


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
        
        if not rows: print(f'WARNING: NO DATABASE RESULT\nfrom {string}')

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

