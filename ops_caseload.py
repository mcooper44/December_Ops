import sys
from file_iface import Menu

from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser
from db_data_models import PROVIDERS

from db_caseload import caseload_manager
from db_caseload import Database
from db_caseload import STRUCTURE


# BASE PATH NAMES
CL_DB = 'databases/caseload.db'
BP_S = 'sources/'
BP_D = 'databases/'
BP_O = 'products/'

def create_database():
    '''
    to create a database for storing caseload data from exports files
    '''
    new_name = input('please enter name of caseload database you want to create ')
    
    if '.db' not in new_name:
        new_name = f'{new_name}.db'
        print('added .db extension')

    try:
        caseload = Database(f'{BP_D}{new_name}') 
        caseload.connect(first_time=True, strings=STRUCTURE)
        print(f'database {new_name} has been created')
        provider_tpl = [(PROVIDERS[k], k) for k in PROVIDERS]
        provider_payld = {'Service_Provider_Table': (provider_tpl,'(?,?)')}
        caseload.insert(provider_payld)
        print(f'{len(provider_tpl)} inserted into Service_Provider_Table')
    except Exception as failed_to_create:
        print('ERROR!')
        print(f'Could not create {new_name} due to error\n{failed_to_create}\n')
        return None

def open_database(chosen_source):
    '''
    for opening caseload databases for the purpose of running reports
    '''
    try:
        caseload_q = Database(chosen_source)
        caseload_q.connect()
        return caseload_q
    except Exception as failed_con:
        print('ERROR!')
        print(f'No connection to {chosen_source} due to error:\n{failed_con}')

def choose_source(base_dir):
    
    menu = Menu(base_path=base_dir)
    menu.get_file_list()
    s_target = menu.handle_input(menu.prompt_input('files'))

    confirm = input(f'''1. Use choice {s_target}\n2. Exit\n ''')

    if str(confirm) == '1':
        print(f'using: {confirm}\n')
        return str(s_target)
    else:
        print(f'exiting. Input was: {confirm}\n')
        sys.exit(0)

def ingest_data(d_file, in_file):

    dbm = caseload_manager({'caseload': d_file})
    dbm.initialize_connections()

    fnames = Field_Names(in_file)
    export_file = Export_File_Parser(in_file, fnames)
    export_file.open_file()

    # LOGIC
    '''
    to insert into the database it needs 
    a tple with structure
    (
    (visit table),
    (visit address table),
    (visit services table),
    ((household visit table), (member 2), (member 3), (...)),
    (household demo table),
    (visit coordinates table),
    ((person table),(person 2), (...))
    )
    '''

    for line in export_file:
        l_o = Visit_Line_Object(line, fnames.ID)
        vt = l_o.visit_table()
        vat = l_o.visit_address_table()
        vs = l_o.visit_services()

        ma = l_o.ma_person_table() # main applicant pt tuple
        fam = l_o.get_family_members(fnames) 
        # fam = tuples of family members
        # ((ID, ln, fn, dob, age, gend, eth, ident, relation, imm date),...)
        # parse them with the static method
        
        hhvt, fpt = Visit_Line_Object.return_hvt_and_people(fam)
        
        # and get tuples for the hh visit table (hhvt)
        # and family members to insert to the Person_Table
        # if there are no family members hhvt, fpt will be []
        hdt = l_o.household_demo_table()
        vct = l_o.visit_coordinates_table()
        per_tab = (ma, *fpt)

        i_tuples = (vt, vat, vs, hhvt, hdt, vct, per_tab) 

        dbm.insert_visit('caseload', i_tuples)   

    dbm.close_all()

def start_menu():
    return input('1. Create Database\n2. Ingest Data\n3. Run Report\n4. Exit\n')


def main_menu():

    yes_loop = True
    while yes_loop:
        m1 = start_menu()
        if str(m1) == '1': # CREATE DATABASE
            create_database()
        elif str(m1) == '2': # INGEST DATA
            d_base = choose_source(BP_D)
            in_file = choose_source(BP_S)
            print('ingesting data... nom nom nom\n')
            ingest_data(d_base, in_file)
        elif str(m1) == '3': # RUN REPORTS
            print('CHOOSE DATABASE SOURCE FILE')
            ops_db_source = choose_source(BP_D)
            qdb = open_database(ops_db_source)
            if not qdb:
                print('no database connection...exiting.')
                sys.exit(0)
            print('we made it!')
            pass
        else: # EXIT OR INVALID INPUT
            print('exiting...')
            yes_loop = False
            sys.exit(0)


def main():
    print('#MAIN MENU#')
    main_menu()

if __name__ == '__main__':
    print('###########')
    main()
