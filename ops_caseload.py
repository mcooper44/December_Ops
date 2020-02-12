from file_iface import Menu


from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser

from db_caseload import caseload_manager

# PATH OF THE DATABASE TO BE USED
CL_DB = 'databases/caseload.db'


if __name__ == '__main__':
    # LOAD CONFIGURATION FILE
    #config = configuration.return_r_config()

    #t_file = config.get_target() # string of target file location
    #add_base = config.get_bases()['address'] 
    
    # MENU INPUT
    menu = Menu(base_path='sources/' )
    menu.get_file_list()
    s_target = menu.handle_input(menu.prompt_input('files'))

    confirm = input(f'''1. Use choice {s_target}\n2. Exit\n ''')

    if str(confirm) == '1':
        print(f'using: {confirm}') 
    else:
        print(f'exiting. Input was: {confirm}')
        sys.exit(0)

    # INIT DATABASE
    dbm = caseload_manager({'test': CL_DB})
    dbm.initialize_connections()

    # INIT TOOLS TO OPEN AND PARSE EXPORT FILES
    fnames = Field_Names(s_target) # I am header names
    export_file = Export_File_Parser(s_target, fnames) # I open a csv 
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

        dbm.insert_visit('test', i_tuples)   

    dbm.close_all()
