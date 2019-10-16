#!/usr/bin/python3.6
'''
Provides a class that allows for choosing
a file to operate on
'''

from pathlib import Path

class Menu:
    pr_str = {'None': 'ERROR:  menu string not specified',
              'files': 'select file:  ',
              's_routes': 'Input starting route to print  ',
              'e_routes': 'Input ending route to print  ',
              'sms_hampers': 'Input Starting Route Number: ',
              'sms_army': 'Gift Appointment Block  ',
              'sponsor': 'Enter a date to print entries '
             }


    def __init__(self, base_path='December_Ops/databases/'):
        self.base_path = base_path
        self.path_dict = None

    def get_file_list(self):
        fls = Path(self.base_path)
        fls_d = {str(x[0]) : x[1] for x in enumerate(fls.iterdir())}
        self.path_dict = fls_d

        for k in fls_d.keys():
            print(f'{k} : {fls_d[k]}')

    def prompt_input(self, prompt_key='None'):
        return input(Menu.pr_str.get(prompt_key, 'ERROR: invalid key string'))

    def handle_input(self, option):
        try:
            return self.path_dict[option]
        except KeyError:
            print('Invalid Choice')
    

def main():
    menu = Menu()
    menu.get_file_list()
    menu.handle_input(menu.prompt_input('files'))

if __name__ == '__main__':
    main()




