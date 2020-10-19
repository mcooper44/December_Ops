'''
The reportlab code in this file is adapted from
https://pythonprogramming.altervista.org/make-a-formal-letter-in-pdf-with-python/
Thanks for cutting through the 132 pages of documentation for this library and
writing something that does exactly what I needed to do!

this file is for writing confirmation letters

Letter_Text is an object that recieves a dictionary and parses the contents to
return the correct letter text with the .get() method

The Confirmation_Letter object recieves letter_Text objects through the
parse_text() method and adds the letter to a reportlab page
when there are no mor eletter letters to add use the .write() method to write
the pdf to file.  It will be named after the date that was pulled from the
database and can be printed as one file, rather than having to print hundreds
of individual letter files.

'''
import time
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.platypus import PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import os

BARCODE_SOURCE = 'bar_codes/'

A_SELECT = {'KW Salvation Army': 'KW Salvation Army',
             'Salvation Army - Cambridge' : 'Cambridge Salvation Army',
             'SPONSOR - SERTOMA': 'KW Sertoma Club',
             'SPONSOR - REITZEL': 'Possibilities International',
             'SPONSOR - DOON' : 'Doon Pioneer Park Christmas Miracle',
             'House of Friendship Delivery': 'House Of Friendship',
             'HoF Zone 1' : 'House of Friendship at the Legion',
             'HoF Zone 2' : 'House of Friendship at St. Marks',
             'HoF Zone 3' : 'House of Friendship at Blessed Sacrament',
             'HoF Zone 4' : 'House of Friendship at St. Anthony Daniel',
             'HoF Zone 5' : 'House of Friendship at St. Francis',
             'HoF Zone 6' : 'House of Friendship at Our Lady of Lourdes',
             'HoF Zone 7' : 'House of Friendship at First Mennonite Church',
             'HoF Zone 8' : 'House of Friendship at Waterloo Pentecostal Assembly',
             'HoF Zone 9' : 'House of Friendship at the Kingsdale Community Centre',
             'Cambridge Self-Help Food Bank': 'Cambridge Self-Help Food Bank',
             'Cambridge Firefighters' : 'Cambridge Firefighters'
           }

DELIVERY_CONTACT = {'House Of Friendship' : ' If you provided us with an email and/or cell phone number we will attempt to email or text you 24-48 hours before to update you.'}

GIFT_LOOKUP = {'KW Salvation Army' : '''KW Salvation Army, located at 75 Tillsley\
 Drive, Kitchener.  Please bring a piece of identification for yourself and your\
children''',
               'Salvation Army - Cambridge': '12 Shade Street, Cambridge.',
               'Cambridge Firefighters': 'at the Preston Auditorium'}

FOOD_LOOKUP = {'HoF Zone 1': '524 Belmont Avenue West, Kitchener',
    'HoF Zone 2': '55 Driftwood Drive, Kitchener.',
    'HoF Zone 3': '305 Laurentian Drive, Kitchener',
    'HoF Zone 4': '29 Midland Drive, Kitchener.',
    'HoF Zone 5': '49 Blueridge Avenue, Kitchener.',
    'HoF Zone 6': '173 Lourdes Street, Waterloo.',
    'HoF Zone 7': '800 King Street East, Kitchener.',
    'HoF Zone 8': '395 King Street North, Waterloo',
    'HoF Zone 9': '72 Wilson Avenue, Kitchener',
    'Cambridge Firefighters': 'the Preston Auditorium.  1458 Hamilton Street, Cambridge',
    'Cambridge Self-Help Food Bank': '50 Ainsley Street, Cambridge'}

# food must be picked up
F_PICKUPS = ['House of Friendship','Cambridge Firefighters','Cambridge Self-Help Food Bank']
# food items are delivered
F_DELIVER = ['House Of Friendship',
             'Possibilities International', 
             'Doon Pioneer Park Christmas Miracle']
# you need to pickup your gifts, but they will tell you when and where
G_NO_APP = ['KW Sertoma Club']
# toys are pickup and you need a day and time to do so
G_YES_APP = ['KW Salvation Army','Cambridge Salvation Army',
             'Cambridge Fire Fighters']
# Food and toys provided at the same time (in the form of a gift card)
ALL_IN_ONE = ['Possibilities International', 'Cambridge Fire Fighters']

def filter_set(a_set):
    if a_set:
        return list(a_set)[0]
    else:
        return None


class Letter_Text:
    '''
    this melted my brain

    this object takes a service pack dict, pulls out the data points extracted
    from the database and based off the facts about the household determines
    who the service providers are, the services and stacks a letter that will
    outline what they have signed up for and insert a barcode image that
    represents the file ID of the applicant
    '''
    
    def __init__(self, service_pack):
        self.file_id = service_pack['file_id']
        self.name = service_pack['name']
        self.service = service_pack['service'] # 1 or 2 services
        self.service_prov = service_pack['service_prov'] # 1 or 2 providers
        self.food_service = service_pack['food_service'] # a string (turkey and
                                                         # gift card etc.
        self.when = service_pack['when']
        self.email = service_pack['email']
        self.phone = service_pack['phone']
        self.food_pu_loc = service_pack['food_pu_loc']
        self.food_pu_date = service_pack['food_pu_date']
        self.zone_pu_num = service_pack['hof_pu_num']
        self.food_del_date = service_pack['food_del_date']
        self.del_f_prov = A_SELECT.get(service_pack['del_f_prov'], '')
        self.gift_pu_loc = service_pack['gift_pu_loc']
        self.gift_pu_date = service_pack['gift_pu_date']
        self.all_in_one = service_pack['all_in_one']
        self.has_food = service_pack['has_food']
        self.has_gifts = service_pack['has_gifts']
        self.provider_number = service_pack['provider_number']
        self.food_set = filter_set(service_pack['food_set'])
        self.gift_set = filter_set(service_pack['gift_set'])
        self.food_prov = A_SELECT.get(service_pack['food_prov'])
        self.gift_sponsor = A_SELECT.get(service_pack['gift_sponsor'])
        self.turkey_sponsor = A_SELECT.get(service_pack['turkey_sponsor'])
        self.voucher_sponsor = A_SELECT.get(service_pack['voucher_sponsor'])
        self.code = service_pack['barcode']
        self.header = f'''
christmas_logo.png
time.ctime()
 
Dear {self.name},
This letter is cofirmation that you registered with the Christmas Bureau.\
 You have registered to recieve {self.service} from {self.service_prov}.  
'''
        self.food_pickup_string_w_app = f'''
You will be able to pick up your {self.food_service} from\
 {A_SELECT.get(self.food_set, 'ERROR')} on\
 {self.food_pu_date}. Please bring this letter with you as well as a piece of\
 identification to {FOOD_LOOKUP.get(self.food_set, 'ERROR')}.  You must wear a\
 mask at all times when you are picking up. 
'''
        self.food_delivery =f'''
{self.del_f_prov} will deliver your {self.food_service} to you on \
{self.food_del_date}. {DELIVERY_CONTACT.get(self.del_f_prov, '')} '''
        self.gift_pickup_w_app = f'''
Your appointment to pick up gifts for the eligible children\
 in your household will be appointment number {self.gift_pu_date}.\
 You will pick up your gifts from {GIFT_LOOKUP.get(self.gift_pu_loc)}. \
You must bring this letter with you to pick up your gifts.  You must also wear\
 a mask at all times while picking up your gifts.
'''

        self.gift_and_food_pu_w_app = f'''
Your appointment to pick up food as well as the gifts for the eligible children\
 in your household will be appointment number {self.gift_pu_date}. You will\
 pick up your gifts from {self.gift_pu_loc} located at \
{GIFT_LOOKUP.get(self.gift_sponsor)}.  You must bring this letter with\
 you to pick up your gifts.  You must also wear a mask at all times while picking up your gifts.
'''
        self.gift_and_food_del = f'''
Your food and gifts for your eligible children will be delivered by\
{self.service_prov}.  They will deliver to you on {self.food_del_date}.
'''
        self.gift_pickup_no_app = f'''
The gifts for the eligible children in your household will be provided by {self.gift_sponsor}.  They will contact you by phone or email to make arrangments for you to come and pick them up soon. 
'''
        self.cambridge_ff = f'''
You food and gifts for your eligible children will be available for pickup\
from the Cambridge Firefighters on {self.food_pu_date} at\
 the Preston Auditorium located at 1458 Hamilton St. Cambridge . You have appointment number {self.zone_pu_num}. \
Please Note: it is very important that you bring this letter with you, and arrive in a vehicle. 
'''
        self.reitzel = f'''
Your food and gifts for your eligible children will be delivered to you by\
 {self.service_prov} on {self.food_del_date}.  They will be in contact with\
 you by phone to confirm your delivery information.
        '''
        self.sertoma = f'''
The gifts for your eligible children will be available from KW Sertoma Club. They will contact you shortly about how and when you can pickup your gifts.
'''
        self.footer = f'''
If you have any questions please check out our frequently asked questions (FAQ) on our website www.christmashampers.ca. 
We can also be reached at 519-742-5860 or info@christmashampers.ca

Regards,

The Christmas Bureau
{self.code}
'''

    def get(self):
        '''
        returns the formatted text of the letter to be parsed by the
        Confirmation_Letter object's .add_text method
        '''
        full_text = None
        
        food = False
        gifts = False
        
        one_provider = False
        two_provider = False
        all_in = False

        delivery = False
        pickup = False

        gift_app = False
        food_app = False

        food_string = None
    
        if self.all_in_one:
            all_in = True
        if self.has_food:
            food = True
        if self.has_gifts:
            gifts = True
        if self.food_del_date:
            delivery = True
        if food and not delivery:
            pickup = True
        if self.gift_pu_date:
            gift_app = True
        
        #print(f'all in: {all_in}')
        #print(f'food: {food}')
        #print(f'gifts: {gifts}')
        #print(f'delivery: {delivery}')
        #print(f'gift_app: {gift_app}')

        #####################
        # DETERMINE STRINGS #
        #####################
        
        # SPECIAL CASES
        if 'Firefighters' in self.service_prov:
            full_text = f'''
{self.header}
{self.cambridge_ff}
{self.footer}
                    '''
        # FOOD ONLY
        elif not gifts:
            # delivery
            if delivery and food:
                full_text = f'''
{self.header}
{self.food_delivery}
{self.footer}
'''
            # food pickup with appointment
            elif food and not delivery:
                full_text = f'''
{self.header}
{self.food_pickup_string_w_app}
{self.footer}
                '''
        
        # FOOD AND GIFTS
        elif food and gifts:
            # food delivery and gift pickup with appointment
            if (delivery and gift_app) and not all_in:
                full_text = f'''
{self.header}
{self.food_delivery}
{self.gift_pickup_w_app}
{self.footer}
                '''
            # food pickup appointment and gift pickup appointment
            elif gift_app and not delivery: 
                # different providers
                if not all_in:
                    full_text = f'''
{self.header}
{self.food_pickup_string_w_app}
{self.gift_pickup_w_app}
{self.footer}
                '''
               
                #else: # gifts and food pu with same provider
                #    full_text = f''' '''

            # gifts with no app, and food pickup with app
            elif (not gift_app and gifts) and not delivery:
                full_text = f'''
{self.header}
{self.food_pickup_string_w_app}
{self.sertoma}
{self.footer}
                '''
            # gifts with no app, and food delivery
            elif (not gift_app and gifts) and (delivery and not all_in):
                full_text = f'''
{self.header}
{self.food_delivery}
{self.sertoma}
{self.footer}
                '''
            # food delivery and gift delivery
            elif delivery and all_in:
                # Food delivery and gifts provided by one provider
                full_text = f'''
{self.header}
{self.reitzel}
{self.footer}
                '''
        elif not food and gifts:
            if not gift_app:
                full_text = f'''
{self.header}
{self.sertoma}
{self.footer}
                '''
            elif gift_app:
                full_text = f'''
{self.header}
{self.gift_pickup_w_app}
{self.footer}
                '''
        #print(full_text)
        #pause2 = input('pause2')

        return full_text

class Confirmation_Letter:
    def __init__(self, run_date):
        self.doc = SimpleDocTemplate(f"{run_date}.pdf",pagesize=letter,
                        rightMargin=52,leftMargin=52,
                        topMargin=18,bottomMargin=18)
        self.page = []
        self.styles=getSampleStyleSheet()
        
        self.styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
 
    def add_image(self, img):
        #im = Image(img, 2*inch, 2*inch)
        im = Image(img)
        self.page.append(im)
 
    def add_space(self):
        self.page.append(Spacer(1, 12))
 
    def add_text(self, text, space=0):
        if type(text)==list:
            for f in text:
                self.add_text(f)
        else:
            ptext = f'<font size="12">{text}</font>'
            self.page.append(Paragraph(ptext, self.styles["Normal"]))
            if space==1:
                self.add_space()
            self.add_space()
    
    def new_page(self):
        self.page.append(PageBreak())
    
    def parse_text(self, file_id, letter_text):
        """
        Prints all the lines in the text multiline string
        param: letter_text is a Letter_Text object that contains
        the formatted text that is parsed by the logic in this
        method and added to the page
        """
        #print(f'file: {file_id} letter_text: {letter_text}')
        text = letter_text.get().splitlines()
        for line in text:
            if ".png" in line:
                self.add_image(line)
            elif "ctime()" in line:
                self.add_text(time.ctime())
            else:
                self.add_text(line)
        self.new_page()    
        os.system(f"echo {file_id} added")

    def write(self): 
        '''
        writes the pdf out to file
        '''

        self.doc.build(self.page)
        #os.system('mv *.pdf products/')

if __name__ == '__main__':

    p1 = Letter_Text({
      'file_id': 123456, 
      'name': 'Frank',
      'service' : 'food and gifts for your children',
      'service_prov': 'House of Friendship and KW Salvation Army',
      'food_service': 'a turkey',
      'when' : ' Monday',
      'email' : 'frank@email.com',
      'phone' : '555-555-5555',
      'f_loc' : 'location 1',
      'f_date' : 'December 5',
      'f_del_date': None,
      'g_loc' : 'toys r us',
      'g_date' : '1 on December 11'})


    p2 = Letter_Text({'file_id': 555777, 'name': 'Francine',
      'service' : 'food',
      'service_prov': 'House of Friendship',
      'food_service': 'a turkey',
      'when' : ' Monday',
      'email' : 'francine@email.com',
      'phone' : '555-555-5555',
      'f_loc' : None,
      'f_date' : None,
      'f_del_date': 'Dec 5 between 10am and 1pm',
      'g_loc' : None,
      'g_date' : None})

    letters = Confirmation_Letter('Dec 12 2020')
    letters.parse_text('123456', p1)
    letters.parse_text('555777', p2)
    letters.write()

