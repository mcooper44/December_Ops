import random

def format_int_to_float(one_element_list):
    '''
    because it is late and i dont' want to dive into random
    and because range() doesnt take floats for some reason
    this takes the result of random.samples in generate_coordinates
    and takes the first number and moves it left of the decimal
    and the rest of the number onto the other side of the right

    '''
    
    value = str(one_element_list[0])
    value_head = float(value[0])
    value_tale = float('.' + value[1:])
    return value_head + value_tale

class test_codes():
    '''
    this class generates test codes for the routing functions
    it starts with lat and long for the middle of Hudson's Bay
    coord_range = number of coordinates to generate
    ref_lat = the latitude to start with
    ref_long = the longditude to start with
    '''
    def __init__(self, coord_range, ref_lat = 59.0, ref_long = -84.0):
        self.lat = ref_lat
        self.lng = ref_long
        self.number_of_coords = coord_range
        self.test_codes_list = []

    def generate_coordinates(self):
        '''
        returns a list of coordinates N elements
        N = coord_range
        '''
        sample_coordinates = []
        for x in range(self.number_of_coords):
            x_additive = random.sample(range(100000, 300000), 1)
            y_additive = random.sample(range(100000, 300000), 1)
            x_value = self.lat + format_int_to_float(x_additive)
            y_value = self.lng + (format_int_to_float(y_additive) * -1)
            sample_coordinates.append([x_value, y_value])       
        self.test_codes_list += sample_coordinates

