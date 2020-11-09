import googlemaps
from r_config import configuration
from address_parser_and_geocoder import GoogleResult
config = configuration.return_r_config()
myapikey = config.get_g_creds()
# INSTANTIATE GOOGLE MAPS CLIENT
gmaps = googlemaps.Client(key=myapikey)

if __name__ == '__main__':
    while True:
        a = input('enter address\n')
        response = gmaps.geocode(a)
        result = GoogleResult(response[0])
