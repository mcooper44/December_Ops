import googlemaps
from r_config import configuration
from address_parser_and_geocoder import GoogleResult
import sys

config = configuration.return_r_config()
myapikey = config.get_g_creds()
# INSTANTIATE GOOGLE MAPS CLIENT
gmaps = googlemaps.Client(key=myapikey)

if __name__ == '__main__':
    go = True
    while go:
        a = input('enter address\n')
        if a:
            response = gmaps.geocode(a)
            result = GoogleResult(response[0])
            print(result.result)
        else:
            sys.exit(0)
