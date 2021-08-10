import os
import sys
from getpass import getpass

import gdata.docs.service
import gdata.spreadsheet.service


'''
    get user information from the command line argument and 
    pass it to the download method
'''
def get_gdoc_information():
    email = 'intigralmailfetcher@gmail.com'
    password = 'intigralmailfetcher_1'
    gdoc_id = '1rTEOXIGcUNwkJsJxqZNKNjjf2XwDeECW4oM5Wc_CUWc'
    
    try:
        download(gdoc_id, email, password)
    
    except (Exception) as e:
        raise e
    
#python gdoc.py 1m5F5TXAQ1ayVbDmUCyzXbpMQSYrP429K1FZigfD3bvk#gid=0
def download(gdoc_id, email, password, download_path=None, ):

    print ("Downloading the CSV file with id "+ gdoc_id)

    gd_client = gdata.docs.service.DocsService()

    #auth using ClientLogin
    gs_client = gdata.spreadsheet.service.SpreadsheetsService()
    gs_client.ClientLogin(email, password)

    #getting the key(resource id and tab id from the ID)
    resource    = gdoc_id.split('#')[0]
    tab         = gdoc_id.split('#')[1].split('=')[1]
    resource_id = 'spreadsheet:'+resource

    if download_path is None:
        download_path = os.path.abspath(os.path.dirname(__file__))

    file_name = os.path.join(download_path, '%s.csv' % (gdoc_id))

    print ('Downloading spreadsheet to %s…' % file_name)

    docs_token = gd_client.GetClientLoginToken()
    gd_client.SetClientLoginToken(gs_client.GetClientLoginToken())
    gd_client.Export(resource_id, file_name, gid=tab)
    gd_client.SetClientLoginToken(docs_token)

    print ("Download Completed!")

    return file_name
    
if __name__=='__main__':
    get_gdoc_information()