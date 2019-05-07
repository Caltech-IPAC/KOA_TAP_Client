import os
import sys 
import getpass 
import logging
import initLogger
import json

import urllib
import http.cookiejar


"""
The auth module validates a user has an account on the project. 
"""

def login (cookiefile, **kwargs):

    """
    The auth function prompts for an authorized project's user ID 
    and password.

    Args:

        cookiefile (string): a file path provided by the user to save 
                             returned cookie which is needed for the 
                             subsequent PRV operations.
        
	userid     (string): a valid user id exists in the KOA's user table.
        
        password   (string): a valid password in the KOA's user table. 


    Calling synopsis: 
    
        import koapy.auth

        login (cookiepath): program will prompt for userid and password 
    """

    cookiepath = cookiefile 

    if (len(cookiepath) == 0):
        print ('A cookiepath name is required')

    logger = None
    handler = None

    userid= ''
    password = ''
    if ('userid' in kwargs):
        userid = kwargs.get ('userid')

    if ('password' in kwargs):
        password = kwargs.get ('password')

    url = ''
    response = ''
    jsondata = ''

    status = ''
    msg = ''

#
#    get userid and password via keyboard input
#
    if (len(userid) == 0):
        userid = input ("Userid: ")

    if (len(password) == 0):
        password = getpass.getpass ("Password: ")

    password = urllib.parse.quote (password)

    cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)
        
#
#  url for login
#
    param = dict()
    param['userid'] = userid
    param['password'] = password
    
    data_encoded = urllib.parse.urlencode (param)
    
    url = "http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-koaLogin?" \
        + data_encoded

#    url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-koaLogin?" \
#        + data_encoded

#     print (f'url= {url:s}')

#
#    build url_opener
#
    data = None

    try:
        opener = urllib.request.build_opener (
            urllib.request.HTTPCookieProcessor (cookiejar))
            
        urllib.request.install_opener (opener)
        
        request = urllib.request.Request (url)
            
        cookiejar.add_cookie_header (request)
            
        response = opener.open (request)

    except urllib.error.URLError as e:
        
        status = 'error'
        msg = 'URLError= ' + e.reason    
        
    except urllib.error.HTTPError as e:
            
        status = 'error'
        msg =  'HTTPError= ' +  e.reason 
            
    except Exception:
           
        status = 'error'
        msg = 'URL exception'

             
    if (status == 'error'):       
        msg = 'Failed to login: %s' % msg
        print (msg)
         
        return;


#
#    check content-type in response header: 
#    if it is 'application/json', then it is an error message
#
    infostr = dict(response.info())

    contenttype = infostr.get('Content-type')

    data = response.read()
    sdata = data.decode ("utf-8");
   
    jsondata = json.loads (sdata);
   
    for key,val in jsondata.items():
                
        if (key == 'status'):
            status = val
                
        if (key == 'msg'):
            msg =  val
		
    if (status == 'ok'):
        cookiejar.save (cookiepath, ignore_discard=True);
        
        msg = 'Successfully login as ' + userid
    
    else:       
        msg = 'Failed to login: ' + msg

    print (msg)
    
    return;

