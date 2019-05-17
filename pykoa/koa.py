import os 
import sys
import getpass 
import logging
import json
import ijson
import urllib 
import http.cookiejar 
import requests

from astropy.coordinates import name_resolve
from astropy.table import Table, Column

from pykoa import koatap

class KOA:

    """
    'KOA' class provides KOA archive access functions for searching the
    Keck On-line Archive (KOA) data via TAP interface.  
    
    The user's KOA credentials (given at login) are used to search the 
    proprietary data.

    Example:
    --------

    import os
    import sys 

    import pykoa 

    koa = pykoa.KOA ()

    koa.query_datetime ('2018-03-16 00:00:00/2018-03-18 00:00:00', \
        outpath= './meta.xml', \
	format='ipac') 
    """
    
    tap = None

    parampath = ''
    outpath = ''
    format = 'ipac'
    maxrec = '0'
    query = ''
    tap_url = ''
    query_url= ''
    
    content_type = ''
    outdir = ''
    astropytbl = None

    ndnloaded = 0
    ndnloaded_calib = 0
    ncaliblist = 0
 
    status = ''
    msg = ''
    
    debugfname = './koa.debug'    
    debug = 0    


    def __init__(self, **kwargs):
    
        """
        'init' method initialize the class with optional debugfile flag

        Optional inputs:
        ----------------
        debugfile: a file path for the debug output
 
	"""
        
        if ('debugfile' in kwargs):
            
            self.debug = 1
            self.debugfname = kwargs.get ('debugfile')

            if (len(self.debugfname) > 0):
      
                logging.basicConfig (filename=self.debugfname, \
                    level=logging.DEBUG)
    
                with open (self.debugfname, 'w') as fdebug:
                    pass
 
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter koa.init:')

        
        self.tap_url = \
	    'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/TAP/nph-tap.py'

        self.query_url = \
	    'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-makeQuery?'

#        self.koaurl = 'http://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&'
#        self.caliburl = \
#            'http://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-getCaliblist?'

        self.koaurl = 'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/getKOA/nph-getKOA?return_mode=json&'
        self.caliburl = \
            'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-getCaliblist?'
       
        if self.debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'query_url= [{self.query_url:s}]')
            logging.debug (f'self.koaurl= {self.koaurl:s}')
            logging.debug (f'self.caliburl= {self.caliburl:s}')
      
        print ('koa class initialized')
	
        return



    def login (self, cookiepath, **kwargs):

        """
        auth method validates a user has a valid KOA account; it takes two
        'keyword' arguments: userid and password. If the inputs are not 
        provided in the keyword, the auth method prompts for inputs.

        Required input:
        ---------------     
        cookiepath (string): a file path provided by the user to save 
                 returned cookie (in login method) or to serve
                 as input parameter for the subsequent koa 
                 query and download methods.
        
        Keyword input:
        ---------------     
	userid     (string): a valid user id  in the KOA's user table.
        
        password   (string): a valid password in the KOA's user table. 

        
        Calling synopsis: 
    
        koa.login (cookiepath, userid='xxxx', password='xxxxxx'), or

        koa.login (cookiepath): and the program will prompt for 
                                 userid and password 
        """

        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter login:')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= [{cookiepath:s}]')

        if (len(cookiepath) == 0):
            print ('A cookiepath is required if you wish to login to KOA')
            return

        cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)
            
        if self.debug:
            logging.debug ('')
            logging.debug ('cookiejar initialized')
       
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

#
#  url for login
#
        param = dict()
        param['userid'] = userid
        param['password'] = password
    
        data_encoded = urllib.parse.urlencode (param)
    
        url = "http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-koaLogin?" + data_encoded

#        url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-koaLogin?" \
#            + data_encoded

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= [{url:s}]')

#         print (f'url= {url:s}')

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
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'send request')

            response = opener.open (request)

            if self.debug:
                logging.debug ('')
                logging.debug (f'response returned')

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

        if self.debug:
            logging.debug ('')
            logging.debug ('no error')

#
#    check content-type in response header: 
#    if it is 'application/json', then it is an error message
#
        infostr = dict(response.info())

        contenttype = infostr.get('Content-type')

        if self.debug:
            logging.debug ('')
            logging.debug (f'contenttype= {contenttype:s}')

        data = response.read()
        sdata = data.decode ("utf-8");
        jsondata = json.loads (sdata);
   
        for key,val in jsondata.items():
                
            if (key == 'status'):
                status = val
                
            if (key == 'msg'):
                msg =  val
		
        if self.debug:
            logging.debug ('')
            logging.debug (f'status= {status:s}')
            logging.debug (f'msg= {msg:s}')


        if (status == 'ok'):
            cookiejar.save (cookiepath, ignore_discard=True);
        
            msg = 'Successfully login as ' + userid
            self.cookie_loaded = 1

#
#    print out cookie values
#   
            for cookie in cookiejar:
                    
                if self.debug:
                    logging.debug ('')
                    logging.debug ('cookie saved:')
                    logging.debug (cookie)
                    logging.debug (f'cookie.name= {cookie.name:s}')
                    logging.debug (f'cookie.value= {cookie.value:s}')
                    logging.debug (f'cookie.domain= {cookie.domain:s}')
            
 
        else:       
            msg = 'Failed to login: ' + msg

        print (msg)
        return;


    def query_datetime (self, instrument, datetime, outpath, **kwargs):
        
        """
        'query_datetime' method search KOA data by 'datetime' range
        
        Required Inputs:
        ---------------    
        instruments: e.g. HIRES, NIRC2, etc...

        time: a datetime string in the format of datetime1/datetime2 where 
            datetime format of is 'yyyy-mm-dd hh:mm:ss.ss'

        outpath: the full output filepath of the returned metadata table
    
        e.g. 
            instrument = 'hires',
            datetime = '2018-03-16 06:10:55.00/2018-03-18 00:00:00.00' 

        Optional inputs:
	----------------
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data.
        
	format:  Output format: votable, ascii.ipac, etc.. 
	         (default: ipac)
        
	maxrec:  maximum records to be returned 
	         default: '0'
        """
   
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_datetime:')
        
        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(datetime) == 0):
            print ('Failed to find required parameter: datetime')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.datetime = datetime
        self.outpath = outpath

        if self.debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'datetime= {self.datetime:s}')
            logging.debug (f'outpath= {self.outpath:s}')

        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'maxrec= {self.maxrec:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        param['instrument'] = self.instrument
        param['datetime'] = self.datetime
       
        if self.debug:
            logging.debug ('')
            logging.debug ('call query_criteria')

        self.query_criteria (param, outpath, **kwargs)

        return



    def query_position (self, instrument, pos, outpath, **kwargs):
        
        """
        'query_position' method search KOA data by 'position' 
        
        Required Inputs:
        ---------------    

        instruments: e.g. HIRES, NIRC2, etc...

        pos: a position string in the format of 
	
	1.  circle ra dec radius;
	
	2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
	
	3.  box ra dec width height;
	
	All ra dec in J2000 coordinate.
            datetime format of is 'yyyy-mm-dd hh:mm:ss.ss'
             
        e.g. 
            instrument = 'hires',
            pos = 'circle 230.0 45.0 0.5'

        outpath: the full output filepath of the returned metadata table
        
        Optional Input:
        ---------------    
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data.
        
	format:  Output format: votable, ascii.ipac, etc.. 

        format: votable, ipac, csv, etc..  (default: ipac)
	
	maxrec:  maximum records to be returned (default: '0')
        """
   
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_position:')
        
        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(pos) == 0):
            print ('Failed to find required parameter: time')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.pos = pos
        self.outpath = outpath
 
        if self.debug:
            logging.debug ('')
            logging.debug (f'instrument=  {self.instrument:s}')
            logging.debug (f'pos=  {self.pos:s}')
            logging.debug (f'outpath= {self.outpath:s}')

#        if ('cookiepath' in kwargs): 
#            self.cookiepath = kwargs.get('cookiepath')

#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'maxrec= {self.maxrec:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        param['instrument'] = self.instrument
        param['pos'] = self.pos

        self.query_criteria (param, outpath, **kwargs)

        return


    def query_object (self, instrument, object, outpath, **kwargs):
        
        """
        'query_object_name' method search KOA data by 'object name' 
        
        Required Inputs:
        ---------------    

        instruments: e.g. HIRES, NIRC2, etc...

        object: an object name resolvable by Astropy name_resolve; 
       
        This method resolves the object's coordiates, uses it as the
	center of the circle position search with default radius of 0.5 deg.

        e.g. 
            instrument = 'hires',
            object = 'WD 1145+017'

        Optional Input:
        ---------------    
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data.
        
	format:  Output format: votable, ascii.ipac, etc.. 

        radius = 1.0 (deg)

        Output format: votable, ascii.ipac, etc.. 
	               (default: ipac)
	
	maxrec:  maximum records to be returned 
	         default: 0
        """
   
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_object_name:')

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(object) == 0):
            print ('Failed to find required parameter: object')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.object = object
        self.outpath = outpath

        if self.debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'object= {self.object:s}')
            logging.debug (f'outpath= {self.outpath:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radiusi_str = kwargs.get('radius')
            radius = float(radius_str)

#        if ('cookiepath' in kwargs): 
#            self.cookiepath = kwargs.get('cookiepath')

#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:s}')
            logging.debug (f'radius= {radius:f}')

        coords = None
        try:
            print (f'resolving object name')
 
            coords = name_resolve.get_icrs_coordinates (object)
        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'name_resolve error: {str(e):s}')
            
            print (str(e))
            return

        ra = coords.ra.value
        dec = coords.dec.value

        if self.debug:
            logging.debug ('')
            logging.debug (f'ra= {ra:f}')
            logging.debug (f'dec= {dec:f}')
        
        self.pos = 'circle ' + str(ra) + ' ' + str(dec) \
            + ' ' + str(radius)
	
        if self.debug:
            logging.debug ('')
            logging.debug (f'pos= {self.pos:s}')
       
        print (f'object name resolved: ra= {ra:f}, dec={dec:f}')
 
#
#    send url to server to construct the select statement
#
        param = dict()
        
        param['instrument'] = self.instrument
        param['pos'] = self.pos

        self.query_criteria (param, outpath, **kwargs)

        return

    
    def query_criteria (self, param, outpath, **kwargs):
        
        """
        'query_criteria' method allows the search of KOA data by multiple
        the parameters specified in a dictionary (param).

        param: a dictionary containing a list of acceptable parameters:

            instruments (required): e.g. HIRES, NIRC2, etc...

            datetime: a datetime string in the format of datetime1/datetime2 
	        where datetime format of is 'yyyy-mm-dd hh:mm:ss'
             
            pos: a position string in the format of 
	
	        1.  circle ra dec radius;
	
	        2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
	
	        3.  box ra dec width height;
	
	        all in ra dec in J2000 coordinate.
             
	    target: target name used in the project, this will be searched 
	        against the database.

        outpath: file path for the returned metadata table 

        Optional parameters:
        --------------------
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data.
        
	format:  Output format: votable, ascii.ipac, etc.. 
        
	    format: votable, ipac, etc.. (default: votable)
	    
            maxrec: max number of output records
        """

        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_criteria')
#
#    send url to server to construct the select statement
#
        self.outpath = outpath
 
        len_param = len(param)

        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
            
            logging.debug ('')
            logging.debug (f'len_param= {len_param:d}')

            for k,v in param.items():
                logging.debug (f'k, v= {k:s}, {v:s}')

        cookiepath = ''
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')

        self.format ='ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:s}')


        data = urllib.parse.urlencode (param)

        url = self.query_url + data            

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        query = ''
        try:
            query = self.__make_query (url) 

            if self.debug:
                logging.debug ('')
                logging.debug ('returned __make_query')
  
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'Error: {str(e):s}')
            
            print (str(e))
            return ('') 
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'query= {query:s}')
       
        self.query = query

#
#    send tap query
#
        self.tap = None
        if (len(cookiepath) > 0):

            if (self.debug):
                self.tap = koatap.KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=cookiepath, \
                    debug=1)
            else:
                self.tap = koatap.KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=cookiepath)

            if self.debug:
                logging.debug('')
                logging.debug('koaTap initialized with cookie')

        else:    
            if (self.debug):
                self.tap = koatap.KoaTap (self.tap_url, \
	            format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
            else: 
                self.tap = koatap.KoaTap (self.tap_url, \
	            format=self.format, \
		    maxrec=self.maxrec)
        
            if self.debug:
                logging.debug('')
                logging.debug('koaTap initialized without cookie')

        if self.debug:
            logging.debug('')
            logging.debug(f'query= {query:s}')
            logging.debug('call self.tap.send_async')

        print ('submitting request...')

        retstr = self.tap.send_async (query, outpath= self.outpath)
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'indx= {indx:d}')

        if (indx >= 0):
            print (retstr)
            sys.exit()

#
#    no error: 
#
        print (retstr)
        return

    
    def query_adql (self, query, outpath, **kwargs):
       
        """
        'query_adql' method receives a qualified ADQL query string from
	user input.
        
        Required Inputs:
        ---------------    
            query:  a ADQL query

            outpath: the output filename the returned metadata table
        
        Optional inputs:
	----------------
            cookiepath (string): cookie file path for query the proprietary 
                                 KOA data.
        
	    format:  Output format: votable, ipac, csv, tsv, etc.. 
	             (default: ipac)
        
	    maxrec:  maximum records to be returned 
	             default: 0
        """
   
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_adql:')
        
        if (len(query) == 0):
            print ('Failed to find required parameter: query')
            return
        
        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return
        
        self.query = query
        self.outpath = outpath
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug (f'query= {self.query:s}')
            logging.debug (f'outpath= {self.outpath:s}')
       
        cookiepath = '' 
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:s}')


#
#    send tap query
#
        self.tap = None
        if (len(cookiepath) > 0):
            self.tap = koatap.KoaTap (self.tap_url, \
	        format=self.format, \
		maxrec=self.maxrec, \
		cookiefile=cookiepath, \
		debug=1)
        else:    
            if self.debug:
                logging.debug('')
                logging.debug('initializing KoaTap')
            
            self.tap = koatap.KoaTap (self.tap_url, \
	        format=self.format, \
		maxrec=self.maxrec, \
		debug=1)
        
        if self.debug:
            logging.debug('')
            logging.debug('koaTap initialized')
            logging.debug(f'query= {query:s}')
            logging.debug('call self.tap.send_async')

        print ('submitting request...')

        if (len(self.outpath) > 0):
            retstr = self.tap.send_async (query, outpath=self.outpath)
        else:
            retstr = self.tap.send_async (query)
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'indx= {indx:d}')

        if (indx >= 0):
            print (retstr)
            sys.exit()

#
#    no error: 
#
        print (retstr)
        return


    def print_data (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter koa.print_data:')

        try:
            self.tap.print_data ()
        except Exception as e:
                
            msg = 'Error print data: ' + str(e)
            print (msg)
        
        return


    def download (self, metapath, format, outdir, **kwargs):
    
        """
        The download method allows users to download FITS files (and/or) 
        associated calibration files shown in their metadata file.

	Required input:
	-----
	metapath: a full path metadata table obtained from running
	          query methods    
        
	format:   metasata table's format: ipac, votable, csv, or tsv.
	
        outdir:   the directory for depositing the returned files      
 
	
        Optional input:
        ----------------
        cookiepath (string): cookie file path for downloading the proprietary 
                             KOA data.
        
        start_row,
	
        end_row,

        calibfile: whether to download the associated calibration files (0/1);
                   default is 0.
        """
    
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter download:')
        
        if (len(metapath) == 0):
            print ('Failed to find required input parameter: metapath')
            return

        if (len(format) == 0):
            print ('Failed to find required input parameter: format')
            return

        if (len(outdir) == 0):
            print ('Failed to find required input parameter: outdir')
            return

        self.metapath = metapath
        self.format = format
        self.outdir = outdir

        if self.debug:
            logging.debug ('')
            logging.debug (f'metapath= {self.metapath:s}')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'outdir= {self.outdir:s}')

        cookiepath = ''
        cookiejar = None
        
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')

        if (len(cookiepath) > 0):
   
            cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)

            try: 
                cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                if self.debug:
                    logging.debug (\
                        f'cookie loaded from file: {cookiepath:s}')
        
                for cookie in cookiejar:
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('cookie=')
                        logging.debug (cookie)
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')

            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'loadCookie exception: {str(e):s}')
                pass
        
        fmt_astropy = self.format
        if (self.format == 'tsv'):
            fmt_astropy = 'ascii.tab'
        if (self.format == 'csv'):
            fmt_astropy = 'ascii.csv'
        if (self.format == 'ipac'):
            fmt_astropy = 'ascii.ipac'

#
#    read metadata to astropy table
#
        self.astropytbl = None
        try:
            self.astropytbl = Table.read (self.metapath, format=fmt_astropy)
        
        except Exception as e:
            self.msg = 'Failed to read metadata table to astropy table:' + \
                str(e) 
            print (self.msg)
            sys.exit()

        self.len_tbl = len(self.astropytbl)

        if self.debug:
            logging.debug ('')
            logging.debug ('self.astropytbl read')
            logging.debug (f'self.len_tbl= {self.len_tbl:d}')

        self.colnames = self.astropytbl.colnames

        if self.debug:
            logging.debug ('')
            logging.debug ('self.colnames:')
            logging.debug (self.colnames)
  
        self.len_col = len(self.colnames)

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.len_col= {self.len_col:d}')

 
        self.ind_instrume = -1
        self.ind_koaid = -1
        self.ind_filehand = -1
        for i in range (0, self.len_col):

            if (self.colnames[i].lower() == 'instrume'):
                self.ind_instrume = i

            if (self.colnames[i].lower() == 'koaid'):
                self.ind_koaid = i

            if (self.colnames[i].lower() == 'filehand'):
                self.ind_filehand = i
             
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.ind_instrume= {self.ind_instrume:d}')
            logging.debug (f'self.ind_koaid= {self.ind_koaid:d}')
            logging.debug (f'self.ind_filehand= {self.ind_filehand:d}')
       
    
        if (self.len_tbl == 0):
            print ('There is no data in the metadata table.')
            sys.exit()
        
        calibfile = 0 
        if ('calibfile' in kwargs): 
            calibfile = kwargs.get('calibfile')
         
        if self.debug:
            logging.debug ('')
            logging.debug (f'calibfile= {calibfile:d}')

        srow = 0;
        erow = self.len_tbl - 1

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
     
        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'erow= {erow:d}')
     
        if (srow < 0):
            srow = 0 
        if (erow > self.len_tbl - 1):
            erow = self.len_tbl - 1 
 
        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
            logging.debug (f'erow= {erow:d}')
     
#
#    create outdir if it doesn't exist
#
#    decimal mode work for both python2.7 and python3;
#
#    0755 also works for python 2.7 but not python3
#  
#    convert octal 0775 to decimal: 493 
#
        d1 = int ('0775', 8)

        if self.debug:
            logging.debug ('')
            logging.debug (f'd1= {d1:d}')
     
        try:
            os.makedirs (self.outdir, mode=d1, exist_ok=True) 

        except Exception as e:
            
            self.msg = 'Failed to create {self.outdir:s}:' + str(e) 
            print (self.msg)
            sys.exit()

        if self.debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs') 


        instrument = '' 
        koaid = ''
        filehand = ''
        self.ndnloaded = 0
        self.ndnloaded_calib = 0
        self.ncaliblist = 0
      
        nfile = erow - srow + 1   
        
        print (f'Start downloading {nfile:d} FITS data you requested;')
        print (f'please check your outdir: {self.outdir:s} for  progress.')
 
        for l in range (srow, erow+1):
       
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d}')
                logging.debug ('')
                logging.debug ('self.astropytbl[l]= ')
                logging.debug (self.astropytbl[l])
                logging.debug ('instrument= ')
                logging.debug (self.astropytbl[l][self.ind_instrume])

            instrument = self.astropytbl[l][self.ind_instrume]
            koaid = self.astropytbl[l][self.ind_koaid]
            filehand = self.astropytbl[l][self.ind_filehand]
	    
            if self.debug:
                logging.debug ('')
                logging.debug ('type(instrument)= ')
                logging.debug (type(instrument))
                logging.debug (type(instrument) is bytes)
            
            if (type (instrument) is bytes):
                
                if self.debug:
                    logging.debug ('')
                    logging.debug ('bytes: decode')

                instrument = instrument.decode("utf-8")
                koaid = koaid.decode("utf-8")
                filehand = filehand.decode("utf-8")
           
            ind = -1
            ind = instrument.find ('HIRES')
            if (ind >= 0):
                instrument = 'HIRES'
            
            ind = -1
            ind = instrument.find ('LRIS')
            if (ind >= 0):
                instrument = 'LRIS'
  
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d} koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')
                logging.debug (f'instrument= {instrument:s}')

#
#   get lev0 files
#
            url = self.koaurl + 'filehand=' + filehand
                
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'filepath= {filepath:s}')
                logging.debug (f'url= {url:s}')

#
#    if file doesn't exist: download
#
            isExist = os.path.exists (filepath)
	    
            if self.debug:
                logging.debug ('')
                logging.debug ('isExist:')
                logging.debug (isExist)


            if (not isExist):

                try:
                    self.__submit_request (url, filepath, cookiejar)
                    self.ndnloaded = self.ndnloaded + 1

                    self.msg =  'Returned file written to: ' + filepath   
           
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('returned __submit_request')
                        logging.debug (f'self.msg= {self.msg:s}')
            
                except Exception as e:
                    print (f'File [{koaid:s}] download: {str(e):s}')


#
#    if calibfile == 1: download calibfile
#
            if (calibfile == 1):

                if self.debug:
                    logging.debug ('')
                    logging.debug ('calibfile=1: downloading calibfiles')
	    
                koaid_base = '' 
                ind = -1
                ind = koaid.rfind ('.')
                if (ind > 0):
                    koaid_base = koaid[0:ind]
                else:
                    koaid_base = koaid

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'koaid_base= {koaid_base:s}')
	    
                caliblist = self.outdir + '/' + koaid_base + '.caliblist.json'
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'caliblist= {caliblist:s}')

                isExist = os.path.exists (caliblist)
	    
                if (not isExist):

                    if self.debug:
                        logging.debug ('')
                        logging.debug ('downloading calibfiles')
	    
                    url = self.caliburl \
                        + 'instrument=' + instrument \
                        + '&koaid=' + koaid

                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'caliblist url= {url:s}')

                    try:
                        self.__submit_request (url, caliblist, cookiejar)
                        self.ncaliblist = self.ncaliblist + 1

                        self.msg =  'Returned file written to: ' + caliblist   
           
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'self.msg= {self.msg:s}')
            
                    except Exception as e:
                        print (f'File [{caliblist:s}] download: {str(e):s}')

#
#    check again after caliblist is successfully downloaded, if caliblist 
#    exists: download calibfiles
#     
                isExist = os.path.exists (caliblist)
	  
                  
                if (isExist):

                    if self.debug:
                        logging.debug ('')
                        logging.debug ('list exist: downloading calibfiles')
	    
                    try:
                        ncalibs = self.__download_calibfiles ( \
                            caliblist, cookiejar)
                        self.ndnloaded_calib = self.ndnloaded_calib + ncalibs
                
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('returned __download_calibfiles')
                            logging.debug (f'{ncalibs:d} downloaded')

                    except Exception as e:
                
                        self.msg = 'Error downloading files in caliblist [' + \
                            filepath + ']: ' +  str(e)
                        continue
                

        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.len_tbl:d} files in the table;')
            logging.debug (f'{self.ndnloaded:d} files downloaded.')
            logging.debug (f'{self.ncaliblist:d} calibration list downloaded.')
            logging.debug (\
                f'{self.ndnloaded_calib:d} calibration files downloaded.')

        print (f'A total of new {self.ndnloaded:d} FITS files downloaded.')
        print (f'{self.ncaliblist:d} new calibration list downloaded.')
        print (f'{self.ndnloaded_calib:d} new calibration FITS files downloaded.')

        return



    def __download_calibfiles (self, listpath, cookiejar):

        if self.debug:
            logging.debug ('')
            logging.debug (f'Enter __download_calibfiles: {listpath:s}')

#
#    read input caliblist JSON file
#
        nrec = 0
        data = ''
        try:
            with open (listpath) as fp:
	    
                jsonData = json.load (fp) 
                data = jsonData["table"]

            fp.close() 

        except Exception as e:
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'caliblist: {caliblist:s} load error')

            self.errmsg = 'Failed to read ' + listpath	
	
            fp.close() 
            
            raise Exception (self.errmsg)

            return

        nrec = len(data)
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'downloadCalibfiles: nrec= {nrec:d}')

        if (nrec == 0):

            self.status = 'error'	
            self.errmsg = 'No data found in the caliblist: ' + listpath
	    
            raise Exception (self.errmsg)


#
#    retrieve koaid from caliblist json structure and download files
#
        ndnloaded = 0
        for ind in range (0, nrec):

            if self.debug:
                logging.debug (f'downloadCalibfiles: ind= {ind:d}')

            koaid = data[ind]['koaid']
            instrument = data[ind]['instrument']
            filehand = data[ind]['filehand']
            
            if self.debug:
                logging.debug (f'instrument= {instrument:s}')
                logging.debug (f'koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')

#
#   get lev0 files
#
            url = self.koaurl + 'filehand=' + filehand
                
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'filepath= {filepath:s}')
                logging.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'isExist: {isExist:d}: skip')
                     
                continue

            try:
                self.__submit_request (url, filepath, cookiejar)
                ndnloaded = ndnloaded + 1
                
                self.msg = 'calib file [' + filepath + '] downloaded.'
#                print (self.msg)

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned __submit_request')
                    logging.debug (f'self.msg: {self.msg:s}')
            
            except Exception as e:
                
                print (f'calib file download error: {str(e):s}')
#                raise Exception (str(e))

        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.ndnloaded:d} files downloaded.')

        return (ndnloaded)


    def __submit_request(self, url, filepath, cookiejar):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter database.__submit_request:')
            logging.debug (f'url= {url:s}')
            logging.debug (f'filepath= {filepath:s}')
       
            if not (cookiejar is None):  
            
                for cookie in cookiejar:
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('cookie saved:')
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            
        try:
            self.response =  requests.get (url, cookies=cookiejar, \
                stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')
        
        except Exception as e:
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to submit the request: ' + str(e)
	    
            raise Exception (self.msg)
            return
                       
        if self.debug:
            logging.debug ('')
            logging.debug ('status_code:')
            logging.debug (self.response.status_code)
      
      
        if (self.response.status_code == 200):
            self.status = 'ok'
            self.msg = ''
        else:
            self.status = 'error'
            self.msg = 'Failed to submit the request'
	    
            raise Exception (self.msg)
            return
                       
            
        if self.debug:
            logging.debug ('')
            logging.debug ('headers: ')
            logging.debug (self.response.headers)
      
      
        self.content_type = ''
        try:
            self.content_type = self.response.headers['Content-type']
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception extract content-type: {str(e):s}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')


        if (self.content_type == 'application/json'):
            
            if self.debug:
                logging.debug ('')
                logging.debug (\
                    'return is a json structure: might be error message')
            
            jsondata = json.loads (self.response.text)
          
            if self.debug:
                logging.debug ('')
                logging.debug ('jsondata:')
                logging.debug (jsondata)

 
            self.status = ''
            try: 
                self.status = jsondata['status']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.status= {self.status:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get status exception: e= {str(e):s}')

            self.msg = '' 
            try: 
                self.msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.msg= {self.msg:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract msg exception: e= {str(e):s}')

            errmsg = ''        
            try: 
                errmsg = jsondata['error']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {errmsg:s}')

                if (len(errmsg) > 0):
                    self.status = 'error'
                    self.msg = errmsg

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get error exception: e= {str(e):s}')


            if self.debug:
                logging.debug ('')
                logging.debug (f'self.status= {self.status:s}')
                logging.debug (f'self.msg= {self.msg:s}')


            if (self.status == 'error'):
                raise Exception (self.msg)
                return

#
#    save to filepath
#
        if self.debug:
            logging.debug ('')
            logging.debug ('save_to_file:')
       
        try:
            with open (filepath, 'wb') as fd:

                for chunk in self.response.iter_content (chunk_size=1024):
                    fd.write (chunk)
            
            self.msg =  'Returned file written to: ' + filepath   
#            print (self.msg)
            
            if self.debug:
                logging.debug ('')
                logging.debug (self.msg)
	
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to save returned data to file: %s' % filepath
            
            raise Exception (self.msg)
            return

        return
                       



























    def __make_query (self, url):
       
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter __make_query:')
            logging.debug (f'url= {url:s}')

        response = None
        try:
            response = requests.get (url, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            msg = 'Error: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)


        content_type = response.headers['content-type']

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')
       
        if (content_type == 'application/json'):
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'response.text: {response.text:s}')

#
#    error message
#
            try:
                jsondata = json.loads (response.text)
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug ('jsondata loaded')
                
                status = jsondata['status']
                msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'status: {status:s}')
                    logging.debug (f'msg: {msg:s}')

            except Exception:
                msg = 'returned JSON object parse error'
                
                if self.debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                
            raise Exception (msg)
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'msg= {msg:s}')
     
        return (response.text)

   


