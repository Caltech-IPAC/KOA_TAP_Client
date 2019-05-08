import os 
import sys
import logging
import initLogger

import json

import urllib 
import http.cookiejar 
import requests

from astropy.coordinates import name_resolve

import koatap


class Archive:

    """
    'Archive' class provides KOA archive access functions for searching the
    Keck On-line Archive (KOA) data via TAP interface.  
    
    The user's KOA credentials (given at login) are used to search the 
    proprietary data.

    Example:
    --------

    import os
    import sys 

    import archive 

    arch = archive.Archive ('./mycookie.txt')

    arch.by_datetime ('2018-03-16 00:00:00/2018-03-18 00:00:00', \
        outpath= './meta.xml', \
	format='ipac') 
    """
    
    tap = None

    cookiepath = ''    
    parampath = ''
    outpath = ''
    format = 'ipac'
    maxrec = '0'
    query = ''
    tap_url = ''
    query_url= ''
    debugfname = './archive.debug'    
    debug = 0    


    def __init__(self, **kwargs):
    
        """
        'init' method initialize the class with optional output metadata file
	and cookiepath.

        Optional inputs:
        ----------------
        cookiepath: This is the cookie file that you have saved while you 
	            login using 'auth.login' module; this cookie file is
		    used for searching and downloading the proprietary data.
                    
		    It is not required if you only search for public data.
	"""
        
        self.loggername = 'archive'    
        self.logger = None
        self.handler = None
        
        self.debug = 0    
        self.debugfname = ''
        
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if ('debugfile' in kwargs): 
            self.debugfname = kwargs.get('debugfile')

        if (len(self.debugfname) > 0):
            
            self.debug = 1

            log = initLogger.initLogger (\
                self.debugfname, self.loggername, 'debug')
            
            self.logger = logging.getLogger (self.loggername)
            self.handler = self.logger.handlers
        
            if (len(self.handler) == 0):
                self.debug = 0
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter archive.init:')
            self.logger.debug (f'cookiepath= [{self.cookiepath:s}]')

        
        self.tap_url = \
	    'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/TAP/nph-tap.py'

        self.query_url = \
	    'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-makeQuery?'

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'tap_url= [{self.tap_url:s}]')
            self.logger.debug (f'query_url= [{self.query_url:s}]')
        
        print ('archive class initialized')
	
        return


    def by_datetime (self, instrument, datetime, **kwargs):
        
        """
        'by_datetime' method search KOA data by 'datetime' range
        
        Required Inputs:
        ---------------    
        instruments: e.g. HIRES, NIRC2, etc...

        time: a datetime string in the format of datetime1/datetime2 where 
            datetime format of is 'yyyy-mm-dd hh:mm:ss.ss'

    
        e.g. 
            instrument = 'hires',
            datetime = '2018-03-16 06:10:55.00/2018-03-18 00:00:00.00' 

        Optional inputs:
	----------------
        outpath: the full output filepath of the returned metadata table
        
	format:  Output format: votable, ascii.ipac, etc.. 
	         (default: ipac)
        
	maxrec:  maximum records to be returned 
	         default: '0'
        """
   
        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(datetime) == 0):
            print ('Failed to find required parameter: datetime')
            return

        self.instrument = instrument
        self.datetime = datetime

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter by_datetime:')
            self.logger.debug (f'instrument= {self.instrument:s}')
            self.logger.debug (f'datetime= {self.datetime:s}')

        self.outpath = '' 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'maxrec= {self.maxrec:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        
        param['instrument'] = self.instrument
        param['datetime'] = self.datetime
       
#        retstr = self.by_paramdict (param, **kwargs)
        
        self.by_paramdict (param, **kwargs)

        return



    def by_position (self, instrument, pos, **kwargs):
        
        """
        'by_position' method search KOA data by 'position' 
        
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

        Optional Input:
        ---------------    

        Output format: votable, ipac, csv, etc.. 
	(default: ipac)
	
	maxrec:  maximum records to be returned 
	         default: '0'
        """
   
        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(pos) == 0):
            print ('Failed to find required parameter: time')
            return

        self.instrument = instrument
        self.pos = pos
 
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter by_datetime:')
            self.logger.debug (f'instrument=  {self.instrument:s}')
            self.logger.debug (f'pos=  {self.pos:s}')
       
        self.outpath = '' 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'maxrec= {self.maxrec:s}')

#
#    send url to server to construct the select statement
#
#
#    send url to server to construct the select statement
#
        param = dict()
        
        param['instrument'] = self.instrument
        param['pos'] = self.pos

#        retstr = self.by_paramdict (param, **kwargs)
        
        self.by_paramdict (param, **kwargs)

        return


    def by_object_name (self, instrument, object, **kwargs):
        
        """
        'by_object_name' method search KOA data by 'object name' 
        
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

        radius = 1.0 (deg)

        Output format: votable, ascii.ipac, etc.. 
	               (default: ipac)
	
	maxrec:  maximum records to be returned 
	         default: 0
        """
   
        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(object) == 0):
            print ('Failed to find required parameter: object')
            return

        self.instrument = instrument
        self.object = object

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter by_object_name:')

        self.outpath = '' 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')
            self.logger.debug (f'instrument= {self.instrument:s}')
            self.logger.debug (f'object= {self.object:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radiusi_str = kwargs.get('radius')
            radius = float(radius_str)

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'format= {self.format:s}')

        self.maxrec = '0'
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'format= {self.format:s}')
            self.logger.debug (f'maxrec= {self.maxrec:s}')
            self.logger.debug (f'radius= {radius:f}')

        coords = None
	
        try:
            coords = name_resolve.get_icrs_coordinates (object)
        except Exception as e:

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'name_resolve error: {str(e):s}')
            
            print (str(e))
            return

        ra = coords.ra.value
        dec = coords.dec.value

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'ra= {ra:f}')
            self.logger.debug (f'dec= {dec:f}')
        
        self.pos = 'circle ' + str(ra) + ' ' + str(dec) \
            + ' ' + str(radius)
	
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'pos= {self.pos:s}')
       
        print (f'object name resolved: ra= {ra:f}, dec={dec:f}')
 
#
#    send url to server to construct the select statement
#
        param = dict()
        
        param['instrument'] = self.instrument
        param['pos'] = self.pos

#        retstr = self.by_paramdict (param, **kwargs)

        self.by_paramdict (param, **kwargs)

        return

    
    def by_paramdict (self, param, **kwargs):
        
        """
        'by_paramdict' method search KOA data by the parameters in the
	intput data dictionary (param).  Here is the entire list of acceptable 
	parameters:

            instruments (required): e.g. HIRES, NIRC2, etc...

            datetime: a datetime string in the format of datetime1/datetime2 
	        where datetime format of is 'yyyy-mm-dd hh:mm:ss'
             
            e.g. 
                instrument = 'hires',
                time = '2018-03-16 06:10:55/2018-03-18 00:00:00' 

            pos: a position string in the format of 
	
	        1.  circle ra dec radius;
	
	        2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
	
	        3.  box ra dec width height;
	
	        all in ra dec in J2000 coordinate.
             
            e.g. 
                instrument = 'hires',
                pos = 'circle 230.0 45.0 0.5'

	    target: target name used in the project, this will be searched 
	        against the database.

	    dptype:  spec/image
        

        Optional parameters:
        
            outpath: file name for the returned metadata table 
	    format: votable, ipac, etc.. (default: votable)
	    maxrec: max number of output records
        """

#
#    send url to server to construct the select statement
#
        param['debug'] = '' 

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter by_paramdict')
            self.logger.debug (param)

        len_param = len(param)

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'len_param= {len_param:d}')

            for k,v in param.items():
                self.logger.debug (f'k, v= {k:s}, {v:s}')


        self.outpath = '' 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')

        self.format ='ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')
            param['maxrec'] = self.maxrec

        if self.debug:
            self.logger.debug ('')
            for k,v in param.items():
                self.logger.debug (f'k, v= {k:s}, {v:s}')

        data = urllib.parse.urlencode (param)

        url = self.query_url + data            

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'url= {url:s}')

        query = ''
        try:
            query = self.__make_query (url) 

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('returned __make_query')
  
        except Exception as e:

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'Error: {str(e):s}')
            
            print (str(e))
            return ('') 
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'query= {query:s}')
       
        self.query = query

#
#    send tap query
#
        self.tap = None
        if (len(self.cookiepath) > 0):

            self.tap = koatap.KoaTap (self.tap_url, \
                format=self.format, \
                maxrec=self.maxrec, \
                cookiefile=self.cookiepath, \
                loggername=self.loggername)

        else:    
            self.tap = koatap.KoaTap (self.tap_url, \
	        format=self.format, \
		maxrec=self.maxrec, \
		loggername=self.loggername)
        
        if self.debug:
            self.logger.debug('')
            self.logger.debug('koaTap initialized')
            self.logger.debug(f'query= {query:s}')
            self.logger.debug('call self.tap.send_async')

        print ('submitting request...')

        retstr = self.tap.send_async (query, outpath= self.outpath)
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'return self.tap.send_async:')
            self.logger.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if self.debug:
#            self.logger.debug ('')
#            self.logger.debug (f'indx= {indx:d}')

        if (indx >= 0):
            print (retstr)
            sys.exit()

#
#    no error: 
#
        print (retstr)
        return

    
    def by_adql (self, query, **kwargs):
       
        """
        'by_adql' method receives a qualified ADQL query string from
	user input.
        
        Required Inputs:
        ---------------    
            query:  a ADQL query


        Optional inputs:
	----------------
            outpath: the output filename the returned metadata table
        
	    format:  Output format: votable, ipac, csv, tsv, etc.. 
	         (default: ipac)
        
	    maxrec:  maximum records to be returned 
	         default: 0
        """
   
        if (len(query) == 0):
            print ('Failed to find required parameter: query')
            return
        
        self.query = query
 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter by_adql:')
            self.logger.debug (f'query= {self.query:s}')
        
        self.outpath = '' 
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')


        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'format= {self.format:s}')

#
#    send tap query
#
        self.tap = None
        if (len(self.cookiepath) > 0):
            self.tap = koatap.KoaTap (self.tap_url, \
	        format=self.format, \
		maxrec=self.maxrec, \
		cookiefile=self.cookiepath, \
		loggername=self.loggername)
        else:    
            if self.debug:
                self.logger.debug('')
                self.logger.debug('initializing KoaTap')
            
            self.tap = koatap.KoaTap (self.tap_url, \
	        format=self.format, \
		maxrec=self.maxrec, \
		loggername=self.loggername)
        
        if self.debug:
            self.logger.debug('')
            self.logger.debug('koaTap initialized')
            self.logger.debug(f'query= {query:s}')
            self.logger.debug('call self.tap.send_async')

        print ('submitting request...')

        if (len(self.outpath) > 0):
            retstr = self.tap.send_async (query, outpath=self.outpath)
        else:
            retstr = self.tap.send_async (query)
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'return self.tap.send_async:')
            self.logger.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if self.debug:
#            self.logger.debug ('')
#            self.logger.debug (f'indx= {indx:d}')

        if (indx >= 0):
            print (retstr)
            sys.exit()

#
#    no error: 
#
        print (retstr)
        return


    def save_data_to_file (self, filepath):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter save_data_to_file:')
            self.logger.debug (f'filepath= {filepath:s}')


        retstr = self.tap.get_data (filepath)
   
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'retstr= {retstr:s}')
        
        print (retstr)

        return

    
    def print_data (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter archive.print_data:')

        try:
            self.tap.print_data ()
        except Exception as e:
                
            msg = 'Error print data: ' + str(e)
            print (msg)
        
        return



    def __make_query (self, url):
       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter __make_query:')
            self.logger.debug (f'url= {url:s}')

        response = None
        try:
            response = requests.get (url, stream=True)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('request sent')

        except Exception as e:
           
            msg = 'Error: ' + str(e)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)


        content_type = response.headers['content-type']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'content_type= {content_type:s}')
       
        if (content_type == 'application/json'):
                
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'response.text: {response.text:s}')

#
#    error message
#
            try:
                jsondata = json.loads (response.text)
                 
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('jsondata loaded')
                
                status = jsondata['status']
                msg = jsondata['msg']
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'status: {status:s}')
                    self.logger.debug (f'msg: {msg:s}')

            except Exception:
                msg = 'returned JSON object parse error'
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('JSON object parse error')
      
                
            raise Exception (msg)
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'msg= {msg:s}')
     
        return (response.text)

   


