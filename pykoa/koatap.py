import os
import sys
import io
import logging
import time

import json
import xmltodict 
import tempfile

import requests
import urllib 
import http.cookiejar

from astropy.table import Table,Column

from pykoa import koajob

class KoaTap:

    """
    KoaTap class provides client access to KOA's TAP service.   

    Public data doesn't not require user login, optional KOA login via 
    KoaLogin class are used to search a user's proprietary data.

    Calling Synopsis (example):

    import koatap
    
    service = KoaTap (url, cookiefile=cookiepath)

    job = service.send_async (query, format='votable', request='doQuery', ...)

    or
    
    job = service.send_sync (query, format='votable', request='doQuery', ...)

    required parameter:
    
        query -- a SQL statement in specified query language;

    optional paramters:
        
	request    -- default 'doQuery',
	lang       -- default 'ADQL',
	phase      -- default 'RUN',
	format     -- default 'votable',
	maxrec     -- default '2000'
       
        cookiefile -- a full path cookie file containing user info; 
	              default is no cookiefile
	debug      -- default is no debug written
    """

    def __init__ (self, url, **kwargs):

        self.url = url 
        self.cookiename = ''
        self.cookiepath = ''
        self.async_job = 0 
        self.sync_job = 0
        
        self.response = None 
        self.response_result = None 
              
        
        self.outpath = ''
        
        self.debug = 0  
 
        self.datadict = dict()
        
        self.status = ''
        self.msg = ''

#
#    koajob contains async job's status;
#    resulttbl is the result of sync saved an astropy table 
#
        self.koajob = None
        self.astropytbl = None
        
        if ('debug' in kwargs):
            self.debug = kwargs.get('debug') 
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter koatap.init (debug on)')
                                
        if ('cookiefile' in kwargs):
            self.cookiepath = kwargs.get('cookiefile')

        self.request = 'doQuery'
        if ('request' in kwargs):
            self.request = kwargs.get('request')

        self.lang = 'ADQL'
        if ('lang' in kwargs):
            self.lang = kwargs.get('lang')

        self.phase = 'RUN'
        if ('phase' in kwargs):
           self.phase = kwargs.get('phase')

        self.format = 'votable'
        if ('format' in kwargs):
           self.format = kwargs.get('format')

        self.maxrec = '0'
        if ('maxrec' in kwargs):
           self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {self.url:s}')
            logging.debug (f'cookiepath= {self.cookiepath:s}')


#
#    turn on server debug
#   
        pid = os.getpid()
        self.datadict['request'] = self.request              
        self.datadict['lang'] = self.lang              
        self.datadict['phase'] = self.phase              
        self.datadict['format'] = self.format              
        self.datadict['maxrec'] = self.maxrec              

        for key in self.datadict:

            if self.debug:
                logging.debug ('')
                logging.debug (f'key= {key:s} val= {self.datadict[key]:s}')
    
        
        self.cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)
         
        if self.debug:
            logging.debug ('')
            logging.debug ('cookiejar')
            logging.debug (self.cookiejar)
   
        if (len(self.cookiepath) > 0):
        
            try:
                self.cookiejar.load (ignore_discard=True, ignore_expires=True);
            
                if self.debug:
                    logging.debug (
                        'cookie loaded from %s' % self.cookiepath)
        
                    for cookie in self.cookiejar:
                        logging.debug ('cookie:')
                        logging.debug (cookie)
                        
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            except:
                if self.debug:
                    logging.debug ('KoaTap: loadCookie exception')
 
                self.msg = 'Error: failed to load cookie file.'
                raise Exception (self.msg) 

        return 
       

    def send_async (self, query, **kwargs):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter send_async:')
 
        self.async_job = 1
        self.sync_job = 0

        url = self.url + '/async'

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')
            logging.debug (f'query= {query:s}')

        self.datadict['query'] = query 

#
#    for async query, there is no maxrec limit
#
        self.maxrec = '0'

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

        
            if self.debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:s}')
        
        self.oupath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
  
        try:

            if (len(self.cookiepath) > 0):
        
                if self.debug:
                    logging.debug ('')
                    logging.debug ('xxx1')

                self.response = requests.post (url, data= self.datadict, \
	            cookies=self.cookiejar, allow_redirects=False)
            else: 
                if self.debug:
                    logging.debug ('')
                    logging.debug ('xxx2')

                self.response = requests.post (url, data= self.datadict, \
	            allow_redirects=False)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

     
        self.statusurl = ''

        if self.debug:
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            logging.debug ('self.response: ')
            logging.debug (self.response)
            logging.debug ('self.response.headers: ')
            logging.debug (self.response.headers)
            
            
#        print (f'status_code= {self.response.status_code:d}')
           
        if self.debug:
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            
#
#    if status_code != 303: probably error message
#
        if (self.response.status_code != 303):
            
            if self.debug:
                logging.debug ('')
                logging.debug ('case: not re-direct')
       
            self.content_type = self.response.headers['Content-type']
            self.encoding = self.response.encoding
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'content_type= {self.content_type:s}')
                logging.debug ('encoding= ')
                logging.debug (self.encoding)


            data = None
            self.status = ''
            self.msg = ''
           
            if (self.content_type == 'application/json'):
#
#    error message
#
                if self.debug:
                    logging.debug ('')
                    logging.debug ('self.response:')
                    logging.debug (self.response.text)
      
                try:
                    data = self.response.json()
                    
                except Exception as e:
                
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'JSON object parse error: {str(e):s}')
      
                    self.status = 'error'
                    self.msg = 'JSON parse error: ' + str(e)
                
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'status= {self.status:s}')
                        logging.debug (f'msg= {self.msg:s}')

                    return (self.msg)

                self.status = data['status']
                self.msg = data['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'status= {self.status:s}')
                    logging.debug (f'msg= {self.msg:s}')

                if (self.status == 'error'):
                    self.msg = 'Error: ' + data['msg']
                    return (self.msg)

#
#    retrieve statusurl
#
        self.statusurl = ''
        if (self.response.status_code == 303):
            self.statusurl = self.response.headers['Location']

        if self.debug:
            logging.debug ('')
            logging.debug (f'statusurl= {self.statusurl:s}')

        if (len(self.statusurl) == 0):
            self.msg = 'Error: failed to retrieve statusurl from re-direct'
            return (self.msg)

#
#    create koajob to save status result
#
        try:
            if (self.debug):
                self.koajob = koajob.KoaJob (\
                    self.statusurl, debug=1)
            else:
                self.koajob = koajob.KoaJob (\
                    self.statusurl)
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'koajob initialized')
                logging.debug (f'phase= {self.koajob.phase:s}')
       
       
        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
#
#    loop until job is complete and download the data
#
        
        phase = self.koajob.phase
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'phase: {phase:s}')
            
        if ((phase.lower() != 'completed') and (phase.lower() != 'error')):
            
            while ((phase.lower() != 'completed') and \
                (phase.lower() != 'error')):
                
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if self.debug:
                    logging.debug ('')
                    logging.debug ('here0-1')
                    logging.debug (f'phase= {phase:s}')
            
        if self.debug:
            logging.debug ('')
            logging.debug ('here0-2')
            logging.debug (f'phase= {phase:s}')
            
#
#    phase == 'error'
#
        if (phase.lower() == 'error'):
	   
            self.status = 'error'
            self.msg = self.koajob.errorsummary
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
            return (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('here2: phase is completed')
            
#
#   phase == 'completed' 
#
        self.resulturl = self.koajob.resulturl
        if self.debug:
            logging.debug ('')
            logging.debug (f'resulturl= {self.resulturl:s}')

#
#   send resulturl to retrieve result table
#
        try:
            self.response_result = requests.get (self.resulturl, stream=True)
        
            if self.debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
       
#
# save table to file
#
        if self.debug:
            logging.debug ('')
            logging.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if self.debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)


#
#    outpath is not given: return resulturl
#
        """
        if (len(self.outpath) == 0):
           
            self.resulturl = self.koajob.resulturl
            if self.debug:
                logging.debug ('')
                logging.debug (f'resulturl= {self.resulturl:s}')

            return (self.resulturl)

        try:
            self.koajob.get_result (self.outpath)

            if self.debug:
                logging.debug ('')
                logging.debug (f'returned self.koajob.get_result')
        
        except Exception as e:
            
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
        if self.debug:
            logging.debug ('')
            logging.debug ('got here: download result successful')
      
        self.status = 'ok'
        self.msg = 'Result downloaded to file: [' + self.outpath + ']'
	    
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.msg = {self.msg:s}')
       
        
	self.msg = self.save_data (self.outpath)
            
	
        if self.debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')


        return (self.msg) 
        """


    def send_sync (self, query, **kwargs):
       
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter send_sync:')
            logging.debug (f'query= {query:s}')
 
        url = self.url + '/sync'

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        self.sync_job = 1
        self.async_job = 0
        self.datadict['query'] = query
    
#
#    optional parameters: format, maxrec, self.outpath
#
        self.maxrec = '0'

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

        
            if self.debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:s}')
        
        self.outpath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
	
        try:
            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
                    cookies=self.cookiejar, allow_redirects=False, stream=True)
            else: 
                self.response = requesrs.post (url, data= self.datadict, \
                    allow_redicts=False, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

#
#    re-direct case not implemented for send_sync
#
#	if (self.response.status_code == 303):
#            self.resulturl = self.response.headers['Location']
        
        self.content_type = self.response.headers['Content-type']
        self.encoding = self.response.encoding

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')
       
        data = None
        self.status = ''
        self.msg = ''
        if (self.content_type == 'application/json'):
#
#    error message
#
            try:
                data = self.response.json()
            except Exception:
                if self.debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                self.status = 'error'
                self.msg = 'Error: returned JSON object parse error'
                
                return (self.msg)
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'status= {self.status:s}')
                logging.debug (f'msg= {self.msg:s}')
     
#
# download resulturl and save table to file
#
        if self.debug:
            logging.debug ('')
            logging.debug ('send request to get resulturl')





#
# save table to file
#
        if self.debug:
            logging.debug ('')
            logging.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if self.debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)


#
# save data to astropy table
#
    def save_data (self, outpath):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter save_data:')
            logging.debug (f'outpath= {outpath:s}')
      
        tmpfile_created = 0

        fpath = ''
        if (len(outpath) >  0):
            fpath = outpath
        else:
            fd, fpath = tempfile.mkstemp(suffix='.xml', dir='./')
            tmpfile_created = 1 
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'tmpfile_created = {tmpfile_created:d}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'fpath= {fpath:s}')
     
        fp = open (fpath, "wb")
            
        for data in self.response_result.iter_content(4096):
                
            len_data = len(data)            
        
            if (len_data < 1):
                break

            fp.write (data)
        
        fp.close()

        if self.debug:
            logging.debug ('')
            logging.debug (f'data written to file: {fpath:s}')
                
        if (len(self.outpath) >  0):
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx1')
                
            self.msg = 'Result downloaded to file [' + self.outpath + ']'
        else:
#
#    read temp outpath to astropy table
#
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx2')
                
            self.astropytbl = Table.read (fpath, format='votable')	    
            self.msg = 'Result saved in memory (astropy table).'
      
        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.msg:s}')
     
        if (tmpfile_created == 1):
            os.remove (fpath)
            
            if self.debug:
                logging.debug ('')
                logging.debug ('tmpfile {fpath:s} deleted')

        return (self.msg)



    def print_data (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter print_data:')

        try:

            """
            len_table = len (self.astropytbl)
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'len_table= {len_table:d}')
       
            for i in range (0, len_table):
	    
                row = self.astropytbl[i]
                print (row)
            """

            self.astropytbl.pprint()

        except Exception as e:
            
            raise Exception (str(e))

        return


#
#    outpath is given: loop until job is complete and download the data
#
    def get_data (self, resultpath):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_data:')
            logging.debug (f'async_job = {self.async_job:d}')
            logging.debug (f'resultpath = {resultpath:s}')



        if (self.async_job == 0):
#
#    sync data is in astropytbl
#
            self.astropytbl.write (resultpath)

            if self.debug:
                logging.debug ('')
                logging.debug ('astropytbl written to resultpath')

            self.msg = 'Result written to file: [' + resultpath + ']'
        
        else:
            phase = self.koajob.get_phase()
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'returned koajob.get_phase: phase= {phase:s}')

            while ((phase.lower() != 'completed') and \
	        (phase.lower() != 'error')):
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if self.debug:
                    logging.debug ('')
                    logging.debug (\
                        f'returned koajob.get_phase: phase= {phase:s}')

#
#    phase == 'error'
#
            if (phase.lower() == 'error'):
	   
                self.status = 'error'
                self.msg = self.koajob.errorsummary
        
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
                return (self.msg)

#
#   job completed write table to disk file
#
            try:
                self.koajob.get_result (resultpath)

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned koajob.get_result')
        
            except Exception as e:
            
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
            
                return (self.msg)    
        
            if self.debug:
                logging.debug ('')
                logging.debug ('got here: download result successful')

            self.status = 'ok'
            self.msg = 'Result downloaded to file: [' + resultpath + ']'

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.msg = {self.msg:s}')
       
        return (self.msg) 


