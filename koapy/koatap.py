import os
import sys
import io
import logging
import initLogger
import time

import json
import xmltodict 
import tempfile

import requests
import urllib 
import http.cookiejar

from astropy.table import Table,Column

import koajob

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
        self.loggername = ''
        self.logger = None
 
        self.datadict = dict()
        
        self.status = ''
        self.msg = ''

#
#    koajob contains async job's status;
#    resulttbl is the result of sync saved an astropy table 
#
        self.koajob = None
        self.astropytbl = None
        
        if ('loggername' in kwargs):

            self.loggername = kwargs.get('loggername') 
           
            if (len(self.loggername) == 0):
                self.debug = 0
            else:
                self.logger = logging.getLogger (self.loggername)
                self.handler = self.logger.handlers

                if (len(self.handler) > 0):
                    self.debug = 1
                else:
                    self.debug = 0

 
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter koatap (debug on)')
                                
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

        self.maxrec = '5000'
        if ('maxrec' in kwargs):
           self.maxrec = kwargs.get('maxrec')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter KoaTap.init:')
            self.logger.debug (f'url= {self.url:s}')

#
#    turn on server debug
#   
        pid = os.getpid()
#        debugfname = '/tmp/koatap.server_' + str(pid) + '.debug' 
#        debugfname = '/home/mihseh/tap/server/koatap.server.debug' 
        
#        if self.debug:
#            self.logger.debug ('')
#            self.logger.debug (f'debugfname= {debugfname:s}')
   
#        self.datadict['debug'] = debugfname
        self.datadict['request'] = self.request              
        self.datadict['lang'] = self.lang              
        self.datadict['phase'] = self.phase              
        self.datadict['format'] = self.format              
        self.datadict['maxrec'] = self.maxrec              

        for key in self.datadict:

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'key= {key:s} val= {self.datadict[key]:s}')
    
        
        self.cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)
         
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('cookiejar')
            self.logger.debug (self.cookiejar)
   
        if (len(self.cookiepath) > 0):
        
            try:
                self.cookiejar.load (ignore_discard=True, ignore_expires=True);
            
                if self.debug:
                    self.logger.debug (
                        'cookie loaded from %s' % self.cookiepath)
        
                    for cookie in self.cookiejar:
                        self.logger.debug ('cookie:')
                        self.logger.debug (cookie)
                        
                        self.logger.debug (f'cookie.name= {cookie.name:s}')
                        self.logger.debug (f'cookie.value= {cookie.value:s}')
                        self.logger.debug (f'cookie.domain= {cookie.domain:s}')
            except:
                if self.debug:
                    self.logger.debug ('KoaTap: loadCookie exception')
 
                self.msg = 'Error: failed to load cookie file.'
                raise Exception (self.msg) 

        return 
       

    def send_async (self, query, **kwargs):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter send_async:')
 
        self.async_job = 1
        self.sync_job = 0

        url = self.url + '/async'

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'url= {url:s}')
            self.logger.debug (f'query= {query:s}')

        self.datadict['query'] = query 

#
#    for async query, there is no maxrec limit
#
        self.maxrec = '0'

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'maxrec= {self.maxrec:s}')
        
        self.oupath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
  
        try:

            if (len(self.cookiepath) > 0):
        
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('xxx1')

                self.response = requests.post (url, data= self.datadict, \
	            cookies=self.cookiejar, allow_redirects=False)
            else: 
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('xxx2')

                self.response = requests.post (url, data= self.datadict, \
	            allow_redirects=False)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

     
        self.statusurl = ''

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'status_code= {self.response.status_code:d}')
            self.logger.debug ('self.response: ')
            self.logger.debug (self.response)
            self.logger.debug ('self.response.headers: ')
            self.logger.debug (self.response.headers)
            
            
#        print (f'status_code= {self.response.status_code:d}')
           
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'status_code= {self.response.status_code:d}')
            
#
#    if status_code != 303: probably error message
#
        if (self.response.status_code != 303):
            
            if debug:
                self.logger.debug ('')
                self.logger.debug ('case: not re-direct')
       
            self.content_type = self.response.headers['Content-type']
            self.encoding = self.response.encoding
        
            if debug:
                self.logger.debug ('')
                self.logger.debug (f'content_type= {self.content_type:s}')
                self.logger.debug (f'encoding= {self.encoding:s}')

            data = None
            self.status = ''
            self.msg = ''
           
            if (self.content_type == 'application/json'):
#
#    error message
#
                try:
                    data = response.json()
                    
                except Exception as e:
                
                    if debug:
                        self.logger.debug ('')
                        self.logger.debug (f'JSON object parse error: {str(e):s}')
      
                    self.status = 'error'
                    self.msg = 'JSON parse error: ' + str(e)
                
                    if debug:
                        self.logger.debug ('')
                        self.logger.debug (f'status= {self.status:s}')
                        self.logger.debug (f'msg= {self.msg:s}')

                    return (self.msg)

                self.status = data['status']
                self.msg = data['msg']
                
                if debug:
                    self.logger.debug ('')
                    self.logger.debug (f'status= {self.status:s}')
                    self.logger.debug (f'msg= {self.msg:s}')

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
            self.logger.debug ('')
            self.logger.debug (f'statusurl= {self.statusurl:s}')

        if (len(self.statusurl) == 0):
            self.msg = 'Error: failed to retrieve statusurl from re-direct'
            return (self.msg)

#
#    create koajob to save status result
#
        try:
            self.koajob = koajob.KoaJob (\
                self.statusurl, loggername=self.loggername)
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'koajob initialized')
                self.logger.debug (f'phase= {self.koajob.phase:s}')
       
       
        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
#
#    loop until job is complete and download the data
#
        
        phase = self.koajob.phase
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'phase: {phase:s}')
            
        if ((phase.lower() != 'completed') and (phase.lower() != 'error')):
            
            while ((phase.lower() != 'completed') and \
                (phase.lower() != 'error')):
                
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('here0-1')
                    self.logger.debug (f'phase= {phase:s}')
            
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('here0-2')
            self.logger.debug (f'phase= {phase:s}')
            
#
#    phase == 'error'
#
        if (phase.lower() == 'error'):
	   
            self.status = 'error'
            self.msg = self.koajob.errorsummary
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'returned get_errorsummary: {self.msg:s}')
            
            return (self.msg)

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('here2: phase is completed')
            
#
#   phase == 'completed' 
#
        self.resulturl = self.koajob.resulturl
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'resulturl= {self.resulturl:s}')

#
#   send resulturl to retrieve result table
#
        try:
            self.response_result = requests.get (self.resulturl, stream=True)
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
       
#
# save table to file
#
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)


#
#    outpath is not given: return resulturl
#
        """
        if (len(self.outpath) == 0):
           
            self.resulturl = self.koajob.resulturl
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'resulturl= {self.resulturl:s}')

            return (self.resulturl)

        try:
            self.koajob.get_result (self.outpath)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'returned self.koajob.get_result')
        
        except Exception as e:
            
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('got here: download result successful')
      
        self.status = 'ok'
        self.msg = 'Result downloaded to file: [' + self.outpath + ']'
	    
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.msg = {self.msg:s}')
       
        
	self.msg = self.save_data (self.outpath)
            
	
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'returned save_data: msg= {self.msg:s}')


        return (self.msg) 
        """


    def send_sync (self, query, **kwargs):
       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter send_sync:')
            self.logger.debug (f'query= {query:s}')
 
        url = self.url + '/sync'

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'url= {url:s}')

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
                self.logger.debug ('')
                self.logger.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'maxrec= {self.maxrec:s}')
        
        self.outpath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'outpath= {self.outpath:s}')
	
        try:
            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
                    cookies=self.cookiejar, allow_redirects=False, stream=True)
            else: 
                self.response = requesrs.post (url, data= self.datadict, \
                    allow_redicts=False, stream=True)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

#
#    re-direct case not implemented for send_sync
#
#	if (self.response.status_code == 303):
#            self.resulturl = self.response.headers['Location']
        
        self.content_type = self.response.headers['Content-type']
        self.encoding = self.response.encoding

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'content_type= {self.content_type:s}')
       
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
                    self.logger.debug ('')
                    self.logger.debug ('JSON object parse error')
      
                self.status = 'error'
                self.msg = 'Error: returned JSON object parse error'
                
                return (self.msg)
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'status= {self.status:s}')
                self.logger.debug (f'msg= {self.msg:s}')
     
#
# download resulturl and save table to file
#
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('send request to get resulturl')





#
# save table to file
#
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)


#
# save data to astropy table
#
    def save_data (self, outpath):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter save_data:')
            self.logger.debug (f'outpath= {outpath:s}')
      
        tmpfile_created = 0

        fpath = ''
        if (len(outpath) >  0):
            fpath = outpath
        else:
            fd, fpath = tempfile.mkstemp(suffix='.xml', dir='./')
            tmpfile_created = 1 
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'tmpfile_created = {tmpfile_created:d}')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'fpath= {fpath:s}')
     
        fp = open (fpath, "wb")
            
        for data in self.response_result.iter_content(4096):
                
            len_data = len(data)            
        
            if (len_data < 1):
                break

            fp.write (data)
        
        fp.close()

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'data written to file: {fpath:s}')
                
        if (len(self.outpath) >  0):
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'xxx1')
                
            self.msg = 'Result downloaded to file [' + self.outpath + ']'
        else:
#
#    read temp outpath to astropy table
#
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'xxx2')
                
            self.astropytbl = Table.read (fpath, format='votable')	    
            self.msg = 'Result saved in memory (astropy table).'
      
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'{self.msg:s}')
     
        if (tmpfile_created == 1):
            os.remove (fpath)
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('tmpfile {fpath:s} deleted')

        return (self.msg)



    def print_data (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter print_data:')

        try:

            """
            len_table = len (self.astropytbl)
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'len_table= {len_table:d}')
       
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
            self.logger.debug ('')
            self.logger.debug ('Enter get_data:')
            self.logger.debug (f'async_job = {self.async_job:d}')
            self.logger.debug (f'resultpath = {resultpath:s}')



        if (self.async_job == 0):
#
#    sync data is in astropytbl
#
            self.astropytbl.write (resultpath)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('astropytbl written to resultpath')

            self.msg = 'Result written to file: [' + resultpath + ']'
        
        else:
            phase = self.koajob.get_phase()
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'returned koajob.get_phase: phase= {phase:s}')

            while ((phase.lower() != 'completed') and \
	        (phase.lower() != 'error')):
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (\
                        f'returned koajob.get_phase: phase= {phase:s}')

#
#    phase == 'error'
#
            if (phase.lower() == 'error'):
	   
                self.status = 'error'
                self.msg = self.koajob.errorsummary
        
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'returned get_errorsummary: {self.msg:s}')
            
                return (self.msg)

#
#   job completed write table to disk file
#
            try:
                self.koajob.get_result (resultpath)

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'returned koajob.get_result')
        
            except Exception as e:
            
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
            
                return (self.msg)    
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('got here: download result successful')

            self.status = 'ok'
            self.msg = 'Result downloaded to file: [' + resultpath + ']'

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.msg = {self.msg:s}')
       
        return (self.msg) 


