import os
import sys
import io
import logging
import initLogger

import json
import xmltodict 
import bs4 as bs

import requests
import urllib 
import http.cookiejar


class KoaJob:

    """
    KoaJob class is used internally by KoaTap class to store the job 
    parameters and returned urls for job status and result files.  
    """

    def __init__ (self, statusurl, **kwargs):

        self.debug = 0 
        self.loggername = '' 
        self.logger = None 
        
        self.statusurl = statusurl

        self.status = ''
        self.msg = ''
        
        self.statusstruct = ''
        self.job = ''


        self.jobid = ''
        self.processid = ''
        self.ownerid = 'None'
        self.quote = 'None'
        self.phase = ''
        self.starttime = ''
        self.endtime = ''
        self.executionduration = ''
        self.destruction = ''
        self.errorsummary = ''
        
        self.parameters = ''
        self.resulturl = ''

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
            self.logger.debug ('Enter koajob (debug on)')
                                
        try:
            self.__get_statusjob()
         
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('returned __get_statusjob')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('done KoaJob.init:')

        return     

    
   
    def get_status (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_status')
            self.logger.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.statusstruct)


    def get_resulturl (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_resulturl')
            self.logger.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.resulturl)



    def get_result (self, outpath):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_result')
            self.logger.debug (f'resulturl= {self.resulturl:s}')
            self.logger.debug (f'outpath= {outpath:s}')

        if (len(outpath) == 0):
            self.status = 'error'
            self.msg = 'Output file path is required.'
            return

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned __get_statusjob')
                    self.logger.debug (f'resulturl= {self.resulturl:s}')

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                
                raise Exception (self.msg)    
    

        if (len(self.resulturl) == 0):
  
            self.get_resulturl()            
            self.msg = 'Failed to retrieve resulturl from status structure.'
            raise Exception (self.msg)    
	    

#
#   send resulturl to retrieve result table
#
        try:
            response = requests.get (self.resulturl, stream=True)
        
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
# retrieve table from response
#
        with open (outpath, "wb") as fp:
            
            for data in response.iter_content(4096):
                
                len_data = len(data)            
            
#                if debug:
#                    self.logger.debug ('')
#                    self.logger.debug (f'len_data= {len_data:d}')
 
                if (len_data < 1):
                    break

                fp.write (data)
        fp.close()
        
        self.resultpath = outpath
        self.status = 'ok'
        self.msg = 'returned table written to output file: ' + outpath
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('done writing result to file')
            
        return        


    def get_parameters (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_parameters')
            self.logger.debug ('parameters:')
            self.logger.debug (self.parameters)

        return (self.parameters)


    def get_phase (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_phase')
            self.logger.debug (f'self.phase= {self.phase:s}')

        if ((self.phase.lower() != 'completed') and \
	    (self.phase.lower() != 'error')):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'phase= {self.phase:s}')

        return (self.phase)
    
    
    
    def get_jobid (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_jobid')

        if (len(self.jobid) == 0):
            self.jobid = self.job['uws:jobId']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'jobid= {self.jobid:s}')

        return (self.jobid)
    
    
    def get_processid (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_processid')
#            self.logger.debug (f'processid= {self.processid:s}')

        if (len(self.processid) == 0):
            self.processid = self.job['uws:processId']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'processid= {self.processid:s}')

        return (self.processid)
    
    
    def get_ownerid (self):
        return ('None')

    def get_quote (self):
        return ('None')


    def get_starttime (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_starttime')
#            self.logger.debug (f'starttime= {self.starttime:s}')

        if (len(self.starttime) == 0):
            self.starttime = self.job['uws:startTime']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'starttime= {self.starttime:s}')

        return (self.starttime)
    

    def get_endtime (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_endtime')
#            self.logger.debug (f'endtime= {self.endtime:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.endtime = self.job['uws:endTime']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'endtime= {self.endtime:s}')

        return (self.endtime)
    


    def get_executionduration (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_executionduration')
#            self.logger.debug (f'executionduration= {self.executionduration:s}')

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.executionduration = self.job['uws:executionDuration']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'executionduration= {self.executionduration:s}')

        return (self.executionduration)


    def get_destruction (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_destruction')
#            self.logger.debug (f'destruction= {self.destruction:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.destruction = self.job['uws:destruction']

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'destruction= {self.destruction:s}')

        return (self.destruction)
    
   
    def get_errorsummary (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter get_errorsummary')
#            self.logger.debug (f'errorsummary= {self.errorsummary:s}')

        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            try:
                self.__get_statusjob ()

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned get_statusjob:')
                    self.logger.debug ('job= ')
                    self.logger.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   
	
        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            self.msg = 'The process is still running.'
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'msg= {self.msg:s}')

            return (self.msg)
	
        elif (self.phase.lower() == 'completed'):
            
            self.msg = 'Process completed without error message.'
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'msg= {self.msg:s}')

            return (self.msg)
        
        elif (self.phase.lower() == 'error'):

            self.errorsummary = self.job['uws:errorSummary']['uws:message']

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'errorsummary= {self.errorsummary:s}')

            return (self.errorsummary)
    
    
    def __get_statusjob (self):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter __get_statusjob')
            self.logger.debug (f'statusurl= {self.statusurl:s}')

#
#   self.status doesn't exist, call get_status
#
        datadict = dict()
        datadict['debug'] = '/home/mihseh/tap/python/koatap.status.debug' 

        try:
#            self.response = requests.get (self.statusurl, stream=True)
            
            self.response = requests.get (self.statusurl, data=datadict, \
	        stream=True)
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('statusurl request sent')

        except Exception as e:
           
           
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('response returned')
            self.logger.debug (f'status_code= {self.response.status_code:d}')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('response.text= ')
            self.logger.debug (self.response.text)
        
        self.statusstruct = self.response.text

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('statusstruct= ')
            self.logger.debug (self.statusstruct)
        
#
#    parse returned status xml structure for parameters
#
        soup = bs.BeautifulSoup (self.statusstruct, 'lxml')
            
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('soup initialized')
        
        self.parameters = soup.find('uws:parameters')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('self.parameters:')
            self.logger.debug (self.parameters)
        
        
#
#    convert status xml structure to dictionary doc 
#
        doc = xmltodict.parse (self.response.text)
        self.job = doc['uws:job']

        self.phase = self.job['uws:phase']
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.phase.lower():{ self.phase.lower():s}')
        
       
        if (self.phase.lower() == 'completed'):

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('xxx1: got here')
            
            results = self.job['uws:results']
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('results')
                self.logger.debug (results)
            
            result = self.job['uws:results']['uws:result']
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('result')
                self.logger.debug (result)
            

            self.resulturl = \
                self.job['uws:results']['uws:result']['@xlink:href']
        
        elif (self.phase.lower() == 'error'):
            self.errorsummary = self.job['uws:errorSummary']['uws:message']


        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('self.job:')
            self.logger.debug (self.job)
            self.logger.debug (f'self.phase.lower(): {self.phase.lower():s}')
            self.logger.debug (f'self.resulturl: {self.resulturl:s}')

        return

