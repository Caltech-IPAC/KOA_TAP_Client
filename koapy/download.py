import os
import sys
import logging
import initLogger

import json
import ijson

import requests
import urllib
import http.cookiejar

import createDir
from astropy.table import Table, Column

class Download:

    """
    The Download class provides methods for users to download FITS files 
    in their metadata file.
    """    

    cookiepath = ''
    userid = ''
    workspace = ''
    cookiestr = ''
    
    target = ''
    cmd = ''
   
    sql = ''
    format = ''
    filepath = ''
    disp = 1 
    
    content_type = ''
    outdir = ''

    astropytbl = None

    status = ''
    msg = ''
    
    debug = 0    
    debugfname = ''
    debugfname = './dnload.debug'
    loggername = 'dnload'    
        
    def __init__ (self, metapath, format, **kwargs):

        """
	Input:
	-----
        
	metapath: a full path metadata table obtained from running
	              archive.py
        
	format: metasata table's table format: ascii.ipac, votable, etc..
	
        
	Optional input:
        ----------------
        cookiepath (string): 
	
	a full cookie file path saved while running auth.Login.
        """
        
        self.logger = None  
        self.handler = None 
        
        self.debug = 0    
        self.debugfname = ''
        
        if (len(metapath) == 0):
            print ('Failed to find required input parameter: metapath')
            return

        if (len(format) == 0):
            print ('Failed to find required input parameter: format')
            return

        self.metapath = metapath
        self.format = format

        self.cookiepath = ''
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
            self.logger.debug ('Enter Download.init:')
            self.logger.debug (f'metapath= {self.metapath:s}')
            self.logger.debug (f'format= {self.format:s}')
            self.logger.debug (f'cookiepath= {self.cookiepath:s}')


        self.cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)
    
        if (len(self.cookiepath) > 0):
   
            try: 
                self.cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                if self.debug:
                    self.logger.debug (\
                        f'cookie loaded from file: {self.cookiepath:s}')
        
                for cookie in self.cookiejar:
                    
                    if self.debug:
                        self.logger.debug ('')
                        self.logger.debug ('cookie=')
                        self.logger.debug (cookie)
                        self.logger.debug (f'cookie.name= {cookie.name:s}')
                        self.logger.debug (f'cookie.value= {cookie.value:s}')
                        self.logger.debug (f'cookie.domain= {cookie.domain:s}')
            except:
                pass

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('loadCookie exception')

        
        fmt_astropy = self.format
        if (self.format == 'tsv'):
            fmt_astropy = 'ascii.tab'
        if (self.format == 'csv'):
            fmt_astropy = 'ascii.csv'
        if (self.format == 'ipac'):
            fmt_astropy = 'ascii.ipac'

        self.astrotbl = None
        try:
            self.astrotbl = Table.read (self.metapath, format=fmt_astropy)
        except Exception as e:
            self.msg = 'Error reading metadata table: ' + str(e) 
            print (self.msg)
            sys.exit()

        self.len_tbl = len(self.astrotbl)

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('self.astrotbl read')
            self.logger.debug (f'self.len_tbl= {self.len_tbl:d}')

        self.colnames = self.astrotbl.colnames

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('self.colnames:')
            self.logger.debug (self.colnames)
  
        self.len_col = len(self.colnames)

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.len_col= {self.len_col:d}')


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
            self.logger.debug ('')
            self.logger.debug (f'self.ind_instrume= {self.ind_instrume:d}')
            self.logger.debug (f'self.ind_koaid= {self.ind_koaid:d}')
            self.logger.debug (f'self.ind_filehand= {self.ind_filehand:d}')
       
#        self.koaurl = 'http://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&'
#        self.caliburl = \
#            'http://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-getCaliblist?'

        self.koaurl = 'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/getKOA/nph-getKOA?return_mode=json&'
        self.caliburl = \
            'http://vmkoadev.ipac.caltech.edu:9010/cgi-bin/KoaAPI/nph-getCaliblist?'
       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.koaurl= {self.koaurl:s}')
            self.logger.debug (f'self.caliburl= {self.caliburl:s}')

        print ('Download class initialized')
 	
        return


    def download_data (self, outdir, **kwargs):
    
        """
        download_data method downloads lev0 data:

	Required inputs:
	    outdir:       a directory for the data to be written

	Optional inputs:
	    start_row,
	    end_row
        """
       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter Download.download_data')
            self.logger.debug (f'outdir= {outdir:s}')
            self.logger.debug (f'len_tbl= {self.len_tbl:d}')
     
        if (len(outdir) == 0):
            print ('Failed to find required input parameter: outdir')
            return

        self.outdir = outdir

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'self.outdir= {self.outdir:s}')
    
        if (self.len_tbl == 0):
            print ('There is no data in the metadata table.')
            sys.exit()
 
        srow = 0;
        erow = self.len_tbl

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'srow= {srow:d}')
     
        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')
        
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'erow= {erow:d}')
     
        if (srow < 0):
            srow = 0 
        if (erow > self.len_tbl):
            erow = self.len_tbl 
 
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'srow= {srow:d}')
            self.logger.debug (f'erow= {erow:d}')
     
#
#    check if outdir exists
#
        dirobj = createDir.createDir (self.outdir) 

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('returned createDir: status= {dirobj.status:s}') 

        if (dirobj.status == 'error'):
            
            print (dirobj.errmsg)
            sys.exit()


        instrument = '' 
        koaid = ''
        filehand = ''
        self.ndnloaded = 0
      
        nfile = erow - srow   
        
        print (f'Start downloading {nfile:d} FITS data you requested;')
        print (f'after printing a couple of returned files, please check the outdir you specified for further progress.')
 
        for l in range (srow, erow+1):
       
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('self.astrotbl[l]= ')
                self.logger.debug (self.astrotbl[l])
                self.logger.debug ('instrument= ')
                self.logger.debug (self.astrotbl[l][self.ind_instrume])

            instrument = self.astrotbl[l][self.ind_instrume]
            koaid = self.astrotbl[l][self.ind_koaid]
            filehand = self.astrotbl[l][self.ind_filehand]
	    
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('type(instrument)= ')
                self.logger.debug (type(instrument))
                self.logger.debug (type(instrument) is bytes)
            
            if (type (instrument) is bytes):
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('bytes: decode')

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
                self.logger.debug ('')
                self.logger.debug (f'l= {l:d} koaid= {koaid:s}')
                self.logger.debug (f'filehand= {filehand:s}')
                self.logger.debug (f'instrument= {instrument:s}')

#
#   get lev0 files
#
            url = self.koaurl + 'filehand=' + filehand
                
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'filepath= {filepath:s}')
                self.logger.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'isExist: {isExist:d}: skip')
                     
                continue

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('got here')

            try:
                self.__submit_request (url, filepath)
                self.ndnloaded = self.ndnloaded + 1

                self.msg =  'Returned file written to: ' + filepath   
           
#                if (self.ndnloaded == 500):
#                    print (f'{self.ndnloaded:d} files downloaded....')

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned __submit_request')
                    self.logger.debug (f'self.msg= {self.msg:s}')
            
            except Exception as e:
                print (f'File [{koaid:s}] download error: {str(e):s}')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'{self.len_tbl:d} files in the table;')
            self.logger.debug (f'{self.ndnloaded:d} files downloaded.')

        print (f'A total of {self.ndnloaded:d} files downloaded.')

        return


    def download_calib (self, outdir, **kwargs):
    
        """
        download_calib method downloads calibration data
	
	Required inputs:
	    outdir:       a directory for the data to be written
	
	Optional inputs:
	    start_row,
	    end_row
        """
       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'Enter Download.download_calib')
            self.logger.debug (f'outdir= {outdir:s}')
     
        if (len(outdir) == 0):
            print ('Failed to find required input parameter: outdir')
            return

        self.outdir = outdir

        srow = 0;
        erow = self.len_tbl

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')

        if (srow < 0):
            sros = 0 
        if (erow > self.len_tbl):
            eros = self.len_tbl 

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'srow= {srow:d}')
            self.logger.debug (f'erow= {erow:d}')

#
#    check if outdir exists
#
        dirobj = createDir.createDir (self.outdir) 

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'returned createDir: status= {dirobj.status:s}')

        if (dirobj.status == 'error'):
            
            print (dirobj.errmsg)
            sys.exit()

#
#    first retrieve calibration list associated each koaid from server
#
        instrument = '' 
        koaid = ''
        filehand = ''
        ncaliblist = 0
        ndnloaded_calib = 0
        caliblist = []
        
        url = ''
        
        print ('Start downloading Calibration data....')
 
        for l in range (srow, erow+1):
        
            if (self.format == 'votable'):
                instrument = self.astrotbl[l][self.ind_instrume].decode("utf-8")
                koaid = self.astrotbl[l][self.ind_koaid].decode("utf-8")
            else:         
                instrument = self.astrotbl[l][self.ind_instrume]
                koaid = self.astrotbl[l][self.ind_koaid]
           
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'l= {l:d}')
                self.logger.debug (f'instrument= {instrument:s}')
                self.logger.debug (f'koaid= {koaid:s}')

            ind = -1
            ind = instrument.find ('HIRES')
            if (ind >= 0):
                instrument = 'HIRES'
	        
            ind = -1
            ind = instrument.find ('LRIS')
            if (ind >= 0):
                instrument = 'LRIS'
  
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'instrument= {instrument:s}')
                self.logger.debug (f'l= {l:d} koaid= {koaid:s}')

            koaid_base = '' 
            ind = -1
            ind = koaid.rfind ('.')
            if (ind > 0):
                koaid_base = koaid[0:ind]
            else:
                koaid_base = koaid

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'koaid_base= {koaid_base:s}')
	    
            url = self.caliburl \
                + 'instrument=' + instrument \
                + '&koaid=' + koaid

#
#    Returned KOA's caliblist is in json format
#
            filepath = self.outdir + '/' + koaid_base + '.caliblist.json'
                
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'filepath= {filepath:s}')
                self.logger.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'isExist: {isExist:d}: skip')
                     
            else:
                try:
                    self.__submit_request (url, filepath)
                
                    if self.debug:
                        self.logger.debug ('')
                        self.logger.debug ('returned __submit_request')
            
                    ncaliblist = ncaliblist + 1
                    caliblist.append(filepath)

                    self.msg = 'caliblist [' + filepath + '] downloaded.'
                
                    if self.debug:
                        self.logger.debug ('')
                        self.logger.debug (f'self.msg= {self.msg:s}')
                    
                
                except Exception as e:
                    
                    self.msg = 'Caliblist of [' + koaid + \
                        '] download error: ' +  str(e)
                    
                    print (f'self.msg= {self.msg:s}')
                    continue

#
#    if caliblist exits, download calibfiles
#
            try:
                ncalibs = self.__download_calibfiles (filepath)
                ndnloaded_calib = ndnloaded_calib + ncalibs
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned __download_calibfiles')
                    self.logger.debug (f'{ncalibs:d} downloaded')

#                if (self.ndnloaded_calib == 500):
#                    print (f'{self.ndnloaded_calib:d} calibration files downloaded....')

            except Exception as e:
                
                self.msg = 'Error downloading files in caliblist [' + \
                    filepath + ']: ' +  str(e)
                    
                continue

        print (f'A total of {self.ndnloaded_calib:d} calibration files downloaded.')
        return


    def __download_calibfiles (self, listpath):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'Enter __download_calibfiles: {listpath:s}')

#
#    read input caliblist JSON file
#
        self.nrec = 0
        self.ndnloaded = 0
        self.nerr = 0
 
        data = ''
        try:
            with open (listpath) as fp:
	    
                jsonData = json.load (fp) 
                data = jsonData["table"]

            fp.close() 

        except Exception as e:
        
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'caliblist: {caliblist:s} load error')

            self.errmsg = 'Failed to read ' + listpath	
	
            fp.close() 
            
            raise Exception (self.errmsg)

            return

        nrec = len(data)
    
        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'downloadCalibfiles: nrec= {nrec:d}')

        if (nrec == 0):

            self.status = 'error'	
            self.errmsg = 'No data found in the caliblist: ' + listpath
	    
            raise Exception (self.errmsg)


#
#    retrieve koaid from caliblist json structure and download files
#

#        if (nrec > 5):
#            nrec = 5

        ndnloaded = 0
        for ind in range (0, nrec):

            if self.debug:
                self.logger.debug (f'downloadCalibfiles: ind= {ind:d}')

            koaid = data[ind]['koaid']
            instrument = data[ind]['instrument']
            filehand = data[ind]['filehand']
            
            if self.debug:
                self.logger.debug (f'instrument= {instrument:s}')
                self.logger.debug (f'koaid= {koaid:s}')
                self.logger.debug (f'filehand= {filehand:s}')

#
#   get lev0 files
#
            url = self.koaurl + 'filehand=' + filehand
                
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'filepath= {filepath:s}')
                self.logger.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'isExist: {isExist:d}: skip')
                     
                continue

            try:
                self.__submit_request (url, filepath)
                ndnloaded = ndnloaded + 1
                
                self.msg = 'calib file [' + filepath + '] downloaded.'
                print (self.msg)

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug ('returned __submit_request')
                    self.logger.debug (f'self.msg: {self.msg:s}')
            
            except Exception as e:
                
                print (f'calib file download error: {str(e):s}')
#                raise Exception (str(e))

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'{self.ndnloaded:d} files downloaded.')

        return (ndnloaded)


    def __submit_request(self, url, filepath):

        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('Enter database.__submit_request:')
            self.logger.debug (f'url= {url:s}')
            self.logger.debug (f'filepath= {filepath:s}')
        
        try:
            self.response =  requests.get (url, cookies=self.cookiejar, \
                stream=True)

            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('request sent')
        
        except Exception as e:
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to submit the request: ' + str(e)
	    
            raise Exception (self.msg)
            return
                       
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('status_code:')
            self.logger.debug (self.response.status_code)
      
      
        if (self.response.status_code == 200):
            self.status = 'ok'
            self.msg = ''
        else:
            self.status = 'error'
            self.msg = 'Failed to submit the request'
	    
            raise Exception (self.msg)
            return
                       
            
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('headers: ')
            self.logger.debug (self.response.headers)
      
      
        self.content_type = ''
        try:
            self.content_type = self.response.headers['Content-type']
        except Exception as e:

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception extract content-type: {str(e):s}')

        if self.debug:
            self.logger.debug ('')
            self.logger.debug (f'content_type= {self.content_type:s}')


        if (self.content_type == 'application/json'):
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (\
                    'return is a json structure: might be error message')
            
            jsondata = json.loads (self.response.text)
          
            if self.debug:
                self.logger.debug ('')
                self.logger.debug ('jsondata:')
                self.logger.debug (jsondata)

 
            self.status = ''
            try: 
                self.status = jsondata['status']
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'self.status= {self.status:s}')

            except Exception as e:

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'get status exception: e= {str(e):s}')

            self.msg = '' 
            try: 
                self.msg = jsondata['msg']
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'self.msg= {self.msg:s}')

            except Exception as e:

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'extract msg exception: e= {str(e):s}')

            errmsg = ''        
            try: 
                errmsg = jsondata['error']
                
                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'errmsg= {errmsg:s}')

                if (len(errmsg) > 0):
                    self.status = 'error'
                    self.msg = errmsg

            except Exception as e:

                if self.debug:
                    self.logger.debug ('')
                    self.logger.debug (f'get error exception: e= {str(e):s}')


            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'self.status= {self.status:s}')
                self.logger.debug (f'self.msg= {self.msg:s}')


            if (self.status == 'error'):
                raise Exception (self.msg)
                return

#
#    save to filepath
#
        if self.debug:
            self.logger.debug ('')
            self.logger.debug ('save_to_file:')
       
        try:
            with open (filepath, 'wb') as fd:

                for chunk in self.response.iter_content (chunk_size=1024):
                    fd.write (chunk)
            
            self.msg =  'Returned file written to: ' + filepath   
#            print (self.msg)
            
            if self.debug:
                self.logger.debug ('')
                self.logger.debug (self.msg)
	
        except Exception as e:

            if self.debug:
                self.logger.debug ('')
                self.logger.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to save returned data to file: %s' % filepath
            
            raise Exception (self.msg)
            return

        return
                       
