import os
import sys
import logging

class createDir:

    """
    createDir class checks if path exists, if not create the directory.

    Required input:

        path:    The directory path

    optional input:
    
        debugfile:  debug filename
    """

    path = ''
    
    status = 'ok'
    errmsg = ''

    debug = 0 
    debugfname = ''

    def __init__ (self, path, **kwargs):

        self.path = path

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
            logging.debug ('Enter createDir')
            logging.debug (f'path= {self.path:s}')

 
        isExist = os.path.exists (self.path)

        if self.debug:
            logging.debug ('')
            logging.debug (f'isExist= {isExist:d}')

        if (isExist):

            if self.debug:
                logging.debug ('')
                logging.debug (f'directory {self.path:s} already exists') 

            self.status = 'ok'
            return


#
#    create path if it doesn't exist: 
#
#    decimal mode work for both python2.7 and python3;
#
#    0755 also works for python 2.7 but not python3
#  
#    convert octal 0775 to decimal: 493 
#
        self.status = 'ok'

        d1 = int ('0775', 8)

        if self.debug:
            logging.debug ('')
            logging.debug (f'd1(0775)= {d1:d}')

        try:
            os.makedirs (self.path, d1)
    
            if self.debug:
                logging.debug ('')
                logging.debug (f'directory: {self.path:s} created')

        except OSError as e:
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'str(e)= {str(e):s}')
	    
            if e.errno == os.errno.EEXIST:
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'{self.path:s} already exists') 

                pass
	    
            else:
                self.status = 'error'
                self.errmsg = 'Create directory [' + self.path + ']: ' \
                    + str(e)
                raise Exception (self.errmsg)
        
        return


