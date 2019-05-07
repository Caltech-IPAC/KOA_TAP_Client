import os
import sys
import logging

class initLogger:

    """

    'initLogger' class initializes the debug/error logging to a file.
    
    Input:

    filename:   debug or error file path 

    loggername: debug or error logger name

    level:      'debug' or 'error' 

    """

    filename = ''
    loggername = ''
    level = ''

    def __init__(self, filename, loggername, level):

        formatter = logging.Formatter ('%(message)s')

        self.filename = filename;
        self.loggername = loggername;
        self.level = level;

        self.handler = logging.FileHandler (self.filename)
        self.handler.setFormatter (formatter)
        self.logger = logging.getLogger (self.loggername)

        if (level == 'debug'):
            self.logger.setLevel (logging.DEBUG)
        elif (level == 'error'):
            self.logger.setLevel (logging.ERROR)
    
        self.logger.addHandler (self.handler)
    
        with open (self.filename, 'w') as fdebug:
            pass
        
        return


