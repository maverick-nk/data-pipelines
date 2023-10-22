import os
from configparser import ConfigParser

class Configuration:
    """
    Class to read and create key value for configurations
    in the config.ini file.

    Access Format: Configuration('./config.ini').<variable_name>

    Note: Specific keys will be given priority over shared variables.
    For instance same key defined in PROD will overwrite SHARED one.
    """
    def __init__(self, conf_file_path:str, env:str='PROD'):
        """
        Initiates the configuration class. 

        Parameters:
            conf_file_path (str): Path to config.ini file.
            env (str): Configuration environments (PROD/DEV/TEST). 
                Default is PROD.
        """
        #Check if file exists
        if not os.path.exists(conf_file_path) or \
            not os.path.isfile(conf_file_path):
            raise FileNotFoundError(f"Check config path: {conf_file_path}")

        self._parser = ConfigParser()
        self._parser.read(conf_file_path)
        
        # Sets shared configurations.
        if 'SHARED' in self._parser:
            self.__set_attributes(self._parser['SHARED'])
            
        if env == 'PROD' and 'PROD' in self._parser:
            self.__set_attributes(self._parser['PROD'])
        elif env == 'DEV' and 'DEV' in self._parser:
            self.__set_attributes(self._parser['DEV'])
        elif env == 'TEST' and 'DEV' in self._parser:
            self.__set_attributes(self._parser['TEST'])

    def __set_attributes(self, configParser:ConfigParser)->None:
        """
        Sets class attributes from configuation parser.

        Parameters:
            configParser (ConfigParser): ConfigParser object.
        """
        for key, value in configParser.items():            
            setattr(self, key, value)


if __name__ == "__main__":
    config = Configuration('./config.ini')