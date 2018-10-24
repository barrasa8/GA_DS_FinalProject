import pandas as pd
import numpy as np
import geoip2.database
import os
import re

class log:

    reader = geoip2.database.Reader('../data/GeoLite2-City_20181009/GeoLite2-City.mmdb')
    log_df =pd.DataFrame()
    
    def ___init__(self):
        print("IN INIT METHOD")
        
    def readLog(self,file):
        self.log_df = pd.read_csv(file
            #,skiprows=[0,1,2,3]
            , comment='#'
            , sep=' ' 
            , usecols=[0,1, 2, 5, 6, 7, 8, 9, 10,11,12,14]
            , na_values='-'
            , names=['date'
                    ,'time'
                    ,'server-ip'
                    ,'cs-uri-query'
                    ,'server-port'
                    ,'cs-username'
                    ,'client-ip'
                    ,'cs(User-Agent)'
                    ,'cs(Referer)'
                    ,'sc-status'
                    ,'sc-substatus'
                   ,'time-taken(ms)'])
        return self.log_df
    
    def getDevice (self,UserAgentResponse):
        device ='Other' 
        if 'Mobi' in UserAgentResponse:
            device = 'Mobile'
        else:
            device = 'Desktop'
        return device
    
    def getBrowser (self,UserAgentResponse):
        browser ='Other' 
        if 'Firefox' in UserAgentResponse and 'Seamonkey' not in UserAgentResponse:
            browser = 'Firefox'
        elif 'Seamonkey' in UserAgentResponse:
            browser = 'Seamonkey'
        elif 'Chrome' in UserAgentResponse and 'Chromium' not in UserAgentResponse:
            browser = 'Chrome'
        elif ('Safari' in UserAgentResponse and 'Chromium' not in UserAgentResponse and 'Chrome' not in UserAgentResponse):
            browser = 'Safari'
        elif 'OPR' in UserAgentResponse and 'Opera'  in UserAgentResponse:
            browser = 'Opera'
        elif '; MSIE' in UserAgentResponse:
            browser = 'IE'
        return browser

    def GetWebPageSection(self,x):
        section = 'Unknown'
        r = re.compile('([A-Z])\w+')
        section = r.search(x)
        if section is not None:
            return section.group()
        return section

    def deriveClientDevice(self,iis_log_df):
        iis_log_df['client-device']  =  iis_log_df['cs(User-Agent)'].apply(lambda x: self.getDevice(str(x)))
        return iis_log_df

    def deriveClientBrowser(self,iis_log_df):
        iis_log_df['client-browser'] =  iis_log_df['cs(User-Agent)'].apply(lambda x: self.getBrowser(str(x)))
        return iis_log_df

    def deriveClientWebPage(self,iis_log_df):
        iis_log_df['client-webPage'] = iis_log_df['cs(Referer)'].apply(lambda x: self.GetWebPageSection(x) if type(x) != float else np.nan)
        return iis_log_df

    def deriveClientCity(self,iis_log_df):
        #print(iis_log_df)
        iis_log_df['client-city'] =  iis_log_df['client-ip'].apply(lambda x: self.reader.city(ip_address=x).city.name if self.reader.city(ip_address=x).city.name != None else np.nan)
        return iis_log_df

    def deriveClientCountry(self,iis_log_df):    
        iis_log_df['client-country'] =  iis_log_df['client-ip'].apply(lambda x: self.reader.city(ip_address=x).country.name if self.reader.city(ip_address=x).country.name != None else np.nan)
        return iis_log_df
    
    
    def getListOfFiles(self,dirName):
        # names in the given directory 
        listOfFile = os.listdir(dirName)
        allFiles = list()

        for file in listOfFile:
            # Create full path
            fullPath = os.path.join(dirName, file)
            # If entry is a directory then get the list of files in this directory 
            if os.path.isdir(fullPath):
                allFiles = allFiles + self.getListOfFiles(fullPath)
            else:
                allFiles.append(fullPath)
        return allFiles

    
    def readLogs(self,logsPath):
        df = pd.DataFrame()
        #instantiate log reader
        #reader = geoip2.database.Reader(geoLiteIPDBPath)

        listOfFiles = self.getListOfFiles(logsPath)

        try:    
            for file in listOfFiles:
                print (file)
                self.log_df = self.readLog(file)
                self.log_df = self.deriveClientDevice(self.log_df)
                self.log_df = self.deriveClientBrowser(self.log_df)
                self.log_df = self.deriveClientWebPage(self.log_df)
                self.log_df = self.deriveClientCity(self.log_df)
                self.log_df = self.deriveClientCountry(self.log_df)
                df = pd.concat([df,self.log_df])
                os.rename(file,'../data/success/' + file[file.find('u'):])
        except Exception as inst:
            
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)          # __str__ allows args to be printed directly,
                                 # but may be overridden in exception subclasses
            x, y = inst.args     # unpack args
            print('x =', x)
            print('y =', y)

            os.rename(file,'../data/error/' + file[file.find('u'):])
            print('Moving file '+ file + ' to ../data/error/')
            print(sys.exc_info()[0])

        finally:        
            self.reader.close()
            return df
