import pandas as pd
import numpy as np
import geoip2.database
import os
import re
import datetime as dt

class log:
    log_df =pd.DataFrame()
    
    def ___init__(self):
        print("IN INIT METHOD")
    
    def openReader(self,geoLiteIPDBPath):
        self.reader = geoip2.database.Reader(geoLiteIPDBPath)
        
    def closeReader(self):
        self.reader.close()
    
    def readLog(self,file):
        self.log_df = pd.read_csv(file
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
                                ,'time-taken(ms)']
                                ,encoding = "ISO-8859-1"
                                 )
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

    def getClientCity(self,ip_address):
        try:
            response = None
            response = self.reader.city(ip_address).city.name 
        except:
            pass
        finally:
            return response
    
    def getClientCountry(self,ip_address):
        try:
            response = None
            response = self.reader.city(ip_address).country.name
        except:
            pass
        finally:
            return response
        
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
        iis_log_df['client-city'] =  iis_log_df['client-ip'].apply(lambda x: self.getClientCity(ip_address=x) if self.getClientCity(ip_address=x) != None else np.nan)
        return iis_log_df

    def deriveClientCountry(self,iis_log_df):    
        iis_log_df['client-country'] =  iis_log_df['client-ip'].apply(lambda x: self.getClientCountry(ip_address=x) if self.getClientCountry(ip_address=x) != None else np.nan)
        return iis_log_df
    
    def deriveDateWeekday(self,iis_log_df):
        iis_log_df['date-IsWeekday']   = iis_log_df['date'].apply(lambda x: 1 if np.int8(str(dt.datetime.strptime(x,'%Y-%m-%d').weekday())) < 5 else 0)
        return iis_log_df
        
    def deriveDateCalendarWeek(self,iis_log_df):
        iis_log_df['date-calendar-week']   = iis_log_df['date'].apply(lambda x: np.int8(str(dt.datetime.strptime(x,'%Y-%m-%d').isocalendar()[1])))
        return iis_log_df
    
    def deriveDateYear(self,iis_log_df):
        iis_log_df['date-year']   = iis_log_df['date'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d').year)
        return iis_log_df
    
    def groupbyCalendarYearWeek_ClientDevice(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['calendar-year-week','client-device'])
                    .agg({'client-device': pd.Series.count})  
                    .rename(columns = {'client-device':'client-device-count'})      
                    .reset_index(level=1)
                    .pivot(columns='client-device',values='client-device-count')
                    .rename(columns = {'Desktop':'Desktop-count'
                                      ,'Mobile':'Mobile-count'}))
        return iis_log_df
    

    def groupbyCalendarYearWeek_ClientBrowser(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['calendar-year-week','client-browser'])
            .agg({'client-browser': pd.Series.count})
            .rename(columns = {'client-browser':'client-browser-count'})      
            .reset_index(level=1)
            .pivot(columns='client-browser',values='client-browser-count')
            .rename(columns = {'Chrome':'Chrome-count'
                              ,'Firefox':'Firefox-count'
                              ,'Other':'Other-count'
                              ,'Safari':'Safari-count'}))
        return iis_log_df
    def groupbyCalendarYearWeek_TotalConnections_TimeTaken(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['calendar-year-week'])
            .agg({'client-ip': pd.Series.count,'time-taken(ms)' : pd.Series.sum})
            .rename(columns = {'client-ip':'client-connections-count','time-taken(ms)' :'time-taken(ms)-sum'}))
        return iis_log_df
    def groupbyCalendarYearWeek_CsUsername(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['calendar-year-week'])
            .agg({'cs-username': pd.Series.nunique})
            .rename(columns = {'cs-username':'cs-username-unique-count'}))
        return iis_log_df
    def groupbyCalendarYearWeek_ClientIp(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['calendar-year-week'])
            .agg({'client-ip': pd.Series.nunique})
            .rename(columns = {'client-ip':'client-ip-unique-count'}))
        return iis_log_df
    
    def aggregateData(self,iis_log_df):
        
        df_ip = self.groupbyCalendarYearWeek_ClientIp(iis_log_df)
        df_username = self.groupbyCalendarYearWeek_CsUsername(iis_log_df)
        df_totalConnections = self.groupbyCalendarYearWeek_TotalConnections_TimeTaken(iis_log_df)
        df_browser = self.groupbyCalendarYearWeek_ClientBrowser(iis_log_df)
        df_device = self.groupbyCalendarYearWeek_ClientDevice(iis_log_df)
        
        df = (pd.concat([self.groupbyCalendarYearWeek_ClientIp(iis_log_df)
                         ,self.groupbyCalendarYearWeek_CsUsername(iis_log_df)
                         ,self.groupbyCalendarYearWeek_TotalConnections_TimeTaken(iis_log_df)
                         ,self.groupbyCalendarYearWeek_ClientBrowser(iis_log_df)
                         ,self.groupbyCalendarYearWeek_ClientDevice(iis_log_df)
                        ]
                        ,axis=1
                        )
             )
        
        return df
    
    def groupbyDate_ClientDevice(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['date','client-device'])
                    .agg({'client-device': pd.Series.count})  
                    .rename(columns = {'client-device':'client-device-count'})      
                    .reset_index(level=1)
                    .pivot(columns='client-device',values='client-device-count')
                    .rename(columns = {'Desktop':'Desktop-count'
                                      ,'Mobile':'Mobile-count'}))
        return iis_log_df
    

    def groupbyDate_ClientBrowser(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['date','client-browser'])
            .agg({'client-browser': pd.Series.count})
            .rename(columns = {'client-browser':'client-browser-count'})      
            .reset_index(level=1)
            .pivot(columns='client-browser',values='client-browser-count')
            .rename(columns = {'Chrome':'Chrome-count'
                              ,'Firefox':'Firefox-count'
                              ,'Other':'Other-count'
                              ,'Safari':'Safari-count'}))
        return iis_log_df
    def groupbyDate_TotalConnections_TimeTaken(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['date'])
            .agg({'client-ip': pd.Series.count,'time-taken(ms)' : pd.Series.sum})
            .rename(columns = {'client-ip':'client-connections-count','time-taken(ms)' :'time-taken(ms)-sum'}))
        return iis_log_df
    def groupbyDate_CsUsername(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['date'])
            .agg({'cs-username': pd.Series.nunique})
            .rename(columns = {'cs-username':'cs-username-unique-count'}))
        return iis_log_df
    def groupbyDate_ClientIp(self,iis_log_df):
        iis_log_df=(iis_log_df.groupby(by=['date'])
            .agg({'client-ip': pd.Series.nunique})
            .rename(columns = {'client-ip':'client-ip-unique-count'}))
        return iis_log_df
    

    def aggregateDataByDate(self,iis_log_df):

        df_ip = self.groupbyDate_ClientIp(iis_log_df)
        df_username = self.groupbyDate_CsUsername(iis_log_df)
        df_totalConnections = self.groupbyDate_TotalConnections_TimeTaken(iis_log_df)
        df_browser = self.groupbyDate_ClientBrowser(iis_log_df)
        df_device = self.groupbyDate_ClientDevice(iis_log_df)

        df = (pd.concat([self.groupbyDate_ClientIp(iis_log_df)
                         ,self.groupbyDate_CsUsername(iis_log_df)
                         ,self.groupbyDate_TotalConnections_TimeTaken(iis_log_df)
                         ,self.groupbyDate_ClientBrowser(iis_log_df)
                         ,self.groupbyDate_ClientDevice(iis_log_df)
                        ]
                        ,axis=1
                        )
             )

        return df
    
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

    
    def readLogs(self,logsPath,numberOfFiles=1000):
        df = pd.DataFrame()
        df_aggregate = pd.DataFrame()
        listOfFiles = self.getListOfFiles(logsPath)
        iterator = 0 
        
        try:    
            for file in listOfFiles:
                iterator = iterator + 1
                if (iterator <= numberOfFiles):
                    print (file)
                    self.log_df = self.readLog(file)
                    self.log_df = self.deriveClientDevice(self.log_df)
                    self.log_df = self.deriveClientBrowser(self.log_df)
                    self.log_df = self.deriveClientWebPage(self.log_df)
                    self.log_df = self.deriveClientCity(self.log_df)
                    self.log_df = self.deriveClientCountry(self.log_df)
                    self.log_df = self.deriveDateWeekday(self.log_df)
                    self.log_df = self.deriveDateCalendarWeek(self.log_df)
                    self.log_df = self.deriveDateYear(self.log_df)
                    self.log_df = self.log_df.iloc[:,[0,5,6,11,12,13,14,15,16,17,18,19]]
                    self.log_df['cs-username'].fillna('Unknown',inplace=True)
                    self.log_df['client-webPage'].fillna('Unknown',inplace=True)
                    self.log_df['client-city'].fillna('Unknown',inplace=True)
                    self.log_df['calendar-year-week']= (self.log_df[['date-year','date-calendar-week']]
                                                        .apply(lambda x: '{}{}{}'.format(x[0],'-',x[1])
                                                        ,axis=1))
                    df = pd.concat([df,self.log_df])
                    if iterator%numberOfFiles == 0:
                        #df =self.aggregateData(df)
                        df=self.aggregateDataByDate(df)
                        df_aggregate = pd.concat([df_aggregate,df])
                        del df
                        df = pd.DataFrame()
                    #else:
                        #df = pd.concat([df,self.log_df])
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
            return df_aggregate
