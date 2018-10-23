import pandas as pd

class log:
    
    def ___init__(self):
        self.df = df
        #print("IN INIT METHOD")
        
    def getDevice (self):
        print ('In the funciton')
        
    def readLog(self,file):
        log_df = pd.read_csv(file
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
        return log_df
 