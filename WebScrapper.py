# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pn
import datetime, sqlite3
import time as tm

def histDataOldReports(Ticker,dDate, noRep):
    histVals = []
    retHistInfo = []
    retStr = ''
    con = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistoricalRepDB.db');
    c = con.cursor();
    cmd = "SELECT * FROM hisTbl WHERE Ticker = \'"+ Ticker+ "\' and zREPDate < \'" + str(dDate) + "\' ORDER BY zREPDate DESC limit "+str(noRep)
#    print cmd
    PP = c.execute(cmd);
    Pdf = pn.DataFrame(list(PP.fetchall()))
#    print Pdf
    conn = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistStk.db');
    hDB = conn.cursor();
    for ii in xrange(len(Pdf.index)):
        repDate = Pdf.ix[ii,1]
        DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")    
        if ('A' in str(Pdf.ix[ii,2])):
            DEnd = DRep + datetime.timedelta(days=10)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print len (Hdf)            
            if len(Hdf) > 1:            
                PercentChange = 100*(Hdf.ix[1,5]-Hdf.ix[0,5])/Hdf.ix[0,5]
                retStr = "%.2f" % PercentChange +" | "+ str(Hdf.ix[0,5]) + "| A: "+ repDate
            else:
                retStr ="-|-| A: "+ repDate
            #print repStr #"B: ", Hdf.ix[0,5], "  %.2f" % PercentChange
        elif ('B' in str(Pdf.ix[ii,2])): 
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=7)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print Hdf
            if len(Hdf)>1:
                PercentChange = 100*(Hdf.ix[0,5]-Hdf.ix[1,5])/Hdf.ix[1,5]
                retStr = "%.2f" % PercentChange +" | "+ str(Hdf.ix[0,5])+"|B: "+ repDate 
            else:
                retStr ="-|-| B: "+ repDate
            #print "B: ", Hdf.ix[1,5], "  %.2f" % PercentChange
        else:
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=10)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            HdfB = pn.DataFrame(list(DD.fetchall()))
            if  len(HdfB) > 1:
                DEnd = DRep + datetime.timedelta(days=10)
                cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 2" 
                #print cmd
                DD = hDB.execute(cmd);
                HdfA = pn.DataFrame(list(DD.fetchall()))        
                if len(HdfA) > 1: 
                    PercentChange = 100*(HdfA.ix[1,5]-HdfB.ix[1,5])/HdfB.ix[1,5]
                    retStr = "%.2f" % PercentChange + " | "+ str(HdfB.ix[1,5]) + "|N: "+ repDate 
            else:
                retStr =  "-|-|N: "+ repDate 
            #print "N: ", HdfB.ix[1,5], "  %.2f" % PercentChange
#            print HdfA
#        retStr = str(reutRes[ii])+" | "+retStr
        #print retStr
        retHistInfo.append(retStr)
    conn.close()
    con.close()
    return retHistInfo


def EW (ticker):
    ### return results in [EWEPSPred, cEPSPred, EWAnalyst, EWVisitors]
    EWRet = []
    URL = "http://www.earningswhispers.com/stocks.asp?symbol="+str(ticker)
    try:    
        df = pn.read_html(URL)
    except:
        return 4*['']
    pp= df[7]
    EWEPSPred = str(pp.ix[1,4]).replace('$','')
    cEPSPred = str(pp.ix[1,6]).replace('$','')
    EWRet.append(EWEPSPred)
    EWRet.append(cEPSPred)
    #print EWEPSPred, "  ", cEPSPred
    aa = df[24]
    EWAnalyst = str(aa.ix[2,1])
    EWVisitors = str(aa.ix[4,1])
    EWRet.append(EWAnalyst)
    EWRet.append(EWVisitors)
    #print EWAnalyst, EWVisitors
    return EWRet

def zInfo (ticker):
    ### It returns zYrLow, zYrHigh, zAvgVol, zMrktCap, zBeta, zFWPE,zPEG ,zEPS, zAccuEPS,zExpEPSGrowth,zRank  
    zRet = []
    zURL = "http://www.zacks.com/stock/quote/"+str(ticker)
    zdf = pn.read_html(zURL)
    Offset = -1
    if len(zdf) < 3:
        return zRet
    for ii in range(0,4):
        zTmp = zdf[ii];
        if 'Open' in str(zTmp.ix[0,0]):
            Offset = ii;
            break
    #print Offset
    zTmp = zdf[Offset]; 
    #print zTmp
    #print "XXXXXXXXXXXXXXXXXXXXXXXXXXX"
    if Offset <0:
        print len(zTmp)
        zRet = [0]*10
        return zRet
    zYrLow = float(zTmp.ix[3,1])
    #zRet.append()
    zYrHigh = float(zTmp.ix[4,1])
    zAvgVol = int(zTmp.ix[5,1])
    zMrktCap = eval(str(zTmp.ix[6,1]).replace('B','* 1000').replace('M','').replace('nan','-1000'))
    zBeta = float(zTmp.ix[8,1])
    zTmpB = zdf[Offset+1]
    #print zTmpB
    zFWPE = float(zTmpB.ix[0,1])
    zPEG = float(zTmpB.ix[1,1])
    zEPS = float(zTmpB.ix[2,1])
    zAccuEPS = float(zTmpB.ix[4,1])
    zExpEPSGrowth = float(str(zTmpB.ix[8,1]).replace('%',''))
    #print zFWPE, " XXX ", zPEG, " XXX ",zExpEPSGrowth
    for ii in range(0,4):
        zTmp = zdf[Offset+ii];
        if 'Rank' in str(zTmp.ix[0,0]):
            Offset = Offset+ii;
            break
    zTmpC = zdf[Offset]
    zRank = int(str(zTmpC.ix[0,1]).replace('Hold','').replace('Strong Buy','').replace('Strong Sell','').replace('Buy','').replace('Sell','').replace('nan','-1'))
    #print zRank
    zRet.append(zYrLow)
    zRet.append(zYrHigh)
    zRet.append(zAvgVol)
    zRet.append(zMrktCap)
    zRet.append(zBeta)
    #zRet.append(zTmpB)
    zRet.append(zFWPE)
    zRet.append(zPEG)
    zRet.append(zEPS)
    zRet.append(zAccuEPS)
    zRet.append(zExpEPSGrowth)
    zRet.append(zRank)
    return zRet


def YInfo (ticker):
    ## Returnin info from Yahoo as [YRankThisWeek, YRankLastWeek, YAVGEPS,YAVGRev,YBeta,YPE,YPrevClose ]
    ##Yahoo Analyts Rankings
    YRet = []
    YRURL = "http://finance.yahoo.com/q/ao?s="+str(ticker)+"+Analyst+Opinion"
    YRdf = pn.read_html(YRURL)
    try:   
        YRTmp = YRdf[4];
    except:
        return ([]*10)
    YRankThisWeek = YRTmp.ix[0,1]
    YRankLastWeek = YRTmp.ix[1,1]
    #print YRankThisWeek, " ZZZ ", YRankLastWeek
    YRet.append(YRankThisWeek)
    YRet.append(YRankLastWeek)
    
    ##Yahoo Analyts Estimates
    YEURL = "http://finance.yahoo.com/q/ae?s="+str(ticker)+"+Analyst+Estimates"
    YEdf = pn.read_html(YEURL)
    YETmp = YEdf[4]
    #print YETmp
    YAVGEPS = float(YETmp.ix[1,1])
    YRet.append(YAVGEPS)
    YETmpB = YEdf[6]
    #print str(YETmpB.ix[2,1])
    YAVGRev = eval(str(YETmpB.ix[2,1]).replace('M','').replace('B','*1000').replace('K','*0.001').replace('nan','0'))
    YRet.append(YAVGRev)
    #print YEdf[6]
    
    ##Yahoo Summary
    YSURL ="http://finance.yahoo.com/q?s="+str(ticker)
    YSdf = pn.read_html(YSURL)
    YPrevClose = float(pn.DataFrame(YSdf[1]).ix[0,1])
    YBeta = float(pn.DataFrame(YSdf[1]).ix[5,1])
    YRet.append(YBeta)
    YPE = float(pn.DataFrame(YSdf[2]).ix[5,1])
    YRet.append(YPE)
    YRet.append(YPrevClose)
    return YRet

def YInfoA (ticker):
    ## Returnin info from Yahoo as [YRankThisWeek, YRankLastWeek, YAVGEPS,YAVGRev,YBeta,YPE,YPrevClose ]
    ##Yahoo Analyts Rankings
    YRet = []
    YRURL = "http://finance.yahoo.com/q/ao?s="+str(ticker)+"+Analyst+Opinion"
    YRdf = pn.read_html(YRURL)
    try:   
        YRTmp = YRdf[4];
    except:
        return ([]*10)
    YRankThisWeek = YRTmp.ix[0,1]
    YRankLastWeek = YRTmp.ix[1,1]
    #print YRankThisWeek, " ZZZ ", YRankLastWeek
    YRet.append(YRankThisWeek)
    YRet.append(YRankLastWeek)
    
    ##Yahoo Analyts Estimates
    YEURL = "http://finance.yahoo.com/q/ae?s="+str(ticker)+"+Analyst+Estimates"
    YEdf = pn.read_html(YEURL)
    YETmp = YEdf[4]
    #print YETmp
    YAVGEPS = float(YETmp.ix[1,1])
    YRet.append(YAVGEPS)
    YETmpB = YEdf[6]
    #print str(YETmpB.ix[2,1])
    YAVGRev = eval(str(YETmpB.ix[2,1]).replace('M','').replace('B','*1000').replace('K','*0.001').replace('nan','0'))
    YRet.append(YAVGRev)
    #print YEdf[6]
    
    ##Yahoo Summary
    YSURL ="http://finance.yahoo.com/q?s="+str(ticker)
    YSdf = pn.read_html(YSURL)
    YPrevClose = float(pn.DataFrame(YSdf[1]).ix[0,1])
    YBeta = float(pn.DataFrame(YSdf[1]).ix[5,1])
    YRet.append(YBeta)
    YPE = float(pn.DataFrame(YSdf[2]).ix[5,1])
    YRet.append(YPE)
    YRet.append(YPrevClose)
    for ii in range(3,5):        
        if 'Forward P/E' in   str(pn.DataFrame(YSdf[ii]).ix[0,0]):  
            FPE = float(pn.DataFrame(YSdf[ii]).ix[0,1])
        else:
            FPE = ' '
    #print FPE
    YRet.append(FPE)
    return YRet

def YInfoB(ticker):
    ## Returnin info from Yahoo as [YRankThisWeek, YRankLastWeek, YAVGEPS,YAVGRev,YBeta,YPE,YPrevClose ]
    ##Yahoo Analyts Rankings
    YRet = []
    YRURL = "http://finance.yahoo.com/q/ao?s="+str(ticker)+"+Analyst+Opinion"
    YRdf = pn.read_html(YRURL)
    try:   
        YRTmp = YRdf[4];
    except:
        return ([]*10)
    YRankThisWeek = YRTmp.ix[0,1]
    YRankLastWeek = YRTmp.ix[1,1]
    #print YRankThisWeek, " ZZZ ", YRankLastWeek
    YRet.append(YRankThisWeek)
    YRet.append(YRankLastWeek)
    
    ##Yahoo Analyts Estimates
    YEURL = "http://finance.yahoo.com/q/ae?s="+str(ticker)+"+Analyst+Estimates"
    YEdf = pn.read_html(YEURL)
    try:    
        YETmp = YEdf[4]
        #print YETmp
        YAVGEPS = float(YETmp.ix[1,1])
        YETmpB = YEdf[6]
        #print str(YETmpB.ix[2,1])
        YAVGRev = eval(str(YETmpB.ix[2,1]).replace('M','').replace('B','*1000').replace('K','*0.001').replace('nan','0'))
    except:
        YAVGEPS = -1
        YAVGRev = -1
    YRet.append(YAVGEPS)    
    YRet.append(YAVGRev)
    #print YEdf[6]
        
    
    ##Yahoo Summary
    YSURL ="http://finance.yahoo.com/q?s="+str(ticker)
    YSdf = pn.read_html(YSURL)
    YPrevClose = float(pn.DataFrame(YSdf[1]).ix[0,1])
    YBeta = float(pn.DataFrame(YSdf[1]).ix[5,1])
    YRet.append(YBeta)
    YPE = float(pn.DataFrame(YSdf[2]).ix[5,1])
    YRet.append(YPE)
    YAvgVol = int(pn.DataFrame(YSdf[2]).ix[3,1])
    YRet.append(YAvgVol)
    
    YRet.append(YPrevClose)
    for ii in range(3,5):        
        if 'Forward P/E' in   str(pn.DataFrame(YSdf[ii]).ix[0,0]):  
            FPE = float(pn.DataFrame(YSdf[ii]).ix[0,1])
        else:
            FPE = ' '
    #print FPE
    YRet.append(FPE)
    return YRet

def YInfoC(ticker):
    ## Returnin info from Yahoo as [YRankThisWeek, YRankLastWeek, YAVGEPS,YAVGRev,YBeta,YPE,YPrevClose ]
    ##Yahoo Analyts Rankings
    YRet = []
    YRURL = "http://finance.yahoo.com/q/ao?s="+str(ticker)+"+Analyst+Opinion"
    YRdf = pn.read_html(YRURL)
    try:   
        YRTmp = YRdf[4];
        YRankThisWeek = YRTmp.ix[0,1]
        YRankLastWeek = YRTmp.ix[1,1]
    except:
        YRankThisWeek = -1
        YRankLastWeek = -1
    YRet.append(YRankThisWeek)
    YRet.append(YRankLastWeek)
        
    
    ##Yahoo Analyts Estimates
    YEURL = "http://finance.yahoo.com/q/ae?s="+str(ticker)+"+Analyst+Estimates"
    YEdf = pn.read_html(YEURL)
    try:    
        YETmp = YEdf[4]
        #print YETmp
        YAVGEPS = float(YETmp.ix[1,1])
        YETmpB = YEdf[6]
        #print str(YETmpB.ix[2,1])
        YAVGRev = eval(str(YETmpB.ix[2,1]).replace('M','').replace('B','*1000').replace('K','*0.001').replace('nan','0'))
    except:
        YAVGEPS = -1
        YAVGRev = -1
    YRet.append(YAVGEPS)    
    YRet.append(YAVGRev)
    #print YEdf[6]
        
    
    ##Yahoo Summary
    YSURL ="http://finance.yahoo.com/q?s="+str(ticker)
    YSdf = pn.read_html(YSURL)
    YPrevClose = float(pn.DataFrame(YSdf[1]).ix[0,1])
    YBeta = float(pn.DataFrame(YSdf[1]).ix[5,1])
    YRet.append(YBeta)
    YPE = float(pn.DataFrame(YSdf[2]).ix[5,1])
    YRet.append(YPE)
    YAvgVol = int(pn.DataFrame(YSdf[2]).ix[3,1])
    YRet.append(YAvgVol)    
    YRet.append(YPrevClose)
    for ii in range(3,5):        
        if 'Forward P/E' in   str(pn.DataFrame(YSdf[ii]).ix[0,0]):  
            FPE = float(pn.DataFrame(YSdf[ii]).ix[0,1])
        else:
            FPE = ' '
    #print FPE
    YRet.append(FPE)
    YMarCap = eval(str(YSdf[2].ix[4,1]).replace('M','').replace('B','*1000').replace('K','*0.001').replace('nan','0'))
    YRet.append(YMarCap)
    return YRet

IPETable = pn.read_csv('http://biz.yahoo.com/p/csv/sum_conameu.csv')
def YIPE(ticker):
    
    URL = 'https://finance.yahoo.com/q/in?s='+str(ticker)+'+Industry'
    ticInd = pn.read_html(URL)
    Industry = ''
    for ii in range(len(ticInd)):
        if "Industry: " in str(pn.DataFrame(ticInd[ii]).iloc[0,0]):
            Industry = str(pn.DataFrame(ticInd[ii]).iloc[0,0]).replace('Industry: ','').replace('Get Industry for:','')
            #print Industry            
            break
    try:
        YIPE = IPETable[IPETable["Industry"]==Industry].iloc[0,3]        
    except:
        print ticker, " has issue with IPE ", Industry 
        YIPE = ''
    return YIPE

def RHist (ticker):
    ## Returnin info from Reuters as [H0,H1, H2, H3,H4 ]
    ##Reuters Analyts History
    RURL = "http://www.reuters.com/finance/stocks/analyst?symbol="+str(ticker)
    #print RURL
    Rdd = pn.read_html(RURL, header=0, flavor = 'bs4')
    for ii in xrange(len(Rdd)):
        RTmp = Rdd[ii]
        #print len(RTmp.columns)
        if  len(RTmp.columns)>2 and 'Difference' in str(RTmp.columns[3]):
            Rdd = RTmp
            #print Rdd
            break
    #print Rdd
    if len(Rdd) < 3:
        return (['']*5);
    try:    
        EPSIndx = Rdd[Rdd["Estimates vs Actual"].str.contains("Earnings") == True].index[0]
    except:
        return (['']*5);
    RevLen = EPSIndx-1;
    EPSLen = len(Rdd.index)-EPSIndx-1
    #print RevLen, " XXX ", EPSLen
    #print Indx
    RevDiff = list(Rdd.ix[1:RevLen,2]- Rdd.ix[1:RevLen,1])
    EPSDiff = list(Rdd.ix[EPSIndx+1:,2]- Rdd.ix[EPSIndx+1:,1])
    RepDiff = []
    Status = ''
    for ii in xrange(min(len(RevDiff),5) ):
        if RevDiff[ii] > 0:
            if EPSDiff[ii] > 0:
                Status = 'UU'
            elif EPSDiff[ii] < 0:
                Status = 'UD'
            elif EPSDiff[ii] == 0:
                Status = 'U-'
        else:
            if EPSDiff[ii] > 0:
                Status = 'DU'
            elif EPSDiff[ii] < 0:
                Status = 'DD'
            elif EPSDiff[ii] == 0:
                Status = 'D-'
        RepDiff.append(Status)
        #print Status
    #print RepDiff
    return RepDiff

def histInfo(Ticker):
    ### Returns ReportDate-ReportTime, CloseValue,Change,RevEPS
    reutRes = RHist(Ticker);
    for jj in range(len(reutRes),5):
        reutRes.append('')
    retHistInfo = []
    retStr = ''
    conn = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistStk.db');
    hDB = conn.cursor();
    con = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistoricalRepDB.db');
    c = con.cursor();
    cmd = "SELECT * FROM hisTbl WHERE Ticker = \'"+ Ticker+ "\' and zREPDate < '2015-06-01' and zREPDate > '2014-01-01' ORDER BY zREPDate DESC limit 5"
    PP = c.execute(cmd);
    Pdf = pn.DataFrame(list(PP.fetchall()))
    #print Pdf
    for ii in xrange(len(Pdf.index)):
        repDate = Pdf.ix[ii,1]
        DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")    
        if ('A' in str(Pdf.ix[ii,2])):
            DEnd = DRep + datetime.timedelta(days=10)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print len (Hdf)            
            if len(Hdf) > 1:            
                PercentChange = 100*(Hdf.ix[1,5]-Hdf.ix[0,5])/Hdf.ix[0,5]
                retStr = "%.2f" % PercentChange +" | "+ str(Hdf.ix[0,5]) + "| A: "+ repDate
            else:
                retStr ="-|-| A: "+ repDate
            #print repStr #"B: ", Hdf.ix[0,5], "  %.2f" % PercentChange
        elif ('B' in str(Pdf.ix[ii,2])): 
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=7)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print Hdf
            if len(Hdf)>1:
                PercentChange = 100*(Hdf.ix[0,5]-Hdf.ix[1,5])/Hdf.ix[1,5]
                retStr = "%.2f" % PercentChange +" | "+ str(Hdf.ix[0,5])+"|B: "+ repDate 
            else:
                retStr ="-|-| B: "+ repDate
            #print "B: ", Hdf.ix[1,5], "  %.2f" % PercentChange
        else:
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=10)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            HdfB = pn.DataFrame(list(DD.fetchall()))
            if  len(HdfB) > 1:
                DEnd = DRep + datetime.timedelta(days=10)
                cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 2" 
                #print cmd
                DD = hDB.execute(cmd);
                HdfA = pn.DataFrame(list(DD.fetchall()))        
                if len(HdfA) > 1: 
                    PercentChange = 100*(HdfA.ix[1,5]-HdfB.ix[1,5])/HdfB.ix[1,5]
                    retStr = "%.2f" % PercentChange + " | "+ str(HdfB.ix[1,5]) + "|N: "+ repDate 
            else:
                retStr =  "-|-|N: "+ repDate 
            #print "N: ", HdfB.ix[1,5], "  %.2f" % PercentChange
#            print HdfA
        retStr = str(reutRes[ii])+" | "+retStr
        #print retStr
        retHistInfo.append(retStr)
    conn.close()
    con.close()
    return retHistInfo

def histInfoB(Ticker):
    ### Returns ReportDate-ReportTime, CloseValue,Change,RevEPS
    reutRes = RHist(Ticker);
    for jj in range(len(reutRes),5):
        reutRes.append('')
    retHistInfo = []
    retStr = ''
    conn = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistStk.db');
    hDB = conn.cursor();
    con = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistoricalRepDB.db');
    c = con.cursor();
    cmd = "SELECT * FROM hisTbl WHERE Ticker = \'"+ Ticker+ "\' and zREPDate < '2015-06-01' and zREPDate > '2014-01-01' ORDER BY zREPDate DESC limit 5"
    PP = c.execute(cmd);
    Pdf = pn.DataFrame(list(PP.fetchall()))
    #print Pdf
    for ii in xrange(len(Pdf.index)):
        repDate = Pdf.ix[ii,1]
        DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")    
        if ('A' in str(Pdf.ix[ii,2])):
            DEnd = DRep + datetime.timedelta(days=12)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 7" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print Hdf            
            if len(Hdf) > 1:            
                PercentChange = 100*(Hdf.ix[1,5]-Hdf.ix[0,5])/Hdf.ix[0,5]
                retStrA = "%.2f" % PercentChange +" | "+ str(Hdf.ix[0,5])  
                retStrB = "| A: "+ repDate +" | "+ str(Hdf.ix[2,5]) +" | "+ str(Hdf.ix[4,5]) +" | "+ str(Hdf.ix[6,5])
            else:
                retStr ="-|-| A: "+ repDate
            
            DEnd = DRep - datetime.timedelta(days=12)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 7" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print Hdf
            if len(Hdf)>1:
                retStr = retStrA + " || "+ str(Hdf.ix[5,5]) +" | "+ str(Hdf.ix[3,5]) +" | "+ str(Hdf.ix[1,5]) +"||" + retStrB
            else:
                retStr = retStrA + retStrB 
                print "WTF!"
            #print repStr #"B: ", Hdf.ix[0,5], "  %.2f" % PercentChange
        elif ('B' in str(Pdf.ix[ii,2])): 
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=12)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 7" 
            #print cmd
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print Hdf
            if len(Hdf)>1:
                PercentChange = 100*(Hdf.ix[0,5]-Hdf.ix[1,5])/Hdf.ix[1,5]
                retStr = "%.2f" % PercentChange +" | "+ str(Hdf.ix[1,5])+" || "+ str(Hdf.ix[6,5]) +" | "+ str(Hdf.ix[4,5]) +" | "+ str(Hdf.ix[2,5]) +"|B: "+ repDate 
            else:
                retStr ="-|-| B: "+ repDate
            DEnd = DRep + datetime.timedelta(days=12)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 7" 
            DD = hDB.execute(cmd);
            Hdf = pn.DataFrame(list(DD.fetchall()))
            #print "We are here!"            
            #print Hdf            
            if len(Hdf) > 1:            
                retStr = retStr + " || "+ str(Hdf.ix[1,5]) +" | "+ str(Hdf.ix[3,5]) +" | "+ str(Hdf.ix[5,5])
            else:
                print "Nothing especial! \n"
            #print "B: ", Hdf.ix[1,5], "  %.2f" % PercentChange
        else:
    #        repDate = Pdf.ix[ii,1]
     #       DRep = datetime.datetime.strptime(str(repDate), "%Y-%m-%d")
            DEnd = DRep - datetime.timedelta(days=10)
            cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date <= \'"+ str(DRep) + "\' and Date > \'"+str(DEnd)+ "\' ORDER BY Date DESC limit 2" 
            #print cmd
            DD = hDB.execute(cmd);
            HdfB = pn.DataFrame(list(DD.fetchall()))
            if  len(HdfB) > 1:
                DEnd = DRep + datetime.timedelta(days=10)
                cmd = "SELECT * FROM test_tblC WHERE Ticker = \'"+ Ticker+ "\' and Date > \'"+ str(repDate) + "\' and Date < \'"+str(DEnd)+ "\' ORDER BY Date ASC limit 2" 
                #print cmd
                DD = hDB.execute(cmd);
                HdfA = pn.DataFrame(list(DD.fetchall()))        
                if len(HdfA) > 1: 
                    PercentChange = 100*(HdfA.ix[1,5]-HdfB.ix[1,5])/HdfB.ix[1,5]
                    retStr = "%.2f" % PercentChange + " | "+ str(HdfB.ix[1,5]) + "|N: "+ repDate 
            else:
                retStr =  "-|-|N: "+ repDate 
            #print "N: ", HdfB.ix[1,5], "  %.2f" % PercentChange
#            print HdfA
        retStr = str(reutRes[ii])+" | "+retStr
        #print retStr
        retHistInfo.append(retStr)
    conn.close()
    con.close()
    return retHistInfo

def FinVIZ(ticker):
    URL = "http://finviz.com/quote.ashx?t="+str(ticker)
    kk = []
    try:    
        FVdf = pn.read_html(URL)
        FVTmp = pn.DataFrame(FVdf[7])
        kk = list(FVTmp.iloc[:,1:12:2].values.flatten())
    except:
        kk = ['']*72
    kk.insert(0,ticker)
        
    return kk

def FinVIZColumns():
    URL = "http://finviz.com/quote.ashx?t=MU"
    kk = []
    try:    
        FVdf = pn.read_html(URL)
        FVTmp = pn.DataFrame(FVdf[7])
        kk = list(FVTmp.iloc[:,0:11:2].values.flatten())
    except:
        kk = ['']*72
#    kk.insert(0,ticker)
        
    return kk
    
    
def zInfoPE (ticker):
    ### It returns zYrLow, zYrHigh, zAvgVol, zMrktCap, zBeta, zFWPE,zPEG ,zEPS, zAccuEPS,zExpEPSGrowth,zRank  
    zRet = []
    zURL = "http://www.zacks.com/stock/research/"+str(ticker)+"/industry-comparison"
    zdf = pn.read_html(zURL)
    Offset = -1
    for ii in range(0,len(zdf)):
        zTmp = zdf[ii];
        if 'Price/Earnings' in str(zTmp.ix[0,0]):
            Offset = ii;
            break
    #print Offset
    zTmp = zdf[Offset]; 
    #print zTmp
    #print "XXXXXXXXXXXXXXXXXXXXXXXXXXX"
    if Offset <0:
        print len(zTmp)
        zRet = [0]*2
        return zRet
    zCurrentPE = float(zTmp.ix[0,1])
    zIPE = float(zTmp.ix[0,2])
    zRet.append(zCurrentPE)
    zRet.append(zIPE)
    return zRet

def avgHistVol(ticker):
    conn = sqlite3.connect('C:\Users\Mehdi\Desktop\Stk\HistStk.db');
    hDB = conn.cursor();
    cmd = "SELECT avg(Volume) FROM test_tblC WHERE Ticker == \'"+ str(ticker).upper()+ "\' ORDER BY RANDOM() limit 100" 
#    print cmd
    DD = hDB.execute(cmd)
#    print list(DD.fetchall())[0]    
    avgVol = list(DD.fetchone())
#    print avgVol
    conn.close()
    try: 
        res = int(avgVol[0])
    except: 
        res = 0
    return res
    
    #print str(FVTmp.iloc[0:12,1]).replace('%','').replace('B','*1000').replace('Yes','1').replace('S&P 500','1')
#print RHist('BDMS')
print histInfo('PPSI')

def stockReportList(daysAhead, sources = 2):
    ###Add date range later  to return all the dates results in between,dateRange=0
#    print daysAhead,dateRange, sources
    BeOpenStkList = {}
    ACloseStkList = {}
    base = datetime.datetime.today()
#    if dateRange == 1:    
#        for ii in range(0,daysAhead+1):
#            date_list = [base + datetime.timedelta(days=ii)] #[base + datetime.timedelta(days=x) for x in range(0, numdays)]
#            print date_list
#    else:
    date_list = [base + datetime.timedelta(days=daysAhead)] #[base + datetime.timedelta(days=x) for x in range(0, numdays)]
    for days in date_list:
        print days
        if days.weekday() <5:
            st =  tm.mktime(days.timetuple())-10000
            URL = "http://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal&date="+str(int(st))
            print URL
            df = pn.read_html(URL);
            zCalDF = df[0];
            YURL = "http://biz.yahoo.com/research/earncal/"+str(days.year)+str(days.month).zfill(2)+str(days.day).zfill(2)+".html"
            print YURL
            Ydf = pn.read_html(YURL, header = 1, skiprows = 1)
            Ydd = Ydf[0]
            YCalDF = Ydd.ix[:,"Company":"Time"]
    BeOpenStkList = list(zCalDF.Symbol[zCalDF.Time == "Before Open"])
    BeOpenStkList = [s for s in BeOpenStkList  if "." not in s]
    BY =  list(YCalDF.Symbol[YCalDF.Time == "Before Market Open"])
    for s in BY:
        if "." not in s:
            BeOpenStkList.append(s) 
    BOLen = len(set(BeOpenStkList))
    ACloseStkList = list(zCalDF.Symbol[zCalDF.Time == "After Close"])
    ACloseStkList = [s for s in ACloseStkList  if "." not in s]
    AY =  list(YCalDF.Symbol[YCalDF.Time == "After Market Close"])
    for s in AY:
        if "." not in s:
            ACloseStkList.append(s) 
    ACLen = len(set(ACloseStkList))
    repDate = str(days.year)+'_'+str(days.month).zfill(2)+'_'+str(days.day).zfill(2)
    return [BeOpenStkList,ACloseStkList, repDate, date_list]    
