#designed to handler the CxAODFramework jobs
#submit cxaod reader/maker
#check the production, resubmit and hadd
import os
from submit_tools import CutflowHandler
from submit_tools import HaddSamples
from submit_tools import CheckSamples


class Submitter(object):
    def __init__(self,period,run_type,outputDir):
        self._config = self.getConfig(period,run_type,outputDir)
        self._outputDir = self.getOutputDir(run_type,outputDir)
        self._logFile = self.getLogFile(outputDir)
        self._run_type = run_type

    def Submit(self,isExecute=True):
        if self._run_type == 'reader':
            self.submitReader(self._outputDir, self._config, self._logFile,isExecute)
        elif self._run_type == 'maker':
            self.submitMaker(self._outputDir,self._config,self._logFile,isExecute)
        else:
            print 'run_type = %s'%(reader)
            raise ValueError('only reader/maker inplemented')

    def getConfig(self,period,run_type,name):
        if run_type == 'reader':
            t0 = 'read'
        elif run_type == 'maker':
            t0 = 'run'
        if period == 'mc16a' or period == 'mc16d' or period == 'mc16e':
            t1 = 'MC16'+period[-1]                
        cfg = 'data/CxAODReader_VHbb/framework-{0:}-0L-Merged-{1:}.cfg'.format(t0,t1)
        return cfg

    def getOutputDir(self,run_type,name):
        outputDir = os.environ['PWD']        
        if run_type == 'reader':
            outputDir += '/ReaderOutput'
        elif run_type == 'maker':
            outputDir += '/MakerOutput'
        
        if not os.path.isdir(outputDir):
            print 'no output directory created, going to create:\n%s'%(outputDir)
            os.system('mkdir -p  %s'%(outputDir))
        outputDir += '/%s'%(name)
        return outputDir
    
    def getLogFile(self,name):
        if not os.path.isdir('./logs'):
            print 'no directory for log files created, going to create:\n./logs'
            os.system('mkdir logs')
        log = 'logs/submit_{0:}.log'.format(name)
        return log

    def submitReader(self,outputDir,config,logFile,isExecute=True):
        if os.path.isdir(outputDir):
            print 'Error outputDir already exits'
            print outputDir
            exit()
            #os.system('rm -rf %s'%(outputDir))
        if os.path.isfile(logFile):
            os.system('rm %s'%(logFile))
        cmd = 'hsg5frameworkReadCxAOD  {0:} {1:} 2>&1 | tee {2:}'.format(outputDir,config,logFile)
        print cmd
        if isExecute:
            os.system(cmd)
    def submitMaker(self,outputDir,config,logFile,isExecute=True):
        if os.path.isdir(outputDir):
            print 'Error outputDir already exits'
            print outputDir
            exit()
        cmd = 'hsg5framework --submitDir {0:} --config {1:} | tee {2:}'.format(outputDir,config,logFile)
        print cmd
        if isExecute:
            os.system(cmd)
        

    def checkJob(self,table_outputDir):
        CheckSamples(self._outputDir, table_outputDir).Stat()

    def Resubmit(self,table_outputDir,queue='8nh'):        
        CheckSamples(self._outputDir, table_outputDir).ResubmitFailed(queue)

    def Hadd(self,driver='direct'):
        hadder = HaddSamples(self._outputDir)
        hadder.Hadd(driver)

    def Cutflow(self,outputDir,cxaod_file,root_file):
        CutflowHandler(outputDir,self._outputDir, self._run_type).PrintCutflow(cxaod_file, root_file)
        

def main():
  pass 

if __name__ == '__main__':        
    main()
