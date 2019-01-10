#designed to handler the CxAODFramework jobs
#submit cxaod reader/maker
#check the production, resubmit and hadd


import os,re,commands

class HaddSamples(object):
    def __init__(self,submitDir):
        self.submitDir = submitDir
        
        self._outputDir = '%s/hadd/samples'%(submitDir)            
        os.system('mkdir -p %s'%(self._outputDir))

        self._dataDir = '%s/%s'%(submitDir,'fetch')
        self._exe = commands.getstatusoutput('which hadd')[1]
        self._jobs = self.getHaddJobs()

    def Init_condor(self):
        self._condor_submitDir = '%s/hadd_condor'%(self.submitDir)
        os.system('mkdir -p %s'%(self._condor_submitDir))
        self._condor_commands = '%s/commands.txt'%(self._condor_submitDir)
        self._condor_sh = '%s/run.sh'%(self._condor_submitDir)
        self._condor_script = '%s/submit.condor'%(self._condor_submitDir)
        
        self._condor_sh_text = '''#!/bin/bash
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase        
source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh
lsetup "root 6.04.16-x86_64-slc6-gcc49-opt"
ID=`expr $1 + 1`
CMD=`cat {0:} | head -n $ID | tail -n 1`
eval ${{CMD}}
        '''.format(self._condor_commands)
        
        self._condor_script_text = '''executable              = run.sh
universe                = vanilla
log                     = run.log
output                  = log-$(Process).out
error                   = log-$(Process).err
initialdir              = {0:}
arguments               = $(Process)

accounting_group = group_atlas.uiowa
queue {{0:}}
        '''.format(self._condor_submitDir)

    def getHaddJobs(self):
        files = os.listdir(self._dataDir)        
        jobs = {}
        for f in files:
            job_info = re.findall('hist-(.+)-([0-9]+).root', f)
            if  len(job_info) != 0:
                
                sample = '%s/hist-%s.root'%(self._outputDir, job_info[0][0])
                if not sample in jobs:
                    jobs[sample] = []
                jobs[sample].append('%s/%s'%(self._dataDir, f))
        return jobs


    def Hadd(self,driver):
        cmds = []
        if driver == 'direct':
            for sum_file,files in self._jobs.iteritems():
                if len(files)==1:
                    #print 'ln -s %s %s'%(sum_file,files[0])
                    cmd = 'ln -s %s %s'%(files[0],sum_file)                
                    os.system(cmd)
                else:
                    in_files = ' '.join(files)                    
                    os.system('%s %s %s'%(self._exe,sum_file,in_files))
        elif driver == 'condor':
            self.Init_condor()
            njobs = 0
            with open(self._condor_commands,'w') as f:
                for sum_file,files in self._jobs.iteritems():
                    if len(files)==1:
                        cmd = 'ln -s %s %s'%(files[0],sum_file)                
                        os.system(cmd)   
                    else:
                        in_files = ' '.join(files)                    
                        print >>f,'%s %s %s'%('hadd',sum_file,in_files)
                        njobs += 1
                print self._condor_commands,'Created!'
            
            with open(self._condor_sh,'w') as f:
                print >>f,self._condor_sh_text                    
            os.system('chmod 777 %s'%(self._condor_sh))
            print self._condor_sh,'Created!'

            with open(self._condor_script,'w') as f:
                print >>f,self._condor_script_text.format(njobs)
            print self._condor_script,'Created'

            os.system('cd {0:};condor_submit {1:}'.format(self._condor_submitDir,self._condor_script))

class CheckSamples(object):
    def __init__(self,submitDir,outputDir):
        self.submitDir = submitDir + '/'        
        self.submitDir_name = os.path.split(os.path.split(self.submitDir)[0])[1]
        self.outputDir = '{0:}/StatusCheck_{1:}'.format(outputDir,self.submitDir_name)
        if not os.path.isdir(self.outputDir):
            os.system('mkdir -p {0:}'.format(self.outputDir))
# cd /afs/cern.ch/work/c/chenc/CxAODFW/CxAODFramework_tag_r31-10_1/run/ReaderOutput/submitDir_reader_mc16d_TT_validation_2/submit && bsub -q 8nh -L /bin/bash /afs/cern.ch/work/c/chenc/CxAODFW/CxAODFramework_tag_r31-10_1/run/ReaderOutput/submitDir_reader_mc16d_TT_validation_2/submit/run 0
    def ResubmitFailed(self,queue):
        submit_dir = '{0:}/submit'.format(self.submitDir)
        submit_sh = '{0:}/run'.format(submit_dir)
        failed_csv = '{0:}/jobs_failed.CSV'.format(self.outputDir)

        with open(failed_csv,'r') as f:
            data = f.readlines()
        jobs = []
        for line in data:
            jobs.append(line[:-1])
        if len(jobs)==0:
            print 'no jobs to resubmit'
            return

        cmd = 'cd {0:}'.format(submit_dir)
        for job in jobs:
            cmd += ' && bsub -q {0:} -L /bin/bash {1:} {2:}'.format(queue,submit_sh,job)
        #print cmd
        os.system(cmd)



    def Stat(self):
        segmentations = self.readSegmentation()
        jobs = self.readJobs()
        results = self.stats(segmentations,jobs)
        self.dumpResults(results)

    def readSegmentation(self):
        seg_file = '{0:}/submit/segments'.format(self.submitDir)
        
        with open(seg_file, 'r') as f:
            seg_data = f.readlines()

        segments = []
        for line in seg_data:
            line = line[:-1]
            n,name,nseg = re.split(' |-', line)
            segments.append([n,name,nseg])
        return segments


    def readJobs(self):
        status_dir = '{0:}/status'.format(self.submitDir)
        files = os.listdir(status_dir)

        _status = {'completed':[],'fail':[],'done':[]}

        for f in files:
            key,num = re.findall('([a-z]+)-([0-9]+)', f)[0]
            if not key in _status:
                _status[key] = []
            _status[key].append(num)
        status = {}
        status['completed'] = set(_status['completed'])
        status['fail'] = set(_status['fail']) - set(_status['completed'])
        for key in set(_status.keys()) - set(['fail','completed']):
            status[key] = _status[key]
        return status

    def stats(self,segs,jobs):

        data = self.transformation(segs,jobs)


        overall = self.statOverall(data['total'],data['done'],data['completed'],data['fail'])
        samples = self.statSamples(data['details'])
        details = self.statDetails(data['details'])

        tables = {
            'overall' : overall,
            'samples' : samples,
            'details' : details,
            'failed'  : data['fail']
        }
        return tables
    def transformation(self,segs,jobs):
        
        total= set([ seg[0] for seg in segs ])
        done = set(jobs['done'])
        completed = set(jobs['completed'])
        fail = set(jobs['fail'])

        details = []
        for num,name,nseg in segs:
            fsize = -1
            if num in completed:
                status = 'completed'
                fname = '{0:}/fetch/hist-{1:}-{2:}.root'.format(self.submitDir,name,nseg)
                if os.path.isfile(fname):
                    fsize = os.stat(fname).st_size
            elif num in fail:
                status = 'fail'
            else:
                status = 'unfinished'

            details.append([num,name,nseg,status,fsize])

        data = {
            'total' : total,
            'done' : done,
            'completed' : completed,
            'fail' : fail,
            'details' : details,
        }
        return data

    def statOverall(self,total,done,completed,fail):
        n_total = len(total)
        n_done = len(done)
        n_sucessed = len(completed)
        n_fail = len(fail)

        if n_fail + n_sucessed == n_done:
            status = 'NONE'
        elif n_fail + n_sucessed > n_done:
            fail_undone = fail - done
            comp_undone = completed - done
            status = ''
            if len(fail_undone)!=0:
                status += '%s fails '%(len(fail_undone))
                print 'fail_undone',fail_undone
            if len(comp_undone)!=0:
                status += '%s completed '%(len(comp_undone))
                print 'comp_undone',comp_undone
            if len(comp_undone)!=0 or len(fail_undone)!=0:
                status += 'not in done'

        else:
            n_done_unknown =  done - fail - completed
            status = '%s done not fail nor completed'
        
        header = 'SubmitDir,total,finished,successed,fail,unfinished,exceptions'                
        contents = map(str,[self.submitDir_name,n_total,n_fail + n_sucessed,n_sucessed,n_fail,n_total-n_fail-n_sucessed,status])
        line = ','.join(contents)
        table = [header,line]
        return table

    def statSamples(self,details):
        samples = {}
        for n,name,nseg,status,fsize in details:
            if not name in samples:
                samples[name] = {'total':0,'completed':0,'fail':0,'unfinished':0}
            samples[name]['total'] += 1
            samples[name][status] += 1
        counter ={ 'total':0, 'completed':0, 'fail':0,'unfinished':0}
        table = ['Sample,total,completed,fail,unfinished']
        for sample,stats in samples.iteritems():
            
            for key in stats:
                counter[key] += stats[key]
            content = map(str,[sample,stats['total'],stats['completed'],stats['fail'],stats['unfinished']])
            line = ','.join(content)
            table.append(line)
        table.append(','*4)
        content = map(str,['Total',counter['total'],counter['completed'],counter['fail'],counter['unfinished']])
        line = ','.join(content)
        table.append(line)

        return table

    def statDetails(self,details):
        header = 'Job num.,Sample name,Sample num.,Status,File size'
        table = [header]
        for content in details:
            
            content = map(str,content)
            line = ','.join(content)
            table.append(line)
        return table
        
    def dumpResults(self,tables):
        for name,table in tables.iteritems():
            table_path = '{0:}/jobs_{1:}.CSV'.format(self.outputDir,name)
            with open(table_path, 'w') as f:
                for line in table:
                    print >>f,line
                print table_path,'\n\tgenerated'




#period = mc16a/mc16d
#sample = Data/DT1/DT2/TT
#run_type = reader/maker
#name = user_defined_id
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

        if period == 'mc16a' or period == 'mc16d':
            t1 = 'MC16'+period[-1]
        #CONFIG="data/FrameworkExe/framework-${task}-2L-${perid}-TP.cfg"    
        
        cfg = 'data/FrameworkExe/framework-{0:}-2L-{1:}-TP.cfg'.format(t0,t1)
        return cfg

    def getOutputDir(self,run_type,name):
        t0 = os.environ['PWD']        
        if run_type == 'reader':
            t1 = 'ReaderOutput'
        elif run_type == 'maker':
            t1 = 'MakerOutput'        
        outputDir = '{0:}/{1:}/{2:}'.format(t0,t1,name)
        return outputDir
    
    def getLogFile(self,name):
        log = 'logs/submit_{0:}.log'.format(name)
        return log

    def submitReader(self,outputDir,config,logFile,isExecute=True):
        if os.path.isdir(outputDir):
            os.system('rm -rf %s'%(outputDir))
        cmd = 'hsg5frameworkReadCxAOD  {0:} {1:} | tee {2:}'.format(outputDir,config,logFile)
        print cmd
        if isExecute:
            os.system(cmd)
    def submitMaker(self,outputDir,config,logFile,isExecute=True):
        if os.path.isdir(outputDir):
            os.system('rm -rf %s'%(outputDir))
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



def main():
  pass 

if __name__ == '__main__':        
    main()
