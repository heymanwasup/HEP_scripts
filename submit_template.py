from submitter import Submitter

def main():

    run_mode = 'resubmit'  # submit/check/resubmit/hadd/cutflow
    submit_to = 'reader' # reader/maker
    period = 'mc16a' # mc16[ade]

    #merge by samples
    hadd_driver = 'direct' # condor or direct

    #resubmit failed jobs according to the jobs_failed.CSV which generated with run_mode = 'check'
    resubmit_driver = 'condor' # condor or LSF
    resubmit_label = 2

    #outout dir
    output_dir = 'Reader_0L_mc16a_EventLabelling_data_r1.0'

    submitter = Submitter(period, submit_to, output_dir)
    if run_mode == 'resubmit':
        submitter.Resubmit('./output',resubmit_driver,resubmit_label)

    elif run_mode == 'check':
        submitter.checkJob('./output')

    elif run_mode == 'submit':
        submitter.Submit(1)

    elif run_mode == 'hadd':
        submitter.Hadd(hadd_driver)

    elif run_mode == 'cutflow':
        submitter.Cutflow('./CountCutflow', cxaod_file='hist-CxAOD.root', root_file='hist-ttbar.root')

if __name__ == '__main__':        
    main()
