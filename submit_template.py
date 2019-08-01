from submitter import Submitter

def main():    
    run_mode = 'hadd_resubmit'  # 1.submit 2.check 3.resubmit 4.hadd 5.cutflow 6.hadd_check 7.hadd_resubmit
    submit_to = 'reader' # reader/maker
    period = 'mc16a' # mc16[ade]

    hadd_driver = 'condor' # condor or direct
    
    resubmit_driver = 'condor' # condor or LSF
    resubmit_label = 0

    output_dir = 'Reader_mc16a_syst_BTAG70_VR'







    submitter = Submitter(period, submit_to, output_dir)
    
    # resubmit failed jobs according to the jobs_failed.CSV which generated with run_mode = 'check'
    if run_mode == 'resubmit':        
        submitter.Resubmit('./output',resubmit_driver,resubmit_label)

    # check the status of the reader/maker production
    elif run_mode == 'check':
        submitter.checkJob('./output')

    # launch reader/maker production
    elif run_mode == 'submit':
        submitter.Submit(1)

    # merge the reader output files by samples
    elif run_mode == 'hadd':    
        submitter.Hadd(hadd_driver)

    # make cutflow tables
    elif run_mode == 'cutflow':
        submitter.Cutflow('./CountCutflow', cxaod_file='hist-CxAOD.root', root_file='hist-ttbar.root')

    # check the status of the merge tasks    
    elif run_mode == 'hadd_check':
        submitter.checkHaddJob('./output')

    # resubmit the failed merge tasks according to the hadd_failed.CSV which generated with run_mode = 'hadd_check'
    elif run_mode == 'hadd_resubmit':
        submitter.ReSubmitHadd(hadd_driver,'./output')

if __name__ == '__main__':
    main()