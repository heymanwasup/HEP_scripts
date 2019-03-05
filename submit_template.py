from submitter import Submitter

def main():
    run_mode = 'cutflow'  # submit/check/resubmit/hadd 
    submit_to = 'maker' # reader/maker
    period = 'mc16a' # mc16a/mc16d
    hadd_driver = 'condor'
    #output_dir = 'Reader_CxAOD_FTAG2_2L_mc16d_fullSys_condor6'
    output_dir = 'Maker_CxAOD_FTAG2_2L_mc16a_test_harmonized_1.0'
    
    #output_dir = 'Reader_CxAOD_FTAG2_2L_mc16a_test_harmonized_1.0'

    if run_mode == 'resubmit':
        Submitter(period, submit_to, output_dir).Resubmit('./output')
    elif run_mode == 'check':
        Submitter(period, submit_to, output_dir).checkJob('./output')
    elif run_mode == 'submit':
        Submitter(period, submit_to, output_dir).Submit(1)
    elif run_mode == 'hadd':
        Submitter(period, submit_to, output_dir).Hadd(hadd_driver)
    elif run_mode == 'cutflow':
            Submitter(period, submit_to, output_dir).Cutflow('./CountCutflow', cxaod_file='hist-CxAOD.root', root_file='hist-ttbar.root')
if __name__ == '__main__':        
    main()
