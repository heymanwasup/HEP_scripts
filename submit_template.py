from submitter import Submitter

def main():
    run_mode = 'submit'  # submit/check/resubmit/hadd 
    submit_to = 'reader' # reader/maker
    period = 'mc16d' # mc16a/mc16d
    hadd_driver = 'condor'
    output_dir = 'Reader_CxAOD_FTAG2_2L_mc16d_fullSys_condor6'

    if run_mode == 'resubmit':
        Submitter(period, submit_to, output_dir).Resubmit('./output')
    elif run_mode == 'check':
        Submitter(period, submit_to, output_dir).checkJob('./output')
    elif run_mode == 'submit':
        Submitter(period, submit_to, output_dir).Submit(1)
    elif run_mode == 'hadd':
        Submitter(period, submit_to, output_dir).Hadd(hadd_driver)

if __name__ == '__main__':        
    main()
