import subprocess
import time
import sys
import logging

L = logging.getLogger(__name__)


def run_gtap_cmf(run_label, call_list):
    """
    Runs a GEMPACK GTAP model using a specified CMF file.

    Parameters:
    run_label is a string for labeling the run
    call_list defines what must be given to subprocess, namely
        cge_executable_path, '-cmf', generated_cmf_path 
    Returns:
    None
    """    


    proc = subprocess.Popen(call_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)

    to_log = ""
    full_log_output = []
    for line in iter(proc.stdout.readline, ''):
        cleaned_line = str(line).replace('b\'\'', '').replace('b\'', '').replace('\\r\\n\'', '').replace('b\"', '').replace('\\r\\n\"', '')
        cleaned_line.rstrip("\r\n")
        if len(cleaned_line) > 0:            
            
            # I have no idea why, but triggering the stdout into the logger makes it not show up, but print does work. Thus, I just append it to a list and report later.
            # print (run_label + ': ' + cleaned_line)
            to_log = cleaned_line            
            print (to_log)            
            full_log_output.append(to_log)

        poll = proc.poll()

        sys.stdout.flush()

        if poll is not None:
            time.sleep(3)
            break

    proc.stdout.close()


    L.info('Finished run_gtap_cmf for ' + run_label)
    if proc.wait() != 0:
        raise RuntimeError("%r failed, exit status: %d" % (str(call_list), proc.returncode))
    L.info('Finished run_gtap_cmf for ' + run_label)
    proc.terminate()
    proc = None

