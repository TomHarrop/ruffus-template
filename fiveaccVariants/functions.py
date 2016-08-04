#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess
import re
import os
import datetime

############################
# JOB SUBMISSION FUNCTIONS #
############################


def submit_job(job_script, ntasks, cpus_per_task, job_name, extras=[]):
    # type: (str, str, str, str, str, list) -> str
    '''
    Submit the job using salloc hack. When complete return job id and write
    output to file.
    '''
    # call salloc as subprocess
    proc = subprocess.Popen(['salloc', '--ntasks=' + ntasks,
                             '--cpus-per-task=' + cpus_per_task,
                             '--job-name=' + job_name, job_script] + extras,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    # get stdout and stderr
    out, err = proc.communicate()
    # parse stderr (salloc output) for job id
    job_regex = re.compile(b'\d+')
    job_id_bytes = job_regex.search(err).group(0)
    job_id = job_id_bytes.decode("utf-8")
    # write stderr & stdout to log file
    out_file = 'ruffus/' + job_name + '.' + job_id + '.ruffus.out.txt'
    with open(out_file, 'wb') as f:
        f.write(out)
    err_file = 'ruffus/' + job_name + '.' + job_id + '.ruffus.err.txt'
    with open(err_file, 'wb') as f:
        f.write(err)
    # mail output
    if proc.returncode != 0:
        subject = "[Tom@SLURM] Pipeline step " + job_name + " FAILED"
    else:
        subject = "[Tom@SLURM] Pipeline step " + job_name + " finished"
    mail = subprocess.Popen(['mail', '-s', subject, '-A', out_file, '-A',
                             err_file, 'tom'], stdin=subprocess.PIPE)
    mail.communicate()
    # check subprocess exit code
    os.remove(out_file)
    os.remove(err_file)
    assert proc.returncode == 0, ("Job " + job_name +
                                  " failed with non-zero exit code")
    return(job_id)


def print_job_submission(job_name, job_id):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print('[', now, '] : Job ' + job_name + ' run with JobID ' + job_id)


# touch function for updating ruffus flag files
def touch(fname, mode=0o666, dir_fd=None, **kwargs):
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else fname,
                 dir_fd=None if os.supports_fd else dir_fd, **kwargs)

######################
# FUNCTION GENERATOR #
######################


def generate_job_function(
        job_script, job_name, job_type='transform', ntasks=1,
        cpus_per_task=1, input_files=False, output_files=False,
        jgi_logon=False, jgi_password=False):

    '''Generate a function for a pipeline job step'''

    # check job_type
    _allowed_job_types = ['transform', 'merge', 'originate', 'download']
    if job_type not in _allowed_job_types:
        raise ValueError('{job_type} not allowed')

    # check that we got the right arguments
    if ((job_type == 'transform' or job_type == 'merge') and
            (not input_files or not output_files)):
        raise ValueError(
            'input_files and output_files required for {job_type}')
    if ((job_type == 'originate' or job_type == 'download') and input_files):
        raise ValueError(
            '''don't know how to use input_files for {job_type}''')
    if job_type == 'download' and (not jgi_logon or not jgi_password):
        raise ValueError(
            'jgi_logon and jgi_password required for {job_type}')

    # set up the args we want to accept
    kwargs = {'input_files': input_files, 'output_files': output_files,
              'jgi_logon': jgi_logon, 'jgi_password': jgi_logon}

    # set up a dict of arguments
    blank_args = []
    # which args were blank
    for key in kwargs:
        if not kwargs[key]:
            blank_args.append(key)
    # remove blank args
    for key in blank_args:
        del(kwargs[key])

    # define the function
    def job_function(**kwargs):
        for arg in kwargs:
            print(arg)
        if not job_type == 'download':
            job_id = submit_job(
                job_script, ntasks, cpus_per_task, job_name)
        if job_type == 'download':
            job_id = submit_job(
                job_script, ntasks, cpus_per_task, job_name,
                extras=['-e', jgi_logon, '-p', jgi_password])
        job_id = print_job_submission(job_name, job_id)

    return job_function
