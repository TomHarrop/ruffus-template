#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

#########################################
# 5 accessions variant-calling pipeline #
#########################################

import functions
import ruffus
import os
import datetime
import re


def main():

    #########
    # SETUP #
    #########

    # catch jgi logon and password from cli
    parser = ruffus.cmdline.get_argparse(
        description='5 accessions variant calling pipeline.')
    parser.add_argument('--email', '-e',
                        help='Logon email address for JGI',
                        type=str,
                        dest='jgi_logon')
    parser.add_argument('--password', '-p',
                        help='JGI password',
                        type=str,
                        dest='jgi_password')
    options = parser.parse_args()
    jgi_logon = options.jgi_logon
    jgi_password = options.jgi_password

    ##################
    # PIPELINE STEPS #
    ##################

    # initialise pipeline
    main_pipeline = ruffus.Pipeline.pipelines["main"]

    # test functions module
#    functions.touch('ruffus/yes.txt')

    # test generator function
    test_files = ['ruffus/foo.txt', 'ruffus/bar.txt']
    test = main_pipeline.originate(
        name="test",
        task_func=functions.generate_job_function(
            job_script='src/test.sh', job_name='test_pl',
            job_type='originate',
            output_files=test_files),
        output=test_files)

    transformed_test_files = ['ruffus/foo2.txt', 'ruffus/bar2.txt']
    test2 = main_pipeline.transform(
        name="test2",
        task_func=functions.generate_job_function(
            job_script='src/test.sh', job_name='test_pl',
            job_type='transform', input_files=True,
            output_files=transformed_test_files),
        input=test,
        filter=ruffus.formatter(),
        output=transformed_test_files)

    ###################
    # RUFFUS COMMANDS #
    ###################

    # print the flowchart
    ruffus.pipeline_printout_graph("ruffus/flowchart.pdf", "pdf",
                                   pipeline_name="5 accessions analysis "
                                                 "pipeline")

    # run the pipeline
    ruffus.cmdline.run(options, multithread=8)

if __name__ == "__main__":
    main()
