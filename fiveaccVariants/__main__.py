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

    # test generator function
    test_originate_files = ['ruffus/foo.txt', 'ruffus/bar.txt']
    test_originate = main_pipeline.originate(
        name='test_originate',
        task_func=functions.generate_job_function(
            job_script='src/test_originate',
            job_name='test_originate',
            job_type='originate'),
        output=test_originate_files)
    print(test_originate)

    test_transform = main_pipeline.transform(
        name="test_transform",
        task_func=functions.generate_job_function(
            job_script='src/test_transform',
            job_name='test_transform',
            job_type='transform'),
        input=test_originate,
        filter=ruffus.suffix(".txt"),
        output=["_transformed.txt", "_transformed.bam"])
    print(test_transform)
    print(ruffus.output_from(test_transform))

    test_merge = main_pipeline.merge(
        name='test_merge',
        task_func=functions.generate_job_function(
            job_script='src/test_merge',
            job_name='test_merge',
            job_type='merge'),
        input=test_transform,
        output='ruffus/foobar_merge.txt'
        )
    print(test_merge)

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
