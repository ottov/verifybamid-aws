#!/usr/bin/env python2.7
from __future__ import print_function

import os
import os.path
import time
import shlex
import subprocess
from argparse import ArgumentParser

from common_utils.s3_utils import download_file, upload_file, get_size
from common_utils.job_utils import generate_working_dir, delete_working_dir


def run_verifybamid_basic(vcf_path, bam_path, bai_path, cmd_args, working_dir):
    """
    Runs verifyBamId
    :param vcf_path: Local path to vcf file
    :param bam_path: Local path to bam file
    :param cmd_args: Additional command-line arguments to pass in
    :param working_dir: Working directory
    :return: path to the output (stats_path)
    """
    results_prefix = os.path.join(working_dir, 'data-out')

    cmd = 'verifyBamID --vcf %s --bam %s --bai %s --out %s ' % (vcf_path, bam_path, bai_path, results_prefix)
    #cmd = 'aws s3 cp %s - | ' % (bam_path)
    #cmd += 'verifyBamID --vcf %s --bam -.bam --bai %s --out %s ' % (vcf_path, bai_path, results_prefix)
    cmd += ' '.join(map(lambda x : ' --' + x.replace("'",''), cmd_args))

    print('Running cmd:= ', end='')
    print(cmd)

    subprocess.check_call(shlex.split(cmd))
    #output=subprocess.check_output(cmd, shell=True)

    return results_prefix


def main():
    argparser = ArgumentParser()

    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--vcf_s3_path', type=str, help='VCF s3 path', required=True)
    file_path_group.add_argument('--bam_s3_path', type=str, help='BAM s3 path', required=True)
    file_path_group.add_argument('--bai_s3_path', type=str, help='BAI s3 path', required=True)
    file_path_group.add_argument('--results_s3_path', type=str, help='S3 Path to upload stats', required=True)

    run_group = argparser.add_argument_group(title='Run command args')
    run_group.add_argument('--cmd_args', type=str, help='Additional Arguments', default=None, nargs='*', action='store', dest='opt_list')

    argparser.add_argument('--working_dir', type=str, default='/scratch')

    args = argparser.parse_args()

    working_dir = generate_working_dir(args.working_dir)

    total_size = 0
    for obj in [args.vcf_s3_path, args.bam_s3_path, args.bai_s3_path]:
        total_size += get_size(obj)

    print("Total Size := {0}".format(total_size) )

    # Declare expected disk usage, triggers host's EBS script (ecs-ebs-manager)
    with open("/TOTAL_SIZE", "w") as text_file:
       text_file.write("{0}".format(total_size))

    # Wait for EBS to appear
    while not os.path.isdir("/scratch"):
       time.sleep(5)

    # Wait for mount verification
    while not os.path.ismount('/scratch'):
       time.sleep(1)

    print("Downloading vcf")
    local_vcf_path = download_file(args.vcf_s3_path, working_dir)
    print("VCF downloaded to %s" % local_vcf_path)

    print("Downloading bam")
    local_bam_path = download_file(args.bam_s3_path, working_dir)
    print("BAM downloaded to %s" % local_bam_path)

    print("Downloading bam index")
    local_bam_index_path = download_file(args.bai_s3_path, working_dir)
    print("BAM index downloaded to %s" % local_bam_index_path)

    print ("Running verifybamid")
    local_stats_path = run_verifybamid_basic(
                           local_vcf_path,
                           local_bam_path,
                           local_bam_index_path,
                           args.opt_list,
                           working_dir)

    for ext in ['.selfSM', '.bestSM', '.depthSM', '.log']:
        if os.path.exists(local_stats_path + ext):
            print ("Uploading %s to %s" % (local_stats_path + ext, args.results_s3_path + ext))
            upload_file(args.results_s3_path + ext, local_stats_path + ext)

    print('Cleaning up working dir')
    delete_working_dir(working_dir)

    print ("Completed")


if __name__ == '__main__':
    main()
