# John Vivian
# 5-9-15

"""
Pipeline for Tumor/Normal Variant Calling
"""
import argparse

from jobTree.src.stack import Stack
from jobTree.src.target import Target

def build_parser():
    """
    Contains arguments for the all of necessary input files
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--reference', required=True, help="Reference Genome URL")
    parser.add_argument('-n', '--normal', required=True, help='Normal BAM URL. Format: UUID.normal.bam')
    parser.add_argument('-t', '--tumor', required=True, help='Tumor BAM URL. Format: UUID.tumor.bam')
    parser.add_argument('-d', '--dbsnp', required=True, help='dbsnp_132_b37.leftAligned.vcf URL')
    parser.add_argument('-c', '--cosmic', required=True, help='b37_cosmic_v54_120711.vcf URL')
    parser.add_argument('-u', '--mutect', required=True, help='Mutect.jar')
    parser.add_argument('-w', '--work_dir', required=True, help='Where you wanna work from? (full path please)')
    return parser

####    Begin Pipeline Targets    ####



def main():
    # Handle parser logic
    parser = build_parser()
    Stack.addJobTreeOptions(parser)
    args = parser.parse_args()

    input_urls = {'reference.fasta': args.reference,
                  'normal.bam': args.normal,
                  'tumor.bam': args.tumor,
                  'dbsnp.vcf': args.dbsnp,
                  'cosmic.vcf': args.cosmic,
                  'gatk.jar': args.gatk,
                  'mutect.jar': args.mutect}

    # Ensure user supplied URLs to files and that BAMs are in the appropriate format
    for bam in [args.normal, args.tumor]:
        if len(bam.split('/')[-1].split('.')) != 3:
            raise RuntimeError('{} BAM is not in the appropriate format: \
            UUID.normal.bam or UUID.tumor.bam'.format(str(bam).split('.')[1]))

 # Symbolic names for all inputs in the pipeline
    symbolic_inputs = ['ref_fasta', 'ref_fai', 'ref_dict', 'normal_bam', 'normal_bai', 'tumor_bam',
                       'tumor_bai', 'cosmic_vcf', 'mutect_jar', 'mutect_vcf', 'mutect_out', 'mutect_cov']

    # Create JobTree Stack which launches the jobs starting at the "Start Node"
    i = Stack(Target.makeTargetFn( __ , (args, input_urls, symbolic_inputs))).startJobTree(args)


if __name__ == '__main__':
    #from jobtree_docker_pipeline import *
    main()