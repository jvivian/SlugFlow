# John Vivian
# 5-9-15

"""
Pipeline for Tumor/Normal Variant Calling

    Get Docker
        |
        v
    Pull Tool Images
        |
        v
    Prepare bams/ref
        |
        v
      MuTect

"""
import argparse
import os
import multiprocessing
import subprocess
import uuid
import errno
import sys

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


class SupportClass(object):

    def __init__(self, target, args, input_urls, symbolic_inputs):
        self.args = args
        self.input_urls = input_urls
        self.symbolic_inputs = symbolic_inputs
        self.cpu_count = multiprocessing.cpu_count()
        self.work_dir = os.path.join(str(self.args.work_dir),
                                     'bd2k-{}'.format(os.path.basename(__file__).split('.')[0]),
                                     str(uuid.uuid4()))

        self.ids = {x : target.getEmptyFileStoreID() for x in self.symbolic_inputs}

    def read_and_rename_global_file(self, target, file_store_id, new_extension, diff_name=None):
        """
        Finds path to file via FileStoreID and takes back control of the extension and filename.
        """
        name = target.readGlobalFile(file_store_id)
        new_name = os.path.splitext(name if diff_name is None else diff_name)[0] + new_extension
        os.rename(name, new_name)
        return new_name

    def unavoidable_download_method(self, name):
        """
        Accepts key from self.input_urls -- Downloads if not present. returns path to file.
        """
        # Get path to file
        file_path = os.path.join(self.work_dir, name)

        # Create necessary directories if not present
        self.mkdir_p(self.work_dir)

        # Check if file exists, download if not presente
        if not os.path.exists(file_path):
            try:
                subprocess.check_call(['curl', '-fs', self.input_urls[name], '-o', file_path])
            except subprocess.CalledProcessError:
                raise RuntimeError('\nNecessary file could not be acquired: {}. Check input URL')
            except OSError:
                raise RuntimeError('Failed to find "curl". Install via "apt-get install curl"')

        assert os.path.exists(file_path)

        return file_path

    @staticmethod
    def mkdir_p(path):
        """
        The equivalent of mkdir -p
        https://github.com/BD2KGenomics/bd2k-python-lib/blob/master/src/bd2k/util/files.py
        """
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    @staticmethod
    def which(program):
        """
        http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
        """
        import os
        def is_exe(fpath):
            return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = os.path.split(program)
        if fpath:
            if is_exe(program):
                return program
        else:
            for path in os.environ["PATH"].split(os.pathsep):
                path = path.strip('"')
                exe_file = os.path.join(path, program)
                if is_exe(exe_file):
                    return exe_file

        return None

####    Begin Pipeline Targets    ####
def acquire_docker(target, args, input_urls, symbolic_inputs):
    """
    Installs Docker if not present on the system.
    """
    sclass = SupportClass(args, input_urls, symbolic_inputs)

    if not sclass.which('docker'):

        # MacOSx
        if sys.platform == 'darwin':
            pass

        # TODO: Ask Hannes about making subprocess calls with sudo
        elif sys.platform == 'ubuntu':
            subprocess.check_call(['sudo', 'apt-get', 'update'])
            subprocess.check_call(['sudo', 'apt-get', 'install', 'linux-image-generic-lts-trusty'])

            if not sclass.which('wget'):
                try:
                    subprocess.check_call(['sudo', 'apt-get', 'install', 'wget'])
                except:
                    raise RuntimeError('Could not install wget, which is required to install Docker')

            subprocess.check_call(['wget', '-qO-', 'https://get.docker.com/', '|', 'sh'])

            assert sclass.which('docker')

        elif sys.platform == 'windows':
            pass


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