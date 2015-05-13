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

1. Given the use of containerized tools, all input/output should be directed to/from: os.path.join(/data, filename)
2. target.updateGlobalFile() should be to:  os.path.join(work_dir, filename)
"""
import argparse
import os
import multiprocessing
import shutil
import subprocess
import uuid
import errno
import sys

from jobTree.src.stack import Stack
from jobTree.src.target import Target


def build_parser():
    """
    Contains arguments for the all of necessary input files and work_directory
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
    """
    Container for necessary information and methods that is passed through the pipeline
    """
    def __init__(self, target, args, input_urls):
        """
        Variables and datatypes
        """
        self.target = target
        self.args = args
        self.input_urls = input_urls
        self.cpu_count = multiprocessing.cpu_count()

        # work_dir has following naming convenction: <user supplied dir>/<bd2k-<file_name>/<Random UUID4>/
        self.work_dir = os.path.join(str(self.args.work_dir),
                                     'bd2k-{}'.format(os.path.basename(__file__).split('.')[0]),
                                     str(uuid.uuid4()))

        # Symbolic names for all inputs in the pipeline.
        self.symbolic_inputs = self.input_urls.keys() + ['ref.fai', 'ref.dict', 'normal.bai', 'tumor.bai', 'mutect.vcf']

        # Dictionary of all FileStoreIds for all input files used in the pipeline
        self.ids = {x: target.getEmptyFileStoreID() for x in self.symbolic_inputs}

        # Dictionary of all tools and their associated docker image
        self.tools = {'samtools': 'jvivian/samtools:1.2',
                      'picard': 'jvivian/picardtools:1.113',
                      'mutect': 'jvivian/mutect:1.1.7'}

        # TODO: Should this be a jobTree method of target? "Given a key, tell me if a file is linked to it"
        # Set of symbolic_inputs that have a FileStoreID linked to a file -- removed as not useful.
        # self.StoredSet = set()

    def unavoidable_download_method(self, name):
        """
        Downloads file if not present from supplied URL.
        Updates the FileStoreID to point to a file.
        :name: Key from self.input_urls.
        :returns: Path to file (work_dir path)
        :rtype: str
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

        # Update FileStoreID
        self.target.updateGlobalFile(self.ids[name], file_path)

        return file_path

    def docker_call(self, tool_command, tool_name):
        """
        Makes subprocess call of a command to a docker container.
        Abstracts away the docker commands needed to run the tool.
        :type tool_command: str
        :type tool_name: str
        :param tool_name: a key to the dictionary self.tools
        """
        base_docker_call = 'sudo docker run -v {}:/data'.format(self.work_dir)
        try:
            subprocess.check_call(base_docker_call.split() + [self.tools[tool_name]] + tool_command.split())
        except subprocess.CalledProcessError:
            raise RuntimeError('docker command returned a non-zero exit status. Check error logs.')
        except OSError:
            raise RuntimeError('docker not found on system. Install on all nodes.')

    # TODO: Ask Hannes about @staticmethod as well as _, __ method convention.
    @staticmethod
    def docker_path(filepath):
        return os.path.join('/data', os.path.basename(filepath))

    def read_and_rename_global_file(self, file_store_id, new_extension='', alternate_name=None):
        """
        Given a FileStoreID, returns the filepath linked to it.
        :new_extension: Adds an extension to the file at the file_path.
        :alternate_name: A path to a filename that you want to use. (allows directory control as well as name)
        """
        name = self.target.readGlobalFile(file_store_id)
        new_name = os.path.splitext(name if alternate_name is None else alternate_name)[0] + new_extension
        #new_name = os.path.splitext(name if diff_name is None else os.path.join(self.work_dir, os.path.basename(diff_name)))[0] + new_extension
        shutil.move(name, new_name)

        # Move to work_dir so docker mount works
        shutil.move(new_name, os.path.join(self.work_dir, os.path.basename(new_name)))

        return os.path.join(self.work_dir, os.path.basename(new_name))

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
        The equivalant in bash of: which
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


# TODO: Ask Hannes about installation of software on distributed nodes.
# This function could be considered unnecessary if it's the user's responsibility to install dependency software
def check_for_docker(target, args, input_urls):
    """
    Checks if Docker is present on system -- installs on linux if not present.
    """
    # Since this is the start node, instantiate support class instance to distribute to all other targets.
    sclass = SupportClass(target, args, input_urls)

    if not sclass.which('docker'):

        # TODO: Ask Hannes about making subprocess calls with sudo.
        if 'linux' in sys.platform:
            subprocess.check_call(['sudo', 'apt-get', 'update'])
            subprocess.check_call(['sudo', 'apt-get', 'install', 'linux-image-generic-lts-trusty'])

            if not sclass.which('wget'):
                try:
                    subprocess.check_call(['sudo', 'apt-get', 'install', 'wget'])
                except subprocess.CalledProcessError:
                    raise RuntimeError('Could not install wget, which is required to install Docker')
                except OSError:
                    raise RuntimeError('Could not find apt-get! This OS should (apt) get with the program.')

            try:
                subprocess.check_call(['wget', '-qO-', 'https://get.docker.com/', '|', 'sh'])
            except subprocess.CalledProcessError:
                raise RuntimeError('Failed to install docker on system: https://docs.docker.com/installation/')

            assert sclass.which('docker')

        elif 'darwin' in sys.platform:
            raise RuntimeError('Docker not installed! Install on Mac here: https://docs.docker.com/installation/mac/')

        elif 'win' in sys.platform:
            raise RuntimeError('Docker not installed! Install on Windows: https://docs.docker.com/installation/windows')

        else:
            raise RuntimeError('Docker not installed. Check if available: https://docs.docker.com/installation/')

    target.addChildTargetFn(create_reference_index, (sclass,))
    target.addChildTargetFn(create_reference_dict, (sclass,))
    target.addChildTargetFn(create_normal_index, (sclass,))
    target.addChildTargetFn(create_tumor_index, (sclass,))
    target.setFollowOnTargetFn(mutect, (sclass,))


def create_reference_index(target, sclass):
    """
    Uses Samtools to create reference index file (.fasta.fai)
    """
    # Retrieve reference & store in FileStoreID
    ref_path = sclass.unavoidable_download_method('ref.fasta')

    # Tool call
    command = 'samtools faidx {}'.format(sclass.docker_path(ref_path))
    sclass.docker_call(command, tool_name='samtools')

    # Update FileStoreID of output
    target.updateGlobalFile(sclass.ids['ref.fai'], ref_path + '.fai')


def create_reference_dict(target, sclass):
    """
    Uses Picardtools to create reference dictionary (.dict)
    """
    # Retrieve reference & store in FileStoreID
    ref_path = sclass.unavoidable_download_method('ref.fasta')

    # Tool call
    output = os.path.splitext(sclass.docker_path(ref_path))[0]
    command = 'picard-tools CreateSequenceDictionary R={} O={}.dict'.format(sclass.docker_path(ref_path), output)
    sclass.docker_call(command, tool_name='picard')

    # Update FileStoreID
    target.updateGlobalFile(sclass.ids['ref.dict'], os.path.splitext(ref_path)[0] + '.dict')


def create_normal_index(target, sclass):
    # Retrieve normal bam
    normal_path = sclass.unavoidable_download_method('normal.bam')

    # Tool call
    command = 'samtools index {}'.format(sclass.docker_path(normal_path))
    sclass.docker_call(command, tool_name='samtools')

    # Update FileStoreID
    target.updateGlobalFile(sclass.ids['normal.bai'], normal_path + '.bai')


def create_tumor_index(target, sclass):
    # Retrieve tumor bam
    tumor_path = sclass.unavoidable_download_method('tumor.bam')

    # Tool call
    command = 'samtools index {}'.format(sclass.docker_path(tumor_path))
    sclass.docker_call(command, tool_name='samtools')

    # Update FileStoreID
    target.updateGlobalFile(sclass.ids['tumor.bai'], tumor_path + '.bai')


def mutect(target, sclass):
    # Retrieve inputs that are not in FileStore
    mutect_path = sclass.docker_path(sclass.unavoidable_download_method('mutect.jar'))
    dbsnp_path = sclass.docker_path(sclass.unavoidable_download_method('dbsnp.vcf'))
    cosmic_path = sclass.docker_path(sclass.unavoidable_download_method('cosmic.vcf'))

    # TODO: Figure out how to refactor... the renaming convention makes it difficult.
    # TODO: Otherwise, I would just iterate over the sclass.StoredSet() object.
    normal_bam = sclass.read_and_rename_global_file(sclass.ids['normal.bam'], '.bam')
    tumor_bam = sclass.read_and_rename_global_file(sclass.ids['tumor.bam'], '.bam')
    ref_fasta = sclass.read_and_rename_global_file(sclass.ids['ref.fasta'], '.fasta')
    sclass.read_and_rename_global_file(sclass.ids['normal.bai'], '.bai', normal_bam)
    sclass.read_and_rename_global_file(sclass.ids['tumor.bai'], '.bai', tumor_bam)
    sclass.read_and_rename_global_file(sclass.ids['ref.fai'], '.fasta.fai', ref_fasta)
    sclass.read_and_rename_global_file(sclass.ids['ref.dict'], '.dict', ref_fasta)

    # TODO: Fix this... having to recast these variables is clunky.
    normal_bam = sclass.docker_path(normal_bam)
    tumor_bam = sclass.docker_path(tumor_bam)
    ref_fasta = sclass.docker_path(ref_fasta)

    # Output VCF
    normal_uuid = sclass.input_urls['normal.bam'].split('/')[-1].split('.')[0]
    tumor_uuid = sclass.input_urls['tumor.bam'].split('/')[-1].split('.')[0]
    output = sclass.docker_path('{}-normal:{}-tumor.vcf'.format(normal_uuid, tumor_uuid))
    mut_out = sclass.docker_path('mutect.out')
    mut_cov = sclass.docker_path('mutect.cov')

    # Tool call
    command = 'java -Xmx{0}g -jar {1} ' \
              '--analysis_type MuTect ' \
              '--reference_sequence {2} ' \
              '--cosmic {3} ' \
              '--dbsnp {4} ' \
              '--input_file:normal {5} ' \
              '--input_file:tumor {6} ' \
              '--tumor_lod 10 ' \
              '--out {7} ' \
              '--coverage_file {8} ' \
              '--vcf {9} '.format(15, mutect_path, ref_fasta, cosmic_path, dbsnp_path, normal_bam,
                                  tumor_bam, mut_out, mut_cov, output)
    sclass.docker_call(command, tool_name='mutect')

    # Update FileStoreID TODO: Once AWS is implemented this would upload final result to S3?
    target.updateGlobalFile(sclass.ids['mutect.vcf'],
                            os.path.join(sclass.work_dir, '{}-normal:{}-tumor.vcf'.format(normal_uuid, tumor_uuid)))

    target.addChildTargetFn(teardown, (sclass,))


def teardown(target, sclass):
    files = [os.path.join(sclass.work_dir, f) for f in os.listdir(sclass.work_dir) if 'tumor.vcf' not in f]
    for f in files:
        os.remove(f)


def main():
    # Handle parser logic
    parser = build_parser()
    Stack.addJobTreeOptions(parser)
    args = parser.parse_args()
    # TODO: Ask Hannes the easiest way to obtain "special type" i.e. target instance, args, etc...

    # URLs to rerieve initial input files
    input_urls = {'ref.fasta': args.reference,
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

    # Create JobTree Stack which launches the jobs starting at the "Start Node"
    i = Stack(Target.makeTargetFn(check_for_docker, (args, input_urls))).startJobTree(args)


if __name__ == '__main__':
    # from jobtree_docker_pipeline import *
    main()