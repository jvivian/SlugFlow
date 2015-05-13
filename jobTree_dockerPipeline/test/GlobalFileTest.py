from optparse import OptionParser
from jobTree.src.stack import Stack
from jobTree.src.target import Target

def test(target):
    a = target.getEmptyFileStoreID()
    with open('log.txt', 'w') as f:
        f.write('\n{}'.format(a))
        f.write('\n{}'.format(target.readGlobalFile(a)))
        target.updateGlobalFile(a, 'log.txt')
        f.write('\n{}'.format(target.readGlobalFile(a)))

if __name__ == '__main__':

    # Boilerplate -- startJobTree requires options
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(Target.makeTargetFn(test)).startJobTree(options)