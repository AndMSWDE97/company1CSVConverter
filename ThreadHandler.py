import queue, argparse, sys, multiprocessing, converter, time
from pathlib import Path

def checkDirectory(d):
    if d.exists():
        return d.is_dir()
    else:
        d.mkdir(parents=True)
        return True

parser = argparse.ArgumentParser(description='converts files from an input directory containing status events to files in an output directory containing files with status information at a 1 second interval')
parser.add_argument('inputDir',type=Path, help='A directory containing only the .csv files that are to be converted.')
parser.add_argument('outputDir', nargs='?', type=Path, default='../convertedFiles/', help='The directory in which the result files will be placed. Relative Paths are used in relation to the input Path. If the directory does not exist, it will be created. Default is "../convertedFiles/"')
args = parser.parse_args()
if not (args.inputDir.exists() and args.inputDir.is_dir()):
    print('InputDir does not exist')
    sys.exit(2)
if not args.outputDir.is_absolute():
    args.outputDir = args.inputDir.joinpath(args.outputDir)
if not checkDirectory(args.outputDir):
    print('OutputDir exists, but is not a directory')
    sys.exit(2)
inputFiles = args.inputDir.glob('*.csv')
inputQueue = queue.Queue()
for iFile in inputFiles:
    inputQueue.put(iFile)
initialLength = inputQueue.qsize()
threads = []
for i in range(multiprocessing.cpu_count()):
    threads.append(converter.Worker(inputQueue, args.outputDir))
for thread in threads:
    thread.run()
while not inputQueue.empty():
    completion = inputQueue.qsize() / initialLength
    print(f'status: {completion:.2f}%')
    time.sleep(1)
inputQueue.join()
print("completed the conversion")