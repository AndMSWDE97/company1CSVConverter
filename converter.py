from pathlib import Path
import csv, datetime
import threading, queue

class Converter:
    def __init__(self, inputPath, outputDir, interval):
        self._inputPath = Path(inputPath)
        self._outputDirectory = Path(outputDir)
        self._interval = interval
        self._stateList = []
        self._firstEntry = []
        self._lastEntry = []
    
    def analyseInput(self):
        with self._inputPath.open() as f:
            reader = csv.reader(f, delimiter=',')
            for i, line in enumerate(reader):
                timeinfo = line[0]
                statusCode = line[1]
                timeFragments = timeinfo[1:-1].split(',')
                timeFragments[5] = timeFragments[5].split('.')[0]
                timeString = '-'.join(timeFragments).replace(' ','')
                timestamp = datetime.datetime.strptime(timeString, '%Y-%m-%d-%H-%M-%S')
                if i != 0:
                    difference = timestamp - self._lastEntry[0]
                    numberOfSteps = round(difference.total_seconds() / self._interval)
                    self._stateList.append([numberOfSteps, self._lastEntry[1]])
                self._lastEntry = [timestamp, statusCode]
                if i == 0:
                    self._firstEntry = self._lastEntry

    def getMachineNameFromFilname(self, filePath):            
        filename = filePath.stem
        return filename.split('-')[-1]

    def convertToBinary(self, normalOperationCode):
        for item in self._stateList:
            if item[1] == str(normalOperationCode):
                item[1] = 0
            else:
                item[1] = 1

    def writeToFile(self, useShortNotationForm):
        machineName = self.getMachineNameFromFilname(self._inputPath)
        outputPath = self._outputDirectory.joinpath(f'{machineName}_' + self._firstEntry[0].strftime('%Y-%m-%d') + '.csv')
        if not outputPath.exists():
            outputPath.touch()
        headerArray = ['$Machine',machineName,'Start', self._firstEntry[0].strftime('%Y-%m-%d-%H-%M-%S'),'End',self._lastEntry[0].strftime('%Y-%m-%d-%H-%M-%S'),'Interval',str(self._interval * 1000)+'ms$']
        contentArray = []
        for state in self._stateList:
            if useShortNotationForm:
                contentArray.append(f'{state[0]}|{state[1]}')
            else:
                for i in range(state[0]):
                    contentArray.append(str(state[1]))
        with outputPath.open('w') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(headerArray)
            writer.writerow(contentArray)


class Worker(threading.Thread):
    def __init__(self, inputQueue, outputDirectory):
        super().__init__()
        self._inputQueue = inputQueue
        self._outputDirectory = outputDirectory
    
    def run(self):
        while not self._inputQueue.empty():
            inputPath = self._inputQueue.get()
            converter = Converter(inputPath, self._outputDirectory, 1)
            converter.analyseInput()
            converter.convertToBinary(2)
            converter.writeToFile(True)
            self._inputQueue.task_done()
            


        

if __name__ == '__main__':
    conv = Converter('D:/andre/Documents/Studium/IPE/ersatzleistung/company1/raw_data/2013-7-22-2013-8-7-BFD1AR01.csv','D:/andre/Documents/Studium/IPE/ersatzleistung/company1/test/', 1)
    conv.analyseInput()
    conv.convertToBinary(2)
    conv.writeToFile(True)