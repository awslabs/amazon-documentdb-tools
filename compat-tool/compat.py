#!/usr/bin/python3
import pathlib
import os
import sys
import re
import argparse
import json
try:
    import pymongo
except:
    pass


versions = ['3.6','4.0','5.0.0','5.0.1','8.0','8.0.1','8.0.2','8.0.3','EC5.0']
processingFeedbackLines = 10000
issuesDict = {}
detailedIssuesDict = {}
supportedDict = {}
skippedFileList = []
exceptionFileList = []
numProcessedFiles = 0
skippedDirectories = []


def ensureDirect(uri):
    # make sure we are directly connecting to the server requested, not via replicaSet

    connInfo = {}
    parsedUri = pymongo.uri_parser.parse_uri(uri)

    for thisKey in sorted(parsedUri['options'].keys()):
        if thisKey.lower() not in ['replicaset','readpreference','readpreferencetags']:
            connInfo[thisKey] = parsedUri['options'][thisKey]

    # make sure we are using directConnection=true
    connInfo['directconnection'] = True

    connInfo['username'] = parsedUri['username']
    connInfo['password'] = parsedUri['password']
    connInfo['host'] = parsedUri['nodelist'][0][0]
    connInfo['port'] = parsedUri['nodelist'][0][1]

    print(" + connecting to the server at {}:{}".format(connInfo['host'],connInfo['port']))

    # use the URI path database as authSource only when authSource is not explicitly provided
    if parsedUri.get('database') is not None and 'authsource' not in connInfo:
        connInfo['authsource'] = parsedUri['database']

    return connInfo


def double_check(checkOperator, checkLine, checkLineLength):
    foundOperator = False
    
    for match in re.finditer(re.escape(checkOperator), checkLine):
        if (match.end() == checkLineLength) or (not checkLine[match.end()].isalpha()):
            foundOperator = True
            break
    
    return foundOperator


def read_file_lines(fileName):
    # returns the lines of a text file, or None if the file can't be read or appears to be binary
    try:
        with open(fileName, "rb") as binaryFile:
            if b"\0" in binaryFile.read(8192):
                print("  binary file, skipping")
                return None

        # always decode as UTF-8 (not the platform default), replace invalid bytes so one bad byte doesn't hide the rest of the file
        with open(fileName, "r", encoding="utf-8", errors="replace") as textFile:
            return textFile.readlines()

    except OSError as e:
        print("  unable to read file ({}), skipping".format(e.strerror or e))
        return None

    except MemoryError:
        print("  not enough memory to read file, skipping")
        return None


def directory_id(dirPath):
    # identify a directory by device and inode so path spelling, trailing slashes, separators, case-insensitive filesystems, and symlinks all compare correctly
    try:
        dirStat = os.stat(dirPath)
    except OSError:
        return None
    if dirStat.st_ino == 0:
        # some filesystems (FAT, certain network shares) do not provide inode numbers, fall back to the resolved path
        return os.path.normcase(os.path.realpath(dirPath))
    return (dirStat.st_dev, dirStat.st_ino)


def scan_code(args, keywords):
    global numProcessedFiles, issuesDict, detailedIssuesDict, supportedDict, skippedFileList, exceptionFileList, skippedDirectories
    
    ver = args.version

    usage_map = {}
    cmd_map = {}
    line_ct = 0
    totalLines = 0
    
    # create the file or list of files
    fileArray = []
    
    includedExtensions = []
    if args.includedExtensions != "ALL":
        includedExtensions = args.includedExtensions.lower().split(",")
    excludedExtensions = []
    if args.excludedExtensions != "NONE":
        excludedExtensions = args.excludedExtensions.lower().split(",")
    
    excludedDirectoryIds = set()
    if args.excludedDirectories != "NONE":
        for thisExcludedDirectory in args.excludedDirectories.split(","):
            thisExcludedDirectory = thisExcludedDirectory.strip()
            if thisExcludedDirectory == "":
                continue
            thisId = directory_id(thisExcludedDirectory)
            if thisId is None or not os.path.isdir(thisExcludedDirectory):
                print("WARNING - excluded directory not found: {}".format(thisExcludedDirectory))
            else:
                excludedDirectoryIds.add(thisId)

    # version control metadata directories are never scanned
    vcsDirectories = ['.git','.hg','.svn']

    if args.scanFile is not None:
        fileArray.append(args.scanFile)
    else:
        # os.walk includes hidden files and directories (glob does not), and allows pruning excluded directories
        visitedDirectoryIds = {directory_id(args.scanDir)}
        for dirPath, dirNames, fileNames in os.walk(args.scanDir, followlinks=True):
            keepDirNames = []
            for thisDirName in sorted(dirNames):
                thisDirPath = os.path.join(dirPath, thisDirName)
                thisId = directory_id(thisDirPath)
                if thisDirName in vcsDirectories or thisId in excludedDirectoryIds:
                    # add to skipped directory list, do not descend
                    skippedDirectories.append(thisDirPath)
                elif thisId is None or thisId in visitedDirectoryIds:
                    # unreadable, or already scanned via a symlink (also prevents symlink loops)
                    continue
                else:
                    visitedDirectoryIds.add(thisId)
                    keepDirNames.append(thisDirName)
            dirNames[:] = keepDirNames

            for thisFileName in sorted(fileNames):
                filename = os.path.join(dirPath, thisFileName)
                if os.path.isfile(filename):
                    if ((pathlib.Path(filename).suffix[1:].lower() not in excludedExtensions) and
                         ((args.includedExtensions == "ALL") or
                          (pathlib.Path(filename).suffix[1:].lower() in includedExtensions))):
                        fileArray.append(filename)
                    else:
                        skippedFileList.append(filename)
                   
                    
    for thisFile in fileArray:
        print("processing file {}".format(thisFile))
        fileLines = read_file_lines(thisFile)
        if fileLines is None:
            exceptionFileList.append(thisFile)
            continue
        numProcessedFiles += 1
            
        fileLineNum = 1
        
        for lineNum, thisLine in enumerate(fileLines):
            # Normalize Kotlin/Groovy template string escapes: ${'$'} -> $
            thisLine = thisLine.replace("${'$'}", "$")
            thisLineLength = len(thisLine)
            
            for checkCompat in keywords:
                if (keywords[checkCompat][ver] == 'No'):
                    # only check for unsupported operators
                    if (thisLine.find(checkCompat) >= 0):
                        # check for false positives - for each position found see if next character is not a..z|A..Z or if at EOL
                        if double_check(checkCompat, thisLine, thisLineLength):
                            # add it to the counters
                            if checkCompat in issuesDict:
                                issuesDict[checkCompat] += 1
                            else:
                                issuesDict[checkCompat] = 1
                            # add it to the filenames/line-numbers
                            if checkCompat in detailedIssuesDict:
                                if thisFile in detailedIssuesDict[checkCompat]:
                                    detailedIssuesDict[checkCompat][thisFile].append(fileLineNum)
                                else:
                                    detailedIssuesDict[checkCompat][thisFile] = [fileLineNum]
                            else:
                                detailedIssuesDict[checkCompat] = {}
                                detailedIssuesDict[checkCompat][thisFile] = [fileLineNum]

                elif (keywords[checkCompat][ver] == 'Yes'):
                    # check for supported operators
                    if (thisLine.find(checkCompat) >= 0):
                        # check for false positives - for each position found see if next character is not a..z|A..Z or if at EOL
                        if double_check(checkCompat, thisLine, thisLineLength):
                            if checkCompat in supportedDict:
                                supportedDict[checkCompat] += 1
                            else:
                                supportedDict[checkCompat] = 1
                            
            if (fileLineNum % processingFeedbackLines) == 0:
                print("  processing line {}".format(fileLineNum))
            fileLineNum += 1


def getOperatorsFromServer(args):
    fullListDict = {}
    filteredOpsList = ['$alwaysFalse','$alwaysTrue','$backupCursor','$backupCursorExtend','$const','$listCachedAndActiveUsers','$listCatalog','$listClusterCatalog','$mergeCursors','$operationMetrics',
                       '$queue','$searchBeta','$setMetadata','$setVariableFromSubPipeline']

    client = pymongo.MongoClient(**ensureDirect(args.uri))
    serverStatus = client.admin.command("serverStatus")
    client.close()

    # uptime
    upSeconds = serverStatus.get('uptime',-1)
    print(" + database server has been up for {:.2f} days".format(upSeconds/86400))

    # get/check version
    majorVersion = int(serverStatus.get('version','0').split('.')[0])
    print(" + database server major version is {}".format(majorVersion))
    if majorVersion < 5:
        print("This tool is only supported for version 5+")
        sys.exit(1)

    for thisKey in serverStatus['metrics']['aggStageCounters']:
        if type(serverStatus['metrics']['aggStageCounters'][thisKey]) is dict:
            for thisKey2 in serverStatus['metrics']['aggStageCounters'][thisKey]:
                if not thisKey2.startswith("$_") and thisKey2 not in filteredOpsList:
                    if thisKey2 in fullListDict:
                        fullListDict[thisKey2] += serverStatus['metrics']['aggStageCounters'][thisKey][thisKey2]
                    else:
                        fullListDict[thisKey2] = serverStatus['metrics']['aggStageCounters'][thisKey][thisKey2]
        else:
            if not thisKey.startswith("$_") and thisKey not in filteredOpsList:
               if thisKey in fullListDict:
                   fullListDict[thisKey] += serverStatus['metrics']['aggStageCounters'][thisKey]
               else:
                   fullListDict[thisKey] = serverStatus['metrics']['aggStageCounters'][thisKey]

    for thisKey in serverStatus['metrics']['operatorCounters']:
        if type(serverStatus['metrics']['operatorCounters'][thisKey]) is dict:
            for thisKey2 in serverStatus['metrics']['operatorCounters'][thisKey]:
                if not thisKey2.startswith("$_") and thisKey2 not in filteredOpsList:
                    if thisKey2 in fullListDict:
                        fullListDict[thisKey2] += serverStatus['metrics']['operatorCounters'][thisKey][thisKey2]
                    else:
                        fullListDict[thisKey2] = serverStatus['metrics']['operatorCounters'][thisKey][thisKey2]
        else:
            if not thisKey.startswith("$_") and thisKey not in filteredOpsList:
                if thisKey in fullListDict:
                    fullListDict[thisKey] += serverStatus['metrics']['operatorCounters'][thisKey]
                else:
                    fullListDict[thisKey] = serverStatus['metrics']['operatorCounters'][thisKey]

    return fullListDict


def main(args):
    parser = argparse.ArgumentParser(description="Parse the command line.")

    group = parser.add_argument_group('scan mode','technique to test compatibility')
    exclusiveGroup = group.add_mutually_exclusive_group(required=True)

    exclusiveGroup.add_argument("--directory", dest="scanDir", action="store", help="Directory containing profiled log files or source code files to scan for compatibility", required=False)
    exclusiveGroup.add_argument("--file", dest="scanFile", action="store", help="Specific log file or source code file to scan for compatibility", required=False)
    exclusiveGroup.add_argument("--uri", dest="uri", action="store", help="URI of MongoDB server for compatibility check", required=False)

    parser.add_argument("--excluded-extensions", dest="excludedExtensions", action="store", default="NONE", help="Filename extensions to exclude from scanning, comma separated", required=False)
    parser.add_argument("--included-extensions", dest="includedExtensions", action="store", default="ALL", help="Filename extensions to include in scanning, comma separated", required=False)
    parser.add_argument("--excluded-directories", dest="excludedDirectories", action="store", default="NONE", help="directories to exclude from scanning, comma separated", required=False)
    parser.add_argument("--version", dest="version", action="store", default="8.0.3", help="Check for DocumentDB version compatibility [default: %(default)s]", choices=versions, required=False)

    args = parser.parse_args()
    
    if args.scanFile is not None and not os.path.isfile(args.scanFile):
        parser.error("unable to locate file {}".format(args.scanFile))
    
    elif args.scanDir is not None and not os.path.isdir(args.scanDir):
        parser.error("unable to locate directory {}".format(args.scanDir))
        
    keywords = load_keywords()


    if args.uri is not None:
        # check for compatibility using db.serverStatus()
        print("Gathering usage data for analysis")

        ver = args.version
        notCompatCounter = 0
        compatCounter = 0
        usageDict = getOperatorsFromServer(args)
        print(" + checking compatibility using db.serverStatus()")

        # get count of compatible and incompatible operators found
        for thisKey in sorted(usageDict.keys()):
            if (usageDict[thisKey] > 0) and ((thisKey not in keywords) or (keywords[thisKey][ver] == 'No')):
                notCompatCounter += 1
            elif (usageDict[thisKey] > 0) and (keywords[thisKey][ver] == 'Yes'):
                compatCounter += 1

        print("")
        # unsupported operators
        if notCompatCounter > 0:
            print("The following {} unsupported operators were found:".format(notCompatCounter))
            for thisKey in sorted(usageDict.keys()):
                if (usageDict[thisKey] > 0) and (thisKey not in keywords):
                    print("  {} | executed {} time(s) - WARNING - operator is missing from compat tool, please file an issue".format(thisKey,usageDict[thisKey]))
                elif (usageDict[thisKey] > 0) and (keywords[thisKey][ver] == 'No'):
                    print("  {} | executed {} time(s)".format(thisKey,usageDict[thisKey]))
        else:
            print("No unsupported operators found.")

        print("")
        # supported operators
        if compatCounter > 0:
            print("The following {} supported operators were found:".format(compatCounter))
            for thisKey in sorted(usageDict.keys()):
                if (usageDict[thisKey] > 0) and (thisKey in keywords) and (keywords[thisKey][ver] == 'Yes'):
                    print("  {} | executed {} time(s)".format(thisKey,usageDict[thisKey]))
        else:
            print("WARNING - No supported operators found, check that the URI provided is correct")

        print("")

        sys.exit(0)

    scan_code(args, keywords)
    
    print("")
    print("Processed {} files, skipped {} files".format(numProcessedFiles,len(skippedFileList)+len(exceptionFileList)))

    if len(issuesDict) > 0:
        print("")
        print("The following {} unsupported operators were found:".format(len(issuesDict)))
        for thisKeyPair in sorted(issuesDict.items(), key=lambda x: (-x[1],x[0])):
            print("  {} | found {} time(s)".format(thisKeyPair[0],thisKeyPair[1]))
            
        # output detailed unsupported operator findings
        print("")
        print("Unsupported operators by filename and line number:")
        for thisKeyPair in sorted(issuesDict.items(), key=lambda x: (-x[1],x[0])):
            print("  {} | lines = found {} time(s)".format(thisKeyPair[0],thisKeyPair[1]))
            for thisFile in detailedIssuesDict[thisKeyPair[0]]:
                print("    {} | lines = {}".format(thisFile,detailedIssuesDict[thisKeyPair[0]][thisFile]))
        
    else:
        print("")
        print("No unsupported operators found.")

    if len(supportedDict) > 0:
        print("")
        print("The following {} supported operators were found:".format(len(supportedDict)))
        for thisKeyPair in sorted(supportedDict.items(), key=lambda x: (-x[1],x[0])):
            print("  {} | found {} time(s)".format(thisKeyPair[0],thisKeyPair[1]))
    else:
        print("")
        print("WARNING - No supported operators found, check that profiling is enabled if scanning logs or using the correct path to scan source code")

    if len(skippedFileList) > 0:
        print("")
        print("List of skipped files - excluded extensions")
        for skippedFile in skippedFileList:
            print("  {}".format(skippedFile))

    if len(exceptionFileList) > 0:
        print("")
        print("List of skipped files - binary or unreadable")
        for exceptionFile in exceptionFileList:
            print("  {}".format(exceptionFile))
    
    if len(skippedDirectories) > 0:
        print("")
        print("List of skipped directories - excluded directories")
        for skippedDirectory in skippedDirectories:
            print("  {}".format(skippedDirectory))

    print("")

    if len(issuesDict) > 0:
        sys.exit(1)
    else:
        sys.exit(0)


def load_keywords():
    thisKeywords = {
        "$$CURRENT":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$$DESCEND":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$$IDX":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$$KEEP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$$PRUNE":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$$REMOVE":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$$ROOT":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$abs":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$accumulator":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$acos":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$acosh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$add":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$addFields":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$addToSet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$all":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$allElementsTrue":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$and":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$anyElementTrue":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$arrayElemAt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$arrayToObject":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$asin":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$asinh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$atan":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$atan2":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$atanh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$avg":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$binarySize":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bit":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitAnd":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitNot":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitOr":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitsAllClear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitsAllSet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitsAnyClear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitsAnySet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bitXor":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bottom":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bottomN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$box":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$bsonSize":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bucket":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$bucketAuto":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$ceil":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$center":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$centerSphere":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$changeStream":{'mongodbversion': '3.6', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$changeStreamSplitLargeEvent":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$cmp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$collStats":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$comment":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$concat":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$concatArrays":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$cond":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$convert":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$cos":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$cosh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$count":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$covariancePop":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$covarianceSamp":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$createObjectId":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$currentDate":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$currentOp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateAdd":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateDiff":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$dateFromParts":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateFromString":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateSubtract":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateToParts":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateToString":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dateTrunc":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dayOfMonth":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dayOfWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$dayOfYear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$degreesToRadians":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$denseRank":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$densify":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$derivative":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$deserializeEJSON":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$divide":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$documentNumber":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$documents":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$each":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$elemMatch":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$encStrContains":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$encStrEndsWith":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$encStrNormalizedEq":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$encStrStartsWith":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$eq":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$exists":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$exp":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$expMovingAvg":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$expr":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$facet":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$fill":{'mongodbversion': '5.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$filter":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$first":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$firstN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$floor":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$function":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$geoIntersects":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$geometry":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$geoNear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$geoWithin":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$getField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$graphLookup":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$group":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$gt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$gte":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$hash":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$hexHash":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$hour":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$ifNull":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$in":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$inc":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$indexOfArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$indexOfBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$indexOfCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$indexStats":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$integral":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$isArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$isNumber":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$isoDayOfWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$isoWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$isoWeekYear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$jsonSchema":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$last":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$lastN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$let":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$limit":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$linearFill":{'mongodbversion': '5.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$listLocalSessions":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$listMqlEntities":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$listSampledQueries":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$listSearchIndexes":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$listSessions":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$literal":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$ln":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$locf":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$log":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$log10":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$lookup":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$lt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$lte":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$ltrim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$map":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$match":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$max":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$maxDistance":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$maxN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$median":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$merge":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$mergeObjects":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$meta":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$millisecond":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$min":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$minDistance":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$minMaxScaler":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$minN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$minute":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$mod":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$month":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$mul":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$multiply":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$natural":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$ne":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$near":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$nearSphere":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$nin":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$nor":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$not":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$objectToArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$or":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$out":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$percentile":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$planCacheStats":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$polygon":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$pop":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$position":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$pow":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$project":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$pull":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$pullAll":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$push":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$querySettings":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$queryStats":{'mongodbversion': '7.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$radiansToDegrees":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$rand":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$range":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$rank":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$rankFusion":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$redact":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$reduce":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$regex":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$regexFind":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$regexFindAll":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$regexMatch":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$rename":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$replaceAll":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$replaceOne":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$replaceRoot":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$replaceWith":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$reverseArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$round":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$rtrim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sample":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sampleRate":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$score":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$scoreFusion":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$search":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$searchMeta":{'mongodbversion': 'atlas', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$second":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$serializeEJSON":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$set":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setDifference":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setEquals":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$setIntersection":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setIsSubset":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setOnInsert":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setUnion":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$setWindowFields":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$shardedDataDistribution":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$shift":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$sigmoid":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$similarityCosine":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$similarityDotProduct":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$similarityEuclidean":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$sin":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sinh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$size":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$skip":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$slice":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sort":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sortArray":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sortByCount":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$split":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$sqrt":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$stdDevPop":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$stdDevSamp":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$strcasecmp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$strLenBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$strLenCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$substr":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$substrBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$substrCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$subtract":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$subtype":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$sum":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$switch":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$tan":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$tanh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$text":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toArray":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$toBool":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toDate":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toDecimal":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toDouble":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toHashedIndexKey":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$toInt":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toLong":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toLower":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toObject":{'mongodbversion': '8.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$toObjectId":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$top":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$topN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toString":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toUpper":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$toUUID":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$trim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$trunc":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$tsIncrement":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$tsSecond":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$type":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$unionWith":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'Yes'},
        "$uniqueDocs":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$unset":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$unsetField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$unwind":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$vectorSearch":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$week":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$where":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No', '8.0.2': 'No', '8.0.3': 'No'},
        "$year":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
        "$zip":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes', '8.0.2': 'Yes', '8.0.3': 'Yes'},
    }
        
    return thisKeywords

    
if __name__ == '__main__':
    main(sys.argv[1:])
