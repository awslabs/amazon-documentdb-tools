#!/usr/bin/python3
import glob
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


versions = ['3.6','4.0','5.0.0','5.0.1','8.0','8.0.1','EC5.0']
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
        if thisKey.lower() not in ['replicaset','readpreference']:
            connInfo[thisKey] = parsedUri['options'][thisKey]

    # make sure we are using directConnection=true
    connInfo['directconnection'] = True

    connInfo['username'] = parsedUri['username']
    connInfo['password'] = parsedUri['password']
    connInfo['host'] = parsedUri['nodelist'][0][0]
    connInfo['port'] = parsedUri['nodelist'][0][1]

    print(" + connecting to the server at {}:{}".format(connInfo['host'],connInfo['port']))

    if parsedUri.get('database') is not None:
        connInfo['authSource'] = parsedUri['database']

    return connInfo


def double_check(checkOperator, checkLine, checkLineLength):
    foundOperator = False
    
    for match in re.finditer(re.escape(checkOperator), checkLine):
        if (match.end() == checkLineLength) or (not checkLine[match.end()].isalpha()):
            foundOperator = True
            break
    
    return foundOperator


def check_all_parents(fileName, excludedDirectories):
    retVal = False

    for thisExcludedDirectory in excludedDirectories:
        if fileName.startswith(thisExcludedDirectory+'/'):
            retVal = True
            break

    return retVal


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
    if args.includedExtensions != "NONE":
        excludedExtensions = args.excludedExtensions.lower().split(",")
    
    excludedDirectories = []
    if args.excludedDirectories != "NONE":
        excludedDirectories = args.excludedDirectories.lower().split(",")
    if args.scanFile is not None:
        fileArray.append(args.scanFile)
        numProcessedFiles += 1
    else:
        for filename in glob.iglob("{}/**".format(args.scanDir), recursive=True):
            if os.path.isdir(filename) and filename in excludedDirectories:
                # add to skipped directory list
                skippedDirectories.append(filename) 
            elif check_all_parents(filename, excludedDirectories): 
                # move on
                continue
            else:
                if os.path.isfile(filename):
                    if ((pathlib.Path(filename).suffix[1:].lower() not in excludedExtensions) and
                         ((args.includedExtensions == "ALL") or 
                          (pathlib.Path(filename).suffix[1:].lower() in includedExtensions))):
                        fileArray.append(filename)
                        numProcessedFiles += 1
                    else:
                        skippedFileList.append(filename)
                   
                    
    for thisFile in fileArray:
        print("processing file {}".format(thisFile))
        with open(thisFile, "r") as code_file:
            # line by line technique
            try:
                fileLines = code_file.readlines()
            except:
                print("  exception reading file, skipping")
                exceptionFileList.append(thisFile)
                continue
                
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
                        fullListDict[thisKey2] = serverStatus['metrics']['operatorCounters'][thisKey][thisKey2]
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
    parser.add_argument("--version", dest="version", action="store", default="8.0.1", help="Check for DocumentDB version compatibility (default is 8.0.1)", choices=versions, required=False)

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
            if (usageDict[thisKey] > 0) and (keywords[thisKey][ver] == 'No'):
                notCompatCounter += 1
            elif (usageDict[thisKey] > 0) and (keywords[thisKey][ver] == 'Yes'):
                compatCounter += 1

        print("")
        # unsupported operators
        if notCompatCounter > 0:
            print("The following {} unsupported operators were found:".format(notCompatCounter))
            for thisKey in sorted(usageDict.keys()):
                if (thisKey not in keywords):
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
                if (usageDict[thisKey] > 0) and (keywords[thisKey][ver] == 'Yes'):
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
        print("List of skipped files - unsupported file type/content")
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
        "$$CURRENT":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$$DESCEND":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$$KEEP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$$PRUNE":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$$REMOVE":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$$ROOT":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$abs":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$accumulator":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$acos":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$acosh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$add":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$addFields":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$addToSet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$all":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$allElementsTrue":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$and":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$anyElementTrue":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$arrayElemAt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$arrayToObject":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$asin":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$asinh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$atan":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$atan2":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$atanh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$avg":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$binarySize":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bit":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bitAnd":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bitNot":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bitOr":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bitXor":{'mongodbversion': '6.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bitsAllClear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bitsAllSet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bitsAnyClear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bitsAnySet":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bottom":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bottomN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$box":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$bsonSize":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$bucket":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$bucketAuto":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$ceil":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$center":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$centerSphere":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$changeStream":{'mongodbversion': '3.6', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$changeStreamSplitLargeEvent":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$cmp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$collStats":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$comment":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$concat":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$concatArrays":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$cond":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$covariancePop":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$covarianceSamp":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$convert":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$cos":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$cosh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$count":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$currentDate":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$currentOp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateAdd":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateDiff":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$dateFromParts":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateFromString":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateSubtract":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateToParts":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateToString":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dateTrunc":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dayOfMonth":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dayOfWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$dayOfYear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$degreesToRadians":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$denseRank":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$densify":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$derivative":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$divide":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$documentNumber":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$documents":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$each":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$elemMatch":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$encStrContains":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$encStrEndsWith":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$encStrNormalizedEq":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$encStrStartsWith":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$eq":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$exists":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$exp":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$expMovingAvg":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$expr":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$facet":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$fill":{'mongodbversion': '5.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$filter":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$first":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$firstN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$floor":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$function":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$geoIntersects":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$geometry":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$geoNear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$geoWithin":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$getField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$graphLookup":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$group":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$gt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$gte":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$hour":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$ifNull":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$in":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$inc":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$indexOfArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$indexOfBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$indexOfCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$indexStats":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$integral":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$isArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$isNumber":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$isoDayOfWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$isoWeek":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$isoWeekYear":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$jsonSchema":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$last":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$lastN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$let":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$limit":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$linearFill":{'mongodbversion': '5.3', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$listLocalSessions":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$listMqlEntities":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$listSampledQueries":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$listSearchIndexes":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$listSessions":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$literal":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$ln":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$locf":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$log":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$log10":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$lookup":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$lt":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$lte":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$ltrim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$map":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$match":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$max":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$maxDistance":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$maxN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$median":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$merge":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$mergeObjects":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$meta":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$millisecond":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$min":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$minDistance":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$minMaxScalar":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$minN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$minute":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$mod":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$month":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$mul":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$multiply":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$natural":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$ne":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$near":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$nearSphere":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$nin":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$nor":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$not":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$objectToArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$or":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$out":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$percentile":{'mongodbversion': '7.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$planCacheStats":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$polygon":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$pop":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$position":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$pow":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$project":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$pull":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$pullAll":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$push":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$querySettings":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$queryStats":{'mongodbversion': '7.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$radiansToDegrees":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$rand":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$range":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$rank":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$rankFusion":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$redact":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$reduce":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$regex":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$regexFind":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$regexFindAll":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$regexMatch":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$rename":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$replaceAll":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$replaceOne":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$replaceRoot":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$replaceWith":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$reverseArray":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$round":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$rtrim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sample":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sampleRate":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$score":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$scoreFusion":{'mongodbversion': '8.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$search":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$searchMeta":{'mongodbversion': 'atlas', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$second":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$set":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setDifference":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setEquals":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$setIntersection":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setIsSubset":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setOnInsert":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setUnion":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$setWindowFields":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$shardedDataDistribution":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$shift":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$sigmoid":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$similarityCosine":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$similarityDotProduct":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$similarityEuclidean":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$sin":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$sinh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$size":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$skip":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$slice":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sort":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sortArray":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sortByCount":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$split":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sqrt":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$stdDevPop":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$stdDevSamp":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$strcasecmp":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$strLenBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$strLenCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$substr":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$substrBytes":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$substrCP":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$subtract":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$sum":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$switch":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$tan":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$tanh":{'mongodbversion': '4.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$text":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toBool":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toDate":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toDecimal":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toDouble":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toInt":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toLong":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toLower":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toObjectId":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$top":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$topN":{'mongodbversion': '5.2', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$toHashedIndexKey":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$toString":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toUpper":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$toUUID":{'mongodbversion': '8.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$trim":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$trunc":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$tsIncrement":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$tsSecond":{'mongodbversion': '5.1', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'Yes'},
        "$type":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$unionWith":{'mongodbversion': '4.4', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$uniqueDocs":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$unset":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$unsetField":{'mongodbversion': '5.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$unwind":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$vectorSearch":{'mongodbversion': '6.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$week":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$where":{'mongodbversion': '4.0', '3.6': 'No', '4.0': 'No', '5.0.0': 'No', '5.0.1': 'No', 'EC5.0': 'No', '8.0': 'No', '8.0.1': 'No'},
        "$year":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
        "$zip":{'mongodbversion': '4.0', '3.6': 'Yes', '4.0': 'Yes', '5.0.0': 'Yes', '5.0.1': 'Yes', 'EC5.0': 'Yes', '8.0': 'Yes', '8.0.1': 'Yes'},
    }
        
    return thisKeywords

    
if __name__ == '__main__':
    main(sys.argv[1:])
