#!/usr/bin/python3
import glob
import pathlib
import os
import sys
import re
import argparse


versions = ['3.6','4.0','5.0','EC5.0']
processingFeedbackLines = 10000
issuesDict = {}
detailedIssuesDict = {}
supportedDict = {}
skippedFileList = []
exceptionFileList = []
numProcessedFiles = 0
skippedDirectories = []


def double_check(checkOperator, checkLine, checkLineLength):
    foundOperator = False
    
    for match in re.finditer(re.escape(checkOperator), checkLine):
        if (match.end() == checkLineLength) or (not checkLine[match.end()].isalpha()):
            foundOperator = True
            break
    
    return foundOperator


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
            if os.path.isdir(filename) and os.path.basename(filename) in excludedDirectories:
                skippedDirectories.append(filename) 
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
        

def main(args):
    parser = argparse.ArgumentParser(description="Parse the command line.")
    parser.add_argument("--version", dest="version", action="store", default="5.0", help="Check for DocumentDB version compatibility (default is 5.0)", choices=versions, required=False)
    parser.add_argument("--directory", dest="scanDir", action="store", help="Directory containing files to scan for compatibility", required=False)
    parser.add_argument("--file", dest="scanFile", action="store", help="Specific file to scan for compatibility", required=False)
    parser.add_argument("--excluded-extensions", dest="excludedExtensions", action="store", default="NONE", help="Filename extensions to exclude from scanning, comma separated", required=False)
    parser.add_argument("--included-extensions", dest="includedExtensions", action="store", default="ALL", help="Filename extensions to include in scanning, comma separated", required=False)
    parser.add_argument("--excluded-directories", dest="excludedDirectories", action="store", default="NONE", help="directories to exclude from scanning, comma separated", required=False)
    args = parser.parse_args()
    
    if args.scanDir is None and args.scanFile is None:
        parser.error("at least one of --directory and --file required")

    elif args.scanDir is not None and args.scanFile is not None:
        parser.error("must provide exactly one of --directory or --file required, not both")
    
    elif args.scanFile is not None and not os.path.isfile(args.scanFile):
        parser.error("unable to locate file {}".format(args.scanFile))
    
    elif args.scanDir is not None and not os.path.isdir(args.scanDir):
        parser.error("unable to locate directory {}".format(args.scanDir))
        
    keywords = load_keywords()
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
            print("  - {} | found {} time(s)".format(thisKeyPair[0],thisKeyPair[1]))

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
        "$$CURRENT":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$$DESCEND":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$$KEEP":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$$PRUNE":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$$REMOVE":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$$ROOT":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$abs":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$accumulator":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$acos":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$acosh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$add":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$addFields":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$addToSet":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$all":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$allElementsTrue":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$and":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$anyElementTrue":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$arrayElemAt":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$arrayToObject":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$asin":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$asinh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$atan":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$atan2":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$atanh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$avg":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$binarySize":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bit":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$bitAnd":{"mongodbversion":"6.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bitNot":{"mongodbversion":"6.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bitOr":{"mongodbversion":"6.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bitXor":{"mongodbversion":"6.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bitsAllClear":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$bitsAllSet":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$bitsAnyClear":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$bitsAnySet":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$bottom":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bottomN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$box":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bsonSize":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bucket":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$bucketAuto":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$ceil":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$center":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$centerSphere":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$changeStreamSplitLargeEvent":{"mongodbversion":"7.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$cmp":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$collStats":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$comment":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$concat":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$concatArrays":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$cond":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$convert":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$cos":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$cosh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$count":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$currentDate":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$currentOp":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$dateAdd":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"Yes"},
        "$dateDiff":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$dateFromParts":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$dateFromString":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$dateSubtract":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"Yes"},
        "$dateToParts":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$dateToString":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$dateTrunc":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$dayOfMonth":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$dayOfWeek":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$dayOfYear":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$degreesToRadians":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$densify":{"mongodbversion":"5.1","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$divide":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$documents":{"mongodbversion":"5.1","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$each":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$elemMatch":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$eq":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$exists":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$exp":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$expr":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$facet":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$fill":{"mongodbversion":"5.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$filter":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$first":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$firstN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$floor":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$function":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$geoIntersects":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$geometry":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$geoNear":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$geoWithin":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$getField":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$graphLookup":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$group":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$gt":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$gte":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$hour":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$ifNull":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$in":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$inc":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$indexOfArray":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$indexOfBytes":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$indexOfCP":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$indexStats":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$isArray":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$isNumber":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$isoDayOfWeek":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$isoWeek":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$isoWeekYear":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$jsonSchema":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$last":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$lastN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$let":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$limit":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$linearFill":{"mongodbversion":"5.3","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$listLocalSessions":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$listSampledQueries":{"mongodbversion":"7.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$listSearchIndexes":{"mongodbversion":"7.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$listSessions":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$literal":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$ln":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$locf":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$log":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$log10":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$lookup":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$lt":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$lte":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$ltrim":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$map":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$match":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$max":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$maxDistance":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$maxN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$median":{"mongodbversion":"7.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$merge":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$mergeObjects":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$meta":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"No"},
        "$millisecond":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$min":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$minDistance":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$minN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$minute":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$mod":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$month":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$mul":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$multiply":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$natural":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$ne":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$near":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$nearSphere":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$nin":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$nor":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$not":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$objectToArray":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$or":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$out":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$percentile":{"mongodbversion":"7.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$planCacheStats":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$polygon":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$pop":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$position":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$pow":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$project":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$pull":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$pullAll":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$push":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$queryStats":{"mongodbversion":"7.1","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$radiansToDegrees":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$rand":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$range":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$redact":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$reduce":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$regex":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$regexFind":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"No"},
        "$regexFindAll":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$regexMatch":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"No"},
        "$rename":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$replaceAll":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$replaceOne":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$replaceRoot":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$replaceWith":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$reverseArray":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$round":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$rtrim":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$sample":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$sampleRate":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$search":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"No"},
        "$second":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$set":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setDifference":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setEquals":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setField":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$setIntersection":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setIsSubset":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setOnInsert":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setUnion":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$setWindowFields":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$shardedDataDistribution":{"mongodbversion":"6.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$sin":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$sinh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$size":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$skip":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$slice":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$sort":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$sortArray":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$sortByCount":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$split":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$sqrt":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$stdDevPop":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$stdDevSamp":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$strcasecmp":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$strLenBytes":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$strLenCP":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$substr":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$substrBytes":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$substrCP":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$subtract":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$sum":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$switch":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"No"},
        "$tan":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$tanh":{"mongodbversion":"4.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$text":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"Yes","EC5.0":"No"},
        "$toBool":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toDate":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toDecimal":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toDouble":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toInt":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toLong":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toLower":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toObjectId":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$top":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$topN":{"mongodbversion":"5.2","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$toString":{"mongodbversion":"4.0","3.6":"No","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$toUpper":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$trim":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$trunc":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$tsIncrement":{"mongodbversion":"5.1","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$tsSecond":{"mongodbversion":"5.1","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$type":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$unionWith":{"mongodbversion":"4.4","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$uniqueDocs":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$unset":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$unsetField":{"mongodbversion":"5.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$unwind":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$week":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$where":{"mongodbversion":"4.0","3.6":"No","4.0":"No","5.0":"No","EC5.0":"No"},
        "$year":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"},
        "$zip":{"mongodbversion":"4.0","3.6":"Yes","4.0":"Yes","5.0":"Yes","EC5.0":"Yes"}}
        
    return thisKeywords

    
if __name__ == '__main__':
    main(sys.argv[1:])
