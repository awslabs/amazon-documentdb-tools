#!/usr/bin/python3

import compat

def main():
    versions = ['3.6','4.0','5.0','EC5.0']
    keywords = compat.load_keywords()

    totOps = 0
    numOps = {}
    numOpsSupported = {}

    for thisKeyword in keywords.keys():
        # get counts by mongodb version
        totOps += 1
        thisMongodbVersion = keywords[thisKeyword]["mongodbversion"]
        if thisMongodbVersion in numOps:
            numOps[thisMongodbVersion] += 1
        else:
            numOps[thisMongodbVersion] = 1

        # get supported count by documentdb version
        for docDbVersion in versions:
            if keywords[thisKeyword][docDbVersion] == "Yes":
                if docDbVersion in numOpsSupported:
                    numOpsSupported[docDbVersion] += 1
                else:
                    numOpsSupported[docDbVersion] = 1

    print("")
    print("MongoDB Operations By Version, total = {}".format(totOps))
    for thisVersion in sorted(numOps.keys()):
        print("  {} in version {}".format(numOps[thisVersion],thisVersion))
        
    print("")
    print("DocumentDB Supported Operations By Version")
    for thisVersion in sorted(numOpsSupported.keys()):
        print("  {} supported by DocumentDB version {} ({:.1f}%)".format(numOpsSupported[thisVersion],thisVersion,numOpsSupported[thisVersion]/totOps*100))
    print("")

    print("")
    print("DocumentDB EC Compat Check")
    for thisKeyword in sorted(keywords.keys()):
        #print("  {} supported by DocumentDB version {} ({:.1f}%)".format(numOpsSupported[thisVersion],thisVersion,numOpsSupported[thisVersion]/totOps*100))
        if keywords[thisKeyword]["5.0"] == "Yes" and keywords[thisKeyword]["EC5.0"] == "No":
            print("  {}".format(thisKeyword))
    print("")

    
if __name__ == '__main__':
    main()
