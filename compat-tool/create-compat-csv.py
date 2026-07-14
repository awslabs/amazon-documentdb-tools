#!/usr/bin/python3

import compat

def main():
    versions = ['3.6','4.0','5.0.0','5.0.1','8.0','8.0.1','EC5.0']
    keywords = compat.load_keywords()

    print("{},{},{},{},{},{},{},{},{}".format('operator','mdb-version','docdb-36','docdb-40','docdb-50','docdb-501','docdb-80','docdb-801','docdb-ec'))

    for thisKeyword in keywords.keys():
        thisEntry = keywords[thisKeyword]
        print("{},{},{},{},{},{},{},{},{}".format(thisKeyword,thisEntry["mongodbversion"],thisEntry["3.6"],thisEntry["4.0"],thisEntry["5.0.0"],thisEntry["5.0.1"],thisEntry["8.0"],thisEntry["8.0.1"],thisEntry["EC5.0"]))

    
if __name__ == '__main__':
    main()
