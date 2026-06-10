#!/usr/bin/python3

import compat

def main():
    existingVersion = "5.0.0"
    newVersion = "5.0.1"

    keywords = compat.load_keywords()

    for thisKeyword in keywords.keys():
        keywords[thisKeyword][newVersion] = keywords[thisKeyword][existingVersion]
        print("        \"{}\":{},".format(thisKeyword,keywords[thisKeyword]))

if __name__ == '__main__':
    main()
