#!/usr/bin/python3

import compat

def main():
    existingVersion = "8.0.1"
    newVersion = "8.0.2"

    keywords = compat.load_keywords()

    for thisKeyword in keywords.keys():
        keywords[thisKeyword][newVersion] = keywords[thisKeyword][existingVersion]
        print("        \"{}\":{},".format(thisKeyword,keywords[thisKeyword]))

if __name__ == '__main__':
    main()
