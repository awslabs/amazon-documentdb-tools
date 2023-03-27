import pymongo
import math
import argparse
import sys
from bson.json_util import dumps
from deepdiff import DeepDiff 
from tqdm import tqdm
import time


passed_forwards = None
passed_forwardsAgains = None
passed_backwards = None

def data_compare(source_uri, target_uri, db1, db2, coll1, coll2, percent, direction):

    global passed_forwards
    global passed_forwardsAgains
    global passed_backwards
    found_error = False
    print("data_compare : "+direction)
    try: 
      client1 = pymongo.MongoClient(source_uri, serverSelectionTimeoutMS=5000)
    except Exception as e:
      print(str(e))
    try: 
      client2 = pymongo.MongoClient(target_uri, serverSelectionTimeoutMS=5000)
    except Exception as e:
      print(str(e))
    
    missing_documtents_file = open(time.strftime("%Y%m%d-%H%M%S")+"-"+direction+'_missing_documents.txt', 'a')
    first_database = client1[db1]
    second_database = client2[db2]
    first_collection = first_database[coll1]
    second_collection = second_database[coll2]
    


    #method will check edge cases to see if any of the collections passed in are empty, or if the 2 collections dont match each other in document length
    empty_coll_or_diff_num_docs_check(first_collection, second_collection) 

    # Based on what the direction is when the function is called (default direction value from the main method is "forwards"), it will get a different set 
    # of documents called sampled_docs to hold which will be used to loop through in the later code and.  
    
    # There are 3 total cases here for direction: 
    #     1) "forwards" only checks to see if ALL (so 100% is passed here) documents from source collection are present in the target collection 
    #     2) "forwardsAgain" does document level diff'ing to see if there are any actual differences based on the user-selected percentage of sample docs 
    #     3) "backwards" checks to see if ALL (once again 100% is passed here) documents from target collection are present in the source collection
    
    # Also based on the direction, the find_one_coll is specified which is specifying which collection we are going to do the find_one on as we loop 
    # through the sampled_docs. So for example, in the forwards case we will do a find_one on collection2 which would be the target namespace we are 
    # looking for those documents in.

    if direction == "forwards":
        sampled_docs = get_rand_sample_docs(first_collection, 100)
        find_one_coll = second_collection
        print("\n\nProgress bar is checking to see if all documents in source collection exist in the target collection...")
    if direction == "forwardsAgain":
        sampled_docs = get_rand_sample_docs(first_collection, percent)
        find_one_coll = second_collection
        print("\nProgress bar this time is checking to see if based on the percent you selected, those random sample of documents match exactly from source -> target...")
    if direction == "backwards":
        sampled_docs = get_rand_sample_docs(second_collection, 100)
        find_one_coll = first_collection
        print("\nReverse Checking! Progress bar is lastly checking to see if all documents in target collection exist in the source collection...")

    #tqdm is the progress bar the terminal outputs to help the user see the progress
    for document in tqdm(sampled_docs): 
        queried_doc = find_one_coll.find_one({"_id": document["_id"]})
        
        
        
        # Only if the direction is "forwards" or "backwards" do we check to see if the documents exist in the opposite collection as described above. "forwardsAgain" 
        # would not need this as you already checked the whole source collection in "forwards" so a random sample checking again would be redundant.
        if direction == "forwards" or direction == "backwards":
            
            if queried_doc == None: 
                if direction == "forwards":
                    print_str = "\n\nXXXXFailed!XXXX\nThe following document based on it's ID was not found in the target but was found in the source:\n{0}\n"
                    found_error = True
                if direction == "backwards":
                    print_str = "\n\nXXXXFailed!XXXX\nThe following document based on it's ID was not found in the source but was found in the target:\n{0}\n"
                    found_error = True
                # print(print_str.format(document["_id"]))
                missing_documtents_file.write(str(document["_id"])+ "\n")
                # sys.exit[0]
                # return False

            
        elif direction == "forwardsAgain":
            # print("starting document content comparison")
            #Only if direction = "forwardsAgain" then we will do the actual document comparison and difference finding 
            if queried_doc == None: 
                continue
            pretty_q_doc_str = dumps(queried_doc, indent = 4, separators =("", " = "))
            pretty_document_str = dumps(document, indent = 4, separators =("", " = "))
            #compare the strings of the documents to ensure they are equal for each document 
            if pretty_document_str == pretty_q_doc_str: 
                continue
            # if the dumps strings are not equal, it can be 2 cases: 
            #     First case is that the right keys with the right values are there, just in the wrong order, 
            #     Second case is there is  guranteed differences between the documents such as for example some portion of doc missing/extra portion added.
            else: 
                # DeepDiff is a package that compares 2 dictionaries, 2 iterables, 2 strings or 2 other objects and returns their differences as a <class 'deepdiff.diff.DeepDiff'>.
                # This will return to user what the differences were between the 2 objects in the output. Learn more about DeepDiff here: https://zepworks.com/deepdiff/current/basics.html
                # It is important to note however, that DeepDiff does not check for order of key/value pairs, so although our dumps strings earlier could have had 
                # a difference (due to order being different) DeepDiff would not return anything as a difference. So for combatting this issue, a check is required 
                # to see if the length of the result of that DeepDiff call = 0, then you know that the only difference was the orders from source to target. If that 
                # is not the case, and the length of the diff is greater than 0, then there are actual differences that you have to report those differences to the 
                # user.

                # To summarize, there are 3 paths when comparing documents: 
                #     1) the dump strings match each other in the document comparison and then we continue. If all the documents go down this path, at the end you print that it passed and return True.
                #     2) there is a difference with the dump strings, but the length of DeepDiff is 0, so then we know the only difference can be the order, so we print that to user and return False.
                #     3) there is a difference with the dump strings, but the length is anything else but 0, so we know there are actual differences, so we print to user and return False.
                diff = DeepDiff(document, queried_doc, verbose_level=2, report_repetition=True).pretty()
                found_error = True
                if len(diff) == 0:
                    print_str = "\n\nXXXXFailed!XXXX\nThe values are all there but the order of values is different from source to target.\nSource document looks like this:\n{0}\nTarget document looks like this:\n{1}\n\n"
                    # print(print_str.format(pretty_document_str, pretty_q_doc_str))
                    missing_documtents_file.write("Order_Difference , "+str(document["_id"])+","+str(queried_doc["_id"])+ "\n")
                    # sys.exit[0]
                    # return False
                else:
                    diff_str = "\n\nXXXXFailed!XXXX\nThere are differences that were found. Refer to the target doc as seen here:\n{0}\nRefer to the source doc as seen here:\n{1}\nThe Differences Consist Specifically of the Following:\n{2}\n\n"
                    # print(diff_str.format(pretty_document_str, pretty_q_doc_str, diff))
                    missing_documtents_file.write("Value_Difference , "+str(document["_id"])+","+str(queried_doc["_id"])+ "\n")
                    # sys.exit[0]
                    # return False
        else:
            sys.exit[0]
    if(found_error) :
        if direction == "forwards":
            passed_forwards = False
        elif direction == "backwards":
            passed_backwards = False
        else :
            passed_forwardsAgains = False
    if direction != None:
        print("\n****DONE!****\n1)  All documents in source collection exist in target collection! - {} \n2)  All documents in target collection exist in source! - {} \n" 
                "3)  All randomly sampled documents based on your defined percentage match exactly from source to target! - {} \n\n".format(passed_forwards,passed_backwards,passed_forwardsAgains))
        missing_documtents_file.close()
        return True

def get_rand_sample_docs(sample_coll, percent):
    #Convert from percent to a number of documents to sample from in the aggregation
    sample_size = math.ceil((percent/100) * sample_coll.count_documents({}))

    #Aggregate the random sample based on sample_size
    sampled_docs = list(sample_coll.aggregate([
        {"$sample": {"size": sample_size}}
    ]))
    return sampled_docs

def empty_coll_or_diff_num_docs_check(first_coll, second_coll):
    coll1_num_docs = first_coll.count_documents({})
    coll2_num_docs= second_coll.count_documents({})

    #Checks if first collection is empty
    if coll1_num_docs == 0: 
        print("\n\nXXXXFailed!XXXX\nYour first collection is empty, please re-check you selected the right source collection.")
        sys.exit[0]
        return False
    
    #Checks if second collection is empty 
    if coll2_num_docs == 0:
        print("\n\nXXXXFailed!XXXX\nYour second collection is empty, please re-check you selected the right target collection.")
        sys.exit[0]
        return False

    #Checks to see if number of documents in both are equal
    if coll1_num_docs != coll2_num_docs:
        print_str = "\n\nXXXXFailed!XXXX\nBoth collections do not have the same number of documents. Source collection has {0} documents. Target collection has {1} documents." 
        print(print_str.format(coll1_num_docs, coll2_num_docs))
        # sys.exit[0]
        # return False

#Main method
if __name__ == "__main__":
    #order diff, same content is "s_one", "s_two"
    #same everything is "sample_one", "sample_two"
    #same num of docs but one doc is missing id is wrong for one of them is "sa_one", "sa_two"
    #extra field in target than source is "sam_one", "sam_two"

    parser = argparse.ArgumentParser(description="DataDiffer Tool.")

    parser.add_argument("--source-uri", type=str, required=True, help="Required Argument. This is the source URI")
    parser.add_argument("--target-uri", type=str, required=True, help="Required Argument. This is the target URI")
    parser.add_argument("--source-namespace", type=str, required=True, help="Required Argument. This is the source namespace and should be in the format <source_database_name>.<source_collection_name>")
    parser.add_argument("--target-namespace", type=str, required=True, help="Required Argument. This is the target namespace and should be in the format <target_database_name>.<target_collection_name>")
    parser.add_argument("--percent", type=int, required=True, help="Required Argument. This is the percent value of source collection to compare in the target collection passed as an integer.")
    
    args = parser.parse_args()
    src_str = args.source_namespace.split(".")
    target_str = args.target_namespace.split(".")

try:
    data_compare(args.source_uri, args.target_uri, src_str[0], target_str[0], src_str[1], target_str[1], 100, "forwards")
    data_compare(args.source_uri, args.target_uri, src_str[0], target_str[0], src_str[1], target_str[1], args.percent, "forwardsAgain")
    data_compare(args.source_uri, args.target_uri, src_str[0], target_str[0], src_str[1], target_str[1], 100, "backwards")
except Exception as e: 
    print("\n\nXXXXUnable to run fully!XXXX\nPlease fix any errors presented and make sure your command line arguments are entered correctly. For help on correct command line arguments syntax, pass --help as a command line argument for more help.\n\n" )
    print(str(e))