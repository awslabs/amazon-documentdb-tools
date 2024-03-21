import pymongo
from datetime import datetime
import argparse, sys
import traceback
import pandas as pd 
import termtables as tt

global args
global client


def init_conn():
    """
    initialize connection to documentdb    
    """     
    global client
    try:
        client = pymongo.MongoClient(host=args.uri,appname='cardinal')
    except Exception as e:
        traceback.print_exception(*sys.exc_info())
        print(e)
        
    
def get_param():
    """
    Accepts command line parameters from users. Parameters are described in README.md and --help of cli
    """     
    global args
    parser = argparse.ArgumentParser(prog='python detect-cardinality.py',
                    description='This program samples documents in each collection to find index cardinality. Sample count is set at default 100K and can be changed with --sample-count parameter. ')
    parser.add_argument("--uri",  help="DocumentDB connnection string", required=True)
    parser.add_argument("--max-collections",default="100", help="Maximum number of collections to scan per database. Default 100")
    parser.add_argument("--threshold",default="1", help="Percentage of Cardinality threshold. Default 1 percent")
    parser.add_argument("--databases",default="All", help="Comma separated list of database names. Default=All")
    parser.add_argument("--collections",default="All", help="Comma separated list of collection names. Default=All")
    # TODO indexes only supports single key indexes. Multi-key indexes require updates to get_index_cardinality function. For planned indexes only single key indexes are supported. 
    parser.add_argument("--indexes", default="All", help="Comma separated list of index names. Default=All")
    parser.add_argument("--sample-count",default="100000", help="Numbers of documents to sample in a collection. Increasing this may increase the execution time for this script.")
    
    args = parser.parse_args()

    if args.uri is None:
        print("Connection string is required")
        sys.exit(1)    
    

def print_output(results):
    """
    Prints the summarized output of cardinality test. Shows the location of detail summary csv

    :param results: Dataframe containing cardinality results for each index
    :return: prints the output on the console. 
    """     
    print("\n------------------------------------------")
    print("Total Databases Found: {}".format(args.db_counter))
    print("Total collections Found across {} database(s): {}".format(args.db_counter, args.coll_counter))
    print("Total indexes found : {}".format(args.index_counter))
    print("------------------------------------------")
    
    print("\n------------------------------------------")
    
    
    low_cardinal_results = results[results["isLowCardinality"]=="Y"]
    low_cardinal_results = low_cardinal_results.sort_values('cardinality', ascending=True)
    print("######Found {} indexes that may have low cardinality values.".format( len(low_cardinal_results) ))
    header=["No","Index Name", "Index Keys", "Cardinality"]
    data =[ ]
    
    print("Top index(es) with lowest cardinality:" )
    i = 1
    for index, row in low_cardinal_results.iterrows():
        keys = ','.join(row['index_keys'])
        
        data.append( [str(i), row['index_name'], keys, str(row['cardinality']) + '%'] )
        i = i + 1
        
    
    tt.print(
            data,
            header=header
        )
    print("------------------------------------------")
    
def save_file(results):
    """
    function saves dataframe of results into a csv file with current date in the working directory. 
    :param results: dataframe containing row for each index and cardinality calculation
    :return: Saves the file to working directory. 
    """     
    date_now = str(datetime.now().isoformat())
    file_name = 'cardinality_output_'+date_now+'.csv'
    
    results.sort_values('cardinality', ascending=True).to_csv(file_name, index=False)
    print("Detailed report is generated and saved at `{}`".format(file_name))
    print("##### Done #####")

def get_indexes(coll, arg_indexes):
        # only extract indexes supplied if supplied in --indexes, else get all indexes
        indexes = []
        existing_indexes = coll.index_information()
        if arg_indexes[0] != 'All':
            for index in arg_indexes:
                obj = {}
                obj['name'] = []
                obj['name'].append(index)
                obj['key'] = index + "_1"
                indexes.append(obj)
        else:
            for key in existing_indexes.keys():
                if key != '_id_':
                    obj = {}
                    obj['key'] = key
                    obj['name'] = []
                    for val in  existing_indexes[key]['key']:
                        obj['name'].append(val[0])
                    
                    indexes.append(obj)
      
        return indexes
        
def get_index_cardinality(db_name, coll_name, index):
    """
    Calculates the cardinality for a given database, collection and index_name. This function is called for each index in the database. 

    :param db_name: database name 
    :param coll_name: collection name to find index in
    :param index_name: check if index is low cardinality. 
    :return: json object containing total values, distinct values and cardinality % for that index
    """     
    global client
    sample_count = int(args.sample_count)
    
    index_keys = []
    for val in index["name"]:
        index_keys.append('$'+val)
    
    pipeline = [  
        { "$sample" : { "size" : sample_count } },
        { "$group" : { "_id": index_keys, "count" : {"$sum" : 1}  } }
        ]
    
    values = client[db_name][coll_name].aggregate( pipeline )
    
    df = pd.DataFrame(values)
    
    #removing 'None' results as they does not need to be counted towards cardinality
    for index, row in df.iterrows():
        if row['_id'][0] == None:
           df.drop(index, inplace=True)
   
    distinct = len(df)

    if distinct > 0:    
        total = df['count'].sum()
        return { "total": total, "distinct": distinct, "cardinality": ( distinct / total ) * 100  }
    else:
        return {"total": 0}
    

def _print_collection_max_msg(coll_count, db_name):
    print(" ### This script will scan maximum {} of total {} in database: {} \n Consider increase --max-collection to include more collections.".format(args.max_collections, coll_count, db_name))        

def start_cardinality_check():
    """
    function does the following:
    1. Gets lists of databases
    2. For each database gets lists of collection
    3. For each collection gets list of indexes 
    4. For each index runs cardinality check for sample_count set 

    User can optionally pass database or collection name to reduce the scope of cardinality check. 

    :return: Returns pandas dataframe containing rows for each index and cardinality calculation. 
    """     
    global args
    global client
    results = []
    connection_string = args.uri
    max_collections = int(args.max_collections)
    threshold = float(args.threshold) 
    
    try:
        
        databases = client.list_database_names()
        if args.databases != "All":
            databases = args.databases.split(",")
        
        db_counter = 0
        coll_counter = 0
        index_counter = 0
        for db_name in databases:
            db_counter = db_counter + 1
            database = client[db_name]
            coll_names = database.list_collection_names()
            
            coll_count = len(coll_names)
            
            if coll_count > max_collections:
                _print_collection_max_msg(coll_count, db_name)
            
            if args.collections != "All":
                coll_names = args.collections.split(",")
            for coll_name in coll_names[:max_collections]:
                print("### Starting cardinality check for collection - {} .... ".format(coll_name)) 
                coll_counter = coll_counter + 1
                collection = database[coll_name]
                indexes = get_indexes(collection, args.indexes.split(','))
                
                for index in indexes:
                    result_row = {}
                    
                    index_name = index['key']

                    print("###     checking index - {} .... ".format(index_name))
                    
                    cardinality = 0
                    isLowCardinality = 'N'
                   
                    index_counter = index_counter + 1
                    rs = get_index_cardinality(db_name, coll_name, index)
                    if rs['total'] > 0:
                        result_row['index_name'] = index_name
                        result_row['index_keys'] = index['name']
                        result_row['collection_name'] = coll_name
                        result_row['cardinality'] = round(rs['cardinality'],4)
                        if rs['cardinality'] < threshold:
                            isLowCardinality = 'Y'
                        result_row['isLowCardinality'] = isLowCardinality
                        result_row['totalDocsWithIndexValue'] = rs['total']
                        result_row['totalDistinctValues'] = rs['distinct']
                        results.append(result_row)
                        
                print("### Finished cardinality check for collection - {}\n".format(coll_name))
                
            args.db_counter = db_counter
            args.coll_counter = coll_counter
            args.index_counter = index_counter
        return pd.DataFrame(results)
        
        
        
    except Exception as e:
        traceback.print_exception(*sys.exc_info())
        print(e)
        
def main():
    """
    main function kicks off parameter collection, initialization of connection and calling cardinality detection.    
    :return: prints output of cardinality check and saves results to csv file
    """     
    try:
        output = {}
        get_param()
        init_conn()
        print("\nStarting Cardinality Check. Script may take few mins to finish.")
        print("Finding indexes where Cardinality/Distinct Values are less than ( {}% )...\n".format(args.threshold))
        results = start_cardinality_check()
        
        
        if results[results["isLowCardinality"]=="Y"].empty:
            print("All indexes are in good health. Cardinality detection script did not find any low cardinality indexes. ")
        else:
            print_output(results)
        
        save_file(results)
    except Exception as e:
        traceback.print_exception(*sys.exc_info())
        print(e)        

"""
Cardinality check script starts here
"""
if __name__ == "__main__":
    main()
