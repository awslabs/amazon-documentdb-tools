#!/bin/env python3
"""
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  SPDX-License-Identifier: MIT-0

  Permission is hereby granted, free of charge, to any person obtaining a copy of this
  software and associated documentation files (the "Software"), to deal in the Software
  without restriction, including without limitation the rights to use, copy, modify,
  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import argparse
import pymongo


# Pass DocumentDB connection info: username, password and uri
parser = argparse.ArgumentParser(
    description="Export Amazon DocumentDB users to user_output.js file, can be used to import them to other instance. Note: Passwords are not exported."
)

parser.add_argument('-u', '--username',
                        required=True,
                        type=str,
                        help='Username for authentication to Amazon DocumentDB')
parser.add_argument('-p', '--password',
                        required=True,
                        type=str,
                        help='Password for authentication to Amazon DocumentDB')
parser.add_argument('--host',
                        required=True,
                        type=str,
                        help='Amazon DocumentDB host to connect to')
parser.add_argument('--port',
                        required=False,
                        type=int,
                        default=27017,
                        help='Specify the Amazon DocumentDB port (defaults to 27017)')
parser.add_argument('--tls',
                        required=False,
                        action='store_true',
                        help='Connect using TLS')
parser.add_argument('--cafile',
                        required=False,
                        type=str,
                        help='Path to CA file used for TLS connection')
args = parser.parse_args()


def get_db_connection():
    """Connect to instance, returning a connection"""
    try:
        mongodb_client = pymongo.MongoClient(
            host=args.host,
            port=args.port,
            tls=args.tls,
            tlsCAFile=args.cafile,
            authSource='admin',
            username=args.username,
            password=args.password,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000)
    except Exception as e:
        print(f"Failed to create new DocumentDB client: {e}")
        raise
    return mongodb_client


def main():
    """ Main function """
    mongodb_client = get_db_connection()
    listusers = mongodb_client.admin.command('usersInfo', {'forAllDBs': True})
    with open("user_output.js", "w+", encoding='utf-8') as f:
        print("use admin", file=f)
        for user in listusers['users']:
            """ Exclude serviceadmin user """
            if user['user'] == "serviceadmin":
                continue
            print(f"Exporting user:  {user['user']}")
            print('db.createUser({user: "' + user['user'] + '", pwd: "REPLACE_THIS_PASS",' + ' roles: ' + str(user['roles']) + '});', file=f)
    print('Done! Users exported to user_output.js.')


if __name__ == "__main__":
    main()
