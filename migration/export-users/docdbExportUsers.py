import sys
import argparse
import pymongo


def exportUsers(appConfig):
    client = pymongo.MongoClient(host=appConfig['uri'],appname='userexp')
    listusers = client.admin.command('usersInfo', {'forAllDBs': True})
    with open(appConfig['usersFile'], "w+", encoding='utf-8') as f:
        print("use admin", file=f)
        for user in listusers['users']:
            """ Exclude serviceadmin user """
            if user['user'] == "serviceadmin":
                continue
            print(f"Exporting user:  {user['user']}")
            print('db.createUser({user: "' + user['user'] + '", pwd: "REPLACE_THIS_PASS",' + ' roles: ' + str(user['roles']) + '});', file=f)
    print(f"Done! Users exported to {appConfig['usersFile']}")


def main():
    """ v1:  Initial script, export users to a file """

    parser = argparse.ArgumentParser(description='Export Amazon DocumentDB users to user_output.js file, can be used to import them to other instance. Note: Passwords are not exported.')

    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='MongoDB Connection URI')

    parser.add_argument('--users-file',
                        required=True,
                        type=str,
                        help='The users output file')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['usersFile'] = args.users_file

    exportUsers(appConfig)


if __name__ == "__main__":
    main()
