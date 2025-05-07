import sys
import argparse
import pymongo


rolesToExport = {}


def exportUsers(appConfig):
    client = pymongo.MongoClient(host=appConfig['uri'], appname='userexp')
    database_names = client.list_database_names()
    database_names.append("$external")

    f = open(appConfig['usersFile'], "w+", encoding='utf-8')
    for database_name in database_names:
        print("")
        if (database_name == 'local'):
            print(f"Skipping database:  {database_name}")
            continue

        print(f"Checking database:  {database_name}")
        database = client[database_name]
        users = database.command('usersInfo')
        if len(users['users']) == 0:
            print(f"No users in database:  {database_name}")
            continue

        use_db_printed = False
        for user in users['users']:
            """ Exclude serviceadmin user """
            if user['user'] == "serviceadmin":
                continue

            if (database_name == "$external") and (user['user'].startswith("arn:aws:iam::") == False):
                print(f"Skipping user:  {user['user']}, user must start with 'arn:aws:iam::'")
                continue

            print(f"Exporting user:  {user['user']}")

            if (use_db_printed == False):
                print(f"use {database_name}", file=f)
                use_db_printed = True

            print('db.createUser({user: "' + user['user'] + '", pwd: "REPLACE_THIS_PASS",' + ' roles: ' + str(user['roles']) + '});', file=f)

            print(f"Checking roles for user:  {user['user']}")
            for userRole in user['roles']:
                checkRole(database, userRole)
    
    f.close()
    print(f"Done! Users exported to {appConfig['usersFile']}")


def checkRole(database, userRole):
    print (f"Checking role {userRole}")
    """ A role can be assigned to multiple users so we only want to export the role definition once """
    """ Build a dictionary to keep track of all user-defined roles assigned to users being exported """
    roleInfo = database.command({'rolesInfo': {'role': userRole['role'], 'db': userRole['db']}, 'showPrivileges': True, 'showBuiltinRoles': False})

    if len(roleInfo['roles']) == 1:
        role = roleInfo['roles'][0]
        if (role['isBuiltin'] == False):
            """ Check role against list of roles supported by DocumentDB """
            if not role['role'] in rolesToExport:
                """ If this is a user-defined role not already marked for export, mark it for export """
                rolesToExport[role['role']] = role


def exportRoles(appConfig):
    with open(appConfig['rolesFile'], "w+", encoding='utf-8') as f:
        print("use admin", file=f)
        for role in rolesToExport:
            print(f"Exporting role:  {role}")
            privileges = str(rolesToExport[role]['privileges'])
            """ convert Python True/False to JSON true/false """
            privileges = privileges.replace(": True}", ": true}")
            privileges = privileges.replace(": False}", ": false}")
            print('db.createRole({role: "' + rolesToExport[role]['role'] + '", privileges: ' + privileges + ', roles: ' + str(rolesToExport[role]['roles']) + '});', file=f)

    f.close()    
    print(f"Done! Roles exported to {appConfig['rolesFile']}")

        
def main():
    """ v1:  Initial script, export users to a file """

    parser = argparse.ArgumentParser(description='Export Amazon DocumentDB users and user defined roles to user_output.js file, can be used to import them to other instance. Note: Passwords are not exported.')

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

    parser.add_argument('--roles-file',
                        required=True,
                        type=str,
                        help='The roles output file')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['usersFile'] = args.users_file
    appConfig['rolesFile'] = args.roles_file

#    client = pymongo.MongoClient(appConfig['uri'])
    
#    exportUsers(appConfig, client)
    exportUsers(appConfig)
    exportRoles(appConfig)


if __name__ == "__main__":
    main()