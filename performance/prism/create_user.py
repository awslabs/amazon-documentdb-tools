#!/usr/bin/env python3
"""Manage Prism application-login accounts (local SQLite store).

Use this to seed the first user before starting the app, add more users, reset
a password, or list/delete accounts. Works identically on a local laptop and on
EC2 (standard-library only).

Examples:
    python create_user.py add alice                 # prompts for password
    python create_user.py add alice --password MySecurePass12
    python create_user.py passwd alice              # reset a password
    python create_user.py list
    python create_user.py delete alice

The store location can be overridden with the PRISM_AUTH_DB env var.
"""
import sys

if sys.version_info < (3, 11):
    print(f"Error: Python 3.11+ required (running {sys.version_info.major}.{sys.version_info.minor}).",
          file=sys.stderr)
    print("On EC2 (Amazon Linux 2023): use python3.11 create_user.py ...", file=sys.stderr)
    sys.exit(1)

import argparse
import getpass

import auth_store


def _read_password(provided):
    if provided:
        return provided
    pw1 = getpass.getpass("Password: ")
    pw2 = getpass.getpass("Confirm password: ")
    if pw1 != pw2:
        print("Passwords do not match.", file=sys.stderr)
        sys.exit(1)
    return pw1


def main():
    parser = argparse.ArgumentParser(description="Manage Prism login accounts.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="Create a new user")
    p_add.add_argument("username")
    p_add.add_argument("--password", help="Password (omit to be prompted securely)")

    p_pw = sub.add_parser("passwd", help="Reset an existing user's password")
    p_pw.add_argument("username")
    p_pw.add_argument("--password", help="New password (omit to be prompted securely)")

    sub.add_parser("list", help="List users")

    p_del = sub.add_parser("delete", help="Delete a user")
    p_del.add_argument("username")

    args = parser.parse_args()

    try:
        if args.command == "add":
            auth_store.create_user(args.username, _read_password(args.password))
            print(f"User '{args.username}' created.")
        elif args.command == "passwd":
            auth_store.set_password(args.username, _read_password(args.password))
            print(f"Password updated for '{args.username}'.")
        elif args.command == "list":
            users = auth_store.list_users()
            if not users:
                print("No users. Create one with: python create_user.py add <username>")
            else:
                for name, created in users:
                    print(f"{name}\t{created}")
        elif args.command == "delete":
            auth_store.delete_user(args.username)
            print(f"User '{args.username}' deleted.")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
