#!/bin env python3

from sys import argv, exit

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from pgiam import Db


def test_pgiam():
    if len(argv) < 2:
        print('missing args')
        print('usage: $dbuser $pw $host $db')
        exit(1)
    user = argv[1]
    pw = argv[2]
    host = argv[3]
    db = argv[4]
    dburi = ''.join(['postgresql://', user, ':', pw, '@', host, ':5432/', db])
    engine = create_engine(dburi, poolclass=QueuePool)
    db = Db(engine)
    print(type(db.tables.persons))
    pid = db.exec_sql('select person_id from persons limit 1', {})[0][0]
    user_name = db.exec_sql('select user_name from users where person_id=:pid', {'pid': pid})[0][0]
    print(db.person_groups(pid))
    print(db.person_capabilities(pid))
    print(db.person_access(pid))
    print(db.user_groups(user_name))
    print(db.user_capabilities(user_name))
    print(db.group_members('p11-export-group'))
    print(db.group_moderators('p11-export-group'))
    print(db.group_member_add('p11-admin-group', 'p11-clinical-group'))
    print(db.group_member_remove('p11-admin-group', 'p11-clinical-group'))
    print(db.group_capabilities('p11-admin-group'))
    names = [
        {'capability_name': 'import',
         'capability_required_groups': ['admin-group'],
         'capability_lifetime': 60,
         'capability_description': 'allows data import'},
        {'capability_name': 'export',
         'capability_required_groups': ['admin-group'],
         'capability_lifetime': 60,
         'capability_description': 'allows data import'},
    ]
    identity = 'p11-leoncd'
    print(db.capabilities_http_sync(names, identity))
    grants = [
        {'capability_name': 'import',
         'capability_grant_id': '46c3e25a-a72a-402a-baba-9e1de840e95a',
         'capability_grant_hostname': 'my.api',
         'capability_grant_namespace': 'iam',
         'capability_grant_http_method': 'PUT',
         'capability_grant_rank': 1,
         'capability_grant_uri_pattern': '/groups/[a-zA-Z0-9]',
         'capability_grant_required_groups': ['self', 'moderator'],
         'capability_grant_group_existence_check': False},
        {'capability_name': 'export',
         'capability_grant_id': '49f1ceed-132f-4ccb-afed-a4ed8350a5ce',
         'capability_grant_hostname': 'api.com',
         'capability_grant_namespace': 'files',
         'capability_grant_http_method': 'HEAD',
         'capability_grant_rank': 2,
         'capability_grant_uri_pattern': '/files/export$',
         'capability_grant_required_groups': ['admin-group', 'export-group']},
    ]
    print(db.capabilities_http_grants_sync(grants, identity))
    return db


if __name__ == '__main__':
    test_pgiam()
