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
    # test data
    _in_full_name = 'Kor Ah'
    _in_uname = 'kor1'
    _in_group1 = 'g1'
    _in_group2 = 'g2'
    _in_group3 = 'g3'
    _in_group4 = 'g4'
    try:
        # create a person
        db.exec_sql('insert into persons(full_name) values (:full_name)',
                   {'full_name': _in_full_name}, fetch=False)
        pid = db.exec_sql('select person_id from persons where full_name = :full_name',
                         {'full_name': _in_full_name})[0][0]
        # create users
        db.exec_sql('insert into users(person_id, user_name) values (:pid, :user_name)',
                   {'pid': pid, 'user_name': _in_uname}, fetch=False)
        # create some groups
        db.exec_sql('insert into groups(group_name, group_class, group_type) values (:name, :class, :type)',
                   {'name': _in_group1, 'class': 'secondary', 'type': 'generic'}, fetch=False)
        db.exec_sql('insert into groups(group_name, group_class, group_type) values (:name, :class, :type)',
                   {'name': _in_group2, 'class': 'secondary', 'type': 'generic'}, fetch=False)
        db.exec_sql('insert into groups(group_name, group_class, group_type) values (:name, :class, :type)',
                   {'name': _in_group3, 'class': 'secondary', 'type': 'web'}, fetch=False)
        db.exec_sql('insert into groups(group_name, group_class, group_type) values (:name, :class, :type)',
                   {'name': _in_group4, 'class': 'secondary', 'type': 'generic'}, fetch=False)
        # add members
        print(db.group_member_add(_in_group1, _in_group2))
        print(db.group_member_add(_in_group1, _in_group3))
        print(db.group_member_add(_in_group2, _in_uname))
        # add moderators
        db.exec_sql('insert into group_moderators(group_name, group_moderator_name) values (:group, :mod)',
                   {'group': _in_group1, 'mod': _in_group4}, fetch=False)
        print(db.person_groups(pid))
        print(db.user_groups(_in_uname))
        print(db.group_members(_in_group1))
        print(db.group_moderators(_in_group1))
        print(db.group_member_remove(_in_group1, _in_group3))
        print(db.group_members(_in_group1))
        # the following functions are not implemented in the SQL library
        # and therefore tested more extensively
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
        identity = 'tester'
        #print(db.capabilities_http_sync(names, identity))
        grants = [
            {'capability_grant_id': '46c3e25a-a72a-402a-baba-9e1de840e95a',
             'capability_grant_hostname': 'my.api',
             'capability_grant_namespace': 'iam',
             'capability_grant_http_method': 'PUT',
             'capability_grant_rank': 1,
             'capability_grant_uri_pattern': '/groups/[a-zA-Z0-9]',
             'capability_grant_required_groups': ['self', 'moderator'],
             'capability_grant_group_existence_check': False},
            {'capability_grant_id': '49f1ceed-132f-4ccb-afed-a4ed8350a5ce',
             'capability_grant_hostname': 'api.com',
             'capability_grant_namespace': 'files',
             'capability_grant_http_method': 'HEAD',
             'capability_grant_rank': 2,
             'capability_grant_uri_pattern': '/files/export$',
             'capability_grant_required_groups': ['admin-group', 'export-group']},
        ]
        #print(db.capabilities_http_grants_sync(grants, identity)))

        #print(db.person_capabilities(pid))
        #print(db.person_access(pid))
        #print(db.user_capabilities(user_name))
        #print(db.group_capabilities('p11-admin-group'))
    except Exception as e:
        print('something went wrong :(')
        print(e)
        db.exec_sql('delete from persons where person_id = :pid', {'pid': pid}, fetch=False)
        db.exec_sql('delete from groups where group_name in (:g1, :g2, :g3, :g4)',
                   {'g1': _in_group1, 'g2': _in_group2, 'g3': _in_group3, 'g4': _in_group4}, fetch=False)
        return
    db.exec_sql('delete from persons where person_id = :pid', {'pid': pid}, fetch=False)
    db.exec_sql('delete from groups where group_name in (:g1, :g2, :g3, :g4)',
               {'g1': _in_group1, 'g2': _in_group2, 'g3': _in_group3, 'g4': _in_group4}, fetch=False)
    print('ALL GOOD')
    return db



if __name__ == '__main__':
    test_pgiam()
