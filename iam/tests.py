
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
    pid = None
    try:
        grid1 = '46c3e25a-a72a-402a-baba-9e1de840e95a'
        grid2 = '49f1ceed-132f-4ccb-afed-a4ed8350a5ce'
        grid3 = 'e2f1e0cf-e6d4-4baa-b546-8f76ed89ef42'
        grid4 = '61ebfa64-39aa-4a5f-bbf9-c9d65c6539cf'
        def cleanup(pid):
            db.capability_grant_delete(grid1)
            db.capability_grant_delete(grid2)
            db.exec_sql('delete from persons where person_id = :pid', {'pid': pid}, fetch=False)
            db.exec_sql('delete from groups where group_name in (:g1, :g2, :g3, :g4)',
                       {'g1': _in_group1, 'g2': _in_group2, 'g3': _in_group3, 'g4': _in_group4}, fetch=False)
            db.exec_sql('delete from capabilities_http where capability_name in (:n1, :n2, :n3)',
                       {'n1': 'test1', 'n2': 'test2', 'n3': 'test3'}, fetch=False)
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
        # capabilities
        names1 = [
            {
                'capability_name': 'test1',
                'capability_required_groups': [_in_group1],
                'capability_lifetime': 60,
                'capability_description': 'allows data import'
            },
            {
                'capability_name': 'test2',
                'capability_required_groups': [_in_group1],
                'capability_lifetime': 60,
                'capability_description': 'allows data import'
            },
        ]
        print(db.capabilities_http_sync(names1))
        caps1 = db.exec_sql('select * from capabilities_http where capability_name in (:n1, :n2)',
                           {'n1': 'test1', 'n2': 'test2'})
        print(caps1)
        # check both are there
        # and have expected groups
        name_col_idx = 2
        group_col_idx = 4
        capabilities = caps1
        assert len(capabilities) == 2
        assert capabilities[0][name_col_idx] == 'test1'
        assert (len(capabilities[0][group_col_idx]) == 1
                and capabilities[0][group_col_idx] == [_in_group1])
        assert capabilities[1][name_col_idx] == 'test2'
        assert (len(capabilities[1][group_col_idx]) == 1
                and capabilities[1][group_col_idx] == [_in_group1])
        names2 = [
            {
                'capability_name': 'test1',
                'capability_required_groups': [_in_group1],
                'capability_lifetime': 60,
                'capability_description': 'allows data import'
            },
            {
                'capability_name': 'test2',
                'capability_required_groups': [_in_group2],
                'capability_lifetime': 60,
                'capability_description': 'allows data import'
            },
            {
                'capability_name': 'test3',
                'capability_required_groups': [_in_group1, '{0}-group'.format(_in_uname)],
                'capability_lifetime': 60,
                'capability_description': 'allows data import'
            },
        ]
        print(db.capabilities_http_sync(names2))
        caps2 = db.exec_sql('select * from capabilities_http where capability_name in (:n1, :n2, :n3)',
                           {'n1': 'test1', 'n2': 'test2', 'n3': 'test3'})
        print(caps2)
        # check test2 has new group, and that test3 is there
        capabilities = caps2
        assert len(capabilities) == 3
        assert capabilities[0][name_col_idx] == 'test1'
        assert (len(capabilities[0][group_col_idx]) == 1
                and capabilities[0][group_col_idx] == [_in_group1])
        assert capabilities[1][name_col_idx] == 'test2'
        assert (len(capabilities[1][group_col_idx]) == 1
                and capabilities[1][group_col_idx] == [_in_group2])
        assert capabilities[2][name_col_idx] == 'test3'
        assert (len(capabilities[2][group_col_idx]) == 2
                and capabilities[2][group_col_idx] == [_in_group1, '{0}-group'.format(_in_uname)])
        # capability grants
        grants1 = [
            {
                'capability_grant_id': grid1,
                'capability_names_allowed': ['test1'],
                'capability_grant_hostnames': ['my.api.com'],
                'capability_grant_namespace': 'files',
                'capability_grant_http_method': 'PUT',
                'capability_grant_rank': 1,
                'capability_grant_uri_pattern': '/groups/[a-zA-Z0-9]',
                'capability_grant_required_groups': ['self', 'moderator'],
                'capability_grant_group_existence_check': False
            },
            {
                'capability_grant_id': grid2,
                'capability_names_allowed': ['test2'],
                'capability_grant_hostnames': ['my.api.com'],
                'capability_grant_namespace': 'files',
                'capability_grant_http_method': 'HEAD',
                'capability_grant_rank': 1,
                'capability_grant_uri_pattern': '/files/export$',
                'capability_grant_required_groups': [_in_group3, _in_group4]
            },
        ]
        print('grant sync 1: \n')
        print(db.capabilities_http_grants_sync(grants1))
        # check the db, then add a new sync, and check the result
        gs1 = db.exec_sql('select * from capabilities_http_grants where capability_grant_id in (:id1, :id2)',
                         {'id1': grid1, 'id2': grid2})
        print(gs1)
        gs = gs1
        g_rank_idx = 7
        g_req_gr_idx = 9
        assert len(gs) == 2
        assert (gs[0][g_rank_idx] == 1 and gs[0][g_req_gr_idx] == ['self', 'moderator'])
        assert (gs[1][g_rank_idx] == 1 and gs[1][g_req_gr_idx] == [_in_group3, _in_group4])
        # test changing groups, ranks, and introducing a new grant
        grants2 = [
            {
                'capability_grant_id': grid1,
                'capability_names_allowed': ['test1'],
                'capability_grant_hostnames': ['my.api.com'],
                'capability_grant_namespace': 'files',
                'capability_grant_http_method': 'PUT',
                'capability_grant_rank': 1,
                'capability_grant_uri_pattern': '/groups/[a-zA-Z0-9]',
                'capability_grant_required_groups': ['self', 'moderator'],
                'capability_grant_group_existence_check': False
            },
            {
                'capability_grant_id': grid2,
                'capability_names_allowed': ['test1'],
                'capability_grant_hostnames': ['my.api.com'],
                'capability_grant_namespace': 'files',
                'capability_grant_http_method': 'HEAD',
                'capability_grant_rank': 1,
                'capability_grant_uri_pattern': '/files/export$',
                'capability_grant_required_groups': [_in_group1, _in_group2]
            },
            {
                'capability_grant_id': grid3,
                'capability_names_allowed': ['test2'],
                'capability_grant_hostnames': ['my.api.com'],
                'capability_grant_namespace': 'files',
                'capability_grant_http_method': 'PUT',
                'capability_grant_rank': 2,
                'capability_grant_uri_pattern': '/groupsps/admin$',
                'capability_grant_required_groups': [_in_group1]
            },
        ]
        print('grant sync 2: \n')
        print(db.capabilities_http_grants_sync(grants2))
        gs2 = db.exec_sql('select * from capabilities_http_grants where capability_grant_id in (:id1, :id2, :id3)',
                         {'id1': grid1, 'id2': grid2, 'id3': grid3})
        print(gs2)
        gs = gs2
        assert len(gs) == 3
        assert (gs[0][g_rank_idx] == 1 and gs[0][g_req_gr_idx] == ['self', 'moderator'])
        assert (gs[1][g_rank_idx] == 1 and gs[1][g_req_gr_idx] == [_in_group1, _in_group2])
        assert (gs[2][g_rank_idx] == 2 and gs[2][g_req_gr_idx] == [_in_group1])
        # set the rank explicitly
        print(db.capability_grant_rank_set(grid3, 1))
        gs = db.exec_sql('select * from capabilities_http_grants where capability_grant_id = :id3',
                        {'id3': grid3})
        assert gs[0][g_rank_idx] == 1
        # delete a grant
        print(db.capability_grant_delete(grid3))
        gs = db.exec_sql('select * from capabilities_http_grants where capability_grant_id in (:id1, :id2, :id3)',
                        {'id1': grid1, 'id2': grid2, 'id3': grid3})
        assert gs[0][g_rank_idx] == 1 # reset automatically
        print(db.person_capabilities(pid))
        print(db.person_access(pid))
        print(db.user_capabilities(_in_uname))
        print(db.group_capabilities('{0}-group'.format(_in_uname)))
        print(db.capabilities_http_grants_group_add(grid1, _in_group2))
        print(db.capabilities_http_grants_group_remove(grid1, _in_group2))
    except (Exception, AssertionError )as e:
        print('something went wrong :(')
        cleanup(pid)
        raise e
        return
    cleanup(pid)
    print('ALL GOOD')
    return db


if __name__ == '__main__':
    test_pgiam()
