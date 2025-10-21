
import os

import pytest

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from .pgiam import Db


class TestPgIam(object):

    def set_db_connection(self) -> None:
        user = os.environ["PYPGIAM_USER"]
        pw = os.environ["PYPGIAM_PW"]
        host = os.environ["PYPGIAM_HOST"]
        db = os.environ["PYPGIAM_DB"]
        engine = create_engine(
            ''.join(['postgresql://', user, ':', pw, '@', host, ':5432/', db]),
            poolclass=QueuePool,
        )
        self.db = Db(engine)

    def grant_id_from_name(self, grant_name: str) -> str:
        out = self.db.exec_sql(
            "select capability_grant_id from capabilities_http_grants \
             where capability_grant_name = :gn",
            {"gn": grant_name},
        )
        return str(out[0][0]) if out else None

    def cleanup(self, pid: str, grants: list, groups: dict) -> None:
        for grant in grants:
            grant_id = self.grant_id_from_name(grant)
            self.db.capability_grant_delete(grant_id) if grant_id else None
        self.db.exec_sql(
            'delete from persons where person_id = :pid',
            {'pid': pid},
            fetch=False,
        )
        self.db.exec_sql(
            'delete from groups where group_name in (:g1, :g2, :g3, :g4)',
            {'g1': groups.get('g1'), 'g2': groups.get('g2'), 'g3':groups.get('g3'), 'g4': groups.get('g4')},
            fetch=False
        )
        self.db.exec_sql(
            'delete from capabilities_http where capability_name in (:n1, :n2, :n3)',
            {'n1': 'test1', 'n2': 'test2', 'n3': 'test3'},
            fetch=False,
        )

    def test_pgiam(self) -> None:
        self.set_db_connection()

        pid = None

        _in_full_name = 'Kor Ah'
        _in_uname = 'kor1'

        _in_group1 = 'g1'
        _in_group2 = 'g2'
        _in_group3 = 'g3'
        _in_group4 = 'g4'

        groups = {
            'g1': _in_group1,
            'g2': _in_group2,
            'g3': _in_group3,
            'g4': _in_group4,
        }

        grname1 = 'grant_1'
        grname2 = 'grant_2'
        grname3 = 'grant_3'
        grname4 = 'grant_4'
        grname5 = 'grant_5'
        grants = [grname1, grname2, grname3, grname4, grname5]

        try:
            # create a person, get the person ID
            self.db.exec_sql(
                'insert into persons(full_name) values (:full_name)',
                {'full_name': _in_full_name},
                fetch=False,
            )
            pid = self.db.exec_sql(
                'select person_id from persons where full_name = :full_name',
                {'full_name': _in_full_name}
            )[0][0]

            # create a user
            self.db.exec_sql(
                'insert into users(person_id, user_name) values (:pid, :user_name)',
                {'pid': pid, 'user_name': _in_uname},
                fetch=False,
            )

            # create groups
            for _, group in groups.items():
                self.db.exec_sql(
                    'insert into groups(group_name, group_class, group_type) values (:name, :class, :type)',
                        {'name': group, 'class': 'secondary', 'type': 'generic'},
                        fetch=False,
                    )

            # add members
            print(self.db.group_member_add(_in_group1, _in_group2))
            print(self.db.group_member_add(_in_group1, _in_group3))
            print(self.db.group_member_add(_in_group2, _in_uname))

            # add moderators
            self.db.exec_sql(
                'insert into group_moderators(group_name, group_moderator_name) values (:group, :mod)',
                {'group': _in_group1, 'mod': _in_group4},
                fetch=False,
            )

            # informational
            print(self.db.person_groups(pid))
            print(self.db.user_groups(_in_uname))
            print(self.db.group_members(_in_group1))
            print(self.db.group_moderators(_in_group1))
            print(self.db.group_member_remove(_in_group1, _in_group3))
            print(self.db.group_members(_in_group1))

            # capabilities
            names1 = [
                {
                    'capability_name': 'test1',
                    'capability_required_groups': [_in_group1],
                    'capability_lifetime': 60,
                    'capability_description': 'allows testing',
                    'capability_hostnames': [],
                },
                {
                    'capability_name': 'test2',
                    'capability_required_groups': [_in_group1],
                    'capability_lifetime': 60,
                    'capability_description': 'allows nothing',
                    'capability_hostnames': [],
                },
            ]
            print(self.db.capabilities_http_sync(names1))
            caps1 = self.db.exec_sql(
                'select * from capabilities_http where capability_name in (:n1, :n2)',
                {'n1': 'test1', 'n2': 'test2'},
            )
            print(caps1)
            # check both are there
            # and have expected groups
            name_col_idx = 2
            group_col_idx = 5
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
                    'capability_description': 'allows one thing',
                    'capability_hostnames': [],
                },
                {
                    'capability_name': 'test2',
                    'capability_required_groups': [_in_group2],
                    'capability_lifetime': 60,
                    'capability_description': 'allows another thing',
                    'capability_hostnames': [],
                },
                {
                    'capability_name': 'test3',
                    'capability_required_groups': [_in_group1, '{0}-group'.format(_in_uname)],
                    'capability_lifetime': 60,
                    'capability_description': 'allows many things',
                    'capability_hostnames': [],
                },
            ]
            print(self.db.capabilities_http_sync(names2))
            caps2 = self.db.exec_sql(
                'select * from capabilities_http where capability_name in (:n1, :n2, :n3)',
                {'n1': 'test1', 'n2': 'test2', 'n3': 'test3'},
            )
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

            # deletion of capabilities that are no longer in the
            # reference data

            names3 = [
                {
                    'capability_name': 'test1',
                    'capability_required_groups': [_in_group1],
                    'capability_lifetime': 10,
                    'capability_description': 'allows one thing',
                    'capability_hostnames': [],
                },
                {
                    'capability_name': 'test2',
                    'capability_required_groups': [_in_group2],
                    'capability_lifetime': 600,
                    'capability_description': 'allows another thing',
                    'capability_hostnames': [],
                },
            ]

            print(self.db.capabilities_http_sync(names3))

            caps3 = self.db.exec_sql(
                'select * from capabilities_http',
            )
            assert len(caps3) == 2

            # grants
            grants1 = [
                {
                    'capability_grant_name': grname1,
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
                    'capability_grant_name': grname2,
                    'capability_names_allowed': ['test2'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files',
                    'capability_grant_http_method': 'HEAD',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/export$',
                    'capability_grant_required_groups': [_in_group3, _in_group4],
                    'capability_grant_required_attributes': {
                        'required_claims': ['lol'],
                    },
                },
            ]
            print('grant sync 1: \n')
            print(self.db.capabilities_http_grants_sync(grants1, static_grants=True))
            # check the db, then add a new sync, and check the result
            gs1 = self.db.exec_sql(
                'select * from capabilities_http_grants where capability_grant_name in (:gn1, :gn2)',
                {'gn1': grname1, 'gn2': grname2},
            )
            print(gs1)
            gs = gs1
            g_rank_idx = 7
            g_req_gr_idx = 9
            g_req_attr_idx = 10
            assert len(gs) == 2
            assert (gs[0][g_rank_idx] == 1 and gs[0][g_req_gr_idx] == ['self', 'moderator'])
            assert (gs[1][g_rank_idx] == 1 and gs[1][g_req_gr_idx] == [_in_group3, _in_group4])
            assert gs[1][g_req_attr_idx] == {'required_claims': ['lol']}

            # test changing groups, ranks, and introducing a new grant
            grants2 = [
                {
                    'capability_grant_name': grname1,
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
                    'capability_grant_name': grname2,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files',
                    'capability_grant_http_method': 'HEAD',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/export$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
                {
                    'capability_grant_name': grname3,
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
            print(self.db.capabilities_http_grants_sync(grants2, static_grants=True))
            gs2 = self.db.exec_sql(
                'select * from capabilities_http_grants where capability_grant_name in (:gn1, :gn2, :gn3)',
                {'gn1': grname1, 'gn2': grname2, 'gn3': grname3},
            )
            print(gs2)
            gs = gs2
            assert len(gs) == 3
            assert (gs[0][g_rank_idx] == 1 and gs[0][g_req_gr_idx] == ['self', 'moderator'])
            assert (gs[1][g_rank_idx] == 1 and gs[1][g_req_gr_idx] == [_in_group1, _in_group2])
            assert (gs[2][g_rank_idx] == 2 and gs[2][g_req_gr_idx] == [_in_group1])

            # set the rank explicitly
            print(self.db.capability_grant_rank_set(self.grant_id_from_name(grname3), 1))
            gs = self.db.exec_sql(
                'select * from capabilities_http_grants where capability_grant_name = :gn3',
                {'gn3': grname3},
            )
            assert gs[0][g_rank_idx] == 1

            # delete a grant
            print(self.db.capability_grant_delete(self.grant_id_from_name(grname3)))
            gs = self.db.exec_sql(
                'select * from capabilities_http_grants where capability_grant_name in (:gn1, :gn2, :gn3)',
                {'gn1': grname1, 'gn2': grname2, 'gn3': grname3}
            )
            assert gs[0][g_rank_idx] == 1 # reset automatically

            # test static grant sync

            # first add some more grants

            grants3 = [
                {
                    'capability_grant_name': grname1,
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
                    'capability_grant_name': grname2,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files',
                    'capability_grant_http_method': 'HEAD',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/export$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
                {
                    'capability_grant_name': grname3,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files',
                    'capability_grant_http_method': 'HEAD',
                    'capability_grant_rank': 2,
                    'capability_grant_uri_pattern': '/files/export/meh$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
                {
                    'capability_grant_name': grname4,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files/import',
                    'capability_grant_http_method': 'PUT',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/import$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
                {
                    'capability_grant_name': grname5,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files/import',
                    'capability_grant_http_method': 'PUT',
                    'capability_grant_rank': 2,
                    'capability_grant_uri_pattern': '/files/import/lol$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
            ]

            print('grant sync 3: \n')
            print(self.db.capabilities_http_grants_sync(grants3, static_grants=True))

            # then sync a subset, and check that the correct ones are deleted

            grants4 = [
                {
                    'capability_grant_name': grname1,
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
                    'capability_grant_name': grname2,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files',
                    'capability_grant_http_method': 'HEAD',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/export$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
                {
                    'capability_grant_name': grname5,
                    'capability_names_allowed': ['test1'],
                    'capability_grant_hostnames': ['my.api.com'],
                    'capability_grant_namespace': 'files/import',
                    'capability_grant_http_method': 'PUT',
                    'capability_grant_rank': 1,
                    'capability_grant_uri_pattern': '/files/import/lol$',
                    'capability_grant_required_groups': [_in_group1, _in_group2]
                },
            ]

            print('grant sync 4: \n')
            results = self.db.capabilities_http_grants_sync(grants4, static_grants=True)
            print(results)

            gs = self.db.exec_sql('select * from capabilities_http_grants')
            assert len(gs) == 3
            existing_names = list(map(lambda x: x[3], gs))
            deleted_names = results.get("deletes")
            # confirm deleted as expected
            assert set(deleted_names).intersection(existing_names) == set()

            # test deleting a namespace

            self.db.capability_grants_delete('files')
            gs = self.db.exec_sql('select * from capabilities_http_grants')
            assert len(gs) == 1

            # informational
            print(self.db.person_capabilities(pid))
            print(self.db.person_access(pid))
            print(self.db.user_capabilities(_in_uname))
            print(self.db.group_capabilities('{0}-group'.format(_in_uname)))
            print(self.db.capabilities_http_grants_group_add(grname1, _in_group2))
            print(self.db.capabilities_http_grants_group_remove(grname1, _in_group2))

        except Exception as e:
            self.cleanup(pid, grants, groups)
            raise e
        finally:
            self.cleanup(pid, grants, groups)
            print('ALL GOOD')
