# coding: utf-8
from sqlalchemy import (ARRAY, Boolean, CheckConstraint, Column, Date,
                        DateTime, ForeignKey, Integer, Table, Text,
                        UniqueConstraint, text, select, event, DDL)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_utils import create_view


Base = declarative_base()
metadata = Base.metadata


event.listen(
    Base.metadata,
    'before_create',
    DDL('CREATE EXTENSION IF NOT EXISTS pgcrypto;').execute_if(dialect='postgresql'))

event.listen(
    Base.metadata,
    'after_drop',
    DDL("DROP extension pgcrypto;"
        "DROP FUNCTION IF EXISTS assert_array_unique (text[], text);"
        "DROP FUNCTION IF EXISTS capabilities_http_grants_group_check ();"
        "DROP FUNCTION IF EXISTS capabilities_http_grants_immutability ();"
        "DROP FUNCTION IF EXISTS capabilities_http_group_check ();"
        "DROP FUNCTION IF EXISTS capabilities_http_immutability ();"
        "DROP FUNCTION IF EXISTS capabilities_http_instances_immutability ();"
        "DROP FUNCTION IF EXISTS capability_grant_delete (text);"
        "DROP FUNCTION IF EXISTS capability_grant_group_add (text, text);"
        "DROP FUNCTION IF EXISTS capability_grant_group_remove (text, text);"
        "DROP FUNCTION IF EXISTS capability_grant_rank_set (text, integer);"
        "DROP FUNCTION IF EXISTS capability_instance_get (text);"
        "DROP FUNCTION IF EXISTS drop_tables (boolean);"
        "DROP FUNCTION IF EXISTS ensure_capability_name_references_consistent ();"
        "DROP FUNCTION IF EXISTS ensure_correct_capability_names_allowed ();"
        "DROP FUNCTION IF EXISTS ensure_sensible_rank_update ();"
        "DROP FUNCTION IF EXISTS ensure_unique_capability_attributes ();"
        "DROP FUNCTION IF EXISTS ensure_unique_grant_arrays ();"
        "DROP FUNCTION IF EXISTS generate_grant_rank ();"
        "DROP FUNCTION IF EXISTS generate_new_posix_gid ();"
        "DROP FUNCTION IF EXISTS generate_new_posix_id (text, text);"
        "DROP FUNCTION IF EXISTS generate_new_posix_uid ();"
        "DROP FUNCTION IF EXISTS get_memberships (text, text);"
        "DROP FUNCTION IF EXISTS group_capabilities (text, boolean);"
        "DROP FUNCTION IF EXISTS group_deletion ();"
        "DROP FUNCTION IF EXISTS group_get_children (text);"
        "DROP FUNCTION IF EXISTS group_get_parents (text);"
        "DROP FUNCTION IF EXISTS group_immutability ();"
        "DROP FUNCTION IF EXISTS group_management ();"
        "DROP FUNCTION IF EXISTS group_member_add (text, text);"
        "DROP FUNCTION IF EXISTS group_member_remove (text, text);"
        "DROP FUNCTION IF EXISTS group_members (text);"
        "DROP FUNCTION IF EXISTS group_memberships_check_dag_requirements ();"
        "DROP FUNCTION IF EXISTS group_memberships_immutability ();"
        "DROP FUNCTION IF EXISTS group_moderators (text);"
        "DROP FUNCTION IF EXISTS group_moderators_check_dag_requirements ();"
        "DROP FUNCTION IF EXISTS group_moderators_immutability ();"
        "DROP FUNCTION IF EXISTS grp_cpbts (text, boolean);"
        "DROP FUNCTION IF EXISTS grp_mems (text);"
        "DROP FUNCTION IF EXISTS person_access (text);"
        "DROP FUNCTION IF EXISTS person_capabilities (text, boolean);"
        "DROP FUNCTION IF EXISTS person_groups (text);"
        "DROP FUNCTION IF EXISTS person_immutability ();"
        "DROP FUNCTION IF EXISTS person_management ();"
        "DROP FUNCTION IF EXISTS person_uniqueness ();"
        "DROP FUNCTION IF EXISTS posix_gid ();"
        "DROP FUNCTION IF EXISTS sync_posix_gid_to_users ();"
        "DROP FUNCTION IF EXISTS test_capability_instances ();"
        "DROP FUNCTION IF EXISTS update_audit_log_objects ();"
        "DROP FUNCTION IF EXISTS update_audit_log_relations ();"
        "DROP FUNCTION IF EXISTS user_capabilities (text, boolean);"
        "DROP FUNCTION IF EXISTS user_groups (text);"
        "DROP FUNCTION IF EXISTS user_immutability ();"
        "DROP FUNCTION IF EXISTS user_management ();"
        "DROP FUNCTION IF EXISTS user_moderators (text);"
        ).execute_if(dialect='postgresql'))


t_audit_log_objects = Table(
    'audit_log_objects', metadata,
    Column('identity', Text),
    Column('operation', Text, nullable=False),
    Column('event_time', DateTime(True), server_default=text("now()")),
    Column('table_name', Text, nullable=False),
    Column('row_id', UUID, nullable=False),
    Column('column_name', Text),
    Column('old_data', Text),
    Column('new_data', Text),
    postgresql_partition_by='LIST (table_name)'
)

event.listen(
    t_audit_log_objects,
    "after_create",
    DDL("CREATE TABLE audit_log_objects_persons PARTITION OF audit_log_objects "
        "FOR VALUES IN ('persons');"
        "CREATE TABLE audit_log_objects_users PARTITION OF audit_log_objects "
        "FOR VALUES IN ('users');"
        "CREATE TABLE audit_log_objects_groups PARTITION OF audit_log_objects "
        "FOR VALUES IN ('groups');"
        "CREATE TABLE audit_log_objects_capabilities_http PARTITION OF audit_log_objects "
        "FOR VALUES IN ('capabilities_http');"
        "CREATE TABLE audit_log_objects_capabilities_http_instances PARTITION OF audit_log_objects "
        "FOR VALUES IN ('capabilities_http_instances');")
    )

event.listen(
    Base.metadata,
    'before_create',
    DDL('''CREATE FUNCTION assert_array_unique (arr text[], name text)  RETURNS void
  VOLATILE
AS $body$
declare err text;
    begin
        if arr is not null then
            err := 'duplicate ' || name;
            assert (select cardinality(array(select distinct unnest(arr)))) =
                   (select cardinality(arr)), err;
        end if;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION capabilities_http_grants_group_check ()  RETURNS trigger
  VOLATILE
AS $body$
declare new_grps text[];
    declare new_grp text;
    declare num int;
    begin
        if NEW.capability_grant_group_existence_check = 'f' then
            return new;
        end if;
        for new_grp in select unnest(NEW.capability_grant_required_groups) loop
            select count(*) from groups where group_name like '%%' || new_grp || '%%' into num;
            if new_grp not in ('self', 'moderator') then
                assert num > 0, new_grp || ' does not exist';
            end if;
        end loop;
        return new;
    end; $body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capabilities_http_grants_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        assert OLD.row_id = NEW.row_id, 'row_id is immutable';
        assert OLD.capability_grant_id = NEW.capability_grant_id, 'capability_grant_id is immutable';
    return new;
    end; $body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capabilities_http_group_check ()  RETURNS trigger
  VOLATILE
AS $body$
declare new_grps text[];
    declare new_grp text;
    declare num int;
    begin
        if NEW.capability_group_existence_check = 'f' then
            return new;
        end if;
        for new_grp in select unnest(NEW.capability_required_groups) loop
            select count(*) from groups where group_name like '%%' || new_grp || '%%' into num;
            assert num > 0, new_grp || ' does not exist';
        end loop;
        return new;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))


event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION capabilities_http_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        assert OLD.row_id = NEW.row_id, 'row_id is immutable';
        assert OLD.capability_id = NEW.capability_id, 'capability_id is immutable';
        assert OLD.capability_name = NEW.capability_name, 'capability_name is immutable';
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capabilities_http_instances_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        assert OLD.row_id = NEW.row_id, 'row_id is immutable';
        assert OLD.capability_name = NEW.capability_name, 'capability_name is immutable';
        assert OLD.instance_id = NEW.instance_id, 'instance_id is immutable';
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capability_grant_delete (grant_id text)  RETURNS boolean
  VOLATILE
AS $body$
declare target_id uuid;
    declare target_rank int;
    declare target_namespace text;
    declare target_http_method text;
    declare ans boolean;
    begin
        target_id := grant_id::uuid;
        select capability_grant_namespace, capability_grant_http_method
            from capabilities_http_grants where capability_grant_id = target_id
            into target_namespace, target_http_method;
        select max(capability_grant_rank) from capabilities_http_grants
            where capability_grant_namespace = target_namespace
            and capability_grant_http_method = target_http_method
            into target_rank;
        select capability_grant_rank_set(target_id::text, target_rank) into ans;
        delete from capabilities_http_grants where capability_grant_id = target_id;
        return true;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capability_grant_group_add (grant_reference text, group_name text)  RETURNS boolean
  VOLATILE
AS $body$
declare current text[];
    declare new text[];
    declare num int;
    begin
        begin
            perform grant_reference::uuid;
            select count(*) from capabilities_http_grants
                where capability_grant_id = grant_reference::uuid into num;
            assert num = 1, 'grant not found';
            select capability_grant_required_groups from capabilities_http_grants
                where capability_grant_id = grant_reference::uuid into current;
            select array_append(current, group_name) into new;
            update capabilities_http_grants set capability_grant_required_groups = new
                where capability_grant_id = grant_reference::uuid;
        exception when invalid_text_representation then
            select count(*) from capabilities_http_grants
                where capability_grant_name = grant_reference into num;
            assert num = 1, 'grant not found';
            select capability_grant_required_groups from capabilities_http_grants
                where capability_grant_name = grant_reference into current;
            select array_append(current, group_name) into new;
            update capabilities_http_grants set capability_grant_required_groups = new
                where capability_grant_name = grant_reference;
        end;
        return true;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capability_grant_group_remove (grant_reference text, group_name text)  RETURNS boolean
  VOLATILE
AS $body$
declare current text[];
    declare new text[];
    declare num int;
    begin
        begin
            perform grant_reference::uuid;
            select count(*) from capabilities_http_grants
                where capability_grant_id = grant_reference::uuid into num;
            assert num = 1, 'grant not found';
            select capability_grant_required_groups from capabilities_http_grants
                where capability_grant_id = grant_reference::uuid into current;
            select array_remove(current, group_name) into new;
            if cardinality(new) = 0 then new := null; end if;
            update capabilities_http_grants set capability_grant_required_groups = new
                where capability_grant_id = grant_reference::uuid;
        exception when invalid_text_representation then
            select count(*) from capabilities_http_grants
                where capability_grant_name = grant_reference into num;
            assert num = 1, 'grant not found';
            select capability_grant_required_groups from capabilities_http_grants
                where capability_grant_name = grant_reference into current;
            select array_remove(current, group_name) into new;
            if cardinality(new) = 0 then new := null; end if;
            update capabilities_http_grants set capability_grant_required_groups = new
                where capability_grant_name = grant_reference;
        end;
        return true;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capability_grant_rank_set (grant_id text, new_grant_rank integer)  RETURNS boolean
  VOLATILE
AS $body$
declare target_id uuid;
    declare target_curr_rank int;
    declare target_namespace text;
    declare target_http_method text;
    declare curr_rank int;
    declare curr_id uuid;
    declare new_val int;
    declare current_max int;
    declare current_max_id uuid;
    begin
        target_id := grant_id::uuid;
        assert target_id in (select capability_grant_id from capabilities_http_grants),
            'grant_id not found';
        select capability_grant_rank from capabilities_http_grants
            where capability_grant_id = target_id into target_curr_rank;
        if new_grant_rank = target_curr_rank then
            return true;
        end if;
        select capability_grant_namespace, capability_grant_http_method
            from capabilities_http_grants where capability_grant_id = target_id
            into target_namespace, target_http_method;
        select max(capability_grant_rank) from capabilities_http_grants
            where capability_grant_namespace = target_namespace
            and capability_grant_http_method = target_http_method
            into current_max;
        assert new_grant_rank - current_max <= 1,
            'grant rank values must be monotonically increasing';
        if current_max = 1 then
            select capability_grant_id from capabilities_http_grants
                where capability_grant_namespace = target_namespace
                and capability_grant_http_method = target_http_method
                and capability_grant_rank = current_max
                into current_max_id;
            if current_max_id = target_id then
                assert new_grant_rank = 1, 'first entry must start at 1';
            end if;
        end if;
        update capabilities_http_grants set capability_grant_rank = null
            where capability_grant_id = target_id;
        if new_grant_rank < target_curr_rank then
            for curr_id, curr_rank in
                select capability_grant_id, capability_grant_rank from capabilities_http_grants
                where capability_grant_rank >= new_grant_rank
                and capability_grant_rank < target_curr_rank
                and capability_grant_namespace = target_namespace
                and capability_grant_http_method = target_http_method
                order by capability_grant_rank desc
            loop
                new_val := curr_rank + 1;
                update capabilities_http_grants set capability_grant_rank = new_val
                    where capability_grant_id = curr_id;
            end loop;
        elsif new_grant_rank > target_curr_rank then
            for curr_id, curr_rank in
                select capability_grant_id, capability_grant_rank from capabilities_http_grants
                where capability_grant_rank <= new_grant_rank
                and capability_grant_rank > target_curr_rank
                and capability_grant_namespace = target_namespace
                and capability_grant_http_method = target_http_method
                order by capability_grant_rank asc
            loop
                new_val := curr_rank - 1;
                update capabilities_http_grants set capability_grant_rank = new_val
                    where capability_grant_id = curr_id;
            end loop;
        end if;
        update capabilities_http_grants set capability_grant_rank = new_grant_rank
            where capability_grant_id = target_id;
        return true;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION capability_instance_get (id text)  RETURNS json
  VOLATILE
AS $body$
declare iid uuid;
    declare cname text;
    declare start_date timestamptz;
    declare end_date timestamptz;
    declare max int;
    declare meta json;
    declare msg text;
    declare new_max int;
    begin
        iid := id::uuid;
        assert iid in (select instance_id from capabilities_http_instances),
            'instance not found';
        select capability_name, instance_start_date, instance_end_date,
               instance_usages_remaining, instance_metadata
        from capabilities_http_instances where instance_id = iid
            into cname, start_date, end_date, max, meta;
        msg := 'instance not active yet - start time: ' || start_date::text;
        assert current_timestamp > start_date, msg;
        msg := 'instance expired - end time: ' || end_date::text;
        if current_timestamp > end_date then
            delete from capabilities_http_instances where instance_id = iid;
            assert false, msg;
        end if;
        new_max := null;
        if max is not null then
            new_max := max - 1;
            if new_max < 1 then
                delete from capabilities_http_instances where instance_id = iid;
            else
                update capabilities_http_instances set instance_usages_remaining = new_max
                    where instance_id = iid;
            end if;
        end if;
        return json_build_object('capability_name', cname,
                                 'instance_id', id,
                                 'instance_start_date', start_date,
                                 'instance_end_date', end_date,
                                 'instance_usages_remaining', new_max,
                                 'instance_metadata', meta);
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION drop_tables (drop_table_flag boolean DEFAULT true)  RETURNS boolean
  VOLATILE
AS $body$
declare ans boolean;
    begin
        if drop_table_flag = 'true' then
            raise notice 'DROPPING CAPABILITIES TABLES';
            drop table if exists capabilities_http cascade;
            drop table if exists capabilities_http_instances cascade;
            drop table if exists capabilities_http_grants cascade;
        else
            raise notice 'NOT dropping tables - only functions will be replaced';
        end if;
    return true;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION ensure_capability_name_references_consistent ()  RETURNS trigger
  VOLATILE
AS $body$
declare name_references text[];
    declare grant_id uuid;
    declare new text[];
    begin
        for name_references, grant_id in
            select capability_names_allowed, capability_grant_id from capabilities_http_grants
            where array[OLD.capability_name] <@ capability_names_allowed loop
            select array_remove(name_references, OLD.capability_name) into new;
            assert cardinality(new) > 0,
                'deleting the capability would leave one or more grants ' ||
                'without a reference to any capability which is not allowed ' ||
                'delete the grant before deleting the capability, or change the reference';
            update capabilities_http_grants set capability_names_allowed = new
                where capability_grant_id = grant_id;
        end loop;
        return old;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION ensure_correct_capability_names_allowed ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        perform assert_array_unique(NEW.capability_names_allowed, 'capability_names_allowed');
        assert NEW.capability_names_allowed <@
            (select array_append(array_agg(capability_name), 'all') from capabilities_http),
            'trying to reference a capability name which does not exists: ' || NEW.capability_names_allowed::text;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION ensure_sensible_rank_update ()  RETURNS trigger
  VOLATILE
AS $body$
declare num int;
    begin
        select count(*) from capabilities_http_grants
            where capability_grant_namespace = NEW.capability_grant_namespace
            and capability_grant_http_method = NEW.capability_grant_http_method
            into num;
        if (num > 0 and NEW.capability_grant_rank > num) then
            assert false, 'Rank cannot be updated to a value higher than the number of entries per hostname, namespace, method';
        end if;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION ensure_unique_capability_attributes ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        perform assert_array_unique(NEW.capability_required_groups, 'capability_required_groups');
        perform assert_array_unique(NEW.capability_hostnames, 'capability_hostnames');
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION ensure_unique_grant_arrays ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        perform assert_array_unique(NEW.capability_grant_required_groups, 'capability_grant_required_groups');
        perform assert_array_unique(NEW.capability_grant_hostnames, 'capability_grant_hostnames');
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION generate_grant_rank ()  RETURNS trigger
  VOLATILE
AS $body$
declare num int;
    begin
        -- check if first grant for (host, namespace, method) combination
        select count(*) from capabilities_http_grants
            where capability_grant_namespace = NEW.capability_grant_namespace
            and capability_grant_http_method = NEW.capability_grant_http_method
            into num;
        if NEW.capability_grant_rank is not null then
            assert NEW.capability_grant_rank = num,
                'grant rank values must be monotonically increasing';
            return new;
        end if;
        update capabilities_http_grants set capability_grant_rank = num
            where capability_grant_id = NEW.capability_grant_id;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION generate_new_posix_gid ()  RETURNS integer
  VOLATILE
AS $body$
declare new_gid int;
    begin
        select generate_new_posix_id('groups', 'group_posix_gid') into new_gid;
        return new_gid;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION generate_new_posix_id (table_name text, colum_name text)  RETURNS integer
  VOLATILE
AS $body$
declare current_max_id int;
    declare new_id int;
    begin
        execute format('select max(%%I) from %%I',
            quote_ident(colum_name), quote_ident(table_name))
            into current_max_id;
        if current_max_id is null then
            new_id := 1000;
        elsif current_max_id >= 0 and current_max_id <= 999 then
            new_id := 1000;
        elsif current_max_id >= 200000 and current_max_id <= 220000 then
            new_id := 220001;
        else
            new_id := current_max_id + 1;
        end if;
        return new_id;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION generate_new_posix_uid ()  RETURNS integer
  VOLATILE
AS $body$
declare new_uid int;
    begin
        select generate_new_posix_id('users', 'user_posix_uid') into new_uid;
        return new_uid;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION get_memberships (member text, grp text)  RETURNS json
  VOLATILE
AS $body$
declare data json;
    begin
        execute format(
            'select json_agg(json_build_object(
                $1, member_name,
                $2, member_group_name,
                $3, group_activated,
                $4, group_expiry_date))
            from (select member_name, member_group_name from group_get_parents($5)
                  union select %%s, %%s)a
            join (select group_name, group_activated, group_expiry_date from groups)b
            on a.member_group_name = b.group_name', quote_literal(member), quote_literal(grp))
            using 'member_name', 'member_group', 'group_activated', 'group_expiry_date', grp
            into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION group_capabilities (group_name text, grants boolean DEFAULT false)  RETURNS json
  VOLATILE
AS $body$
declare data json;
    begin
        select grp_cpbts(group_name, grants) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_deletion ()  RETURNS trigger
  VOLATILE
AS $body$
declare amount int;
    begin
        if OLD.group_type = 'person' then
            select count(*) from persons where person_group = OLD.group_name into amount;
            if amount = 1 then
                raise exception using
                message = 'person groups are automatically created and deleted based on person objects';
            end if;
        elsif OLD.group_type = 'user' then
            select count(*) from users where user_group = OLD.group_name into amount;
            if amount = 1 then
                raise exception using
                message = 'user groups are automatically created and deleted based on user objects';
            end if;
        end if;
    return old;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_get_children (parent_group text)  RETURNS SETOF pgiam.members
  VOLATILE
AS $body$
declare num int;
    declare gn text;
    declare gmn text;
    declare gpm text;
    declare gc text;
    declare row record;
    declare current_member text;
    declare new_current_member text;
    declare recursive_current_member text;
    begin
        create temporary table if not exists sec(group_name text, group_member_name text, group_class text, group_primary_member text) on commit drop;
        create temporary table if not exists mem(group_name text, group_member_name text, group_class text, group_primary_member text) on commit drop;
        delete from sec;
        delete from mem;
        select count(*) from pgiam.first_order_members where group_name = parent_group
            and group_class = 'secondary' into num;
        if num = 0 then
            return query execute format ('select group_name, group_member_name, group_class, group_primary_member
                from pgiam.first_order_members where group_name = $1 order by group_primary_member') using parent_group;
        else
            for gn, gmn, gc, gpm in select group_name, group_member_name, group_class, group_primary_member
                from pgiam.first_order_members where group_name = parent_group
                and group_class = 'primary' loop
                insert into mem values (gn, gmn, gc, gpm);
            end loop;
            for gn, gmn, gc, gpm in select group_name, group_member_name, group_class, group_primary_member
                from pgiam.first_order_members where group_name = parent_group
                and group_class = 'secondary' loop
                insert into sec values (gn, gmn, gc, gpm);
            end loop;
            select count(*) from sec into num;
            while num > 0 loop
                select group_member_name from sec limit 1 into current_member;
                select group_name, group_member_name, group_class, group_primary_member
                    from sec where group_member_name = current_member
                    into gn, gmn, gc, gpm;
                if gc = 'primary' then
                    insert into mem values (gn, gmn, gc, gpm);
                elsif gc = 'secondary' then
                    insert into mem values (gn, gmn, gc, gpm);
                    new_current_member := gmn;
                    -- first add primary groups to members, and remove them from sec
                    for gn, gmn, gc, gpm in select group_name, group_member_name, group_class, group_primary_member
                        from pgiam.first_order_members where group_name = new_current_member loop
                        if gc = 'primary' then
                            insert into mem values (gn, gmn, gc, gpm);
                            delete from sec where group_member_name = gmn;
                        else
                            recursive_current_member := gmn;
                            insert into mem values (gn, gmn, gc, gpm);
                            -- this new secondary member can have both primary and seconday
                            -- members itself, but just add all its members to sec, and we will handle them
                            for gn, gmn, gc, gpm in select group_name, group_member_name, group_class, group_primary_member
                                from pgiam.first_order_members where group_name = recursive_current_member loop
                                insert into sec values (gn, gmn, gc, gpm);
                            end loop;
                        end if;
                    end loop;
                end if;
                delete from sec where group_member_name = current_member;
                select count(*) from sec into num;
            end loop;
            return query select * from mem order by group_primary_member;
        end if;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION group_get_parents (child_group text)  RETURNS SETOF pgiam.memberships
  VOLATILE
AS $body$
declare num int;
    declare mgn text;
    declare mn text;
    declare gn text;
    begin
        create temporary table if not exists candidates(member_name text, member_group_name text) on commit drop;
        create temporary table if not exists parents(member_name text, member_group_name text) on commit drop;
        delete from candidates;
        delete from parents;
        for gn in select group_name from pgiam.first_order_members where group_member_name = child_group loop
            insert into candidates values (child_group, gn);
        end loop;
        select count(*) from candidates into num;
        while num > 0 loop
            select member_name, member_group_name from candidates limit 1 into mn, mgn;
            insert into parents values (mn, mgn);
            delete from candidates where member_name = mn and member_group_name = mgn;
            -- now check if the current candidate has parents
            -- so we find all recursive memberships
            for gn in select group_name from pgiam.first_order_members where group_member_name = mgn loop
                insert into candidates values (mgn, gn);
            end loop;
            select count(*) from candidates into num;
        end loop;
        return query select * from parents;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if OLD.row_id != NEW.row_id then
            raise exception using message = 'row_id is immutable';
        elsif OLD.group_id != NEW.group_id then
            raise exception using message = 'group_id is immutable';
        elsif OLD.group_name != NEW.group_name then
            raise exception using message = 'group_name is immutable';
        elsif OLD.group_class != NEW.group_class then
            raise exception using message = 'group_class is immutable';
        elsif OLD.group_type != NEW.group_type then
            raise exception using message = 'group_type is immutable';
        elsif OLD.group_primary_member != NEW.group_primary_member then
            raise exception using message = 'group_primary_member is immutable';
        elsif OLD.group_posix_gid != NEW.group_posix_gid then
            raise exception using message = 'group_posix_gid is immutable';
        elsif NEW.group_posix_gid is null and OLD.group_posix_gid is not null then
            raise exception using message = 'group_posix_gid cannot be set to null once set';
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION group_management ()  RETURNS trigger
  VOLATILE
AS $body$
declare primary_member_state boolean;
    declare curr_user_exp date;
    begin
        if OLD.group_activated != NEW.group_activated then
            if OLD.group_type = 'person' then
                select person_activated from persons where person_group = OLD.group_name into primary_member_state;
                if NEW.group_activated != primary_member_state then
                    raise exception using message = 'person groups can only be deactived by deactivating persons';
                end if;
            elsif OLD.group_type = 'user' then
                select user_activated from users where user_group = OLD.group_name into primary_member_state;
                if NEW.group_activated != primary_member_state then
                    raise exception using message = 'user groups can only be deactived by deactivating users';
                end if;
            end if;
        elsif OLD.group_expiry_date != NEW.group_expiry_date then
            select user_expiry_date from users where user_name = NEW.group_primary_member into curr_user_exp;
            if NEW.group_expiry_date != curr_user_exp then
                raise exception using message = 'primary group dates are modified via modifications on persons/users';
            end if;
        end if;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_member_add (group_name text, member text)  RETURNS json
  VOLATILE
AS $body$
declare gnam text;
    declare unam text;
    declare mem text;
    begin
        gnam := $1;
        assert (select exists(select 1 from groups where groups.group_name = gnam)) = 't', 'group does not exist';
        if member in (select groups.group_name from groups) then
            mem := member;
        else
            begin
                assert (select exists(select 1 from persons where persons.person_id = member::uuid)) = 't';
                select person_group from persons where persons.person_id = member::uuid into mem;
            exception when others or assert_failure then
                begin
                    assert (select exists(select 1 from users where users.user_name = member)) = 't';
                    select user_group from users where users.user_name = member into mem;
                exception when others or assert_failure then
                    return json_build_object('message', 'could not add member');
                end;
            end;
        end if;
        execute format('insert into group_memberships values ($1, $2)')
            using gnam, mem;
        return json_build_object('message', 'member added');
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_member_remove (group_name text, member text)  RETURNS json
  VOLATILE
AS $body$
declare gnam text;
    declare unam text;
    declare mem text;
    begin
        gnam := $1;
        assert (select exists(select 1 from groups where groups.group_name = gnam)) = 't', 'group does not exist';
        if member in (select groups.group_name from groups) then
            mem := member;
        else
            begin
                assert (select exists(select 1 from persons where persons.person_id = member::uuid)) = 't';
                select person_group from persons where persons.person_id = member::uuid into mem;
            exception when others or assert_failure then
                begin
                    assert (select exists(select 1 from users where users.user_name = member)) = 't';
                    select user_group from users where users.user_name = member into mem;
                exception when others or assert_failure then
                    return json_build_object('message', 'could not remove member');
                end;
            end;
        end if;
        execute format('delete from group_memberships where group_name = $1 and group_member_name = $2')
            using gnam, mem;
        return json_build_object('message', 'member removed');
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_members (group_name text)  RETURNS json
  VOLATILE
AS $body$
declare direct_data json;
    declare transitive_data json;
    declare primary_data json;
    declare data json;
    begin
        assert (select exists(select 1 from groups where groups.group_name = $1)) = 't', 'group does not exist';
        select json_agg(distinct group_primary_member) from group_get_children($1)
            where group_primary_member is not null into primary_data;
        select json_agg(json_build_object(
            'group', gm.group_name,
            'group_member', gm.group_member_name,
            'primary_member', gm.group_primary_member,
            'activated', gm.group_activated,
            'expiry_date', gm.group_expiry_date))
            from grp_mems($1) gm where gm.group_name = $1 into direct_data;
        select json_agg(json_build_object(
            'group', gm.group_name,
            'group_member', gm.group_member_name,
            'primary_member', gm.group_primary_member,
            'activated', gm.group_activated,
            'expiry_date', gm.group_expiry_date))
            from grp_mems($1) gm where gm.group_name != $1 into transitive_data;
        select json_build_object('group_name', group_name,
                                 'direct_members', direct_data,
                                 'transitive_members', transitive_data,
                                 'ultimate_members', primary_data) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION group_memberships_check_dag_requirements ()  RETURNS trigger
  VOLATILE
AS $body$
declare response text;
    begin
        -- Ensure we have only Directed Acylic Graphs, where primary groups are only allowed in leaves
        -- if a any of the groups are currently inactive or expired, the membership cannot be created
        -- also disallow any self-referential entries
        assert NEW.group_name != NEW.group_member_name, 'groups cannot be members of themselves';
        response := NEW.group_name || ' is a primary group - which cannot have members other than its primary member';
        assert (select NEW.group_name in
            (select group_name from groups where group_class = 'primary')) = 'f', response;
        assert (select group_activated from groups where group_name = NEW.group_name) = 't',
            NEW.group_name || ' is deactived - to use it in new group memberships it must be active';
        assert (select group_activated from groups where group_name = NEW.group_member_name) = 't',
            NEW.group_member_name || ' is deactived - to use it in new group memberships it must be active';
        assert (select case when group_expiry_date is not null then group_expiry_date else current_date end
                from groups where group_name = NEW.group_name) >= current_date,
            NEW.group_name || ' has expired - to use it in new group memberships its expiry date must be later than the current date';
        assert (select case when group_expiry_date is not null then group_expiry_date else current_date end
                from groups where group_name = NEW.group_member_name) >= current_date,
            NEW.group_member_name || ' has expired - to use it in new group memberships its expiry date must be later than the current date';
        response := 'Making ' || NEW.group_member_name || ' a member of ' || NEW.group_name
                    || ' would create a cyclical graph which is not allowed';
        assert (select NEW.group_member_name in
            (select member_group_name from group_get_parents(NEW.group_name))) = 'f', response;
        response := NEW.group_member_name || ' is already a member of ' || NEW.group_name;
        assert (select NEW.group_member_name in
            (select group_member_name from group_get_children(NEW.group_name))) = 'f', response;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_memberships_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if OLD.group_name != NEW.group_name then
            raise exception using message = 'group_name is immutable';
        elsif OLD.group_member_name != NEW.group_member_name then
            raise exception using message = 'group_member_name is immutable';
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_moderators (group_name text)  RETURNS json
  VOLATILE
AS $body$
declare data json;
    begin
        assert (select exists(select 1 from groups where groups.group_name = $1)) = 't', 'group does not exist';
        select json_agg(json_build_object(
            'moderator', a.group_moderator_name,
            'activated', b.group_activated,
            'expiry_date', b.group_expiry_date)) from
        (select gm.group_name, gm.group_moderator_name
            from group_moderators gm where gm.group_name = $1)a join
        (select g.group_name, g.group_activated, g.group_expiry_date
            from groups g)b on a.group_name = b.group_name into data;
        return json_build_object('group_name', group_name, 'group_moderators', data);
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_moderators_check_dag_requirements ()  RETURNS trigger
  VOLATILE
AS $body$
declare response text;
    declare new_grp text;
    declare new_mod text;
    begin
        assert NEW.group_name != NEW.group_moderator_name, 'groups cannot be moderators of themselves';
        response := NEW.group_name || ' is deactived - to use it in new group moderators it must be active';
        assert (select group_activated from groups where group_name = NEW.group_name) = 't', response;
        response := NEW.group_moderator_name || ' is deactived - to use it in new group moderators it must be active';
        assert (select group_activated from groups where group_name = NEW.group_moderator_name) = 't', response;
        response := NEW.group_name || ' has expired - to use it in new group moderators its expiry date must be later than the current date';
        assert (select case when group_expiry_date is not null then group_expiry_date else current_date end
                from groups where group_name = NEW.group_name) >= current_date, response;
        response := NEW.group_moderator_name || ' has expired - to use it in new group moderators its expiry date must be later than the current date';
        assert (select case when group_expiry_date is not null then group_expiry_date else current_date end
                from groups where group_name = NEW.group_moderator_name) >= current_date, response;
        response := NEW.group_name || ' is a primary group, and cannot be moderated';
        assert (select group_class from groups where group_name = NEW.group_name) = 'secondary', response;
        response := 'Making ' || NEW.group_name || ' a moderator of '
                   || NEW.group_moderator_name || ' will create a cyclical graph - which is not allowed.';
        assert (select count(*) from group_moderators
                where group_name = NEW.group_moderator_name
                and group_moderator_name = NEW.group_name) = 0, response;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION group_moderators_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if OLD.group_name != NEW.group_name then
            raise exception using message = 'group_name is immutable';
        elsif OLD.group_member_name != NEW.group_member_name then
            raise exception using message = 'group_member_name is immutable';
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION grp_cpbts (grp text, grants boolean DEFAULT false)  RETURNS json
  VOLATILE
AS $body$
declare ctype text;
    declare cgrps text[];
    declare rgrp text;
    declare reg text;
    declare matches boolean;
    declare grant_data json;
    declare data json;
    declare grnt_grp text[];
    declare grnt_mthd text;
    declare grnt_ptrn text;
    begin
        assert (select exists(select 1 from groups where group_name = grp)) = 't', 'group does not exist';
        create temporary table if not exists cpb(ct text unique not null) on commit drop;
        delete from cpb;
        -- exact group matches
        for ctype in select capability_name from capabilities_http
            where capability_group_match_method = 'exact'
            and array[grp] && capability_required_groups loop
            insert into cpb values (ctype);
        end loop;
        -- wildcard group matches
        for ctype, cgrps in select capability_name, capability_required_groups from capabilities_http
            where capability_group_match_method = 'wildcard' loop
            for rgrp in select unnest(cgrps) loop
                reg := '.*' || rgrp || '.*';
                if grp ~ reg then
                    begin
                        insert into cpb values (ctype);
                    exception when unique_violation then
                        null;
                    end;
                end if;
            end loop;
        end loop;
        select json_agg(ct) from cpb into data;
        if grants = 'f' then
            return json_build_object('group_name', grp, 'group_capabilities_http', data);
        else
            create temporary table if not exists grnts(method text, pattern text,
                unique (method, pattern)) on commit drop;
            for grnt_grp, grnt_mthd, grnt_ptrn in
                select capability_grant_required_groups, capability_grant_http_method, capability_grant_uri_pattern
                from capabilities_http_grants loop
                    for rgrp in select unnest(grnt_grp) loop
                        reg := '.*' || rgrp || '.*';
                        if grp ~ reg then
                            begin
                                insert into grnts values (grnt_mthd, grnt_ptrn);
                            exception when unique_violation then
                                null;
                            end;
                        end if;
                    end loop;
            end loop;
            select json_agg(json_build_object('method', method, 'pattern', pattern)) from grnts into grant_data;
            return json_build_object('group_name', grp, 'group_capabilities_http', data, 'grants', grant_data);
        end if;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION person_access (person_id text)  RETURNS json
  VOLATILE
AS $body$
declare pid uuid;
    declare p_data json;
    declare u_data json;
    declare data json;
    begin
        pid := $1::uuid;
        assert (select exists(select 1 from persons where persons.person_id = pid)) = 't', 'person does not exist';
        select person_capabilities($1, 't') into p_data;
        select json_agg(user_capabilities(user_name, 't')) from users, persons
            where users.person_id = persons.person_id and users.person_id = pid into u_data;
        select json_build_object('person_id', person_id,
                                 'person_group_access', p_data,
                                 'users_groups_access', u_data) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION person_capabilities (person_id text, grants boolean DEFAULT false)  RETURNS json
  VOLATILE
AS $body$
declare pid uuid;
    declare pgrp text;
    declare data json;
    begin
        pid := $1::uuid;
        assert (select exists(select 1 from persons where persons.person_id = pid)) = 't', 'person does not exist';
        select person_group from persons where persons.person_id = pid into pgrp;
        select json_agg(grp_cpbts(member_group_name, grants)) from group_get_parents(pgrp) into data;
        return json_build_object('person_id', person_id, 'person_capabilities', data);
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION person_groups (person_id text)  RETURNS json
  VOLATILE
AS $body$
declare pid uuid;
    declare pgrp text;
    declare res json;
    declare pgroups json;
    declare data json;
    begin
        pid := $1::uuid;
        assert (select exists(select 1 from persons where persons.person_id = pid)) = 't', 'person does not exist';
        select person_group from persons where persons.person_id = pid into pgrp;
        select get_memberships(person_id, pgrp) into pgroups;
        select json_build_object('person_id', person_id, 'person_groups', pgroups) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION person_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if OLD.row_id != NEW.row_id then
            raise exception using message = 'row_id is immutable';
        elsif OLD.person_id != NEW.person_id then
            raise exception using message = 'person_id is immutable';
        elsif OLD.person_group != NEW.person_group then
            raise exception using message = 'person_group is immutable';
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION person_management ()  RETURNS trigger
  VOLATILE
AS $body$
declare new_pid text;
    declare new_pgrp text;
    declare exp date;
    declare unam text;
    begin
        if (TG_OP = 'INSERT') then
            if OLD.person_group is null then
                new_pgrp := NEW.person_id || '-group';
                update persons set person_group = new_pgrp where person_id = NEW.person_id;
                insert into groups (group_name, group_class, group_type, group_primary_member, group_description, group_expiry_date)
                    values (new_pgrp, 'primary', 'person', NEW.person_id, 'personal group', NEW.person_expiry_date);
            end if;
        elsif (TG_OP = 'DELETE') then
            delete from groups where group_name = OLD.person_group;
        elsif (TG_OP = 'UPDATE') then
            if OLD.person_activated != NEW.person_activated then
                update users set user_activated = NEW.person_activated where person_id = OLD.person_id;
                update groups set group_activated = NEW.person_activated where group_name = OLD.person_group;
            end if;
            if OLD.person_expiry_date != NEW.person_expiry_date then
                new_pgrp := NEW.person_id || '-group';
                update groups set group_expiry_date = NEW.person_expiry_date where group_name = new_pgrp;
                for exp, unam in select user_expiry_date, user_name from users where person_id = NEW.person_id loop
                    if NEW.person_expiry_date < exp then
                        update users set user_expiry_date = NEW.person_expiry_date where person_id = NEW.person_id;
                        update groups set group_expiry_date = NEW.person_expiry_date where group_primary_member = unam;
                    end if;
                end loop;
            end if;
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION person_uniqueness ()  RETURNS trigger
  VOLATILE
AS $body$
declare element jsonb;
    begin
        begin
            for element in select jsonb_array_elements(NEW.identifiers) loop
                if 't' in (select element <@ jsonb_array_elements(identifiers) from persons) then
                    raise integrity_constraint_violation
                        using message = 'value already contained in identifiers';
                end if;
            end loop;
        exception when invalid_parameter_value then
            raise exception
                using message = 'identifiers should be a json array, like [{k,v}, {...}]';
        end;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION posix_gid ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if NEW.group_type not in ('person', 'web') then
            if NEW.group_posix_gid is null then
                -- only auto select if nothing is provided
                -- to enable the transition historical data
                -- risk: possibility to generate holes
                select generate_new_posix_gid() into NEW.group_posix_gid;
            end if;
        else
            NEW.group_posix_gid := null;
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION sync_posix_gid_to_users ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if NEW.group_type = 'user' then
            update users set user_group_posix_gid = NEW.group_posix_gid
                where user_group = NEW.group_name;
        end if;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION test_capability_instances ()  RETURNS boolean
  VOLATILE
AS $body$
declare iid uuid;
    declare instance json;
    begin
        insert into capabilities_http_instances
            (capability_name, instance_start_date, instance_end_date,
             instance_usages_remaining, instance_metadata)
        values ('export', now() - interval '1 hour', current_timestamp + '2 hours',
                3, '{"claims": {"proj": "p11", "user": "p11-anonymous"}}');
        select instance_id from capabilities_http_instances into iid;
        select capability_instance_get(iid::text) into instance;
        -- decrementing instance_usages_remaining
        assert (select instance_usages_remaining from capabilities_http_instances
                where instance_id = iid) = 2,
            'instance_usages_remaining not being decremented after instance creation';
        assert instance->>'instance_usages_remaining' = 2::text,
            'instance_usages_remaining incorrectly reported by instance creation function';
        -- auto deletion
        select capability_instance_get(iid::text) into instance;
        select capability_instance_get(iid::text) into instance;
        begin
            select capability_instance_get(iid::text) into instance;
        exception when assert_failure then
            raise notice 'automatic deletion of capability instances works';
        end;
        -- cannot use if expired
        insert into capabilities_http_instances
            (capability_name, instance_start_date, instance_end_date,
             instance_usages_remaining, instance_metadata)
        values ('export', now() - interval '3 hour', now() - interval '2 hour',
                3, '{"claims": {"proj": "p11", "user": "p11-anonymous"}}');
        select instance_id from capabilities_http_instances into iid;
        begin
            select capability_instance_get(iid::text) into instance;
        exception when assert_failure then
            raise notice 'cannot use expired capability instance - as expected';
        end;
        delete from capabilities_http_instances where instance_id = iid;
        -- cannot use if not active yet
        insert into capabilities_http_instances
            (capability_name, instance_start_date, instance_end_date,
             instance_usages_remaining, instance_metadata)
        values ('export', now() + interval '3 hour', now() + interval '4 hour',
                3, '{"claims": {"proj": "p11", "user": "p11-anonymous"}}');
        select instance_id from capabilities_http_instances into iid;
        begin
            select capability_instance_get(iid::text) into instance;
        exception when assert_failure then
            raise notice 'cannot use capability instance before start time - as expected';
        end;
        -- immutable cols
        begin
            update capabilities_http_instances set row_id = '44c23dc9-d759-4c1f-a72e-04e10dbe2523'
                where instance_id = iid;
        exception when assert_failure then
            raise notice 'capabilities_http_instances: row_id immutable';
        end;
        begin
            update capabilities_http_instances set capability_name = 'parsley'
                where instance_id = iid;
        exception when assert_failure then
            raise notice 'capabilities_http_instances: capability_name immutable';
        end;
        begin
            update capabilities_http_instances set instance_id = '44c23dc9-d759-4c1f-a72e-04e10dbe2523'
                where instance_id = iid;
        exception when assert_failure then
            raise notice 'capabilities_http_instances: instance_id immutable';
        end;
        return true;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION update_audit_log_objects ()  RETURNS trigger
  VOLATILE
AS $body$
declare old_data text;
    declare new_data text;
    declare colname text;
    declare table_name text;
    declare session_identity text;
    begin
        table_name := TG_TABLE_NAME::text;
        session_identity := current_setting('session.identity', 't');
        for colname in execute
            format('select c.column_name::text
                    from pg_catalog.pg_statio_all_tables as st
                    inner join information_schema.columns c
                    on c.table_schema = st.schemaname and c.table_name = st.relname
                    left join pg_catalog.pg_description pgd
                    on pgd.objoid = st.relid
                    and pgd.objsubid = c.ordinal_position
                    where st.relname = $1') using table_name
        loop
            execute format('select ($1).%%s::text', colname) using OLD into old_data;
            execute format('select ($1).%%s::text', colname) using NEW into new_data;
            if old_data != new_data or (old_data is null and new_data is not null) then
                insert into audit_log_objects (identity, operation, table_name, row_id, column_name, old_data, new_data)
                    values (session_identity, TG_OP, table_name, NEW.row_id, colname, old_data, new_data);
            end if;
        end loop;
        if TG_OP = 'DELETE' then
            insert into audit_log_objects (identity, operation, table_name, row_id, column_name, old_data, new_data)
                values (session_identity, TG_OP, table_name, OLD.row_id, null, null, null);
        end if;
        return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION update_audit_log_relations ()  RETURNS trigger
  VOLATILE
AS $body$
declare table_name text;
    declare parent text;
    declare child text;
    declare session_identity text;
    begin
        session_identity := current_setting('session.identity', 't');
        table_name := TG_TABLE_NAME::text;
        if TG_OP in ('INSERT', 'UPDATE') then
            if table_name = 'group_memberships' then
                parent := NEW.group_name;
                child := NEW.group_member_name;
            elsif table_name = 'group_moderators' then
                parent := NEW.group_name;
                child := NEW.group_moderator_name;
            elsif table_name = 'capabilities_http_grants' then
                parent := NEW.capability_grant_id;
                child := NEW.capability_grant_hostnames::text || ','
                      || NEW.capability_grant_namespace || ','
                      || NEW.capability_grant_http_method || ','
                      || NEW.capability_grant_uri_pattern || ','
                      || quote_nullable(NEW.capability_grant_rank) || ','
                      || quote_nullable(NEW.capability_grant_required_groups);
            end if;
        elsif TG_OP = 'DELETE' then
            if table_name = 'group_memberships' then
                parent := OLD.group_name;
                child := OLD.group_member_name;
            elsif table_name = 'group_moderators' then
                parent := OLD.group_name;
                child := OLD.group_moderator_name;
            elsif table_name = 'capabilities_http_grants' then
                parent := OLD.capability_grant_id;
                child := OLD.capability_grant_http_method || ',' || OLD.capability_grant_uri_pattern;
            end if;
        end if;
        insert into audit_log_relations(identity, operation, table_name, parent, child)
            values (session_identity, TG_OP, table_name, parent, child);
        return new;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

event.listen(
    t_audit_log_objects,
    'after_create',
    DDL('''CREATE FUNCTION user_capabilities (user_name text, grants boolean DEFAULT false)  RETURNS json
  VOLATILE
AS $body$
declare ugrp text;
    declare exst boolean;
    declare data json;
    begin
        execute format('select exists(select 1 from users where users.user_name = $1)') using $1 into exst;
        assert exst = 't', 'user does not exist';
        select user_group from users where users.user_name = $1 into ugrp;
        select json_agg(grp_cpbts(member_group_name, grants)) from group_get_parents(ugrp) into data;
        return json_build_object('user_name', user_name, 'user_capabilities', data);
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION user_groups (user_name text)  RETURNS json
  VOLATILE
AS $body$
declare ugrp text;
    declare ugroups json;
    declare exst boolean;
    declare data json;
    begin
        execute format('select exists(select 1 from users where users.user_name = $1)') using $1 into exst;
        assert exst = 't', 'user does not exist';
        select user_group from users where users.user_name = $1 into ugrp;
        select get_memberships(user_name, ugrp) into ugroups;
        select json_build_object('user_name', user_name, 'user_groups', ugroups) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION user_immutability ()  RETURNS trigger
  VOLATILE
AS $body$
begin
        if OLD.row_id != NEW.row_id then
            raise exception using message = 'row_id is immutable';
        elsif OLD.user_id != NEW.user_id then
            raise exception using message = 'user_id is immutable';
        elsif OLD.user_name != NEW.user_name then
            raise exception using message = 'user_name is immutable';
        elsif OLD.user_group != NEW.user_group then
            raise exception using message = 'user_group is immutable';
        elsif OLD.user_posix_uid != NEW.user_posix_uid then
            raise exception using message = 'user_posix_uid is immutable';
        elsif NEW.user_posix_uid is null and NEW.user_posix_uid is not null then
            raise exception using message = 'user_posix_uid cannot be set to null once set';
        elsif NEW.user_group_posix_gid is null and OLD.user_group_posix_gid is not null then
            raise exception using message = 'user_group_posix_gid cannot be set to null once set';
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION user_management ()  RETURNS trigger
  VOLATILE
AS $body$
declare new_unam text;
    declare new_ugrp text;
    declare person_exp date;
    declare user_exp date;
    declare ugroup_posix_gid int;
    begin
        if (TG_OP = 'INSERT') then
            if OLD.user_group is null then
                new_ugrp := NEW.user_name || '-group';
                update users set user_group = new_ugrp where user_name = NEW.user_name;
                -- if caller provides user_group_posix_gid then set it, otherwise don't
                if NEW.user_group_posix_gid is not null then
                    ugroup_posix_gid := NEW.user_group_posix_gid;
                else
                    ugroup_posix_gid := null;
                end if;
                insert into groups (group_name, group_class, group_type, group_primary_member, group_description, group_posix_gid)
                    values (new_ugrp, 'primary', 'user', NEW.user_name, 'user group', ugroup_posix_gid);
                select person_expiry_date from persons where person_id = NEW.person_id into person_exp;
                if NEW.user_expiry_date is not null then
                    if NEW.user_expiry_date > person_exp then
                        raise exception using message = 'a user cannot expire _after_ the person';
                    end if;
                    user_exp := NEW.user_expiry_date;
                else
                    user_exp := person_exp;
                end if;
                update users set user_expiry_date = user_exp where user_name = NEW.user_name;
                update groups set group_expiry_date = user_exp where group_name = new_ugrp;
            end if;
        elsif (TG_OP = 'DELETE') then
            delete from groups where group_name = OLD.user_group;
        elsif (TG_OP = 'UPDATE') then
            if OLD.user_activated != NEW.user_activated then
                update groups set group_activated = NEW.user_activated where group_name = OLD.user_group;
            end if;
            if OLD.user_expiry_date != NEW.user_expiry_date then
                select person_expiry_date from persons where person_id = NEW.person_id into person_exp;
                if NEW.user_expiry_date > person_exp then
                    raise exception using message = 'a user cannot expire _after_ the person';
                else
                    update groups set group_expiry_date = NEW.user_expiry_date where group_primary_member = NEW.user_name;
                end if;
            end if;
        end if;
    return new;
    end;
$body$ LANGUAGE plpgsql;'''
        '''CREATE FUNCTION user_moderators (user_name text)  RETURNS json
  VOLATILE
AS $body$
declare exst boolean;
    declare ugrps json;
    declare mods json;
    declare data json;
    begin
        execute format('select exists(select 1 from users where users.user_name = $1)') using $1 into exst;
        assert exst = 't', 'user does not exist';
        select user_groups->>'user_groups' from user_groups(user_name) into ugrps;
        if ugrps is null then
            mods := '[]'::json;
        else
            select json_agg(group_name) from group_moderators
                where group_moderator_name in
                (select json_array_elements(user_groups->'user_groups')->>'member_group'
                from user_groups(user_name)) into mods;
        end if;
        select json_build_object('user_name', user_name, 'user_moderators', mods) into data;
        return data;
    end;
$body$ LANGUAGE plpgsql;''').execute_if(dialect='postgresql'))

t_audit_log_relations = Table(
    'audit_log_relations', metadata,
    Column('identity', Text),
    Column('operation', Text, nullable=False),
    Column('event_time', DateTime(True), server_default=text("now()")),
    Column('table_name', Text, nullable=False),
    Column('parent', Text),
    Column('child', Text),
    postgresql_partition_by='LIST (table_name)'
)

event.listen(
    t_audit_log_relations,
    "after_create",
    DDL("CREATE TABLE audit_log_relations_group_memberships PARTITION OF audit_log_relations "
        "FOR VALUES IN ('group_memberships');"
        "CREATE TABLE audit_log_relations_group_moderators PARTITION OF audit_log_relations "
        "FOR VALUES IN  ('group_moderators');"
        "CREATE TABLE audit_log_relations_capabilities_http_grants PARTITION OF audit_log_relations "
        "FOR VALUES IN  ('capabilities_http_grants');")
    )


class CapabilitiesHttp(Base):
    __tablename__ = 'capabilities_http'
    __table_args__ = (
        CheckConstraint("capability_group_match_method = ANY (ARRAY['exact'::text, 'wildcard'::text])"),
        CheckConstraint('capability_lifetime > 0')
    )

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    capability_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    capability_name = Column(Text, primary_key=True)
    capability_hostnames = Column(ARRAY(Text()), nullable=False)
    capability_default_claims = Column(JSONB(astext_type=Text()))
    capability_required_groups = Column(ARRAY(Text()))
    capability_required_attributes = Column(JSONB(astext_type=Text()))
    capability_group_match_method = Column(Text, server_default=text("'wildcard'::text"))
    capability_lifetime = Column(Integer, nullable=False)
    capability_description = Column(Text, nullable=False)
    capability_expiry_date = Column(Date)
    capability_group_existence_check = Column(Boolean, server_default=text("true"))
    capability_metadata = Column(JSONB(astext_type=Text()))


event.listen(
    CapabilitiesHttp.__table__,
    'after_create',
    DDL('''CREATE TRIGGER "capabilities_http_audit"
           AFTER INSERT OR DELETE OR UPDATE ON capabilities_http
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_objects();'''
        '''CREATE TRIGGER "capabilities_http_consistent_name_references"
           AFTER DELETE ON capabilities_http
           FOR EACH ROW
           EXECUTE PROCEDURE ensure_capability_name_references_consistent();'''
        '''CREATE TRIGGER "capabilities_http_unique_groups"
           BEFORE INSERT OR UPDATE ON capabilities_http
           FOR EACH ROW
           EXECUTE PROCEDURE ensure_unique_capability_attributes();'''
        '''CREATE TRIGGER "ensure_capabilities_http_group_check"
           BEFORE INSERT OR UPDATE ON capabilities_http
           FOR EACH ROW
           EXECUTE PROCEDURE capabilities_http_group_check();'''
        '''CREATE TRIGGER "ensure_capabilities_http_immutability"
           BEFORE UPDATE ON capabilities_http
           FOR EACH ROW
           EXECUTE PROCEDURE capabilities_http_immutability();'''
        ).execute_if(dialect='postgresql'))


class CapabilitiesHttpGrant(Base):
    __tablename__ = 'capabilities_http_grants'
    __table_args__ = (
        CheckConstraint("capability_grant_http_method = ANY (ARRAY['OPTIONS'::text, 'HEAD'::text, 'GET'::text, 'PUT'::text, 'POST'::text, 'PATCH'::text, 'DELETE'::text])"),
        CheckConstraint('capability_grant_rank > 0'),
        UniqueConstraint('capability_grant_namespace', 'capability_grant_http_method', 'capability_grant_rank')
    )

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    capability_names_allowed = Column(ARRAY(Text()), nullable=False)
    capability_grant_id = Column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    capability_grant_name = Column(Text, unique=True)
    capability_grant_hostnames = Column(ARRAY(Text()), nullable=False)
    capability_grant_namespace = Column(Text, nullable=False)
    capability_grant_http_method = Column(Text, nullable=False)
    capability_grant_rank = Column(Integer)
    capability_grant_uri_pattern = Column(Text, nullable=False)
    capability_grant_required_groups = Column(ARRAY(Text()))
    capability_grant_required_attributes = Column(JSONB(astext_type=Text()))
    capability_grant_quick = Column(Boolean, server_default=text("true"))
    capability_grant_start_date = Column(DateTime(True))
    capability_grant_end_date = Column(DateTime(True))
    capability_grant_max_num_usages = Column(Integer)
    capability_grant_group_existence_check = Column(Boolean, server_default=text("true"))
    capability_grant_metadata = Column(JSONB(astext_type=Text()))

event.listen(
    CapabilitiesHttpGrant.__table__,
    'after_create',
    DDL('''CREATE TRIGGER "capabilities_http_grants_correct_names_allowed"
           BEFORE INSERT OR UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE ensure_correct_capability_names_allowed();'''
        '''CREATE TRIGGER "capabilities_http_grants_audit"
           AFTER INSERT OR DELETE OR UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_relations();'''
        '''CREATE TRIGGER "capabilities_http_grants_grant_generation"
           AFTER INSERT ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE generate_grant_rank();'''
        '''CREATE TRIGGER "capabilities_http_grants_rank_update"
           BEFORE UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE ensure_sensible_rank_update();'''
        '''CREATE TRIGGER "capabilities_http_grants_unique_arrays"
           BEFORE INSERT OR UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE ensure_unique_grant_arrays();'''
        '''CREATE TRIGGER "ensure_capabilities_http_grants_group_check"
           BEFORE INSERT OR UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE capabilities_http_grants_group_check();'''
        '''CREATE TRIGGER "ensure_capabilities_http_grants_immutability"
           BEFORE UPDATE ON capabilities_http_grants
           FOR EACH ROW
           EXECUTE PROCEDURE capabilities_http_grants_immutability();'''
        ).execute_if(dialect='postgresql'))


class Group(Base):
    __tablename__ = 'groups'
    __table_args__ = (
        CheckConstraint('((group_posix_gid > 999) AND (group_posix_gid < 200000)) OR (group_posix_gid > 220000)'),
        CheckConstraint("group_class = ANY (ARRAY['primary'::text, 'secondary'::text])"),
        CheckConstraint("group_type = ANY (ARRAY['person'::text, 'user'::text, 'generic'::text, 'web'::text])")
    )

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    group_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    group_activated = Column(Boolean, nullable=False, server_default=text("true"))
    group_expiry_date = Column(DateTime(True))
    group_name = Column(Text, primary_key=True)
    group_class = Column(Text)
    group_type = Column(Text)
    group_primary_member = Column(Text)
    group_description = Column(Text)
    group_posix_gid = Column(Integer, unique=True)
    group_metadata = Column(JSONB(astext_type=Text()))

    parents = relationship(
        'Group',
        secondary='group_memberships',
        primaryjoin='Group.group_name == group_memberships.c.group_member_name',
        secondaryjoin='Group.group_name == group_memberships.c.group_name'
    )
    parents1 = relationship(
        'Group',
        secondary='group_moderators',
        primaryjoin='Group.group_name == group_moderators.c.group_moderator_name',
        secondaryjoin='Group.group_name == group_moderators.c.group_name'
    )

event.listen(
    Group.__table__,
    'after_create',
    DDL('''CREATE FUNCTION grp_mems (gn text) RETURNS TABLE(group_name text, group_member_name text, group_primary_member text, group_activated boolean, group_expiry_date timestamp with time zone)
  VOLATILE
AS $body$
select a.group_name,
           a.group_member_name,
           a.group_primary_member,
           b.group_activated,
           b.group_expiry_date
    from (select group_name, group_member_name, group_primary_member from group_get_children(gn))a
    join (select group_name, group_activated, group_expiry_date from groups)b
    on a.group_name = b.group_name
$body$ LANGUAGE sql''').execute_if(dialect='postgresql'))

event.listen(
    Group.__table__,
    'after_create',
    DDL('''CREATE TRIGGER "ensure_group_deletion_policy"
           BEFORE DELETE ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE group_deletion();'''
        '''CREATE TRIGGER "ensure_group_immutability"
           BEFORE UPDATE ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE group_immutability();'''
        '''CREATE TRIGGER "groups_audit"
           AFTER INSERT OR DELETE OR UPDATE ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_objects();'''
        '''CREATE TRIGGER "group_management_trigger"
           BEFORE UPDATE ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE group_management();'''
        '''CREATE TRIGGER "set_posix_gid"
           BEFORE INSERT ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE posix_gid();'''
        '''CREATE TRIGGER "sync_user_group_posix_gid"
           AFTER INSERT ON groups
           FOR EACH ROW
           EXECUTE PROCEDURE sync_posix_gid_to_users();'''
        ).execute_if(dialect='postgresql'))


class Person(Base):
    __tablename__ = 'persons'

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    person_id = Column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    person_activated = Column(Boolean, nullable=False, server_default=text("true"))
    person_expiry_date = Column(DateTime(True))
    person_group = Column(Text)
    full_name = Column(Text, nullable=False)
    identifiers = Column(JSONB(astext_type=Text()))
    password = Column(Text)
    otp_secret = Column(Text)
    email = Column(Text)
    person_metadata = Column(JSONB(astext_type=Text()))


event.listen(
    Person.__table__,
    'after_create',
    DDL('''CREATE TRIGGER "ensure_person_immutability"
           BEFORE UPDATE ON persons
           FOR EACH ROW
           EXECUTE PROCEDURE person_immutability();'''
        '''CREATE TRIGGER "ensure_person_uniqueness"
           BEFORE INSERT ON persons
           FOR EACH ROW
           EXECUTE PROCEDURE person_uniqueness();'''
        '''CREATE TRIGGER "persons_audit"
           AFTER INSERT OR DELETE OR UPDATE ON persons
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_objects();'''
        '''CREATE TRIGGER "person_group_trigger"
           AFTER INSERT OR DELETE OR UPDATE ON persons
           FOR EACH ROW
           EXECUTE PROCEDURE person_management();'''
        ).execute_if(dialect='postgresql'))


class CapabilitiesHttpInstance(Base):
    __tablename__ = 'capabilities_http_instances'

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    capability_name = Column(ForeignKey('capabilities_http.capability_name', ondelete='CASCADE'))
    instance_id = Column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    instance_start_date = Column(DateTime(True), server_default=text("CURRENT_TIMESTAMP"))
    instance_end_date = Column(DateTime(True), nullable=False)
    instance_usages_remaining = Column(Integer)
    instance_metadata = Column(JSONB(astext_type=Text()))

    capabilities_http = relationship('CapabilitiesHttp')


event.listen(
    CapabilitiesHttpInstance.__table__,
    'after_create',
    DDL('''CREATE TRIGGER "capabilities_http_instances_audit"
           AFTER INSERT OR DELETE OR UPDATE ON capabilities_http_instances
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_objects();'''
        '''CREATE TRIGGER "ensure_capabilities_http_instances_immutability"
           BEFORE UPDATE ON capabilities_http_instances
           FOR EACH ROW
           EXECUTE PROCEDURE capabilities_http_instances_immutability();'''
        ).execute_if(dialect='postgresql'))


t_group_memberships = Table(
    'group_memberships', metadata,
    Column('group_name', ForeignKey('groups.group_name', ondelete='CASCADE'), nullable=False),
    Column('group_member_name', ForeignKey('groups.group_name', ondelete='CASCADE'), nullable=False),
    UniqueConstraint('group_name', 'group_member_name')
)


event.listen(
    t_group_memberships,
    'after_create',
    DDL('''CREATE TRIGGER "ensure_group_memberships_immutability"
           BEFORE UPDATE ON group_memberships
           FOR EACH ROW
           EXECUTE PROCEDURE group_memberships_immutability();'''
        '''CREATE TRIGGER "group_memberships_audit"
           AFTER INSERT OR DELETE OR UPDATE ON group_memberships
           FOR EACH ROW
           EXECUTE PROCEDURE update_audit_log_relations();'''
        '''CREATE TRIGGER "group_memberships_dag_requirements_trigger"
           BEFORE INSERT ON group_memberships
           FOR EACH ROW
           EXECUTE PROCEDURE group_memberships_check_dag_requirements();'''
        ).execute_if(dialect='postgresql'))


class FirstOrderMember(Base):
    __table__ = create_view(
        name='pgiam.first_order_members',
        selectable=select(
            [
                t_group_memberships.c.group_name,
                t_group_memberships.c.group_member_name,
                Group.group_class,
                Group.group_type,
                Group.group_primary_member
            ],
            from_obj=(
                t_group_memberships
                .join(Group, t_group_memberships.c.group_member_name == Group.group_name)
            )
        ),
        metadata=Base.metadata,
        cascade_on_drop=True
    )


t_group_moderators = Table(
    'group_moderators', metadata,
    Column('group_name', ForeignKey('groups.group_name', ondelete='CASCADE'), nullable=False),
    Column('group_moderator_name', ForeignKey('groups.group_name', ondelete='CASCADE'), nullable=False),
    UniqueConstraint('group_name', 'group_moderator_name')
)


event.listen(
    t_group_moderators,
    'after_create',
    DDL("CREATE TRIGGER ensure_group_moderators_immutability "
        "BEFORE UPDATE ON group_moderators "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE group_moderators_immutability();"
        "CREATE TRIGGER group_memberships_dag_requirements_trigger "
        "AFTER INSERT ON group_moderators "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE group_moderators_check_dag_requirements();"
        "CREATE TRIGGER group_moderators_audit "
        "AFTER INSERT OR DELETE OR UPDATE ON group_moderators "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE update_audit_log_relations(); "
        ).execute_if(dialect='postgresql'))


class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        CheckConstraint('((user_group_posix_gid > 999) AND (user_group_posix_gid < 200000)) OR (user_group_posix_gid > 220000)'),
        CheckConstraint('((user_posix_uid > 999) AND (user_posix_uid < 200000)) OR (user_posix_uid > 220000)')
    )

    row_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    person_id = Column(ForeignKey('persons.person_id', ondelete='CASCADE'), nullable=False)
    user_id = Column(UUID, nullable=False, unique=True, server_default=text("gen_random_uuid()"))
    user_activated = Column(Boolean, nullable=False, server_default=text("true"))
    user_expiry_date = Column(DateTime(True))
    user_name = Column(Text, primary_key=True)
    user_group = Column(Text)
    user_posix_uid = Column(Integer, unique=True, server_default=text("generate_new_posix_uid()"))
    user_group_posix_gid = Column(Integer)
    user_metadata = Column(JSONB(astext_type=Text()))

    person = relationship('Person')


event.listen(
    User.__table__,
    'after_create',
    DDL("CREATE TRIGGER ensure_user_immutability "
        "BEFORE UPDATE ON users "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE user_immutability(); "
        "CREATE TRIGGER user_group_trigger "
        "AFTER INSERT OR DELETE OR UPDATE ON users "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE user_management(); "
        "CREATE TRIGGER users_audit "
        "AFTER INSERT OR DELETE OR UPDATE ON users "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE update_audit_log_objects();"
        ).execute_if(dialect='postgresql'))
