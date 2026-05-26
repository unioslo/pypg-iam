"""Constants shared between sync and async implementations."""

# Constants for capabilities_http_sync
CAPABILITIES_REQUIRED_KEYS = [
    "capability_name",
    "capability_hostnames",
    "capability_required_groups",
    "capability_lifetime",
    "capability_description",
]

CAPABILITIES_JSON_COLUMNS = [
    "capability_default_claims",
    "capability_required_attributes",
    "capability_metadata",
]

CAPABILITIES_TABLE_COLUMNS = [
    "capability_name",
    "capability_hostnames",
    "capability_default_claims",
    "capability_required_groups",
    "capability_required_attributes",
    "capability_group_match_method",
    "capability_lifetime",
    "capability_description",
    "capability_expiry_date",
    "capability_group_existence_check",
    "capability_metadata",
]


# Constants for capabilities_http_grants_sync
GRANTS_REQUIRED_KEYS = [
    "capability_names_allowed",
    "capability_grant_name",
    "capability_grant_hostnames",
    "capability_grant_namespace",
    "capability_grant_http_method",
    "capability_grant_rank",
    "capability_grant_uri_pattern",
    "capability_grant_required_groups",
]

GRANTS_JSON_COLUMNS = [
    "capability_grant_required_attributes",
    "capability_grant_metadata",
]

GRANTS_TABLE_COLUMNS = [
    "capability_names_allowed",
    "capability_grant_name",
    "capability_grant_hostnames",
    "capability_grant_namespace",
    "capability_grant_http_method",
    "capability_grant_uri_pattern",
    "capability_grant_required_groups",
    "capability_grant_required_attributes",
    "capability_grant_quick",
    "capability_grant_start_date",
    "capability_grant_end_date",
    "capability_grant_max_num_usages",
    "capability_grant_group_existence_check",
    "capability_grant_metadata",
    "capability_grant_static",
]


# SQL Queries for capabilities_http_sync
CAPABILITIES_HTTP_UPDATE_QUERY = """
    update capabilities_http set
        capability_hostnames = :capability_hostnames,
        capability_default_claims = :capability_default_claims,
        capability_required_groups = :capability_required_groups,
        capability_required_attributes = :capability_required_attributes,
        capability_group_match_method = :capability_group_match_method,
        capability_lifetime = :capability_lifetime,
        capability_description = :capability_description,
        capability_expiry_date = :capability_expiry_date,
        capability_group_existence_check = :capability_group_existence_check,
        capability_metadata = :capability_metadata
    where capability_name = :capability_name"""

CAPABILITIES_HTTP_INSERT_QUERY = """
    insert into capabilities_http
        (capability_name,
         capability_hostnames,
         capability_default_claims,
         capability_required_groups,
         capability_required_attributes,
         capability_group_match_method,
         capability_lifetime,
         capability_description,
         capability_expiry_date,
         capability_group_existence_check,
         capability_metadata)
      values
        (:capability_name,
         :capability_hostnames,
         :capability_default_claims,
         :capability_required_groups,
         :capability_required_attributes,
         :capability_group_match_method,
         :capability_lifetime,
         :capability_description,
         :capability_expiry_date,
         :capability_group_existence_check,
         :capability_metadata)"""

CAPABILITIES_HTTP_DELETE_QUERY = (
    "delete from capabilities_http where capability_name = ANY(:deletes)"
)


# SQL Queries for capabilities_http_grants_sync
GRANTS_EXISTS_QUERY = """select count(*) from capabilities_http_grants
                          where capability_grant_name = :capability_grant_name"""

GRANTS_GET_ID_FROM_NAME_QUERY = """select capability_grant_id from capabilities_http_grants
                                    where capability_grant_name = :name"""

GRANTS_UPDATE_QUERY = """
    update capabilities_http_grants set
        capability_names_allowed = :capability_names_allowed,
        capability_grant_hostnames = :capability_grant_hostnames,
        capability_grant_namespace = :capability_grant_namespace,
        capability_grant_http_method = :capability_grant_http_method,
        capability_grant_uri_pattern = :capability_grant_uri_pattern,
        capability_grant_required_groups = :capability_grant_required_groups,
        capability_grant_required_attributes = :capability_grant_required_attributes,
        capability_grant_quick = :capability_grant_quick,
        capability_grant_start_date = :capability_grant_start_date,
        capability_grant_end_date = :capability_grant_end_date,
        capability_grant_max_num_usages = :capability_grant_max_num_usages,
        capability_grant_group_existence_check = :capability_grant_group_existence_check,
        capability_grant_metadata = :capability_grant_metadata,
        capability_grant_static = :capability_grant_static
    where capability_grant_name = :capability_grant_name"""

GRANTS_INSERT_QUERY = """
    insert into capabilities_http_grants
        (capability_names_allowed,
         capability_grant_name,
         capability_grant_hostnames,
         capability_grant_namespace,
         capability_grant_http_method,
         capability_grant_uri_pattern,
         capability_grant_required_groups,
         capability_grant_required_attributes,
         capability_grant_quick,
         capability_grant_start_date,
         capability_grant_end_date,
         capability_grant_max_num_usages,
         capability_grant_group_existence_check,
         capability_grant_metadata,
         capability_grant_static)
    values
        (:capability_names_allowed,
         :capability_grant_name,
         :capability_grant_hostnames,
         :capability_grant_namespace,
         :capability_grant_http_method,
         :capability_grant_uri_pattern,
         :capability_grant_required_groups,
         :capability_grant_required_attributes,
         :capability_grant_quick,
         :capability_grant_start_date,
         :capability_grant_end_date,
         :capability_grant_max_num_usages,
         :capability_grant_group_existence_check,
         :capability_grant_metadata,
         :capability_grant_static)"""

GRANTS_FIND_EXISTING_QUERY = """select capability_grant_name from capabilities_http_grants
                                 where capability_grant_namespace = :namespace
                                 and capability_grant_http_method = :method
                                 and capability_grant_static = 't'"""


# Database function call query templates
# These are used by methods in both pgiam.py and async_pgiam.py

# Person-related queries
QUERY_PERSON_GROUPS = "select person_groups('{0}')"  # {0}: person_id
QUERY_PERSON_CAPABILITIES = "select person_capabilities('{0}', '{1}')"  # {0}: person_id, {1}: grants (bool as 't'/'f')
QUERY_PERSON_ACCESS = "select person_access('{0}')"  # {0}: person_id

# User-related queries
QUERY_USER_GROUPS = "select user_groups('{0}')"  # {0}: user_name
QUERY_USER_MODERATORS = "select user_moderators('{0}')"  # {0}: user_name
QUERY_USER_CAPABILITIES = "select user_capabilities('{0}', '{1}')"  # {0}: user_name, {1}: grants (bool as 't'/'f')

# Group-related queries
QUERY_GROUP_MEMBERS = "select group_members({0})"  # {0}: limit (int, not quoted)
QUERY_GROUP_MODERATORS = "select group_moderators('{0}')"  # {0}: group_name
QUERY_GROUP_MEMBER_ADD = "select group_member_add('{0}', '{1}', {2}, {3}, {4})"  # {0}: group_name, {1}: member, {2}: start_date, {3}: end_date, {4}: weekdays
QUERY_GROUP_MEMBER_REMOVE = (
    "select group_member_remove('{0}', '{1}')"  # {0}: group_name, {1}: member
)
QUERY_GROUP_CAPABILITIES = "select group_capabilities('{0}', '{1}')"  # {0}: group_name, {1}: grants (bool as 't'/'f')

# Institution-related queries
QUERY_INSTITUTION_GROUP_ADD = (
    "select institution_group_add('{0}', '{1}')"  # {0}: institution, {1}: group_name
)
QUERY_INSTITUTION_GROUP_REMOVE = (
    "select institution_group_remove('{0}', '{1}')"  # {0}: institution, {1}: group_name
)
QUERY_INSTITUTION_GROUPS = "select institution_groups('{0}')"  # {0}: institution
QUERY_INSTITUTION_MEMBER_ADD = (
    "select institution_member_add('{0}', '{1}')"  # {0}: institution, {1}: member
)
QUERY_INSTITUTION_MEMBER_REMOVE = (
    "select institution_member_remove('{0}', '{1}')"  # {0}: institution, {1}: member
)
QUERY_INSTITUTION_MEMBERS = "select institution_members('{0}')"  # {0}: institution

# Project-related queries
QUERY_PROJECT_GROUP_ADD = (
    "select project_group_add('{0}', '{1}')"  # {0}: project, {1}: group_name
)
QUERY_PROJECT_GROUP_REMOVE = (
    "select project_group_remove('{0}', '{1}')"  # {0}: project, {1}: group_name
)
QUERY_PROJECT_GROUPS = "select project_groups('{0}')"  # {0}: project
QUERY_PROJECT_INSTITUTIONS = "select project_institutions('{0}')"  # {0}: project

# Capability-related queries
QUERY_CAPABILITIES_HTTP_LIST = "select capability_name from capabilities_http"
QUERY_CAPABILITY_GRANT_RANK_SET = "select capability_grant_rank_set('{0}', '{1}')"  # {0}: grant_id, {1}: new_grant_rank
QUERY_CAPABILITY_GRANT_DELETE = (
    "select capability_grant_delete('{0}')"  # {0}: grant_name
)
QUERY_CAPABILITY_GRANTS_DELETE = "delete from capabilities_http_grants where capability_grant_namespace like '{0}'"  # {0}: namespace
QUERY_CAPABILITY_INSTANCE_GET = (
    "select capability_instance_get('{0}')"  # {0}: capability_name
)
QUERY_CAPABILITY_GRANT_GROUP_ADD = "select capability_grant_group_add('{0}', '{1}')"  # {0}: grant_name, {1}: group_name
QUERY_CAPABILITY_GRANT_GROUP_REMOVE = "select capability_grant_group_remove('{0}', '{1}')"  # {0}: grant_name, {1}: group_name
