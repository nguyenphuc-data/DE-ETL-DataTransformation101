from pyspark.sql import Row,SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, FloatType, BooleanType, TimestampType, ArrayType, MapType, DateType, DecimalType
from datetime import datetime
from decimal import Decimal
spark = SparkSession.builder \
    .appName("Phuc") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

jsonSchema = StructType([
    StructField("id", StringType(), False),
    StructField("type", StringType()),
    StructField("actor", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
        StructField("gravatar_id", StringType()),
        StructField("url", StringType()),
        StructField("avatar_url", StringType())
    ])),
    StructField("repo", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("url", StringType())
    ])),
    StructField("payload", StructType([
        StructField("forkee", StructType([
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("full_name", StringType()),
            StructField("owner", StructType([
                StructField("login", StringType()),
                StructField("id", LongType()),
                StructField("avatar_url", StringType()),
                StructField("gravatar_id", StringType()),
                StructField("url", StringType()),
                StructField("html_url", StringType()),
                StructField("followers_url", StringType()),
                StructField("following_url", StringType()),
                StructField("gists_url", StringType()),
                StructField("starred_url", StringType()),
                StructField("subscriptions_url", StringType()),
                StructField("organizations_url", StringType()),
                StructField("repos_url", StringType()),
                StructField("events_url", StringType()),
                StructField("received_events_url", StringType()),
                StructField("type", StringType()),
                StructField("site_admin", BooleanType())
            ])),
            StructField("private", BooleanType()),
            StructField("html_url", StringType()),
            StructField("description", StringType()),
            StructField("fork", BooleanType()),
            StructField("url", StringType()),
            StructField("forks_url", StringType()),
            StructField("keys_url", StringType()),
            StructField("collaborators_url", StringType()),
            StructField("teams_url", StringType()),
            StructField("hooks_url", StringType()),
            StructField("issue_events_url", StringType()),
            StructField("events_url", StringType()),
            StructField("assignees_url", StringType()),
            StructField("branches_url", StringType()),
            StructField("tags_url", StringType()),
            StructField("blobs_url", StringType()),
            StructField("git_tags_url", StringType()),
            StructField("git_refs_url", StringType()),
            StructField("trees_url", StringType()),
            StructField("statuses_url", StringType()),
            StructField("languages_url", StringType()),
            StructField("stargazers_url", StringType()),
            StructField("contributors_url", StringType()),
            StructField("subscribers_url", StringType()),
            StructField("subscription_url", StringType()),
            StructField("commits_url", StringType()),
            StructField("git_commits_url", StringType()),
            StructField("comments_url", StringType()),
            StructField("issue_comment_url", StringType()),
            StructField("contents_url", StringType()),
            StructField("compare_url", StringType()),
            StructField("merges_url", StringType()),
            StructField("archive_url", StringType()),
            StructField("downloads_url", StringType()),
            StructField("issues_url", StringType()),
            StructField("pulls_url", StringType()),
            StructField("milestones_url", StringType()),
            StructField("notifications_url", StringType()),
            StructField("labels_url", StringType()),
            StructField("releases_url", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
            StructField("pushed_at", StringType()),
            StructField("git_url", StringType()),
            StructField("ssh_url", StringType()),
            StructField("clone_url", StringType()),
            StructField("svn_url", StringType()),
            StructField("homepage", StringType()),
            StructField("size", LongType()),
            StructField("stargazers_count", LongType()),
            StructField("watchers_count", LongType()),
            StructField("language", StringType()),
            StructField("has_issues", BooleanType()),
            StructField("has_downloads", BooleanType()),
            StructField("has_wiki", BooleanType()),
            StructField("has_pages", BooleanType()),
            StructField("forks_count", LongType()),
            StructField("mirror_url", StringType()),
            StructField("open_issues_count", LongType()),
            StructField("forks", LongType()),
            StructField("open_issues", LongType()),
            StructField("watchers", LongType()),
            StructField("default_branch", StringType()),
            StructField("public", BooleanType())
        ]))
    ])),
    StructField("public", BooleanType()),
    StructField("created_at", StringType()),
    StructField("org", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
        StructField("gravatar_id", StringType()),
        StructField("url", StringType()),
        StructField("avatar_url", StringType())
    ]))
])

jsonFile = spark.read.schema(jsonSchema).json("/home/nguyenphuc/Documents/DataEngineerAnhDat/data/2015-03-01-17.json")
jsonFile.show()

jsonFile.select(
    "id",
    "type",
    "actor.id",
    "actor.login"
).show()