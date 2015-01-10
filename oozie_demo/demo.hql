-- To test from the command-line:
--
--   hive -f demo.hql -d database=qchris
--
--


ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;

CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';

USE ${database};



DROP TABLE IF EXISTS ellery_demo;

CREATE TABLE ellery_demo(
    user_agent STRING,
    browser_family STRING
);

INSERT INTO TABLE ellery_demo
    SELECT
        user_agent AS user_agent,
        parse_ua(user_agent)['browser_family'] AS browser_family
    FROM wmf_raw.webrequest
    WHERE
        webrequest_source='mobile'
        AND year=2014
        AND month=12
        AND day=15
        AND hour=3
    LIMIT 10;


INSERT INTO TABLE ellery_demo_persistent
    SELECT * FROM ellery_demo;