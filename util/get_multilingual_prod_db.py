import argparse
from db_utils import exec_hive_stat2 as exec_hive
from sqoop_utils import sqoop_prod_dbs

def get_fields(fields):
        s = ''
        for f in fields:
            s += (f + ' STRING,')
        return s[:-1]


def create_multilingual_table(db, table, fields):
    """
    Create a Table partitioned by day and host
    """
    query = """
    DROP TABLE IF EXISTS %(db)s.%(table)s;
    CREATE TABLE %(db)s.%(table)s (
        %(fields)s
    )
    PARTITIONED BY (lang STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    """

    params = {  'db': db,
                'table': table,
                'fields': get_fields(fields)
                }
    exec_hive(query % params)



def fill_multilingual_table(db, table, fields, langs):
    query = """
    INSERT OVERWRITE TABLE %(db)s.%(table)s
    PARTITION(lang='%(lang)s')
    SELECT
        %(fields)s
    FROM %(db)s.%(lang)s_%(table)s
    """

    # HACK
    if table =='page':
        table = 'page_raw'

    params = {  'db': db,
                'table': table,
                'fields': get_fields(fields)
                }


    for lang in langs:
        params ['lang'] = lang
        # fill table
        exec_hive(query % params)
        # delete helper tables
        exec_hive('DROP TABLE IF EXISTS %(db)s.%(lang)s_%(table)s' % params)
        exec_hive('DROP TABLE IF EXISTS %(db)s.%(lang)s_%(table)s_raw' % params)




if __name__ == '__main__':

    table_field_map = {
                        'page': ['page_id', 'page_title'],
                        'redirect': ['rd_from', 'rd_to'],
                        'page_props': ['page_title', 'propname'],
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required = True, help ='destinaton hive db name' )
    parser.add_argument('--langs', required = True,  help='comma seperated list of languages' )
    parser.add_argument('--tables', required = True,  help='comma seperated list of tables' )
    args = parser.parse_args()
    langs = args.langs.split(',')
    db = args.db
    tables = args.tables.split(',')
    sqoop_prod_dbs(db, langs, tables)
    for table in tables:
        create_multilingual_table(db, table, table_field_map[table])
        fill_multilingual_table(db, table, table_field_map[table], langs)

