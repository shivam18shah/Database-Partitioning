import psycopg2


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    """
        Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    createDB()
    cur = openconnection.cursor()
    cur.execute('Create table ' + ratingstablename +
                ' (userid integer, extra1 char, movieid integer, extra2 char, rating float,' +
                ' extra3 char, timestamp bigint);')
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
    cur.execute('Alter table ' + ratingstablename +
         ' Drop column extra1, drop column extra2, drop column extra3, drop column timestamp;')
    cur.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings
    """
    cur = openconnection.cursor()
    parts = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(numberofpartitions):
        left = i * parts
        right = left + parts
        table_name = RANGE_TABLE_PREFIX + str(i)

        cur.execute('Create table ' + table_name + ' (userid integer, movieid integer, rating float);')
        if i == 0:
            cur.execute('Insert into ' + table_name +
                        ' (userid, movieid, rating) select userid, movieid, rating from ' + ratingstablename +
                        ' where rating >= ' + str(left) + ' and rating <= ' + str(right) + ';')
        else:
            cur.execute('Insert into ' + table_name + ' (userid, movieid, rating) select userid, movieid, rating from '
                        + ratingstablename + ' where rating > ' + str(left) + ' and rating <= ' + str(right) + ';')
    cur.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on a round robin format
    """
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute('Create table ' + table_name + ' (userid integer, movieid integer, rating float);')
        cur.execute(
            'Insert into ' + table_name +
            ' (userid, movieid, rating) select userid, movieid, rating from '
            + '(select userid, movieid, rating, ROW_NUMBER() over() as rnum from '
            + ratingstablename + ') as temp where mod(temp.rnum-1, 5) = '
            + str(i) + ';')
    cur.close()
    openconnection.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute('Insert into ' + ratingstablename + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(
        itemid) + ',' + str(rating) + ');')
    cur.execute('Select count(*) from ' + ratingstablename + ';');
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute('Insert into ' + table_name + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(
        itemid) + ',' + str(rating) + ');')
    cur.close()
    openconnection.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute('Insert into ' + table_name + '(userid, movieid, rating) values (' + str(userid) + ',' + str(
        itemid) + ',' + str(rating) + ');')
    cur.close()
    openconnection.commit()

def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    openconnection = getOpenConnection(dbname='postgres')
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    openconnection.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\'')
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()


def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    cur = openconnection.cursor()
    cur.execute('Select count(*) from pg_stat_user_tables where relname like ' + '\'' + prefix + '%\';')
    count = cur.fetchone()[0]
    cur.close()
    return count
