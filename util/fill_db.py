from collections import defaultdict
from subprocess import call
import MySQLdb
import random
import sys


def get_connection():
    return MySQLdb.connect(host="localhost",
                           user="root",
                           passwd="test",
                           db="recommender")


def insert(cursor, userId, itemId):
    cursor.execute("""SELECT user_id FROM recommender_pref WHERE """
                   "user_id = " + str(userId) + " AND item_id = " + str(itemId))
    if cursor.rowcount == 0:
        cursor.execute("""INSERT INTO recommender_pref (user_id, item_id)
                         VALUES
                           (""" + str(userId) + ", " + str(itemId) + ")")


def create_data(num_rows):
    conn = get_connection()
    cursor = conn.cursor()
   # cursor.execute("""CREATE TABLE IF NOT EXISTS recommender_pref (
   #                   user_id BIGINT NOT NULL,
   #                   item_id BIGINT NOT NULL,
   #                   PRIMARY KEY (user_id, item_id),
   #                   INDEX (user_id),
   #                   INDEX (item_id)
   #                   )""")
    random.seed(42)
    for num_rows in range(0, num_rows):
        userId = random.randint(2, 1000)
        itemId = random.randint(1, 100)
        insert(cursor, userId, itemId)


def create_similarities():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE recommender_sim (
                      item_id_1 BIGINT NOT NULL,
                      item_id_2 BIGINT NOT NULL,
                      similarity FLOAT NOT NULL,
                      PRIMARY KEY (item_id_1, item_id_2)
                      )""")


def delete_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE recommender_pref.* FROM recommender_pref")


def dump_preferences():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""SELECT user_id, item_id FROM recommender_pref
                        INTO OUTFILE '/tmp/preferences.csv'
                        FIELDS TERMINATED BY ','
                        LINES TERMINATED BY '\n'""")
   
    call(['hadoop', 'fs', '-put', '/tmp/preferences.csv',
          '/tmp/input/preferences.csv'])
    call(['rm', '-f', '/tmp/preferences.csv'])


def compute_similarities():
    call("""export MAHOUT_LOCAL=true ; mahout itemsimilarity -i /tmp/input -o /tmp/output -s SIMILARITY_LOGLIKELIHOOD -m 10 --tempDir /tmp/tmp""", shell=True)
    call(['hadoop', 'fs', '-rmr', '/tmp/tmp'])


def load_similarities():
    call(['rm', '-f', '/tmp/similarities.csv'])
    call(['hadoop', 'fs', '-get', '/tmp/output/part-r-00000',
          '/tmp/similarities.csv'])
    call(['hadoop', 'fs', '-rmr', '/tmp/input'])
    call(['hadoop', 'fs', '-rmr', '/tmp/output'])
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""DELETE recommender_sim.* FROM recommender_sim""")
    cursor.execute("""LOAD DATA LOCAL
                        INFILE '/tmp/similarities.csv'
                        INTO TABLE recommender_sim
                        FIELDS TERMINATED BY '\t'
                        LINES TERMINATED BY '\n'
                        (item_id_1, item_id_2, similarity)""")


def main():
    if sys.argv[1] == "create":
        create_data(int(sys.argv[2]))
    elif sys.argv[1] == "delete":
        delete_data()
    elif sys.argv[1] == "create_similarities":
        create_similarities()
    elif sys.argv[1] == "dump_preferences":
        dump_preferences()
    elif sys.argv[1] == "compute_similarities":
        compute_similarities()
    elif sys.argv[1] == "load_similarities":
        load_similarities()
    else:
        print "Unknown command"


if __name__ == "__main__":
    main()
