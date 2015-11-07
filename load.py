import mysql.connector
import sys
from json import load
from os.path import basename, splitext

def execute_script(fn, cur):
    script = open(fn).read()
    cur.execute(script, multi=True)

def schema(fn):
    bn = basename(fn)
    bn = splitext(bn)[0]
    with open(fn) as fh:
        line = fh.readline()
        line = line.strip().decode("utf8").strip()
        keys = line.split(",")
        keys[0] = keys[0][1:]
    print "CREATE TABLE {0} (".format(bn)
    print ",\n".join("\t {0} INT".format(key) for key in keys)
    print ");"


def main():
    config = load(open("config.json"))
    db_user = config["user"]
    pw = config["pw"]
    db = config["db"]

    # Setup MySQL connection
    conn = (mysql.connector.
            connect(host='health-db-internet.c6clocfz5zxy.us-east-1.rds.amazonaws.com',
                port=3306,
                    user=db_user,
                    password=pw,
                    database=db))
    cur = conn.cursor()
    execute_script(sys.argv[1], cur)

    cur.close()
    conn.close()

if __name__ == '__main__':
    schema(sys.argv[1])
