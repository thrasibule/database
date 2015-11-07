import mysql.connector
import sys
from json import load
from os.path import basename, splitext
from csv import DictReader

constraints = {
    "househld": [("primary", "HHX")],
    "familyxx": [("primary", "FMX"), ("foreign", "HHX", "househld")],
    "personsx": [("primary", "FPX"), ("foreign", "FMX", "familyxx"), ("foreign", "HHX", "househld")],
    "samadult": [("primary", "FPX"), ("foreign", "FMX", "familyxx"), ("foreign", "HHX", "househld")],
    "samchild": [("primary", "FPX"), ("foreign", "FMX", "familyxx"), ("foreign", "HHX", "househld")],
    "injpoiep": [("primary", "IPEPNO"), ("foreign", "FMX", "familyxx"), ("foreign", "HHX", "househld"), ("foreign", "FPX", "personsx")]
}


def execute_script(fn, cur):
    query = " ".join(line.strip() for line in open(fn).readlines())
    for q in query.split(";"):
        if q:
            cur.execute(q)


def get_keys(fh):
    line = fh.readline()
    line = line.strip().decode("utf8").strip()
    keys = line.split(",")
    keys = [key[1:-1] for key in keys]
    return keys


def format_constraint(db):
    l = constraints[db]

    def aux(l):
        for e in l:
            if e[0] == "primary":
                yield "PRIMARY KEY ({0})".format(e[1])
            if e[0] == "foreign":
                yield "FOREIGN KEY ({0}) REFERENCES {1}({0})".format(e[1],
                                                                     e[2])
    return ",\n".join(aux(l))


def schema(fn):
    bn = basename(fn)
    bn = splitext(bn)[0]
    r = "CREATE TABLE {0} (".format(bn) + "\n"

    def get_fields():
        with open(bn + "_schema.csv") as fh:
            reader = DictReader(fh)
            for row in reader:
                yield "{0} {1}".format(row["column"], row["dtype"])

    r += ",\n".join(get_fields())

    r += ",\n"
    r += format_constraint(bn)
    r += "\n)"
    return r


def clean(row, keys):
    for k in row:
        if row[k] == "NA" or row[k] == " ":
            row[k] = None
    return tuple(row[k] for k in keys)


def load_file(fn, cur, conn):
    bn = basename(fn)
    bn = splitext(bn)[0]
    with open(fn) as fh:
        keys = get_keys(fh)
        statement = "INSERT INTO {0} ".format(bn)
        statement += "(" + ",".join(key for key in keys) + ") "
        statement += "VALUES (" + ",".join("%s" for key in keys) + ")"
        reader = DictReader(fh, fieldnames=keys)
        data = [clean(row, keys) for row in reader]
    chunk_size = 1000
    chunks = [data[i:i+chunk_size] for i in xrange(0, len(data), chunk_size)]
    for i, c in enumerate(chunks):
        print i
        cur.executemany(statement, c)
        conn.commit()


def main():
    config = load(open("config.json"))
    db_user = config["user"]
    pw = config["pw"]
    db = config["db"]

    # Setup MySQL connection
    conn = mysql.connector.connect(host='health-db-internet.c6clocfz5zxy.us-east-1.rds.amazonaws.com',
                                   port=3306,
                                   user=db_user,
                                   passwd=pw,
                                   db=db)
    cur = conn.cursor()
    #execute_script(sys.argv[1], cur)
    #print ";\n\n".join(schema(fn) for fn in sys.argv[1:])
    load_file(sys.argv[1], cur, conn)

    cur.close()
    conn.close()

if __name__ == '__main__':
    #print ";\n\n".join(schema(fn) for fn in sys.argv[1:])
    main()
