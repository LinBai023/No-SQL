import pymysql
import json



cnx = pymysql.connect(host='localhost',
                                user='root',
                                password='Bailin960203',
                                db='lahman2017raw',
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)


def run_q( q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result

def template_to_where_clause(t):
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"

    if s != "":
        s = "WHERE " + s

    return s


def find_by_template(table, template, fields):
    wc = template_to_where_clause(template)

    q = "select " + fields[0] + " from " + table + " " + wc
    result = run_q(q, None, True)
    return result


def find_by_primarykey(table, key_values, fields):

    q = "SHOW KEYS FROM " + table + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column=keyname[0]['Column_name']
    template = "WHERE "+key_column+"= '"+key_values +"'"
    q = "select " + fields[0] + " from " + table + " " + template
    print(q)
    result = run_q(q, None, True)
    return result
    print(result)
    '''
    q = "SHOW KEYS FROM " + table + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column = keyname[0]['Column_name']
    template = dict(zip([key_column], [key_values]))
    print(template)
    result = find_by_template(template, fields)
    print(result)

    if len(result)>0:
        return result
    else:
        return None
    '''

def delete(table, template):
    w = template_to_where_clause(template)
    q = "delete from " + table + " " + w
    run_q(q)

def insert(table, row):
    pass

