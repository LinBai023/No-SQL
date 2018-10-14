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


def find_by_template(table, template, fields, offset, limit):
    wc = template_to_where_clause(template)

    q = "select " + fields[0] + " from " + table + " " + wc + "LIMIT "+limit[0] +" OFFSET "+offset[0]
    result = run_q(q, None, True)
    links = "links:[{"
    s1 = ""
    for (k , v) in template.items():
        if s1 != "":
            s1 += " AND "
        s1 += k + "='" + v[0] + "'"
    links += "current:api/"+table+"?"+s1+"fields="+fields[0]+"&offset="+offset[0]+"&limit="+limit[0]+"},{"
    links += "next:api/"+table+"?"+s1+"fields="+fields[0]+"&offset="+offset[0]+limit[0]+"&limit="+limit[0]+"}]}"
    result.append(links)
    return result


def find_by_primarykey(table, key_values, fields):

    q = "SHOW KEYS FROM " + table + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column=keyname[0]['Column_name']
    template = "WHERE "+key_column+"= '"+key_values +"'"
    q = "select " + fields[0] + " from " + table + " " + template
    result = run_q(q, None, True)
    result.append("link")
    return result

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

def update_by_primarykey(table, primarykey, in_args):
    s=""
    for (k, v) in in_args.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"
    '''
    wc=template_to_where_clause(in_args)
    '''

    q1 = "SHOW KEYS FROM " + table + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q1, None, True)
    key_column=keyname[0]['Column_name']
    q2 = "UPDATE "+ table + " SET " + s +" WHERE "+key_column+"="+"'"+primarykey+"';"

    result=run_q(q2 , None, True)
    print(result)
    return result



def delete(table, primarykey):
    q = "SHOW KEYS FROM " + table + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column=keyname[0]['Column_name']
    q = "delete from " + table + " WHERE " + key_column+" ='"+primarykey+"';"
    run_q(q, None, True)

def insert(table, in_args):
    key=""
    value=""
    for(k,v) in in_args.items():
        key += k+","
        value += "'"+v[0]+"'"+","
    key = key.rstrip(',')
    value = value.rstrip(',')
    print("key:"+key+"\n")
    print("value:"+value)
    q="INSERT INTO "+table+"("+key+")"+" VALUES "+"("+value+")"
    print(q)
    run_q(q, None, True)

def find_by_dependent(resource, related_resource, primarykey, fields):
    q = "SHOW KEYS FROM " + resource + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column=keyname[0]['Column_name']
    template = "WHERE "+key_column+"= '"+primarykey +"'"
    q = "select " + fields[0] + " from " + related_resource + " " + template
    result = run_q(q, None, True)
    return result

def insert_by_dependent(resource, related_resource, primarykey, body):
    q = "SHOW KEYS FROM " + resource + " WHERE Key_name ='PRIMARY'"
    keyname = run_q(q, None, True)
    key_column=keyname[0]['Column_name']
    q1 = "INSERT INTO "+ resource+"("+key_column+") "+"VALUES ('"+primarykey+"');"
    print("q1: "+q1+"\n")
    run_q(q1, None, True)
    q2 = "INSERT INTO "+ related_resource+"("+key_column+") "+"VALUES ('"+primarykey+"');"
    print("q2:"+q2+"\n")
    run_q(q2, None, True)
    result=insert(related_resource, body)
    return result

def find_by_teammate(primarykey):
    q= "SELECT m.playerID as IDs, n.playerID as teammates , min(n.yearID) as firstyear," \
       " max(n.yearID) as lastyear, count(*) as seasons FROM Appearances n LEFT JOIN Appearances m " \
       "ON m.teamID=n.teamID " \
       " WHERE m.playerID='"+ primarykey+ "' " \
                                        "GROUP BY n.playerID"
    result=run_q(q,None, True)
    return result

def find_stats(playerid):
    q = "SELECT m.playerID AS playerID, m.yearID AS yearid, m.G AS g_all," \
       "m.H AS hits, m.AB AS ABs, n.A as assists, n.E as errors " \
       "FROM Batting m LEFT JOIN Fielding n " \
       "ON m.playerID=n.playerID WHERE m.playerID='"+playerid+"'"
    print(q)
    result=run_q(q, None, True)
    return result

def find_roster(teamid, yearid):
    q = "SELECT a.nameLast as nameLast, a.nameFirst as nameFirst," \
         "m.playerID AS playerID, m.yearID AS yearid, m.G AS g_all," \
       "m.H AS hits, m.AB AS ABs, n.A as assists, n.E as errors " \
       "FROM Batting m LEFT JOIN Fielding n " \
        "ON m.playerID=n.playerID " \
         "LEFT JOIN People a " \
       "ON m.playerID=a.playerID" \
         " WHERE m.teamID='"+teamid+"' AND m.yearID="+ yearid
    print(q)
    result=run_q(q, None, True)
    return result

def test1():
    result=update_by_primarykey("People", "willite01",  {"nameLast": ["Williams"]})
    print(result)

def test2():
    result=find_by_primarykey("People", "ME4.0", "*")
    print(result)

def test3():
    insert("People", {"playerID":["ME2.0"], "nameLast":["BAI"]})

def test4():
    delete("People", "ME2.0")

def test5():
    result = find_by_primarykey("People", "ME2.0", "*")
    print(result)

def test6():
    result = find_by_dependent("People","Batting","willite01", "*")
    print(result)

def test7():
    result = insert_by_dependent("People", "Batting","ME6.0", {"yearID":["1996"], "teamID":["BAI"]})
    print(result)

def test8():
    result = find_by_template("Batting",{"yearID":["1996"], "teamID":["BAI"]},["playerID"])
    print(result)

def test9():
    result = find_by_teammate("allisar01")
    print(result)

def test10():
    result = find_stats("willite01")
    print(result)

def test11():
    result = find_roster("BOS", "2004")
    print(result)

test11()









