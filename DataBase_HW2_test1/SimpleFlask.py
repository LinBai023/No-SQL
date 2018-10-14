# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def parse_and_print_args():

    fields = None
    in_args = None
    offset = None
    limit = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        fields = copy.copy(in_args.get('fields', None))
        offset = copy.copy(in_args.get('offset', None))
        limit = copy.copy(in_args.get('limit', None))
        if fields:
            del(in_args['fields'])
        if offset:
            del(in_args['offset'])
        if limit:
            del(in_args['limit'])

    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None



    print("Request.args : ", json.dumps(in_args))
    print("Request.fields : ", json.dumps(fields))
    print("body: ", json.dumps(body))
    return in_args, fields, body, offset, limit


@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource(resource):

    in_args, fields, body, offset, limit = parse_and_print_args()
    print(in_args)
    if request.method == 'GET':
        result = SimpleBO.find_by_template(resource, \
                                           in_args, fields,offset,limit)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'POST':
        result=SimpleBO.insert(resource, body)
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404
'''
        print("This would be a really good place to call insert()")
        print("on table ", resource)
        print("with row ", body)
        print("But there has to be some HW not written in class.")
        return "Method " + request.method + " on resource " + resource + \
            " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
'''

@app.route('/api/<resource>/<primarykey>', methods=['GET', 'PUT', 'DELETE'])
def get_primarykey(resource, primarykey):


    if request.method=='GET':
        fields = parse_and_print_args()[1]
        result=SimpleBO.find_by_primarykey(resource, primarykey, fields)
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404

    elif request.method=='PUT':
        in_args=parse_and_print_args()[0]
        result=SimpleBO.update_by_primarykey(resource, primarykey, in_args)
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404

    elif request.method=='DELETE':
        result=SimpleBO.delete(resource, primarykey)
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404


@app.route('/api/<resource>/<primarykey>/<related_resource>', methods=['GET', 'POST'])
def get_depedent(resource, related_resource, primarykey):

    if request.method=='GET':
        fields=parse_and_print_args()[1]
        result=SimpleBO.find_by_dependent(resource, related_resource,primarykey,fields)

        if result:
            return json.dumps(result), 200, {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404

@app.route('/api/teammates/<primarykey>', methods=['GET'])
def get_teammates(primarykey):

    if request.method=='GET':
        result = SimpleBO.find_by_teammate(primarykey)

        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404

@app.route('/api/people/<playerid>/career_stats', methods=['GET'])
def get_stats(playerid):

    if request.method=='GET':
        result = SimpleBO.find_stats(playerid)
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404

@app.route('/api/roster/', methods=['GET'])
def find_roster():
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        teamid = copy.copy(in_args.get('teamid', None))
        yearid = copy.copy(in_args.get('yearid', None))
        if teamid:
            del(in_args['teamid'])
        if yearid:
            del(in_args['yearid'])
    if request.method=='GET':
        result = SimpleBO.find_roster(teamid[0], yearid[0])
        if result:
            return json.dumps(result), 200, \
                {"content-type": "application/json; charset: utf-8"}
        else:
            return "Not Found", 404










if __name__ == '__main__':
    app.run()

