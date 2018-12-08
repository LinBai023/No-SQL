from redis_cache import data_cache
from utils import utils as ut
from dbservice import dataservice as ds
import json


ut.set_debug_mode(True)

t = {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}
r = data_cache.compute_key("people", {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}, \
                           ['nameLast', "birthCity"])


# def test1():
#     data_cache.add_to_cache(r, t)
# test1()

#
# def test2():
#     result = data_cache.get_from_cache(r)
#     print("Result = ", result)
#
#     t2 = {"nameLast": "Williams", "bats": "L"}
#     f2 = {"playerID", "nameLast", "nameFirst", "birthCity"}
#
#     ds.set_config()
#     result = ds.retrieve_by_template("people", t2, f2)
#     print("Result = ", json.dumps(result, indent=2))
#
#     # new_k = data_cache.compute_key("people", t2, f2, result)
#     # result = data_cache.add_to_query_cache("people",t2, f2, result)
#     result = data_cache.check_query_cache("people", t2, f2)
#     print("Result = ", json.dumps(result, indent=2))
# test2()
#
def test3():
    t3 = {"nameLast": "Williams", "bats": "L", "throws":"R"}
    f3 = ["playerID", "nameLast","nameFirst","birthCity","bats"]
    result = ds.retrieve_by_template("people", t3, f3)
    print("Result = ", json.dump(result))
test3()
