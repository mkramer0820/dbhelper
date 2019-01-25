from mongo.MongoConn import Connection
import yaml
import os
directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'settings.yaml'))
import json
from bson import ObjectId


data_loaded = yaml.load(open(directory))
matched_reports = data_loaded['paths']['matched_reports'].replace("*", "")
blotter_reports = data_loaded['paths']['blotter_reports'].replace("*", "")
print(matched_reports)


blotter = Connection(collection='tracereports')
matched = Connection(collection='tracematched')

bdata = blotter.FindAll()
mdata = matched.FindAll()

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)



def BackUp(data, path):

    encoder = JSONEncoder

    data = [i for i in data]

    for doc in data:

        doc.pop('_id', None)

    with open(path, 'w') as bk:

        json.dumps(data)

    bk.close()






