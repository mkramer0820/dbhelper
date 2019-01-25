from pymongo import MongoClient
import datetime
import dateutil.parser
import glob

import yaml
import os

directory = os.path.realpath(os.path.join(os.path.dirname(__file__), "settings.yaml"))


data_loaded = yaml.load(open(directory))

directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'settings.yaml'))
matched_reports = data_loaded['paths']['matched_reports']
blotter_reports = data_loaded['paths']['blotter_reports']


matched_reports = glob.glob(matched_reports+"*csv")
blotter_reports = glob.glob(blotter_reports+"*csv")

class Connection(object):


    def __init__(self, client=MongoClient, db='trace', collection='tracereports'):

        self.client = client('mongodb://localhost/trace').get_database(db)
        self.coll = collection


    @property
    def collection(self):

        collection = self.client.get_collection(self.coll)
        return collection

    def FindAll(self):

        client = self.client
        tracereports = self.collection

        data = tracereports.find()

        return data

    def FindOne(self):

        dbCollection = self.client.get_collection(self.coll)

        data = dbCollection.find_one()
        print(data)
        return data

    def GetColumns(self):

        data = self.FindOne()

        columns = [k for k,v in data.items()]
        return columns



    def UpdateDate(self, datekey, timekey=None):

        columns = self.GetColumns()
        data = [i for i in self.FindAll()]
        print(data[:10])

        total = self.collection.find().count()
        print('original toal is', total)

        for doc in data:
            try:
                date = doc[datekey].strftime('%m/%d/%Y')+" "+ doc[timekey]
                doc[datekey] = datetime.datetime.strptime(date, '%m/%d/%Y %H:%M:%S')
            except AttributeError:
                try:
                    date = doc[datekey] + " " + doc[timekey]
                    doc[datekey] = datetime.datetime.strptime(date, '%m/%d/%Y %H:%M:%S')

                except KeyError:
                    date = datetime.datetime.strptime(doc[datekey], '%m/%d/%Y')
                    doc[datekey] = date




        coll = self.collection
        counter = 0
        bulk = coll.initialize_unordered_bulk_op()
        print("bulk intitialized")
        for doc in data:

            bulk.find({'_id': doc['_id']}).update({'$set': doc})
            counter += 1
            print(counter)
            if ((counter % 500) == 0):
                bulk.execute()
                bulk = coll.initialize_ordered_bulk_op()

            #if counter == total:
            #    bulk.execute()

    def UpdateTime(self, key):

        columns = self.GetColumns()
        data = [i for i in self.FindAll()]
        print(data[:10])
        total = self.collection.find().count()

        for doc in data:
            doc[key] = datetime.time.strptime(doc[key], '%H:%M:%S')

        print(data[:10])

        coll = self.collection
        counter = 0
        bulk = coll.initialize_unordered_bulk_op()

        for doc in data:

            bulk.find({'_id': doc['_id']}).update({'$set': doc})
            counter += 1

            if ((counter % 500) == 0):
                bulk.execute()
                bulk = coll.initialize_ordered_bulk_op()

            if counter == total:
                bulk.execute()

    def BulkBlotterInsert(self, blotter=True):

        if blotter == True:
            data = ReadBlotterCsvs(blotter_reports)
        else:
            data = ReadMatched(matched_reports)

        coll = self.collection
        counter = 0

        bulk = coll.initialize_unordered_bulk_op()
        total = self.collection.find().count()
        print('initial total ', total)
        for doc in data:

            bulk.insert(doc)
            counter += 1

            if ((counter % 500) == 0):
                bulk.execute()
                bulk = coll.initialize_ordered_bulk_op()

            if counter == total:
                bulk.execute()

        total = self.collection.find().count()

        print("finiished new total   ", total)


    def WhiteSpaceToNull(self):

        columns = self.GetColumns()
        data = [i for i in self.FindAll()]
        print(data[:10])
        total = self.collection.find().count()

        for doc in data:

            for k,v in doc.items():

                if doc[k] == '':

                    doc[k] = None

        coll = self.collection
        counter = 0
        bulk = coll.initialize_unordered_bulk_op()
        total = len(data)
        for doc in data:

            bulk.find({'_id': doc['_id']}).update({'$set': doc})
            counter += 1

            if ((counter % 500) == 0):
                bulk.execute()
                bulk = coll.initialize_ordered_bulk_op()

            if counter == total:
                bulk.execute()

    def BackUpJsonDump(self):

        data = self.FindAll()
        print(data)




def ReadBlotterCsvs(path):



    conn = Connection()
    cols = conn.GetColumns()
    cols = cols[1:]
    print(path)
    data = BlotterReader(cols, path)


    return data


def BlotterReader(cols, *args):
    import csv
    data = []

    for arg in args:

        for file in arg:
            with open(file, 'r') as f:

                reader = csv.DictReader(f, delimiter=",", quotechar='"', fieldnames=cols)
                next(reader)
                for l in reader:
                    l = dict(l)
                    for k, v in l.items():
                        l[k] = l[k].rstrip()
                        if l[k] == "":
                            l[k]= None

                    l['quantity'] = float(l['quantity'].replace(",", ''))
                    l['price'] = float(l['price'])
                    l['trade_report_date'] = datetime.datetime.strptime(l['trade_report_date']+" "+l['trade_report_time']
                                                                        , '%m/%d/%Y %H:%M:%S')
                    l['execution_date'] = datetime.datetime.strptime(
                        l['execution_date'] + " " + l['execution_time']
                        , '%m/%d/%Y %H:%M:%S')

                    data.append(l)
    return data


def ReadMatched(path):


    conn = Connection(collection='tracematched')
    cols = conn.GetColumns()
    cols = cols[1:]

    print(cols)
    data = MatchedReader(cols, path)
    print(data[:10])

    return data

def MatchedReader(cols, *args):
    import csv
    data = []

    for arg in args:

        for file in arg:
            with open(file, 'r') as f:

                reader = csv.DictReader(f, delimiter=",", quotechar='"', fieldnames=cols)
                next(reader)
                for l in reader:
                    l = dict(l)
                    for k, v in l.items():
                        l[k] = l[k].rstrip()
                        if l[k] == "":
                            l[k]= None

                    l['quantity'] = float(l['quantity'].replace(",", ''))
                    l['price'] = float(l['price'])
                    l['trade_report_date'] = datetime.datetime.strptime(l['trade_report_date'], '%m/%d/%Y')
                    l['execution_date'] = datetime.datetime.strptime(
                        l['execution_date'] + " " + l['execution_time']
                        , '%m/%d/%Y %H:%M:%S')

                    l['matched_date'] = datetime.datetime.strptime(l['matched_date'], '%m/%d/%Y')

                    data.append(l)
    return data

mathched = Connection(collection='tracematched')
blotter = Connection(collection='tracereports')

blotter.BulkBlotterInsert(blotter=True)
mathched.BulkBlotterInsert(blotter=False)



if __name__ == "__main__":
    pass








#ReadCsvs()


#conn = Connection(collection='tracematched')
#conn.BulkBlotterInsert(blotter=False)
#conn.UpdateDate('trade_report_date')
#conn.UpdateDate('execution_date', 'execution_time')
#conn.WhiteSpaceToNull()
#conn.UpdateTime('trade_report_time')

#client = MongoClient()
#client.connect()
#print(client.db)

#conn = MongoClient('mongodb://localhost/trace')
#db = conn.get_database()
#tracereports = db.get_collection('tracereports')


#db = conn.GetDatabase('trace')
#print(conn)