from mongo.MongoConn import Connection
import csv
import datetime
import yaml
import os
import glob
directory = os.path.realpath(os.path.join(os.path.dirname(__file__), "settings.yaml"))



data_loaded = yaml.load(open(directory))
matched_reports = data_loaded['paths']['matched_reports']
blotter_reports = data_loaded['paths']['blotter_reports']


matched_reports = glob.glob(matched_reports)
blotter_reports = glob.glob(blotter_reports)


class MongoCsvHelper(object):


    def ReadBlotterCsvs(self, path):
        """
        returns the list dictionary used for uploading blotter

        :param path:
        :return:
        """


        conn = Connection(collection='tracereports')
        cols = conn.GetColumns()
        print(cols)
        cols = cols[1:]
        print(cols)

        data = self.BlotterReader(cols, path)

        return data

    def BlotterReader(self, cols, *paths):
        """
        use for the trace report donwload not the matched report
        used for formatting and reading the csv


        :param cols: Use MongoConn columns method to get this
        :param args: Args will be the file path
        :return:
        """



        data = []


        for arg in paths:
            for file in arg:
                with open(file, 'r') as f:

                    csvf = csv.reader(f, delimiter=",", quotechar='"', dialect=csv.excel)
                    header = next(csvf)
                    print(header)
                    if "Matched Date" in header:

                        raise IOError('wrong file input')
                        return

                    reader = csv.DictReader(f, delimiter=",", quotechar='"', dialect=csv.excel, fieldnames=cols)
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


    def ReadMatched(self, path):


        conn = Connection(collection='tracematched')
        cols = conn.GetColumns()
        cols = cols[1:]

        print(cols)
        data = self.MatchedReader(cols, path)
        print(data[:10])

        return data

    def MatchedReader(self, cols, *args):
        import csv
        data = []

        for arg in args:

            for file in arg:
                with open(file, 'r') as f:

                    csvf = csv.reader(f, delimiter=",", quotechar='"', dialect=csv.excel)
                    header = next(csvf)
                    print(header)
                    if "trade_market_indicator" in header:
                        raise IOError('wrong file input')
                        return

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


#helper = MongoCsvHelper()

#blotter = helper.ReadBlotterCsvs(blotter_reports)
#matched = helper.ReadMatched(matched_reports)


if __name__ == 'main':
    pass