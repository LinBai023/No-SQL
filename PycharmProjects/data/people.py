import csv
import json
import copy


class CSVDataTable:

    data_dir = '/Users/zs/PycharmProjects/data/'

# initialize the class
    def __init__(self, t_name, t_file, key_column):
        self.table_name = t_name
        self.file_name = t_file
        self.key_columns = key_column
        self.columns = None
        self.rows = None

# read the csv file as dictionary
    def __str__(self):
        result = ""
        result += "Name: {}, File: {}, No.of columns:{},Key: {}".format(self.table_name, self.file_name,len(self.rows),self.key_columns)
        result += "\n"
        result += "Columns = " + str(self.columns)
        result += "\nRow = \n"
        for r in self.rows:
            result += str(r) + "\n"
        return result

    # load the file by the file name into class instance data

    def load(self):
        file_address=self.data_dir+self.file_name

        with open(file_address, 'r') as csvfile:
            reader= csv.DictReader(csvfile)
# initialize the column and row: column is the list of keys, and row is the data storage
            for row in reader:
                if self.columns is None:
                    self.columns = list(row.keys())
                if self.rows is None:
                    self.rows = []
                self.rows.append(row)

    def matches_template(self, template, row):
        keys = list(template.keys())
        for k in keys:
            if template[k] != row[k]:
                return False
            return True

    def find_by_template(self, template, fields):
        res=[]
        for r in self.rows:
            if self.matches_template(template, r):
                res.append(r)
        res=self.project(res, fields)
        return res


    def project(self, rows, fields):
        projection=[]
        for r in rows:
            new_row={}
            if fields is None:
                new_row=r
            else:
                for k in fields:
                    new_row[k] = r[k]
            projection.append(new_row)
        return projection

    def find_by_primary_key(self, key, fields):
        t = {}
        for i in range(0, len(self.key_columns)):
            t[self.key_columns[i]] = key[i]
        result = self.find_by_template(t, fields)
        return result


    def insert(self, dict):
        if not self.check_in_table(dict):
            self.rows.append(dict)

    def check_in_table(self, dict):
        for r in self.rows:
            if self.matches_template(dict, r):
                return True
        return False


    def delete(self, template):
        m = self.find_by_template(template, None)
        for r in m:
            del(r)




# save the new table
# write the dictionary files into csv files


csvt = CSVDataTable("People", "short.csv", ['playerID'])
csvt.load()
csvt.insert({'playerID': 'Me', 'birthYear': '1996', 'birthMonth': '2', 'birthDay': '5', 'birthCountry': 'USA', 'birthState': 'AL', 'birthCity': 'Mobile', 'deathYear': '1984', 'deathMonth': '8', 'deathDay': '16', 'deathCountry':'USA', 'deathState': 'GA', 'deathCity': 'Atlanta', 'nameFirst': 'Tommie', 'nameLast': 'Aaron', 'nameGiven': 'Tommie Lee', 'weight': '190', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '1962-04-10', 'finalGame': '1971-09-26', 'retroID': 'aarot101', 'bbrefID': 'aaronto01'})
#csvt.save()
print("Table=\n", csvt)



    


