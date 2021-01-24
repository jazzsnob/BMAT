import argparse
import pandas as pd
import pymongo
from pymongo import MongoClient

parser = argparse.ArgumentParser(prog='BMAT .csv parser',
                            description='A script used to parse and ingest .csv files')
parser.add_argument('name', help='Name of a .csv file')
parser.add_argument('-u', '--upload', action='store_true', help='Upload .csv to MongoDB.')
parser.add_argument('-f', '--find', action='store_true', help='Find from MongoDB.')
args = parser.parse_args()


def main():

    def columns(df):
        df = df.rename(str.lower, axis='columns')
        df = df.rename(columns={"right owner": "name", "id society": "_id"})
        cols = df.columns.tolist()
        df = df[cols[-1:] + cols[:-1]]
        return df

    def work(id):
        return {"_id": int(str(id))}

    def iswc(df, i):
        cols = df.columns.tolist()
        iswc = df.loc[df["_id"] == i, cols[1]].drop_duplicates()
        #return iswc
        return {"iswc": str(iswc.iloc[0])}

    def title(df, i):
        cols = df.columns.tolist()
        title = df.loc[df["_id"] == i, cols[2:6]].drop_duplicates()
        title = title.fillna(0)
        title = title.iloc[0].to_dict()
        titles = []
        for key, value in title.items():
            if value != 0:
                temp = {}
                temp["title"] = value
                temp["type"] = key
                titles.append(temp)
        return titles

    def right_owners(df, i):
        cols = df.columns.tolist()
        right_owners = df.loc[df["_id"] == i, cols[6:9]].drop_duplicates()
        right_owners = right_owners.fillna(0)
        right_owners = right_owners.to_dict('records')
        rights = []
        for r in right_owners:
            interim = {}
            for key, values in r.items():
                if values:
                    temp = {}
                    temp[key] = values
                    interim.update(temp)
            rights.append(interim)
        return rights


    cluster = MongoClient("mongodb+srv://bmatUser:bmatUserPassword@cluster0.rtjcd.mongodb.net/bmatUser?retryWrites=true&w=majority")
    db = cluster["bmatDB"]
    collection = db["musicalworks"]

    df = pd.read_csv(args.name, converters={"IPI NUMBER":str})
    df = columns(df)

    if args.upload:
        for i in df['_id'].unique():
            work_dic = work(i)
            iswc_dic = iswc(df, i)
            work_dic.update(iswc_dic)
            title_dic = {"title": title(df, i)}
            work_dic.update(title_dic)
            right_owners_dic = {"right_owners": right_owners(df, i)}
            work_dic.update(right_owners_dic)
            collection.insert_one(work_dic)

    elif args.find:

        cols = df.columns.tolist()
        find_list = list(df[cols[1]].unique())
        results = collection.find({cols[1]:{"$in": find_list}})
        for i in results:
            print(i["right_owners"])
            print('*' * 100)

if __name__ == "__main__":
    main()
