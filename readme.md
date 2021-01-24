# BMAT
A python script for uploading db_works_test.csv data to MongoDB, as well as querying for right owners metadata by ISWC.
## Installation
```
pip install -r requirements.txt
```
Copy and past the connection string into the code (line 63), within the double quotes of MongoClient("**connection string**"):
```
cluster = MongoClient("mongodb+srv://bmatUser:<password>@cluster0.rtjcd.mongodb.net/<dbname>?retryWrites=true&w=majority")
```
Replace ```<password>``` with the password for the ```bmatUser``` user. Replace ```<dbname>``` with the name of the database that connections will use by default.
## Usage
to upload db_works_test.csv to MongoDB:
```
python bmat.py db_works_test.csv -u
```
to search for right owners' metadata in MongoDB by ISWC:
```
python bmat.py db_works_test.csv -f
```
### [SSL: CERTIFICATE_VERIFY_FAILED]
In case of the `urllib.error.URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate`, navigate to Application/Python 3.7, you will see the command file shipped with **Python: Install Certificates.command** and run that command.
