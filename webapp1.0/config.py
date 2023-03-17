import datetime
user_details = {
    "admin": {"Schema": "KIV.ACCOUNT_USAGE",
              "database": "",
              "name": "Peter"},

    "admin2": {"Schema": "KNT.ACCOUNT_USAGE",
               "database": "",
               "name": "John"}
}

params = dict(
    # Schema="KIV.ACCOUNT_USAGE",
    sdate=datetime.datetime.strptime('2022-10-05', "%Y-%m-%d").date(),
    edate=datetime.datetime.strptime('2022-10-12', "%Y-%m-%d").date(),
    TRAINING_LENGTH=30,
    data_loaded=False
)

usernames = ['admin', 'admin2']
passwords = ['admin', 'admin2']
names = ["Peter", "John"]
