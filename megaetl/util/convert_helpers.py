def date_to_mongo(dictlist: list) -> list:
    import datetime
    import pandas as pd
    for d in dictlist:
        for k, v in d.items():
            print(type(v))
            if type(v) == datetime.date:
                try:
                    v = pd.Timestamp(datetime.datetime.strftime(v, '%Y-%m-%d %H:%M:%S'))
                except:
                    v = None
                d.update({k: v})
    return dictlist
