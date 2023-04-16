from app.app_functions import AppFunctions
from datetime import datetime, timedelta
import sys

if __name__ == "__main__":
    if len(sys.argv) > 4:
        raise ValueError('Too many arguments passed')
    args_dict = {"days": 0, "hours": 0, "minutes": 0}
    for i, arg in enumerate(sys.argv):
        if i == 1:
            args_dict["days"] = int(arg)
        elif i == 2:
            args_dict["hours"] = int(arg)
        elif i == 3:
            args_dict["minutes"] = int(arg)
    app = AppFunctions(hdfs_path="http://localhost:50070", path="data/")
    app.append_master_data(datetime.now()-timedelta(days=args_dict["days"], hours=args_dict["hours"], minutes=args_dict["minutes"]))

