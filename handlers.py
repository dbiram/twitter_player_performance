from app.app_functions import AppFunctions
from datetime import datetime, timedelta

if __name__ == "__main__":
    app = AppFunctions(hdfs_path="http://localhost:50070", path="data/")
    app.append_master_data(datetime.now()-timedelta(days=1))