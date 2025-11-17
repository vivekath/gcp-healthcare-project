from configs.constants import Constants
import datetime

class ConfigReader:
    def __init__(self):        
        pass

    # Function to Read Config File from GCS
    def read_config_file(self,spark, config_file_path):
        df = spark.read.csv(config_file_path, header=True)
        return df