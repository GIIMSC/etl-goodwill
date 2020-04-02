from etl.utils.logger import logger

class PathwaysLoader():
    def __init__(self, dataframe, engine):
        self.dataframe = dataframe
        self.engine = engine
    
    def load_data(self):
        pass