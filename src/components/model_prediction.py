import os
import sys
sys.path.append(os.path.join(os.getcwd()))
from constant import *

from src.exceptions import *
from src.logger import *
from src.components.get_data import *
from src.components.data_transformation import *
from src.components.model_training import *

import warnings
warnings.filterwarnings('ignore')




if __name__ == '__main__':
    obj = train_model(0)
    obj.train_model()