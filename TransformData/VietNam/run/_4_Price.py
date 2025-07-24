import _lib_path_
import sys

from Flow import Folder
import math
from Flow.PATH_env import PATH_ENV
from Flow.ulis import *
from base.Price import *
from base.Setup import *

TP = TransformPrice(FU,FC,F_RANGE)
CURRENT = 0
for symbol in SYMBOL:
    CURRENT+=1
    try:
        TP.concat_source(symbol).to_csv(FU.joinPath(FU.PATH_CLOSE,f"{symbol}.csv"),index=False)
    except:
        print(symbol)
    progress_bar(CURRENT,TOTAL,text="Biến đổi giá")