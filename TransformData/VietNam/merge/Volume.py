import _lib_path_
import pandas as pd
import sys

from base.PATH_UPDATE import *
from VAR_GLOBAL_CONFIG import *
from base.Setup import *
from Flow.ulis import *

VOLUME = pd.read_excel(f"{PATH_COMPARE}/Volume.xlsx")
VOLUME = VOLUME[["Symbol","FIX"]].rename(columns={"FIX":"Volume"})
VOLUME.to_excel(f"{FU.PATH_MAIN_CURRENT}/Volume.xlsx",index=False)