import _lib_path_
import sys

from base.Volume import *
from base.PATH_UPDATE import *
from base.Compare import *
from base.Setup import *
C = Compare()
V_CF = VolumeCafeF(dict_path_cf)
V_VS = VolumeVietStock(dict_path_vs)
df_volume = pd.DataFrame({})

for symbol in SYMBOL:
    temp = pd.DataFrame({"Cafef":[V_CF.getVolumeNow(symbol)],"VietStock":[V_VS.getVolumeNow(symbol)],"Symbol":[symbol]})
    df_volume = pd.concat([df_volume,temp],ignore_index=True)
df_volume["Compare"] = df_volume.apply(lambda row: C.CompareNumber(row["Cafef"],row["VietStock"]),axis=1)
df_volume.to_excel(f"{PATH_COMPARE}/Volume.xlsx",index=False)