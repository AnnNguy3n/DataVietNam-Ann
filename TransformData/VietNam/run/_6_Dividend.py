import _lib_path_
import sys

from base.PATH_UPDATE import *
from base.Dividend import *
from base.Setup import *
from base.Compare import *
C = Compare()
CF = DividendCF(dict_path_cf)
VS = DividendVS(dict_path_vs)
print(F_START,F_END)

for symbol in SYMBOL:
    VS.Dividend_VS(symbol).to_csv(f'{dict_path_vs["F1"]["Dividend"]}/{symbol}.csv',index=False)
for symbol in SYMBOL:
    try:
        CF.Dividend_CF(symbol).to_csv(f'{dict_path_cf["F1"]["Dividend"]}/{symbol}.csv',index=False)
    except:
        print(symbol,'-------------format loi-------------')

def CreateCode(df,field):
    try:
        df[field] = df.apply(lambda row: "_".join([row["Money"],row["Stock"]]),axis=1)
    except ValueError:
        df[field] = ["NAN" for i in df.index]
    return df

print(F_START,F_END)

for symbol in SYMBOL:
    try:
        df_cf =  pd.read_csv(f'{dict_path_cf["F1"]["Dividend"]}/{symbol}.csv')
        df_vs =  pd.read_csv(f'{dict_path_vs["F1"]["Dividend"]}/{symbol}.csv')
        df_cf = CreateCode(df_cf,"Code_CF")
        df_vs = CreateCode(df_vs,"Code_VS")
        data = pd.merge(df_cf,df_vs,on="Time",how="outer").replace(np.nan,"NAN")
        try:
            data["Compare"] = data.apply(lambda row: C.compare_2_string(row["Code_CF"],row["Code_VS"]),axis=1)
        except:
            data["Compare"] = ["NAN" for i in data.index]
        data = data[(data["Time"]<=F_END)&(data["Time"]>F_START)].reset_index(drop=True)
        data.to_csv(f'{PATH_COMPARE}/Dividend/{symbol}.csv',index=False)
    except:
        print(symbol)
