import os
import time
import pandas as pd
import numpy as np
import akshare as ak
import fastparquet
from datetime import datetime, timedelta

from data.factor import Alpha158

from typing import Union, List

def updateD(instruments: Union[str, List], freq="daily"):

    if isinstance(instruments, str):
        if instruments in ["all", "csi100", "csi300", "csi500", "csi800", "csi1000", "csiall"]:   
            instruments = pd.read_csv(f"instruments/{instruments}.txt", sep='\t', header=None)
            instruments.columns = ["品种代码", "纳入日期", "剔除日期"]
            instruments = instruments.set_index(['纳入日期', '剔除日期'])

            instruments = instruments.loc[instruments.index[-1]]["品种代码"].tolist()
            #instruments = set(instruments["品种代码"].tolist())
            instruments = [i.lower() for i in instruments]
        else:
            raise ValueError("Unsupported input market for param `instrument`")
        
    elif isinstance(instruments, (list, tuple, pd.Index, np.ndarray)):
        # list or tuple of a group of instruments
        instruments = list(instruments)
    
    os.makedirs(f"cn_data/origin", exist_ok=True)
    
    for code in instruments:
        try:
            symbol = code[2:]
            origin_path = f"cn_data/origin/{code}.parquet"

            if os.path.exists(origin_path):
                df_old = pd.read_parquet(origin_path, engine="fastparquet")
                df_old['日期'] = pd.to_datetime(df_old['日期'])

                last_date = df_old['日期'].max()
                start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y%m%d')
                current_date =  pd.to_datetime(datetime.now()).strftime('%Y%m%d')

                if start_date > current_date:
                    #print(f"✅ {symbol} 已是最新，无需更新")
                    continue

                df_new = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, adjust="qfq")
                
                if df_new.empty:
                    #print(f"✅ {symbol} 无新增数据")
                    continue
                else:
                    df_new['日期'] = pd.to_datetime(df_new['日期'])

            else:
                df_old = None
                df_new = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")

            if df_old is not None:
                df_all = pd.concat([df_old, df_new], ignore_index=True)
                df_all.drop_duplicates(subset=['日期'], inplace=True)
                df_all.sort_values('日期', inplace=True)
                #df_all["股票代码"] = df_all["股票代码"].astype(str).str.zfill(6)
                df_all["股票代码"] = code
                df_all["日期"] = pd.to_datetime(df_all["日期"])
            else:
                df_all = df_new
                #df_all["股票代码"] = df_all["股票代码"].astype(str).str.zfill(6)
                df_all["股票代码"] = code
                df_all["日期"] = pd.to_datetime(df_all["日期"])

            df_all.to_parquet(origin_path, engine="fastparquet")
            #print(f"✅ 已更新 {symbol} 到 {df_all['日期'].max().strftime('%Y-%m-%d')}")
            time.sleep(0.4)

        except Exception as e:
            #print(f"❌ {symbol} 下载失败: {e}")
            continue

def updateF(instruments: Union[str, List]):
      
    if isinstance(instruments, str):
        if instruments in ["all", "csi100", "csi300", "csi500", "csi800", "csi1000", "csiall"]:   
            instruments = pd.read_csv(f"instruments/{instruments}.txt", sep='\t', header=None)
            instruments.columns = ["品种代码", "纳入日期", "剔除日期"]
            instruments = instruments.set_index(['纳入日期', '剔除日期'])

            instruments = instruments.loc[instruments.index[-1]]["品种代码"].tolist()
            #instruments = set(instruments["品种代码"].tolist())

            instruments = [i.lower() for i in instruments]
        else:
            raise ValueError("Unsupported input market for param `instrument`")
        
    elif isinstance(instruments, (list, tuple, pd.Index, np.ndarray)):
        # list or tuple of a group of instruments
        instruments = list(instruments)
    
    os.makedirs(f"cn_data/factor", exist_ok=True)

    for code in instruments:
        try:
            origin_path = f"cn_data/origin/{code}.parquet"
            factor_path = f"cn_data/factor/{code}.parquet"

            df_origin = pd.read_parquet(origin_path)
            df_origin['日期'] = pd.to_datetime(df_origin['日期'])

            if os.path.exists(factor_path):
                
                df_factor_old = pd.read_parquet(factor_path)
                df_factor_old['日期'] = pd.to_datetime(df_factor_old['日期'])

                last_date = df_factor_old['日期'].max()
                start_date = pd.to_datetime(last_date + pd.timedelta(days=1))
                current_date = pd.to_datetime(datetime.now())

                if start_date > current_date:
                    #print(f"✅ {symbol} 已是最新，无需更新")
                    continue
                
                df_origin = df_origin[df_origin["日期"] >= start_date]
                df_factor_new = Alpha158.Alpha158Factor(df_origin)

                if df_factor_new.empty:
                    #print(f"✅ {symbol} 无新增数据")
                    continue
            
            else:
                df_factor_old = None
                df_factor_new = Alpha158.Alpha158Factor(df_origin)

            if df_factor_old is not None:
                df_factor_all = pd.concat([df_factor_old, df_factor_new], ignore_index=True)
                df_factor_all.drop_duplicates(subset=['日期'], inplace=True)
                df_factor_all.sort_values('日期', inplace=True)
                df_factor_all["股票代码"] = code
                df_factor_all["日期"] = pd.to_datetime(df_factor_all["日期"])
            else:
                df_factor_all = df_factor_new
                df_factor_all["股票代码"] = code
                df_factor_all["日期"] = pd.to_datetime(df_factor_all["日期"])

            df_factor_all.to_parquet(factor_path, index=False)
            #print(f"✅ 已更新 {symbol} 到 {df_factor_all['日期'].max().strftime('%Y-%m-%d')}")
    
        except Exception as e:
            #print(f"❌ 读取失败: {origin_path}，原因: {e}")
            continue
    