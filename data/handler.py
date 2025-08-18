import os
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
from copy import deepcopy

from typing import Union, List, Tuple, Text

class Handler():

    def __init__(self, instruments, segments, label):

        self.instruments = instruments
        self.segments = segments
        
        self.label = label

        #self.trainData, self.testData, self.columns = self._prepare_data()
    
    DK_R = "raw"
    DK_I = "infer"
    DK_L = "learn"

    def _labelfunc(self, df_origin, label):
        
        '''
        for col in df_origin.columns:
            label = label.replace(f"[{col}]", f'df_origin["{col}"]')
        
        df_origin["LABEL0"] = eval(label)
        '''

        match = re.search(r'shift\((-?\d+)\)', label)
        if match:
            shift = int(match.group(1))

            with np.errstate(divide='ignore', invalid='ignore'):
                df_origin["LABEL0"] = np.log(df_origin["收盘"].shift(shift)) - np.log(df_origin["收盘"])
            
            #df_origin.loc[df_origin["收盘"] <= 0, "LABEL0"] = np.nan
            #df_origin.loc[df_origin["收盘"].shift(shift) <= 0, "LABEL0"] = np.nan

    def prepare(self,
        key=None,      
        col_set=None,
        segments=None,
        data_key=None,):

        if segments is None:
            segments = deepcopy(self.segments)

        if data_key == Handler.DK_L:
            assert key in ["train", "valid"]
        elif data_key == Handler.DK_I:
            assert key == "test"

        start_time, end_time = segments[key]
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)

        all_origin = []
        all_factor = []
        all_label = []

        instruments = self.instruments

        ops = []
        if "label" in col_set:
            def append_label():
                self._labelfunc(df_origin, self.label)
                all_label.append(df_origin.loc[masko, ["日期", "股票代码", "LABEL0"]].copy())
            ops.append(append_label)

        if "feature" in col_set:
            def append_feature():
                all_factor.append(df_factor.loc[maskf].copy())
            ops.append(append_feature)

        if "raw" in col_set:
            def append_raw():
                all_origin.append(df_origin.loc[masko, df_origin.columns[:-1]].copy())
            ops.append(append_raw)

        if isinstance(instruments, str):
            if  instruments in ["all", "csi100", "csi300", "csi500", "csi800", "csi1000", "csiall"]:   
                instruments = pd.read_csv(f"instruments/{instruments}.txt", sep='\t', header=None)
                instruments.columns = ["品种代码", "纳入日期", "剔除日期"]
                
                period_map = {
                    code: group[["纳入日期", "剔除日期"]].values.tolist()
                    for code, group in instruments.groupby("品种代码")
                }

                for code, periods in period_map.items():
                    code = code.lower()

                    origin_path = f"cn_data/origin/{code}.parquet"
                    factor_path = f"cn_data/factor/{code}.parquet"

                    df_origin = pd.read_parquet(origin_path)
                    df_origin['日期'] = pd.to_datetime(df_origin['日期'])
                    
                    df_factor = pd.read_parquet(factor_path)
                    df_factor['日期'] = pd.to_datetime(df_factor['日期'])

                    masko = pd.Series(False, index=df_origin.index)
                    maskf = pd.Series(False, index=df_factor.index)

                    total = len(periods)
                    for j, in_and_out in enumerate(periods):

                        in_time, out_time = in_and_out[0], in_and_out[1]

                        in_time = pd.to_datetime(in_time)
                        out_time = pd.to_datetime(out_time)

                        start = max(in_time, start_time)
                        end = min(out_time, end_time)

                        if j == total - 1 and key == "test":
                                end = end_time

                        if start <= end:  # 有交集才保留
                            masko |= df_origin["日期"].between(start, end)
                            maskf |= df_factor["日期"].between(start, end)

                    for op in ops:
                        op()

                append_map = {
                    "raw": all_origin,
                    "feature": all_factor,
                    "label": all_label,
                }

                # 只保留 col_set 里需要的
                active_map = {k: v for k, v in append_map.items() if k in col_set}

                df_parts = {k: pd.concat(v, ignore_index=True).set_index(["日期", "股票代码"]).sort_index(level="日期") for k, v in active_map.items()}
                df = pd.concat(df_parts, axis=1)

                if "label" in col_set:

                    mask = ~np.isfinite(np.array(df["label"]))
                    mask |= np.array(df["label"]) >= 0.2
                    mask |= np.array(df["label"]) <= -0.2

                    df = df[~mask]

                    mean = df["label"].mean()
                    std = df["label"].std()

                    df["label"] -= mean
                    df["label"] /= std
                
                return  df