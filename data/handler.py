import os
import pandas as pd
import numpy as np
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

    def _labelfunc(self, df_origin, label, start_time, end_time):

        df = pd.DataFrame()

        df["日期"] = df_origin["日期"]
        df["股票代码"] = df_origin["股票代码"]

        for col in df_origin.columns:
            label = label.replace(f"[{col}]", f'df_origin["{col}"]')
        df["LABEL0"] = eval(label)

        df = df[(df["日期"] >= start_time) & (df["日期"] <= end_time)]

        return df

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

        append_actions = []

        # factor（feature）
        if "feature" in col_set:
            def append_feature(code, left_time, right_time):
                factor_path = f"cn_data/factor/{code}.parquet"
                df_factor = pd.read_parquet(factor_path)
                df_factor['日期'] = pd.to_datetime(df_factor['日期'])
                df_factor_trunc = df_factor[(df_factor["日期"] >= left_time) & (df_factor["日期"] <= right_time)]
                all_factor.append(df_factor_trunc)
            append_actions.append(append_feature)

        # origin（raw）
        if "raw" in col_set:
            def append_raw(code, left_time, right_time):
                origin_path = f"cn_data/origin/{code}.parquet"
                df_origin = pd.read_parquet(origin_path)
                df_origin['日期'] = pd.to_datetime(df_origin['日期'])
                df_origin_trunc = df_origin[(df_origin["日期"] >= left_time) & (df_origin["日期"] <= right_time)]
                all_origin.append(df_origin_trunc)
            append_actions.append(append_raw)

        # label
        if "label" in col_set:
            def append_label(code, left_time, right_time):
                origin_path = f"cn_data/origin/{code}.parquet"
                df_origin = pd.read_parquet(origin_path)
                df_origin['日期'] = pd.to_datetime(df_origin['日期'])
                df_origin_trunc = df_origin[(df_origin["日期"] >= left_time) & (df_origin["日期"] <= right_time + pd.Timedelta(days=10))]
                df_label = self._labelfunc(
                    df_origin=df_origin_trunc,
                    label=self.label,
                    start_time=left_time,
                    end_time=right_time
                )
                all_label.append(df_label)
            append_actions.append(append_label)

        instruments = self.instruments

        if isinstance(instruments, str):
            if  instruments in ["all", "csi100", "csi300", "csi500", "csi800", "csi1000", "csiall"]:   
                instruments = pd.read_csv(f"instruments/{instruments}.txt", sep='\t', header=None)
                instruments.columns = ["品种代码", "纳入日期", "剔除日期"]
                instruments = instruments.set_index(['纳入日期', '剔除日期'])

                grouped = list(instruments.groupby(level=[0, 1]))
                total = len(grouped)

                for j, ((in_time, out_tiem), group) in enumerate(tqdm(grouped)):
                    codes = group['品种代码'].tolist()
                    in_time = pd.to_datetime(in_time)
                    out_tiem = pd.to_datetime(out_tiem)

                    if (out_tiem >= start_time and in_time <= end_time):

                            left_time = max(in_time, start_time)
                            right_time = min(out_tiem, end_time)
                            
                            if j == total - 1:
                                right_time = end_time

                            for code in codes:
                                code = code.lower()

                                for action in append_actions:
                                    action(code, left_time, right_time)

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
                    df = df[~mask] 

                    mean = df["label"].mean()
                    std = df["label"].std()

                    df["label"] -= mean
                    df["label"] /= std
                
            return  df

    