import lightgbm as lgb
import numpy as np
from data.handler import Handler

from typing import List, Tuple

class LGBModel():
    """LightGBM Model"""

    def __init__(self, loss="mse", early_stopping_rounds=50, num_boost_round=1000, **kwargs):
        if loss not in {"mse", "binary"}:
            raise NotImplementedError
        self.params = {"objective": loss, "verbosity": -1}
        self.params.update(kwargs)
        self.early_stopping_rounds = early_stopping_rounds
        self.num_boost_round = num_boost_round
        self.model = None
        # 🔹 加一个缓存字典
        self._cache = {}
    
    def update_params(self, **kwargs):
        """只更新参数，不清空缓存"""
        self.params.update(kwargs)

    def _prepare_data(self, dataset: Handler, reweighter=None) -> List[Tuple[lgb.Dataset, str]]:
        ds_l = []
        assert "train" in dataset.segments

        for key in ["train", "valid"]:
            if key in dataset.segments:
                cache_key = (id(dataset), key)  # 用 dataset 对象和分段名作为 key
                if cache_key in self._cache:
                    df = self._cache[cache_key]
                else:
                    df = dataset.prepare(key, col_set=["feature", "label"], data_key=Handler.DK_L)
                    if df.empty:
                        raise ValueError("Empty data from dataset, please check your dataset config.")
                    self._cache[cache_key] = df  # 🔹 存缓存

                x, y = df["feature"], df["label"]

                # LightGBM need 1D array as its label
                if y.values.ndim == 2 and y.values.shape[1] == 1:
                    y = np.squeeze(y.values)
                else:
                    raise ValueError("LightGBM doesn't support multi-label training")

                if reweighter is None:
                    w = None
                else:
                    raise ValueError("Unsupported reweighter type.")

                ds_l.append((lgb.Dataset(x.values, label=y, weight=w), key))
        return ds_l

    def fit(
        self,
        dataset: Handler,
        num_boost_round=None,
        early_stopping_rounds=None,
        verbose_eval=20,
        evals_result=None,
        reweighter=None,
        **kwargs,
    ):
        if evals_result is None:
            evals_result = {}  # in case of unsafety of Python default values
        ds_l = self._prepare_data(dataset, reweighter)
        ds, names = list(zip(*ds_l))
        early_stopping_callback = lgb.early_stopping(
            self.early_stopping_rounds if early_stopping_rounds is None else early_stopping_rounds
        )
        # NOTE: if you encounter error here. Please upgrade your lightgbm
        verbose_eval_callback = lgb.log_evaluation(period=verbose_eval)
        evals_result_callback = lgb.record_evaluation(evals_result)
        self.model = lgb.train(
            self.params,
            ds[0],  # training dataset
            num_boost_round=self.num_boost_round if num_boost_round is None else num_boost_round,
            valid_sets=ds,
            valid_names=names,
            callbacks=[early_stopping_callback, verbose_eval_callback, evals_result_callback],
            **kwargs,
        )
        '''
        for k in names:
            for key, val in evals_result[k].items():
                name = f"{key}.{k}"
                for epoch, m in enumerate(val):
                    R.log_metrics(**{name.replace("@", "_"): m}, step=epoch)
        '''
'''
    def predict(self, dataset: DatasetH, segment: Union[Text, slice] = "test"):
        if self.model is None:
            raise ValueError("model is not fitted yet!")
        x_test = dataset.prepare(segment, col_set="feature", data_key=DataHandlerLP.DK_I)
        return pd.Series(self.model.predict(x_test.values), index=x_test.index)
'''