import os
import bisect

import pandas as pd
import numpy as np
import akshare as ak

from joblib import delayed

from typing import List, Union, Optional

from utils.paral import ParallelExt
from config.config import C

class CalendarProvider():
    """Calendar provider base class

    Provide calendar data.
    """

    def calendar(self, start_time=None, end_time=None, freq="day", future=False):
        """Get calendar of certain market in given time range.

        Parameters
        ----------
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.
        freq : str
            time frequency, available: year/quarter/month/week/day.
        future : bool
            whether including future trading day.

        Returns
        ----------
        list
            calendar list
        """
        _calendar, _calendar_index = self._get_calendar(freq, future)
        if start_time == "None":
            start_time = None
        if end_time == "None":
            end_time = None
        # strip
        if start_time:
            start_time = pd.Timestamp(start_time)
            if start_time > _calendar[-1]:
                return np.array([])
        else:
            start_time = _calendar[0]
        if end_time:
            end_time = pd.Timestamp(end_time)
            if end_time < _calendar[0]:
                return np.array([])
        else:
            end_time = _calendar[-1]
        _, _, si, ei = self.locate_index(start_time, end_time, freq, future)
        return _calendar[si : ei + 1]

    def locate_index(
        self, start_time: Union[pd.Timestamp, str], end_time: Union[pd.Timestamp, str], freq: str, future: bool = False
    ):
        """Locate the start time index and end time index in a calendar under certain frequency.

        Parameters
        ----------
        start_time : pd.Timestamp
            start of the time range.
        end_time : pd.Timestamp
            end of the time range.
        freq : str
            time frequency, available: year/quarter/month/week/day.
        future : bool
            whether including future trading day.

        Returns
        -------
        pd.Timestamp
            the real start time.
        pd.Timestamp
            the real end time.
        int
            the index of start time.
        int
            the index of end time.
        """
        start_time = pd.Timestamp(start_time)
        end_time = pd.Timestamp(end_time)
        calendar, calendar_index = self._get_calendar(freq=freq, future=future)
        if start_time not in calendar_index:
            try:
                start_time = calendar[bisect.bisect_left(calendar, start_time)]
            except IndexError as index_e:
                raise IndexError(
                    "`start_time` uses a future date, if you want to get future trading days, you can use: `future=True`"
                ) from index_e
        start_index = calendar_index[start_time]
        if end_time not in calendar_index:
            end_time = calendar[bisect.bisect_right(calendar, end_time) - 1]
        end_index = calendar_index[end_time]
        return start_time, end_time, start_index, end_index

    def _get_calendar(self, freq, future):
        """Load calendar using memcache.

        Parameters
        ----------
        freq : str
            frequency of read calendar file.
        future : bool
            whether including future trading day.

        Returns
        -------
        list
            list of timestamps.
        dict
            dict composed by timestamp as key and index as value for fast search.
        """
        flag = f"{freq}_future_{future}"

        _calendar = np.array(self.load_calendar(freq, future))
        _calendar_index = {x: i for i, x in enumerate(_calendar)}  # for fast search

        return _calendar, _calendar_index

    def load_calendar(self, freq, future):
        """Load original calendar timestamp from file.

        Parameters
        ----------
        freq : str
            frequency of read calendar file.
        future: bool

        Returns
        ----------
        list
            list of timestamps
        """
        cal = ak.tool_trade_date_hist_sina()
        cal = pd.to_datetime(cal['trade_date'], format='%Y%m%d')

        return cal.tolist()

class DatasetProvider():
    """Dataset provider class

    Provide Dataset data.
    """

    def dataset(self, instruments, fields, start_time=None, end_time=None, freq="day", inst_processors=[]):
        """Get dataset data.

        Parameters
        ----------
        instruments : list or str
            list of instruments or str of instruments.
        fields : list
            list of feature instances or str all.
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.
        freq : str
            time frequency.


        Returns
        ----------
        pd.DataFrame
            a pandas dataframe with <instrument, datetime> index.
        """
        instruments_d = self.get_instruments_d(instruments, freq)

        column_names = self.get_column_names(fields)

        # NOTE: if the frequency is a fixed value.
        # align the data to fixed calendar point
        cal = Cal.calendar(start_time, end_time, freq)
        if len(cal) == 0:
            return pd.DataFrame(
                index=pd.MultiIndex.from_arrays([[], []], names=("股票代码", "日期")), columns=column_names
            )
        start_time = cal[0]
        end_time = cal[-1]

        data = self.dataset_processor(
            instruments_d, column_names, start_time, end_time, freq, inst_processors=inst_processors
        )

        return data

    def factorset(self, instruments, fields, start_time=None, end_time=None, inst_processors=[]):
        """Get factorset data.

        Parameters
        ----------
        instruments : list or str
            list of instruments or str of instruments.
        fields : list
            list of feature instances or str all.
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.

        Returns
        ----------
        pd.DataFrame
            a pandas dataframe with <instrument, datetime> index.
        """
        instruments_d = self.get_instruments_d(instruments, "day")

        column_names = self.get_column_names(fields)

        # NOTE: if the frequency is a fixed value.
        # align the data to fixed calendar point
        cal = Cal.calendar(start_time, end_time, "day")
        if len(cal) == 0:
            return pd.DataFrame(
                index=pd.MultiIndex.from_arrays([[], []], names=("股票代码", "日期")), columns=column_names
            )
        start_time = cal[0]
        end_time = cal[-1]

        data = self.factorset_processor(
            instruments_d, column_names, start_time, end_time, inst_processors=inst_processors
        )

        return data

    @staticmethod
    def get_instruments_d(instruments, freq):
        """
        Parse different types of input instruments to output instruments_d
        Wrong format of input instruments will lead to exception.

        """
        if isinstance(instruments, str):
            if instruments in ["all", "csi100", "csi300", "csi500", "csi800", "csi1000"]:
                # dict of stockpool config
                
                instruments_d = pd.read_csv(f"instruments/{instruments}.txt", sep='\t', header=None)

                instruments_d.columns = ["品种代码", "纳入日期", "剔除日期"]

                instruments_d = instruments_d.set_index(['纳入日期', '剔除日期'])

                instruments_d = instruments_d.loc[instruments_d.index[-1]]["品种代码"].tolist()

                instruments_d = [i.lower() for i in instruments_d]

            else:
                raise ValueError("Unsupported input market for param `instrument`")
            
        elif isinstance(instruments, (list, tuple, pd.Index, np.ndarray)):
            # list or tuple of a group of instruments
            instruments_d = list(instruments)
        else:
            raise ValueError("Unsupported input type for param `instrument`")
        
        return instruments_d

    @staticmethod
    def get_column_names(fields):
        """
        Get column names from input fields

        """

        if isinstance(fields, str):

            return fields

        else:
            if len(fields) == 0:
                raise ValueError("fields cannot be empty")
            
            column_names = [str(f) for f in fields]
            
            return column_names
    
    @staticmethod
    def dataset_processor(instruments_d, column_names, start_time, end_time, freq, inst_processors=[]):
        """
        Load and process the data, return the data set.
        - default using multi-kernel method.

        """

        # One process for one task, so that the memory will be freed quicker.
        workers = max(min(C.kernels, len(instruments_d)), 1)

        # create iterator
        it = zip(instruments_d, [None] * len(instruments_d))

        '''
        inst_l = []
        task_l = []
        for inst, spans in it:
            inst_l.append(inst)
            task_l.append(
                delayed(DatasetProvider.dataset_read)(
                    inst, start_time, end_time, freq, column_names, spans, inst_processors
                )
            )

        data = dict(
            zip(
                inst_l,
                ParallelExt(n_jobs=workers, backend=C.joblib_backend, maxtasksperchild=C.maxtasksperchild)(task_l),
            )
        )
        '''

        inst_l = []
        task_l = []
        for inst, spans in it:
            inst_l.append(inst)
            task_l.append(
                (DatasetProvider.dataset_read)(
                    inst, start_time, end_time, freq, column_names, spans, inst_processors
                )
            )
        data = dict(
            zip(
                inst_l,
                task_l,
            )
        )       

        new_data = dict()
        for inst in sorted(data.keys()):
            if len(data[inst]) > 0:
                # NOTE: Python version >= 3.6; in versions after python3.6, dict will always guarantee the insertion order
                new_data[inst] = data[inst]

        if len(new_data) > 0:
            data = pd.concat(new_data, names=["股票代码"], sort=False)
            #data = DiskDatasetCache.cache_to_origin_data(data, column_names)
        else:
            data = pd.DataFrame(
                index=pd.MultiIndex.from_arrays([[], []], names=("instrument", "datetime")),
                columns=column_names,
                dtype=np.float32,
            )

        return data

    @staticmethod
    def dataset_read(inst, start_time, end_time, freq, column_names, spans=None, inst_processors=[]):
        """
        Calculate the expressions for **one** instrument, return a df result.
        If the expression has been calculated before, load from cache.

        return value: A data frame with index 'datetime' and other data columns.

        """
        # FIXME: Windows OS or MacOS using spawn: https://docs.python.org/3.8/library/multiprocessing.html?highlight=spawn#contexts-and-start-methods
        # NOTE: This place is compatible with windows, windows multi-process is spawn
        #C.register_from_C(g_config)

        '''
        obj = dict()
        for field in column_names:
            #  The client does not have expression provider, the data will be loaded from cache using static method.
            obj[field] = ExpressionD.expression(inst, field, start_time, end_time, freq)

        data = pd.DataFrame(obj)
        '''

        origin_path = f"cn_data/origin/{inst}.parquet"

        if os.path.exists(origin_path):

            temp = pd.read_parquet(origin_path, engine="fastparquet")

            data = temp[(temp["日期"] >= start_time) & (temp["日期"] <= end_time)]

            if isinstance(column_names, str):

                data = data.iloc[:, 2:]

            else:

                data = data[column_names]

            data.index = temp["日期"][data.index.values.astype(int)]

        '''
        
        if not data.empty and not np.issubdtype(data.index.dtype, np.dtype("M")):
            # If the underlaying provides the data not in datetime format, we'll convert it into datetime format
            _calendar = Cal.calendar(freq=freq, start_time=start_time, end_time=end_time)

            data.index = _calendar[range(len(data.index.values.astype(int)))]
        
        data.index.names = ["日期"]

        '''
        
        
        '''
        if not data.empty and spans is not None:
            mask = np.zeros(len(data), dtype=bool)
            for begin, end in spans:
                mask |= (data.index >= begin) & (data.index <= end)
            data = data[mask]

        for _processor in inst_processors:
            if _processor:
                _processor_obj = init_instance_by_config(_processor, accept_types=InstProcessor)
                data = _processor_obj(data, instrument=inst)
        
        '''

        return data

    @staticmethod
    def factorset_processor(instruments_d, column_names, start_time, end_time, inst_processors=[]):
        """
        Load and process the data, return the data set.
        - default using multi-kernel method.

        """

        # One process for one task, so that the memory will be freed quicker.
        workers = max(min(C.kernels, len(instruments_d)), 1)

        # create iterator
        it = zip(instruments_d, [None] * len(instruments_d))

        '''
        inst_l = []
        task_l = []
        for inst, spans in it:
            inst_l.append(inst)
            task_l.append(
                delayed(DatasetProvider.factorset_read)(
                    inst, start_time, end_time, column_names, spans, inst_processors
                )
            )

        data = dict(
            zip(
                inst_l,
                ParallelExt(n_jobs=workers, backend=C.joblib_backend, maxtasksperchild=C.maxtasksperchild)(task_l),
            )
        )
        '''

        inst_l = []
        task_l = []
        for inst, spans in it:
            inst_l.append(inst)
            task_l.append(
                (DatasetProvider.factorset_read)(
                    inst, start_time, end_time, column_names, spans, inst_processors
                )
            )

        data = dict(
            zip(
                inst_l,
                task_l,
            )
        )

        new_data = dict()
        for inst in sorted(data.keys()):
            if len(data[inst]) > 0:
                # NOTE: Python version >= 3.6; in versions after python3.6, dict will always guarantee the insertion order
                new_data[inst] = data[inst]

        if len(new_data) > 0:
            data = pd.concat(new_data, names=["股票代码"], sort=False)
            #data = DiskDatasetCache.cache_to_origin_data(data, column_names)
        else:
            data = pd.DataFrame(
                index=pd.MultiIndex.from_arrays([[], []], names=("instrument", "datetime")),
                columns=column_names,
                dtype=np.float32,
            )

        return data

    @staticmethod
    def factorset_read(inst, start_time, end_time, column_names, spans=None, inst_processors=[]):
        """
        Calculate the expressions for **one** instrument, return a df result.
        If the expression has been calculated before, load from cache.

        return value: A data frame with index 'datetime' and other data columns.

        """
        # FIXME: Windows OS or MacOS using spawn: https://docs.python.org/3.8/library/multiprocessing.html?highlight=spawn#contexts-and-start-methods
        # NOTE: This place is compatible with windows, windows multi-process is spawn
        #C.register_from_C(g_config)

        '''
        obj = dict()
        for field in column_names:
            #  The client does not have expression provider, the data will be loaded from cache using static method.
            obj[field] = ExpressionD.expression(inst, field, start_time, end_time, freq)

        data = pd.DataFrame(obj)
        '''

        factor_path = f"cn_data/factor/{inst}.parquet"

        if os.path.exists(factor_path):

            temp = pd.read_parquet(factor_path, engine="fastparquet")

            data = temp[(temp["日期"] >= start_time) & (temp["日期"] <= end_time)]

            if isinstance(column_names, str):

                data = data.iloc[:, 2:]

            else:

                data = data[column_names]

            data.index = temp["日期"][data.index.values.astype(int)]

        '''
        
        if not data.empty and not np.issubdtype(data.index.dtype, np.dtype("M")):
            # If the underlaying provides the data not in datetime format, we'll convert it into datetime format
            _calendar = Cal.calendar(freq=freq, start_time=start_time, end_time=end_time)

            data.index = _calendar[range(len(data.index.values.astype(int)))]
        
        data.index.names = ["日期"]

        '''
        
        
        '''
        if not data.empty and spans is not None:
            mask = np.zeros(len(data), dtype=bool)
            for begin, end in spans:
                mask |= (data.index >= begin) & (data.index <= end)
            data = data[mask]

        for _processor in inst_processors:
            if _processor:
                _processor_obj = init_instance_by_config(_processor, accept_types=InstProcessor)
                data = _processor_obj(data, instrument=inst)
        
        '''

        return data

class BaseProvider:
    """Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """

    def calendar(self, start_time=None, end_time=None, freq="day", future=False):
        return Cal.calendar(start_time, end_time, freq, future=future)

    '''
    def instruments(self, market="all", filter_pipe=None, start_time=None, end_time=None):
        if start_time is not None or end_time is not None:
            get_module_logger("Provider").warning(
                "The instruments corresponds to a stock pool. "
                "Parameters `start_time` and `end_time` does not take effect now."
            )
        return InstrumentProvider.instruments(market, filter_pipe)

    def list_instruments(self, instruments, start_time=None, end_time=None, freq="day", as_list=False):
        return Inst.list_instruments(instruments, start_time, end_time, freq, as_list)
    '''

    def features(
        self,
        instruments,
        fields,
        start_time=None,
        end_time=None,
        freq="day",
        disk_cache=None,
        inst_processors=[],
    ):
        """
        Parameters
        ----------
        disk_cache : int
            whether to skip(0)/use(1)/replace(2) disk_cache


        This function will try to use cache method which has a keyword `disk_cache`,
        and will use provider method if a type error is raised because the DatasetD instance
        is a provider class.
        """
        #disk_cache = C.default_disk_cache if disk_cache is None else disk_cache
        #fields = list(fields)  # In case of tuple.

        return DatasetD.dataset(instruments, fields, start_time, end_time, freq, inst_processors=inst_processors)

    def factors(
        self,
        instruments,
        fields,
        start_time=None,
        end_time=None,
        disk_cache=None,
        inst_processors=[],
    ):
        """
        Parameters
        ----------
        disk_cache : int
            whether to skip(0)/use(1)/replace(2) disk_cache


        This function will try to use cache method which has a keyword `disk_cache`,
        and will use provider method if a type error is raised because the DatasetD instance
        is a provider class.
        """
        #disk_cache = C.default_disk_cache if disk_cache is None else disk_cache
        #fields = list(fields)  # In case of tuple.

        return DatasetD.factorset(instruments, fields, start_time, end_time, inst_processors=inst_processors)

    def instruments(self, instruments, freq):

        return DatasetD.get_instruments_d(instruments, freq)

Cal = CalendarProvider()
DatasetD = DatasetProvider()
D = BaseProvider()