from datetime import datetime, time, date, timedelta
from typing import List, Optional, Tuple, Union
import functools
import re

import pandas as pd

def is_single_value(start_time, end_time, freq):
    """Is there only one piece of data for stock market.

    Parameters
    ----------
    start_time : Union[pd.Timestamp, str]
        closed start time for data.
    end_time : Union[pd.Timestamp, str]
        closed end time for data.
    freq :
    region: str
        Region, for example, "cn", "us"
    Returns
    -------
    bool
        True means one piece of data to obtain.
    """

    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)

    if end_time - start_time < freq:
        return True
    if start_time.hour == 11 and start_time.minute == 29 and start_time.second == 0:
        return True
    if start_time.hour == 14 and start_time.minute == 59 and start_time.second == 0:
        return True
    return False


class Freq:
    NORM_FREQ_MONTH = "month"
    NORM_FREQ_WEEK = "week"
    NORM_FREQ_DAY = "day"
    NORM_FREQ_MINUTE = "min"  # using min instead of minute for align with Qlib's data filename
    SUPPORT_CAL_LIST = [NORM_FREQ_MINUTE, NORM_FREQ_DAY]  # FIXME: this list should from data

    def __init__(self, freq: Union[str, "Freq"]) -> None:
        if isinstance(freq, str):
            self.count, self.base = self.parse(freq)
        elif isinstance(freq, Freq):
            self.count, self.base = freq.count, freq.base
        else:
            raise NotImplementedError(f"This type of input is not supported")

    def __eq__(self, freq):
        freq = Freq(freq)
        return freq.count == self.count and freq.base == self.base

    def __str__(self):
        # trying to align to the filename of Qlib: day, 30min, 5min, 1min...
        return f"{self.count if self.count != 1 or self.base != 'day' else ''}{self.base}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)})"

    @staticmethod
    def parse(freq: str) -> Tuple[int, str]:
        """
        Parse freq into a unified format

        Parameters
        ----------
        freq : str
            Raw freq, supported freq should match the re '^([0-9]*)(month|mon|week|w|day|d|minute|min)$'

        Returns
        -------
        freq: Tuple[int, str]
            Unified freq, including freq count and unified freq unit. The freq unit should be '[month|week|day|minute]'.
                Example:

                .. code-block::

                    print(Freq.parse("day"))
                    (1, "day" )
                    print(Freq.parse("2mon"))
                    (2, "month")
                    print(Freq.parse("10w"))
                    (10, "week")

        """
        freq = freq.lower()
        match_obj = re.match("^([0-9]*)(month|mon|week|w|day|d|minute|min)$", freq)
        if match_obj is None:
            raise ValueError(
                "freq format is not supported, the freq should be like (n)month/mon, (n)week/w, (n)day/d, (n)minute/min"
            )
        _count = int(match_obj.group(1)) if match_obj.group(1) else 1
        _freq = match_obj.group(2)
        _freq_format_dict = {
            "month": Freq.NORM_FREQ_MONTH,
            "mon": Freq.NORM_FREQ_MONTH,
            "week": Freq.NORM_FREQ_WEEK,
            "w": Freq.NORM_FREQ_WEEK,
            "day": Freq.NORM_FREQ_DAY,
            "d": Freq.NORM_FREQ_DAY,
            "minute": Freq.NORM_FREQ_MINUTE,
            "min": Freq.NORM_FREQ_MINUTE,
        }
        return _count, _freq_format_dict[_freq]

    @staticmethod
    def get_timedelta(n: int, freq: str) -> pd.Timedelta:
        """
        get pd.Timedeta object

        Parameters
        ----------
        n : int
        freq : str
            Typically, they are the return value of Freq.parse

        Returns
        -------
        pd.Timedelta:
        """
        return pd.Timedelta(f"{n}{freq}")

    @staticmethod
    def get_min_delta(left_frq: str, right_freq: str):
        """Calculate freq delta

        Parameters
        ----------
        left_frq: str
        right_freq: str

        Returns
        -------

        """
        minutes_map = {
            Freq.NORM_FREQ_MINUTE: 1,
            Freq.NORM_FREQ_DAY: 60 * 24,
            Freq.NORM_FREQ_WEEK: 7 * 60 * 24,
            Freq.NORM_FREQ_MONTH: 30 * 7 * 60 * 24,
        }
        left_freq = Freq(left_frq)
        left_minutes = left_freq.count * minutes_map[left_freq.base]
        right_freq = Freq(right_freq)
        right_minutes = right_freq.count * minutes_map[right_freq.base]
        return left_minutes - right_minutes

    @staticmethod
    def get_recent_req(base_freq: Union[str, "Freq"], freq_list: List[Union[str, "Freq"]]) -> Optional["Freq"]:
        """Get the closest freq to base_freq from freq_list

        Parameters
        ----------
        base_freq
        freq_list

        Returns
        -------
        if the recent frequency is found
            Freq
        else:
            None
        """
        base_freq = Freq(base_freq)
        # use the nearest freq greater than 0
        min_freq = None
        for _freq in freq_list:
            _min_delta = Freq.get_min_delta(base_freq, _freq)
            if _min_delta < 0:
                continue
            if min_freq is None:
                min_freq = (_min_delta, str(_freq))
                continue
            min_freq = min_freq if min_freq[0] <= _min_delta else (_min_delta, _freq)
        return min_freq[1] if min_freq else None

