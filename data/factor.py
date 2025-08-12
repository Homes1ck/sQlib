import os
import numpy as np
import pandas as pd
import fastparquet
from datetime import datetime, timedelta

class Alpha158:

	@staticmethod
	def Alpha158Factor(df_origin):
		
		df_factor = pd.DataFrame()

		df_factor["日期"] = df_origin["日期"]
		df_factor["股票代码"] = df_origin["股票代码"].astype(str).str.zfill(6)

		df_factor["KMID"] = (df_origin["收盘"] - df_origin["开盘"]) / df_origin["开盘"]
		df_factor["KLEN"] = (df_origin["最高"] - df_origin["最低"]) / df_origin["开盘"]
		df_factor["KMID2"] = (df_origin["收盘"] - df_origin["开盘"]) / (df_origin["最高"] - df_origin["最低"] + 1e-12)

		df_factor["KUP"] = (df_origin["最高"] - np.maximum(df_origin["开盘"], df_origin["收盘"])) / df_origin["开盘"]
		df_factor["KUP2"] = (df_origin["最高"] - np.maximum(df_origin["开盘"], df_origin["收盘"])) / (df_origin["最高"] - df_origin["最低"] + 1e-12)
		
		df_factor["KLOW"] = (np.minimum(df_origin["开盘"], df_origin["收盘"]) - df_origin["最低"]) / df_origin["开盘"]
		df_factor["KLOW2"] = (np.minimum(df_origin["开盘"], df_origin["收盘"]) - df_origin["最低"]) / (df_origin["最高"] - df_origin["最低"] + 1e-12)
		
		df_factor["KSFT"] = (2 * df_origin["收盘"] - df_origin["最高"] - df_origin["最低"]) / df_origin["开盘"]
		df_factor["KSFT2"] = (2 * df_origin["收盘"] - df_origin["最高"] - df_origin["最低"]) / (df_origin["最高"] - df_origin["最低"] + 1e-12)

		df_factor["OPEN0"] = df_origin["开盘"] / df_origin["收盘"]
		df_factor["HIGH0"] = df_origin["最高"] / df_origin["收盘"]
		df_factor["LOW0"] = df_origin["最低"] / df_origin["收盘"]

		# A股成交量为手数，每手是100股
		df_factor["VWAP0"] = (df_origin["成交额"] / df_origin["成交量"] / 100) / df_origin["收盘"]

		windows = [5, 10, 20, 30, 60]

		for d in windows:
			# Rate of change, the price change in the past d days, divided by latest close price to remove unit
			df_factor[f'ROC{d}'] = df_origin['收盘'].shift(d) / df_origin['收盘']

		for d in windows:
			# Simple Moving Average, the simple moving average in the past d days, divided by latest close price to remove unit
			df_factor[f'MA{d}'] = df_origin['收盘'].rolling(d).mean() / df_origin['收盘']

		for d in windows:
			# The standard diviation of close price for the past d days, divided by latest close price to remove unit
			df_factor[f'STD{d}'] = df_origin['收盘'].rolling(d).std() / df_origin['收盘']

		#temp3 = [df_origin['收盘'].rolling(d).apply(slope_r2_res, raw=True) for d in windows]
		
		for d in windows:
			# The rate of close price change in the past d days, divided by latest close price to remove unit
			# For example, price increase 10 dollar per day in the past d days, then Slope will be 10.
			
			#df_factor[f'BETA{d}'] = temp3[d][0] / df_origin['收盘']
			df_factor[f'BETA{d}'] = df_origin['收盘'].rolling(d).apply(Alpha158.slope, raw=True) / df_origin['收盘']

		for d in windows:
			# The R-sqaure value of linear regression for the past d days, represent the trend linear
			
			#df_factor[f'RSQR{d}'] = temp3[d][1]
			df_factor[f'RSQR{d}'] = df_origin['收盘'].rolling(d).apply(Alpha158.rsquare, raw=True)
		
		for d in windows:
			# The redisdual for linear regression for the past d days, represent the trend linearity for past d days.

			#df_factor[f'RESI{d}'] = temp3[d][2] / df_origin['收盘']
			df_factor[f'RESI{d}'] = df_origin['收盘'].rolling(d).apply(Alpha158.residual_std, raw=True) / df_origin['收盘']
		
		for d in windows:
			# The max price for past d days, divided by latest close price to remove unit

			df_factor[f'MAX{d}'] = df_origin['最高'].rolling(d).max() / df_origin['收盘']            

		for d in windows:
			# The low price for past d days, divided by latest close price to remove unit
			
			df_factor[f'MIN{d}'] = df_origin['最低'].rolling(d).min() / df_origin['收盘']

		for d in windows:
			# The 80% quantile of past d day's close price, divided by latest close price to remove unit
			# Used with MIN and MAX

			df_factor[f'QTLU{d}'] = df_origin['收盘'].rolling(d).quantile(0.8) / df_origin['收盘']            

		for d in windows:
			# The 20% quantile of past d day's close price, divided by latest close price to remove unit
			
			df_factor[f'QTLD{d}'] = df_origin['收盘'].rolling(d).quantile(0.2) / df_origin['收盘']

		for d in windows:
			# Get the percentile of current close price in past d day's close price.
			# Represent the current price level comparing to past N days, add additional information to moving average.
			
			df_factor[f'RANK{d}'] = df_origin['收盘'].rolling(d).rank() / d

		for d in windows:
			# Represent the price position between upper and lower resistent price for past d days.

			df_factor[f'RSV{d}'] = (df_origin['收盘'] - df_origin['最低'].rolling(d).min()) / (df_origin['最高'].rolling(d).max() - df_origin['最低'].rolling(d).min() + 1e-12)

		for d in windows:
			# The number of days between current date and previous highest price date.
			df_factor[f'IMAX{d}'] = 1.0 - df_origin['最高'].rolling(d).apply(Alpha158.idxmax) / d

		for d in windows:
			# The number of days between current date and previous lowest price date.
			df_factor[f'IMIN{d}'] = 1.0 - df_origin['最低'].rolling(d).apply(Alpha158.idxmin) / d

		for d in windows:
			# The time period between previous lowest-price date occur after highest price date.
			# Large value suggest downward momemtum.

			df_factor[f'IMXD{d}'] = - (df_origin['最高'].rolling(d).apply(Alpha158.idxmax) - df_origin['最低'].rolling(d).apply(Alpha158.idxmin)) / d

		for d in windows:
			# The correlation between absolute close price and log scaled trading volume
			
			df_factor[f'CORR{d}'] = df_origin['收盘'].rolling(d).corr(np.log(df_origin['成交量'] + 1))

		for d in windows:
			# The correlation between price change ratio and volume change ratio
			
			df_factor[f'CORD{d}'] = (df_origin['收盘'] / df_origin['收盘'].shift(1)).rolling(d).corr(np.log(df_origin['成交量'] / df_origin['成交量'].shift(1) + 1))

		CNTP_COLS = {}
		CNTN_COLS = {}
		CNTD_COLS = {}

		for d in windows:
			# The percentage of days in past d days that price go up.

			# df_factor[f'CNTP{d}'] = (df_origin['收盘'] > df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()              
			CNTP_COLS[f'CNTP{d}'] = (df_origin['收盘'] > df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()              

		df_factor = pd.concat([df_factor, pd.DataFrame(CNTP_COLS)], axis=1)

		for d in windows:
			# The percentage of days in past d days that price go down.

			# df_factor[f'CNTN{d}'] = (df_origin['收盘'] < df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()
			CNTN_COLS[f'CNTN{d}'] = (df_origin['收盘'] < df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()            

		df_factor = pd.concat([df_factor, pd.DataFrame(CNTN_COLS)], axis=1)

		for d in windows:
			# The diff between past up day and past down day

			# df_factor[f'CNTD{d}'] = (df_origin['收盘'] > df_origin['收盘'].shift(1)).astype(int).rolling(d).mean() - (df_origin['收盘'] < df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()              
			CNTD_COLS[f'CNTD{d}'] = (df_origin['收盘'] > df_origin['收盘'].shift(1)).astype(int).rolling(d).mean() - (df_origin['收盘'] < df_origin['收盘'].shift(1)).astype(int).rolling(d).mean()              

		df_factor = pd.concat([df_factor, pd.DataFrame(CNTD_COLS)], axis=1)

		price_diff = df_origin['收盘'].diff()
		gain = price_diff.clip(lower=0)
		loss = -price_diff.clip(upper=0)
		abs_change = price_diff.abs()

		SUMP_COLS = {}
		SUMN_COLS = {}
		SUMD_COLS = {}

		for d in windows:
			# The total gain / the absolute total price changed
			# Similar to RSI indicator. https://www.investopedia.com/terms/r/rsi.asp

			# df_factor[f'SUMP{d}'] = gain.rolling(d).sum() / (abs_change.rolling(d).sum() + 1e-12)
			SUMP_COLS[f'SUMP{d}'] = gain.rolling(d).sum() / (abs_change.rolling(d).sum() + 1e-12)
		
		df_factor = pd.concat([df_factor, pd.DataFrame(SUMP_COLS)], axis=1)

		for d in windows:
			# The total lose / the absolute total price changed
			# Similar to RSI indicator. https://www.investopedia.com/terms/r/rsi.asp

			# df_factor[f'SUMN{d}'] = loss.rolling(d).sum() / (abs_change.rolling(d).sum() + 1e-12)
			SUMN_COLS[f'SUMN{d}'] = loss.rolling(d).sum() / (abs_change.rolling(d).sum() + 1e-12)
		
		df_factor = pd.concat([df_factor, pd.DataFrame(SUMN_COLS)], axis=1)

		for d in windows:
			# The diff ratio between total gain and total lose

			# df_factor[f'SUMD{d}'] = (gain.rolling(d).sum() - loss.rolling(d).sum()) / (abs_change.rolling(d).sum() + 1e-12)
			SUMD_COLS[f'SUMD{d}'] = (gain.rolling(d).sum() - loss.rolling(d).sum()) / (abs_change.rolling(d).sum() + 1e-12)

		df_factor = pd.concat([df_factor, pd.DataFrame(SUMD_COLS)], axis=1)

		for d in windows:
			# Simple Volume Moving average: https://www.barchart.com/education/technical-indicators/volume_moving_average

			df_factor[f'VMA{d}'] = df_origin['成交量'].rolling(d).mean() / (df_origin['成交量'] + 1e-12)

		for d in windows:
			# The standard deviation for volume in past d days.

			df_factor[f'VSTD{d}'] = df_origin['成交量'].rolling(d).std() / (df_origin['成交量'] + 1e-12)

		price_return = df_origin['收盘'].pct_change().abs()
		volume_weighted_vol = price_return * df_origin['成交量']

		for d in windows:
			# The volume weighted price change volatility

			rolling_std = volume_weighted_vol.rolling(d).std()
			rolling_mean = volume_weighted_vol.rolling(d).mean()
			df_factor[f'WVMA{d}'] = rolling_std / (rolling_mean + 1e-12)

		volume_diff = df_origin['成交量'].diff()
		gain_volume = volume_diff.clip(lower=0)
		loss_volume = -volume_diff.clip(upper=0)
		abs_change_volume = volume_diff.abs()            

		for d in windows:
			# The total volume increase / the absolute total volume changed

			df_factor[f'VUMP{d}'] = gain_volume.rolling(d).sum() / (abs_change_volume.rolling(d).sum() + 1e-12)

		for d in windows:
			# The total volume decrease / the absolute total volume changed

			df_factor[f'VUMN{d}'] = loss_volume.rolling(d).sum() / (abs_change_volume.rolling(d).sum() + 1e-12)

		for d in windows:
			# The diff ratio between total volume increase and total volume decrease

			df_factor[f'VUMD{d}'] = (gain_volume.rolling(d).sum() - loss_volume.rolling(d).sum()) / (abs_change_volume.rolling(d).sum() + 1e-12)

		return df_factor

	@staticmethod
	def slope(y):
		if np.any(np.isnan(y)):
			return np.nan
		x = np.arange(len(y))
		slope = np.polyfit(x, y, 1)[0]
		return slope

	@staticmethod
	def rsquare(y):
		if np.any(np.isnan(y)):
			return np.nan
		x = np.arange(len(y))
		slope, intercept = np.polyfit(x, y, 1)
		y_pred = slope * x + intercept
		ss_res = np.sum((y - y_pred)**2)
		ss_tot = np.sum((y - np.mean(y))**2)
		r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0
		return r2

	@staticmethod
	def residual_std(y):
		if np.any(np.isnan(y)):
			return np.nan
		x = np.arange(len(y))
		slope, intercept = np.polyfit(x, y, 1)
		y_pred = slope * x + intercept
		residuals = y - y_pred
		return np.sqrt(np.mean(residuals**2))

	@staticmethod
	def idxmax(x):
		return len(x) - 1 - np.argmax(x)

	@staticmethod
	def idxmin(x):
		return len(x) - 1 - np.argmin(x)
