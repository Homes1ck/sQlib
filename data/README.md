# data.py 
对本地所有数据封装了一个模块D，对应与from qlib.data import D
拿来查询不同股票不同日期的数据

# update.py

## updateD(instruments)
akshare下载更新本地股市数据

## updateF(instruments)
利用本地股市数据计算不同的股票不同因子的数据， 因子这里对应的是Qlib Alpha158因子

instruments = [id1, id2, id3, ...] : list
instruments = "csi100", "csi300", "csi1000" : str

# factor.py
Alpha158类中定义Alpha158的计算方式，通过股市的原始数据即可计算