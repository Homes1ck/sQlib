import multiprocessing

NUM_USABLE_CPU = max(multiprocessing.cpu_count() - 2, 1)

_default_config = {
    "kernels": NUM_USABLE_CPU,
	"joblib_backend": "multiprocessing",
	"maxtasksperchild": None,

    "trade_unit": 100,
    "limit_threshold": 0.095,
    "deal_price": "收盘",
}

class Config:
    def __init__(self, data: dict):
        # 把字典里的键值对直接更新到实例的属性字典
        self.__dict__.update(data)

C = Config(_default_config)