
from hdf5helper import Hdf5helper
from pinyin import PinYin
import functools


def singleton(cls):
    ''' Use class as singleton. '''
    cls.__new_original__ = cls.__new__

    @functools.wraps(cls.__new__)
    def singleton_new(cls, *args, **kw):
        it = cls.__dict__.get('__it__')
        if it is not None:
            return it
        cls.__it__ = it = cls.__new_original__(cls, *args, **kw)
        it.__init_original__(*args, **kw)
        return it
    cls.__new__ = singleton_new
    cls.__init_original__ = cls.__init__
    cls.__init__ = object.__init__
    return cls


class Industry:
    def __init__(self):
        pass



@singleton
class Classification:

    def __new__(cls):
        return object.__new__(cls)

    __industry_hdf5 = "industry.h5"

    def __init__(self):
        self.industry = Hdf5helper(self.__industry_hdf5)
        pass


    def sync():
        pass







if __name__ ==  "__main__":
    cf = Classification()
    cf.x = 3
    cf1= Classification()
    import tushare as ts
    ts.get_industry_classified()
    pass
