# -*- coding: utf-8 -*-
import core.context
import numpy as np


class Filter:
    def __init__(self):
        self.symbols = core.context.get_all_symbols()

    def remove(self, symbols):
        self.symbols = np.setxor1d(self.symbols, symbols)
        return self

    def result(self):
        return self.symbols

    def filter(self, func, symbols):
        return self


if __name__ == "__main__":
    f = Filter()
    # print(context.lefted_symbols())
    c = f.remove(context.lefted_symbols())
    # f.dump()
