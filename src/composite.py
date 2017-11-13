


class Composite:

    professions_hdf5 = 'professions.h5'

    def __init__(self):
        self.professions_hdf5 = Hdf5helper(professions_hdf5)
        pass

    def sync(self):
        if not self.professions_hdf5.exist():
            pass

        ## get all shares 
        ## TODO we need a database to save hdf5 to save all index
        pass


    def create_share(code):
        share = Share(code)
        return share

    def _get_stock_list():
        pass



if __name__ == "__main__":
    composite  = Composite()
    #composite.create_share('000002')
    pass

    