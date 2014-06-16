from .exceptions import CommandError, ClientQuitError
from .objects import RedisObject


def abort(errtype='ERR', message=''):
    raise CommandError(errtype, message)


def close_connection():
    raise ClientQuitError()


def nargs_greater_than(argnum):
    def nargs_func(nargs):
        return True if nargs > argnum else False
    return nargs_func


def nargs_greater_equal(argnum):
    def nargs_func(nargs):
        return True if nargs >= argnum else False
    return nargs_func


def group_iter(iterator, n=2):
    """ Transforms a sequence of values into a sequence of n-tuples.
    e.g. [1, 2, 3, 4, ...] => [(1, 2), (3, 4), ...] (when n == 2)
    If strict, then it will raise ValueError if there is a group of fewer
    than n items at the end of the sequence. """
    # accumulator = []
    # for item in iterator:
    #     accumulator.append(item)
    # if len(accumulator) == n:  # tested as fast as separate counter
    #         yield tuple(accumulator)
    # accumulator = []  # tested faster than accumulator[:] = []
    # and tested as fast as re-using one list object
    # if strict and len(accumulator) != 0:
    #     raise ValueError("Leftover values")
    return zip(*[iterator[i::n] for i in range(n)])


def get_object(db, key, type=RedisObject):
    '''
    Ensure the key is exists.

    :return: the stored value
    :rtype: RedisObject
    '''

    try:
        obj = db.key_space[key]
        if obj.expired():
            del db.key_space[key]
            raise KeyError('%s not exists' % key)
        if not isinstance(obj, type):
            raise ValueError('%s is not a %s' % (obj, type))
        return obj
    except KeyError:
        raise
