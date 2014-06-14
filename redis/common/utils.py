from .exceptions import CommandError, ClientQuitError


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
