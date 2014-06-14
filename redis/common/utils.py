from .exceptions import CommandError


def abort(errtype='ERR', message=''):
    raise CommandError(errtype, message)
