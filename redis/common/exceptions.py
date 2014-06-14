class CommandError(Exception):

    def __init__(self, *args, **kwargs):
        super(CommandError, self).__init__(*args, **kwargs)


class CommandNotFoundError(Exception):

    def __init__(self, *args, **kwargs):
        super(CommandNotFoundError, self).__init__(*args, **kwargs)
