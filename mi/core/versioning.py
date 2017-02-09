from functools import wraps

def version(version_number):
    """Wrapper to add version name to parser.
    Placing the version number on the parse function seems to make
    more sense than adding __version__ to the code and relying on that.
   """
    def put_version(to_wrap):
        @wraps(to_wrap)
        def inner(*args, **kwargs):
            return to_wrap(*args, **kwargs)
        inner.version = version_number
        return inner
    return put_version

