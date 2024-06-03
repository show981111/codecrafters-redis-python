from rdbtools import RdbParser, RdbCallback
from rdbtools.encodehelpers import bytes_to_unicode


class RdbParserCallback(RdbCallback):
    """Simple example to show how callback works.
    See RdbCallback for all available callback methods.
    See JsonCallback for a concrete example
    """

    def __init__(self):
        super(RdbParserCallback, self).__init__(string_escape=None)

    def encode_key(self, key):
        return bytes_to_unicode(key, self._escape, skip_printable=True)

    def encode_value(self, val):
        return bytes_to_unicode(val, self._escape)

    def set(self, key, value, expiry, info):
        print("%s = %s" % (self.encode_key(key), self.encode_value(value)))

    def hset(self, key, field, value):
        print(
            "%s.%s = %s"
            % (self.encode_key(key), self.encode_key(field), self.encode_value(value))
        )

    def sadd(self, key, member):
        print("%s has {%s}" % (self.encode_key(key), self.encode_value(member)))

    def rpush(self, key, value):
        print("%s has [%s]" % (self.encode_key(key), self.encode_value(value)))

    def zadd(self, key, score, member):
        print("%s has {%s : %s}" % (str(key), str(member), str(score)))
