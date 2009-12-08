
from urllib2 import urlopen
import promise

def urlopen_promise(url, data=None, timeout=None):
    def func():
        return urlopen(url, data, timeout)
    return promise.Promise(func)

