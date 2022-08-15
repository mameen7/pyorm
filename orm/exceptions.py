import sys

class MissingParameter(Exception):
    def __init__(self, msg):
        self._msg = msg
        print(self._msg)
        sys.exit()

class ObjectDoesNotExiet(Exception):
    def __init__(self, msg):
        self._msg = msg
        print(self._msg)
        sys.exit()
