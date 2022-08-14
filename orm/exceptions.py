import sys

class MissingParameter:
    def __init__(self, msg):
        self._msg = msg
        print(self._msg)
        sys.exit()
