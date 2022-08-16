import sys

class MissingParameter(Exception):
    def __init__(self, message='Missing required parameter(s)!'):
        self.message = message
        super().__init__(self.message)

class ObjectDoesNotExist(Exception):
    def __init__(self, message='Object does not exist!'):
        self.message = message
        super().__init__(self.message)

class InvalidParameter(Exception):
    def __init__(self, message='Encountered an unknown query parameter'):
        self.message = message
        super().__init__(self.message)
