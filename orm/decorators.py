from orm.utils import Q
from orm.exceptions import MissingParameter, InvalidParameter


def process_fields_and_q_params(fields, condition):
    if fields and not (isinstance(fields, list) or isinstance(fields, Q) or None):
        raise InvalidParameter
    if isinstance(fields, Q):
        condition, fields = fields, None
    return fields, condition

def validate_condition(condition, kwargs):
    if not (condition or kwargs):
        raise MissingParameter

def validate_data(data):
    if not data:
        raise MissingParameter

def validate_filter_params(func):
    def _validate_filter_params(self, fields=None, condition=None, limit=None, **kwargs):
        fields, condition = process_fields_and_q_params(fields, condition)
        if limit and not isinstance(limit, int):
            raise InvalidParameter
        return func(self, fields, condition, limit, **kwargs)
    return _validate_filter_params

def validate_get_params(func):
    def _validate_get_params(self, fields=None, condition=None, **kwargs):
        fields, condition = process_fields_and_q_params(fields, condition)
        validate_condition(condition, kwargs)
        return func(self, fields, condition, **kwargs)
    return _validate_get_params

def validate_update_params(func):
    def _validate_update_params(self, new_data, condition=None, **kwargs):
        validate_data(new_data)
        validate_condition(condition, kwargs)
        return func(self, new_data, condition, **kwargs)
    return _validate_update_params

def validate_create_data(func):
    def _validate_create_data(self, new_data):
        validate_data(new_data)
        return func(self, new_data)
    return _validate_create_data

def validate_delete_params(func):
    def _validate_delete_params(self, condition=None, **kwargs):
        validate_condition(condition, kwargs)
        return func(self, condition, **kwargs)
    return _validate_delete_params
