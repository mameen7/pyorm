from orm.manager import BaseManager


class BaseModel(type):
    model_manager = BaseManager

    def _get_manager(cls):
        return cls.model_manager(model_class=cls)

    @property
    def objects(cls):
        return cls._get_manager()


class Model(metaclass=BaseModel):
    table_name = ""

    def __init__(self, **row_data):
        for field_name, value in row_data.items():
            setattr(self, field_name, value)

    def __repr__(self):
        attrs_format = ", ".join([f'{field}={value}' for field, value in self.__dict__.items()])
        return f"<{self.__class__.__name__}: ({attrs_format})>"

    @classmethod
    def get_fields(cls):
        return tuple(cls._get_manager()._get_fields())