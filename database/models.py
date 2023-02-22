import inspect
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String

from src.database.db import Base


class BaseModel(Base):
    __abstract__ = True

    @property
    def current_datetime(self) -> datetime:
        return datetime.utcnow().replace(tzinfo=timezone.utc)

    def properties_to_dict(self) -> dict:
        return {
            name: getattr(self, name)
            for name, _ in inspect.getmembers(type(self), lambda p: isinstance(p, property))
        }

    def fields_to_dict(self) -> dict:
        fields = {}

        for name, value in self.__dict__.items():
            # Ignore the state attribute
            if name == '_sa_instance_state':
                continue

            # If the field is a relation
            if isinstance(value, BaseModel):
                for sub_name, sub_value in value.to_dict().items():
                    fields[f'{name}.{sub_name}'] = sub_value
            else:
                fields[name] = value

        return fields

    def to_dict(self) -> dict:
        return dict(**self.fields_to_dict(), **self.properties_to_dict())


class TestCase(BaseModel):
    __tablename__ = "test_case"

    test_case_id = Column(Integer, primary_key=True)
    test_case_name = Column(String(length=256), nullable=False)
