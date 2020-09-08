import ndjson
from genson import SchemaBuilder


class NdjsonCrawler(object):
    def __init__(self):
        self.schema_builder = SchemaBuilder()
        self.types = {
            'string': self.__primitive_type,
            'bool': self.__primitive_type,
            'array': self.__array_type,
            'object': self.__object_type,
            'anyOf': self.__get_any_of_type,
            'null': self.__primitive_type,
            'integer': self.__primitive_type
        }

    def __get_json_schema(self, obj: dict):
        self.schema_builder.add_object(obj)
        return self.schema_builder.to_schema()

    def __get_type(self, column_prop):
        if 'type' in column_prop:
            if isinstance(column_prop['type'], str):
                type_getter = self.types[column_prop['type']]
                return type_getter(column_prop)
            if isinstance(column_prop['type'], list):
                column_types = [t for t in column_prop['type'] if t != 'null']
                if len(column_types) != 1:
                    return 'string'
                else:
                    type_getter = self.types[column_types[0]]
                    return type_getter(column_prop)
        elif 'anyOf' in column_prop:
            return self.__get_any_of_type(column_prop)

    def __get_any_of_type(self, column_prop):
        any_of = [type_prop for type_prop in column_prop['anyOf']
                  if type_prop['type'] != 'null']
        if len(any_of) != 1:
            return 'complex'
        type_prop = any_of[0]
        type_getter = self.types[type_prop['type']]
        return type_getter(type_prop)

    def __is_nullable(self, column_prop):
        _type = column_prop.get('type') or column_prop.get("anyOf")
        if isinstance(_type, str):
            return _type == 'null'
        if isinstance(_type, list):
            if 'null' in _type:
                return True
            for t in _type:
                if t['type'] == 'null':
                    return True
        return False

    def crawler(self, data: str):
        data = ndjson.loads(data)
        self.json_schema = self.__get_json_schema(data)
        columns = []
        for column_name, column_prop in self.json_schema['items']['properties'].items():
            column = {
                'name': column_name,
                'type': self.__get_type(column_prop),
                'nullable': self.__is_nullable(column_prop),
                'default': None
            }
            columns.append(column)
        return columns

    def __primitive_type(self, column_prop):
        if isinstance(column_prop['type'], str):
            return column_prop['type'] if column_prop['type'] != 'null' else 'string'
        if isinstance(column_prop['type'], list):
            column_types = [t for t in column_prop['type'] if t != 'null']
            return column_types[0]

    def __array_type(self, column_prop):
        items = column_prop['items']
        array_type = self.__get_type(items)
        return(F"ARRAY<{array_type}>")

    def __object_type(self, column_prop):
        properties = column_prop['properties']
        schema = []
        for k, v in properties.items():
            _type = self.__get_type(v)
            schema.append(f"{k}: {_type}")
        return f"OBJECT<{', '.join(schema)}>"
