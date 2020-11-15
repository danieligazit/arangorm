import os
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from utils import convert_camel_to_snake

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


class ModelBuilder:
    CLASS_TEMPLATE_FNAME = '_class.tmpl'
    COLLECTION_DEFINITION_TEMPLATE_FNAME = '_collection_definition.tmpl'

    J2P_TYPES = {
        'string': str,
        'integer': int,
        'number': float,
        'object': type,
        'array': list,
        'boolean': bool,
        'null': None
    }

    def __init__(self, schema: dict):
        self.jinja = Environment(loader=FileSystemLoader(searchpath=SCRIPT_DIR), trim_blocks=True)
        self.schema = schema
        self.definitions = []
        self.edges = []

    def get_model_dependencies(self, model):
        deps = set()
        for prop in model['properties']:
            if prop['_type']['type'] not in self.J2P_TYPES.values():
                deps.add(prop['_type']['type'])
            if prop['_type']['subtype'] not in self.J2P_TYPES.values():
                deps.add(prop['_type']['subtype'])
        return list(deps)

    def process(self):
        for _obj_name, _obj in self.schema['definitions'].items():
            model = self.definition_parser(_obj_name, _obj)
            self.definitions.append(model)

    def definition_parser(self, _obj_name, _obj):

        model = {
            'name': _obj_name,
            'snake_case_name': convert_camel_to_snake(_obj_name),
            'properties': [],
            'edges': []
        }

        if 'properties' not in _obj:
            return model

        for _prop_name, _prop in _obj['properties'].items():
            is_relation, _type = self.parse_type(_prop)

            if is_relation:
                edge = {'from': _obj_name, 'name': _prop_name, 'to': _type['to']}
                self.edges.append(edge)
                model['edges'].append(edge)
                continue

            _default = None
            if 'default' in _prop:
                _default = _type['type'](_prop['default'])
                if _type['type'] == str:
                    _default = "'{}'".format(_default)

            _enum = None
            if 'enum' in _prop:
                _enum = _prop['enum']

            _format = None
            if 'format' in _prop:
                _format = _prop['format']
            if _type['type'] == list and 'items' in _prop and isinstance(_prop['items'], list):
                _format = _prop['items'][0]['format']

            prop = {
                '_name': _prop_name,
                '_type': _type,
                '_default': _default,
                '_enum': _enum,
                '_format': _format
            }
            model['properties'].append(prop)

        return model

    def parse_type(self, t):
        _type = None
        _subtype = None
        if 'type' in t:
            if t['type'] == 'array' and 'items' in t:
                _type = self.J2P_TYPES[t['type']]
                if isinstance(t['items'], list):
                    if 'type' in t['items'][0]:
                        _subtype = self.J2P_TYPES[t['items'][0]['type']]
                    elif '$ref' in t['items'][0] or 'oneOf' in t['items'][0] and len(t['items'][0]['oneOf']) == 1:
                        if '$ref' in t['items'][0]:
                            ref = t['items'][0]['$ref']
                        else:
                            ref = t['items'][0]['oneOf'][0]['$ref']
                        _subtype = ref.split('/')[-1]
                elif isinstance(t['items'], dict):
                    if 'type' in t['items']:
                        _subtype = self.J2P_TYPES[t['items']['type']]
                    elif '$ref' in t['items'] or 'oneOf' in t['items'] and len(t['items']['oneOf']) == 1:
                        if '$ref' in t['items']:
                            ref = t['items']['$ref']
                        else:
                            ref = t['items']['oneOf'][0]['$ref']
                        _subtype = ref.split('/')[-1]
            elif isinstance(t['type'], list):
                _type = self.J2P_TYPES[t['type'][0]]
            elif t['type'] in self.J2P_TYPES:
                _type = self.J2P_TYPES[t['type']]

        elif '$ref' in t:
            _type = t['$ref'].split('/')[-1]
        elif 'anyOf' in t or 'allOf' in t or 'oneOf' in t:
            _type = list

        elif 'connects_to' in t:
            return True, {'to': t['connects_to']}

        return False, {'type': _type, 'subtype': _subtype}

    def render(self):
        self.process()
        return {
            f'{convert_camel_to_snake(model["name"])}.py': self.jinja.get_template(self.CLASS_TEMPLATE_FNAME).render(
                model=model
            )
            for model in self.definitions
        }

    def render_into(self, destination_dir: str):
        self.process()
        destination = Path(destination_dir)

        for model in self.definitions:
            destination_file = destination / Path(f'{convert_camel_to_snake(model["name"])}.py')
            self.jinja.get_template(self.CLASS_TEMPLATE_FNAME).stream(model=model).dump(str(destination_file))

        self.jinja.get_template(self.COLLECTION_DEFINITION_TEMPLATE_FNAME).stream(
            relations=self.edges,
            models=self.definitions
        ).dump(str(destination / Path('collection_definition.py')))


if __name__ == '__main__':
    schema = {
        'definitions': {
            'Country': {
                'name': 'Country',
                'properties': {
                    'name': {'type': 'string'},
                    'abbreviation': {'type': 'string'},
                },
                'additionalProperties': False,
            },

            'Company': {
                'name': 'Country',
                'properties': {
                    'name': {'type': 'string'},
                    'employee_number': {'type': 'integer'},
                    'located_at': {'connects_to': "Country"}
                },
                'additionalProperties': False,
            }
        }

    }

    mb = ModelBuilder(schema)
    mb.render_into('model')
    # for file_name, model in mb.render().items():
    #     print(file_name)
    #     print(model)
