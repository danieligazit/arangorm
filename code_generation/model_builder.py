import os
import argparse
import json
import networkx
from jinja2 import Environment, FileSystemLoader

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


class ModelBuilder:
    """Converts a JSON Schema to a Plain Old Python Object class"""

    CLASS_TEMPLATE_FNAME = '_class.tmpl'

    J2P_TYPES = {
        'string': str,
        'integer': int,
        'number': float,
        'object': type,
        'array': list,
        'boolean': bool,
        'null': None
    }

    def __init__(self):
        self.jinja = Environment(loader=FileSystemLoader(searchpath=SCRIPT_DIR), trim_blocks=True)

        self.definitions = []

    def load(self, schema):
        self.process(schema)

    def get_model_depencencies(self, model):
        deps = set()
        for prop in model['properties']:
            if prop['_type']['type'] not in self.J2P_TYPES.values():
                deps.add(prop['_type']['type'])
            if prop['_type']['subtype'] not in self.J2P_TYPES.values():
                deps.add(prop['_type']['subtype'])
        return list(deps)

    def process(self, json_schema):
        for _obj_name, _obj in json_schema['definitions'].items():
            model = self.definition_parser(_obj_name, _obj)
            self.definitions.append(model)

        # topological oderd dependencies
        g = networkx.DiGraph()
        models_map = {}
        for model in self.definitions:
            models_map[model['name']] = model
            deps = self.get_model_depencencies(model)
            if not deps:
                g.add_edge(model['name'], '')
            for dep in deps:
                g.add_edge(model['name'], dep)

        self.definitions = []
        for model_name in networkx.topological_sort(g):
            if model_name in models_map:
                self.definitions.append(models_map[model_name])

    def definition_parser(self, _obj_name, _obj):
        model = {}
        model['name'] = _obj_name
        if 'type' in _obj:
            model['type'] = self.type_parser(_obj)

        model['properties'] = []
        if 'properties' in _obj:
            for _prop_name, _prop in _obj['properties'].items():
                _type = self.type_parser(_prop)
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

    def type_parser(self, t):
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
            elif t['type']:
                _type = self.J2P_TYPES[t['type']]
        elif '$ref' in t:
            _type = t['$ref'].split('/')[-1]
        elif 'anyOf' in t or 'allOf' in t or 'oneOf' in t:
            _type = list
        return {'type': _type, 'subtype': _subtype}

    def render(self):
        return self.jinja.get_template(self.CLASS_TEMPLATE_FNAME).render(models=self.definitions)


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
            }
        }

    }

    mb = ModelBuilder()
    mb.load(schema)
    print(mb.render())
