# -*- coding: utf-8 -*-
import json
from jsonschema import validate, ValidationError


with open("./avatar.schema", "r") as fin:
    schema = json.load(fin)

with open("./avatar.sample.json", "r") as fin:
    items = json.load(fin)

for i, item in enumerate(items):
    try:
        validate(item, schema)
        print "{} passed validation".format(i)
    except ValidationError:
        print "{} FAILED validation".format(i)




