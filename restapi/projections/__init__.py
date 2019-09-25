import os
import json
from munch import munchify

json_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'bin')

__all__ = ('projections', 'names', 'wkt', 'linearUnits')

projections = munchify(json.loads(open(os.path.join(json_dir, 'projections.json')).read()))
names = munchify(json.loads(open(os.path.join(json_dir, 'projection_names.json')).read()))
wkt = munchify(json.loads(open(os.path.join(json_dir, 'projection_strings.json')).read()))
gtfs = munchify(json.loads(open(os.path.join(json_dir, 'gtf.json')).read()))
linearUnits = munchify(json.loads(open(os.path.join(json_dir, 'linearUnits.json')).read()))
