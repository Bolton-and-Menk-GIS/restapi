import os
import sys
folder = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# disable certificate verification and arcpy (use open source)
env_flags = [
    'RESTAPI_VERIFY_CERT',
    'RESTAPI_USE_ARCPY'
]

for flag in env_flags:
    os.environ[flag] = 'FALSE'

sys.path.append(folder)
# now we can import restapi with these presets