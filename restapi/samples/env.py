import os
import sys
folder = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
print('env: ', folder)
sys.path.append(folder)