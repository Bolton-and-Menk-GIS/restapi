import os
import sys
parentDir = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
print(parentDir)
if parentDir not in sys.path:
  sys.path.append(parentDir)

test_data_folder = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'testData')
if not os.path.exists(test_data_folder):
  os.makedirs(test_data_folder)