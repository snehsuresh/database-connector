import sys
import os

# Adding src directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
print("sys.path:", sys.path)