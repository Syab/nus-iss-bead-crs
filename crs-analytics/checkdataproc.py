import getpass
import sys
import importlib

print('This job is running as "{}".'.format(getpass.getuser()))
print(sys.executable, sys.version_info)
for package in sys.argv[1:]:
    print(importlib.find_module(package))
