import os
def foo():
    print(os.environ.get("__name__"))
    print(os.environ.get("__file__"))