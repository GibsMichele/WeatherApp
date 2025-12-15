import os
os.environ["DISABLE_MQTT"] = "1"  

import station1

def test_import_ok():
    assert hasattr(station1, "main")
