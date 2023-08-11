import os
import tempfile

import pytest


@pytest.fixture
def dummy_file():
    """Creates a temporary file with a specific size in MB and returns its path.
    The file is automatically removed after the test where it's used.
    """

    size_mb = 20
    size_bytes = size_mb * 1024 * 1024

    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"\0" * size_bytes)

    try:
        yield f.name
    except Exception:
        pass
    finally:
        try:
            os.remove(f.name)
        except FileNotFoundError:
            pass
