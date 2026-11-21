"""The daavar driver (DRIVER-DESIGN.md rev 3.6): new | build | check | vocab | render | replay.

Python >= 3.11, standard library only. Only `tf` and `jmc` know a tool's command line.
"""

__version__ = "0.1.0"


class DaavarError(Exception):
    """A refusal the driver reports with exit 2: a message for stderr, never a traceback."""
