import logging
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

from elx.state import StateManager
from elx.tap import Tap
from elx.target import Target
from elx.runner import Runner

logger = logging.getLogger("pipx")
logger.setLevel(logging.CRITICAL)
