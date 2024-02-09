import enum
import pathlib


# Have to do this because StrEnum, which handles this conversion only got added in Python 3.11
class StrEnum(str, enum.Enum):
  def __str__(self) -> str:
    return self.value


class LogLevel(StrEnum):
    '''
    Level of detail of logging. DEBUG is the most verbose, and CRITICAL is the least.
    '''
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


def get_dir_path(base_path: str, directory: str) -> str:
    win_base_path = base_path.replace('~', str(pathlib.Path.home())).replace('/', '\\')
    full_path = pathlib.Path(win_base_path + '\\' + directory)
    full_path.mkdir(parents=True, exist_ok=True)
    return str(full_path) + '\\'