import enum


# Have to do this because StrEnum, which handles this conversion only got added
# in Python 3.11
class StrEnum(str, enum.Enum):
  def __str__(self) -> str:
    return self.value


class MsAccessDriver(StrEnum):
    '''
    Most modern Windows installations will have/use the x64 driver.
    '''
    x64 = 'x64'
    x32 = 'x32'


class OutputType(StrEnum):
    '''
    Currently only csv is supported
    '''
    csv = 'csv'
    # ipc = 'ipc'
    # parquet = 'parquet'
    # avro = 'avro'
    # excel = 'excel'


class MsAccessTable(StrEnum):
    production = 'Colorado Annual Production'
    completions = 'Colorado Well Completions'


class ODBCKey(StrEnum):
    driver = 'Driver'
    max_buffer_size = 'MAXBUFFERSIZE'
    dsn = 'DSN'
    dbq = 'DBQ'
    description = 'DESCRIPTION'
    exclusive = 'EXCLUSIVE'
    page_timeout = 'PAGETIMEOUT'
    read_only = 'READONLY'
    system_database = 'SYSTEMDB'
    threads = 'THREADS'
    user_commit_sync = 'USERCOMMITSYNC'


class LogLevel(StrEnum):
    '''
    Level of detail of logging. DEBUG is the most verbose, and CRITICAL is the
    least.
    '''
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'