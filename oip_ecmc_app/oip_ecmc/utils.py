import datetime
import enum
import hashlib
import json
import logging
import pathlib
from typing import List, Optional


# Have to do this because StrEnum, which handles this conversion only got added
# in Python 3.11
class StrEnum(str, enum.Enum):
  def __str__(self) -> str:
    return self.value
  

def str_to_path(
        path_str: str, logger: Optional[logging.Logger] = None) -> pathlib.Path:
    if path_str.startswith('~'):
        return pathlib.Path.home() / path_str[2:]
    return pathlib.Path(path_str)


def remove_files(
        path: pathlib.Path,
        extensions: List[str],
        logger: Optional[logging.Logger] = None,
) -> None:
    ext_str = ' '.join(extensions)
    for f in path.glob(f'*.[{ext_str}]*'):
        f.unlink()
        if logger is not None:
            logger.info(f'removed {f}')


def move_files(
    from_dir: pathlib.Path,
    to_dir: pathlib.Path,
    extensions: List[str],
    logger: Optional[logging.Logger] = None,
) -> None:
    ext_str = ' '.join(extensions)
    files = list(from_dir.glob(f'*.[{ext_str}]*'))
    if len(files) == 0:
        return
    to_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        f.rename(to_dir / f.name)
        if logger is not None:
            logger.info(f'moved {f} to {to_dir / f.name}')


def hash_file(f: pathlib.Path, logger: Optional[logging.Logger] = None) -> str:
    return hashlib.sha256(f.read_bytes()).hexdigest()


def new_hashes(
    metadata: dict,
    prev_metadata_file: pathlib.Path,
    logger: Optional[logging.Logger] = None
) -> bool:
    if not prev_metadata_file.exists():
        if logger is not None:
            logger.info('no previous metadata exists')
        return True
    with prev_metadata_file.open('r') as f:
        return not set(metadata.keys()) == set(json.load(f).keys())
    

def update_metadata(
    metadata_file: pathlib.Path,
    backup_path: pathlib.Path,
    path_keys: list[str],
    keys_to_delete: Optional[list[str]],
    logger: Optional[logging.Logger] = None,
) -> None:
    if logger is not None:
        logger.info(f'updating for backup: {metadata_file}')
    with metadata_file.open('r') as f:
        metadata = json.load(f)
    for _, hash_dict in metadata.items():
        if keys_to_delete is not None:
            for k in keys_to_delete:
                del hash_dict[k]
        for path in path_keys:
            hash_dict[path] = backup_path / pathlib.Path(hash_dict[path]).name
    with metadata_file.open('w') as f:
        json.dump(to_json(metadata), f)


def backup(
    path: pathlib.Path,
    backup_path: pathlib.Path,
    filetype: str,
    path_keys: list[str] = ['path'],
    keys_to_delete: Optional[list[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    metadata_path = path / 'metadata.json'
    if metadata_path.exists():
        backup_path = backup_path \
            / datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        update_metadata(
            metadata_path, backup_path, path_keys, keys_to_delete)
        move_files(path, backup_path, ['json', filetype])
    else:
        if logger is not None:
            logger.info(f'metadata does not exist. wiping {path}')
        remove_files(path, ['json', filetype])
  

def to_json(
        non_json_dict: dict, logger: Optional[logging.Logger] = None) -> dict:
    return {k: _to_json(v) for k, v in non_json_dict.items()}


def _to_json(non_json):
    if isinstance(non_json, dict):
        return to_json(non_json)
    elif isinstance(non_json, list):
        return _to_json_list(non_json)
    return _to_json_item(non_json)


def _to_json_list(non_json_list: list) -> list:
    return [_to_json(i) for i in non_json_list]


def _to_json_item(non_json_item):
    if isinstance(non_json_item, pathlib.Path):
        return str(non_json_item)
    return non_json_item