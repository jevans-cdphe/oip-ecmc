# Dependencies

## Python

requires Python 3.9

### dependency management:
- Python Poetry

can be installed globally with `python -m pip install poetry` in Windows PowerShell

This is not entirely necessary, but is recommended.

### Python packages:
- PyYAML
- pola.rs
- Spock Config
- requests

can be installed globally with `python -m pip install pyyaml polars spock-config wget` in Windows PowerShell

If Python Poetry was installed, the recommended method for installing dependencies is to run

`python -m poetry install`

from the root repository directory in Windows PowerShell

## Git
Some of the Python package dependencies require Git. This is not available by default on Colorado State Windows Installations. One option to get access to Git on your machine is to request Git and/or GitHub Desktop, though I don't know yet if these installations will update the correct environment variable.

Alternatively, there is a portable version of Git that can be downloaded [here](https://git-scm.com/download/win) and can be used without installation. Simply download the portable version, run the executable file to extract the contents, and choose a destination directory when prompted.

`C:\Users\MyUserName\portable_git\` is a good option.

Once extracted, to tell Python where the Git executable is you will have to add an environment variable. This can easily be done on a per-shell basis without needing to run any scripts at login. To set the required environment variable for use in Windows PowerShell, run this command in PowerShell:

`$Env:GIT_PYTHON_GIT_EXECUTABLE = 'C:\path\to\portable\git\bin\git.exe'`

or using the recommended directory above:

`$Env:GIT_PYTHON_GIT_EXECUTABLE = '%USERPROFILE%\portable_git\bin\git.exe'`

Alternatively, you can set environment variables for git in the built-in Windows Environment Variables application. Under "User variables for MyUserName", add the following new evironment variable:

Variable name: `GIT_PYTHON_GIT_EXECUTABLE`

Variable value: `C:\path\to\portable\git\bin\git.exe`

Optionally, also edit the `Path` variable to add the following lines:

`C:\path\to\portable\git\bin`

`C:\path\to\portable\git\cmd`

# Running the scripts
For the remainder of the instructions, `$python` will refer to `python -m poetry run python` if dependencies were installed with Poetry or `python` if dependencies were installed globally.

Default configurations for all scripts are available in the [[default_config]] directory. For each script, you can pass a configuration file with the `-c` command line option like this:

`$python path\to\script.py -c path\to\config_file.yaml`

Individual configuration options can also be modified as command line options. To see available command line options, use the command line option `--help`.

## `load_from_xlsx.py`
This script is deprecated and is only included for posterity.

## `scrape_from_ecmc.py`
#TODO

## `convert_access_to_parquet.py`
#TODO

## `transform_ecmc.py`
#TODO