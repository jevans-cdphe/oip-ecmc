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
- wget

can be installed globally with `python -m pip install pyyaml polars spock-config wget` in Windows PowerShell

If Python Poetry was installed, the recommended method for installing dependencies is to run

``

from the root repository directory in Windows PowerShell

## Git
Some of the Python package dependencies require Git. This is not available by default on Colorado State Windows Installations.

There is a portable version of Git that can be downloaded [here](https://git-scm.com/download/win) and can be used without installation. Simply download the portable version, run the executable file to extract the contents, and choose a destination directory when prompted.

`C:\Users\MyUserName\portable_git\` is a good option.

Once extracted, to tell Python where the Git executable is you will have to add an environment variable. This can easily be done on a per-shell basis without needing to run any scripts at login. To set the required environment variable for use in Windows PowerShell, run this command in PowerShell:

`$Env:GIT_PYTHON_GIT_EXECUTABLE = 'C:\PortableGitDestinationDirectory\bin\git.exe'`

or using the recommended directory above:

`$Env:GIT_PYTHON_GIT_EXECUTABLE = 'C:\Users\MyUserName\portable_git\bin\git.exe'`

# 