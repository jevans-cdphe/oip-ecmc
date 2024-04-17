# Temporary Documentation Notes

- `BOEd` is calculated on a per-formation basis, and then summed for each well. `Prod_days` is the maximum of `Prod_days` for each formation. Because different formations could potentially have different `Prod_days` at a given well, the information used to calculate `BOEd` is lost in the data transformation, and a recalculation using the transformed data may result in different values than are in the `BOEd` column. Additionally, taking the maximum `Prod_days` may not be correct because there could potentially be activity days for one formation and not another, and vice-versa, therefore under-reporting `Prod_days` in the transformed data. This data loss and data discrepancy is not expected to be important, but is worth documenting.

# Installation

```bash
pip install ecmc-scraper
```

# Usage

Optional:
```bash
ecmc-scraper --install-completion
```

Currently, only production summaries are implemented.

To see the options available:

```bash
ecmc-scraper production-summaries --help
```

To generate an editable config file:

```bash
ecmc-scraper production-summaries --write-default-config-to-file /path/to/file.yaml
```

To run the script using a config file:

```bash
ecmc-scraper production-summaries -c /path/to/file.yaml
```

# Manual Installation

## Python

requires Python 3.9

### dependency and virtual environment management:

Python Poetry can be installed globally with

```bash
python -m pip install poetry
```

in Windows PowerShell.

This is not entirely necessary, but is recommended.

### Python packages:

- arrow-odbc
- PyYAML
- pola.rs
- requests
- typer

can be installed globally with

```bash
python -m pip install pyyaml polars requests typer
```

in Windows PowerShell.

If Python Poetry was installed, the recommended method for installing dependencies is to run

```bash
python -m poetry install
```

from the root repository directory in Windows PowerShell. This will also install this package as an executable!

# Git and Colorado State Computers

Some of the Python package dependencies require Git. This is not available by default on Colorado State Windows Installations. One option to get access to Git on your machine is to request Git and/or GitHub Desktop, though I don't know yet if these installations will update the correct environment variable.

Alternatively, there is a portable version of Git that can be downloaded [here](https://git-scm.com/download/win) and can be used without installation. Simply download the portable version, run the executable file to extract the contents, and choose a destination directory when prompted.

`%USERPROFILE%\portable_git\` is a good option.

Once extracted, to tell Python where the Git executable is you will have to add an environment variable. This can easily be done on a per-shell basis without needing to run any scripts at login. To set the required environment variable for use in Windows PowerShell, run this command in PowerShell:

```powershell
$Env:GIT_PYTHON_GIT_EXECUTABLE = 'C:\path\to\portable\git\bin\git.exe'
```

or using the recommended directory above:

```powershell
$Env:GIT_PYTHON_GIT_EXECUTABLE = '%USERPROFILE%\portable_git\bin\git.exe'
```

Alternatively, you can set environment variables for git in the built-in Windows Environment Variables application. Under "User variables for MyUserName", add the following new evironment variable:

Variable name: `GIT_PYTHON_GIT_EXECUTABLE`

Variable value: `C:\path\to\portable\git\bin\git.exe`

Optionally, also edit the `Path` variable to add the following lines:

```
C:\path\to\portable\git\bin
C:\path\to\portable\git\cmd
```
