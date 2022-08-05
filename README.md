# BGFA_rclone
Manage files for BGFA.

**NOTE: Minimum version of python required: Python 3.10+**

## Usage
First clone the repo and checkout `test-move` branch:
```shell
$ git clone https://github.com/sayandipdutta/gdrive-move.git
$ cd gdrive-move
$ git checkout test-move
```

Then you need to generate credentials.json and token.json files from Google Developer Console. For more instructions see [AutoRClone Guide](https://github.com/sayandipdutta/AutoRclone/blob/66a9d88c0a34bbbaf3f2a6f057e0b3dbaa53564b/Readme.md). Once you have downloaded the `credentials.json` and `token.json` files, you need to place the files in `internal` directory.

Replace fields in [shared.env](shared.env) and [internal](./internal/appconfig.ini) with appropriate values. Then run the following commands to install the dependencies.

```shell
$ pip install -r requirements.txt
```

In order to move files from one folder to another:
edit `source` and `destination` keys in `DEAFULT` section in the [appconfig.ini](internal/appconfig.ini) with appropriate folder ids from GoogleDrive. Then go to the project root, and run:
```shell
$ python3.10 -m internal.main
```

