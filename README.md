# Hosts Inventory

This is a gRPC prototype for the Insights Platform.  Currently using a simple
Python dict to store inventory; there is no permanent storage.

## Getting Started

### Setting up an environment

This project uses pipenv to manage the development and deployment environments.
To set the project up for development do the following:

```
pipenv --python 3
pipenv install --dev 
```

Afterwards you can activate the virtual environment by running:

```
pipenv shell
```

The next step is to generate the gRPC stubs from the `.proto` file:

```
sh build.sh
```

This needs to be done after any changes to `proto/hbi.proto`.

## Running

### Running tests

Just run `pytest` or, if you want more fancy output, `pytest -vs`.  Make sure
you run `build.sh` first so the gRPC stubs are generated.

### Running server

```
python serve.py
```

This uses `watchdog` to cycle the server after any code modifications.

### Running client

```
python -m hbi.client
```
