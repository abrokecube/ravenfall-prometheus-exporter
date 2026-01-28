# ravenfall-prometheus-exporter
A Prometheus exporter for Ravenfall.

## Configuration
Create a new file, `servers.json`, and copy the contents of `servers_example.json` into it.
Config is just a JSON array of Ravenfall query servers to fetch from.

## Running
With Python 3.12+ and `uv` installed, run `uv run fastapi run main.py` in a terminal.  
Specify a port using the `--port` option.  
ex. `uv run fastapi run main.py --port=7050`
