# ISBD Database Server

## Building and Running

### Running locally
To run the server locally:
```bash
go run main.go
```
or
```bash
make run
```

The server stores metadata in `.ms_data` directory.

### Building
To build a Linux binary:
```bash
make all
```

### Running in Docker
To build the Docker image:
```bash
make docker
```

To run the container:
```bash
docker run -p 8080:8080 -v /path/to/local/data:/data isbd-dbms
```
- The server will be available at `http://localhost:8080`.
- Mount your data directory to `/data` so that `COPY` commands can access CSV files (e.g. at `/data/tables/mytable/data.csv`).
- To enter the container use `docker exec -it <container_id> /bin/sh`.
- To persist the metastore, mount a volume to `.ms_data` directory inside the container by adding `-v /path/to/local/ms_data:/app/.ms_data` to the `docker run` command.

### API Documentation
Swagger UI is available at `http://localhost:8080/swagger`.

