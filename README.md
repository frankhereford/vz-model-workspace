# VZ Model Workspace

## Database

- Start the database

```bash
docker compose up -d db
```

- connect on localhost on 5432
- username: `vz`
- password: `vz`
- database: `visionzero`
- it trusts all connections, so the password is optional-ish

## Adminer (DB UI)

- Fire up the Adminer to inspect DB contents via a web UI

```bash
docker compose up -d adminer
```

- connect with a browser on 8080
- use the DB credentials

## Python 🐍

- If you want a python container with `psycopg2` installed, run

```bash
docker compose run python
```

- that should dump you in a shell where the `python` directory is bind-mounted on `/application`
