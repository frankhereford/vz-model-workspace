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

## Test Steps

1. Spin up the database and run the Python container
```
docker compose up -d db
docker compose run python
```
2. Now, you can connect to the DB with you DB GUI as described above.
3. Run all up migrations on the DB. You can also migrate down if needed. Explore the `cris` schema.
```bash
python migrate.py -d up
```
```bash
python migrate.py -d down
```
3. Run the seed script with a limit. Finding a fast way to seed crashes has been a challenge for me.
```bash
python seed.py -l 50000
```
4. You can check out the changes applied to the DB by exploring `/python/migrations/`
5. Run through the test cases using copy/paste from the files in `/python/test_cases/`
