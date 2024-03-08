# VZ Model Workspace

## Frank's proposal instructions and notes

## Usage

### Erasing everything to start truely blank-slate

```bash
docker compose down -v # v for volumes
```
* If you see `! Network vz-model-workspace_default  Resource is still in use`  from docker compose, you likely need to `exit` your python shell and run this command again.
* The first thing the build process does is wipe out the custom schemata and the things we've created in public, so it's not necessary to use the command above to wipe out everything. Until I run down the occasional hang-up from the `pg_imv` extension, you may need to do that though.

### Entering into the python shell

```bash
docker compose run python
```
* This will start the database automatically for you.

### Rebuilding / Building the DB

```bash
./model.py --mode build # full seed file building; ~ 8-10 minutes
./model.py --mode build --quick # only do 100K inserts from each seed file, so this is a ~ 2m build time
```
The program build and test processes are both imdempotent, so you can just run the build process on top of a built database, and it'll clean up everything it needs to do rebuild.
  * See the note above about the hangup in the `create_immv` function. 

### Running the test steps

```bash
./model.py --mode test # run all of them without pausing between them
./model.py --mode test --step # stop and wait for user input between them

### Running the test steps

```bash
./model.py -m test # run all of them without pausing between them
./model.py -m test -s # stop and wait for user input between them

## Environment

### Database

- Start the database

```bash
docker compose up -d db
```

- connect on localhost on 5432
- username: `vz`
- password: `vz`
- database: `visionzero`
- it trusts all connections, so the password is optional-ish

### Adminer (DB UI)

- Fire up the Adminer to inspect DB contents via a web UI

```bash
docker compose up -d adminer
```

- connect with a browser on 8080
- use the DB credentials

### Python 🐍

- If you want a python container with `psycopg2` installed, run

```bash
docker compose run python
```

- that should dump you in a shell where the `python` directory is bind-mounted on `/application`
