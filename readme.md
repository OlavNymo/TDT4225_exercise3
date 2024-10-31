# Part 1: Creating and filling in the database

You need `Docker` to run the following scripts. Other requirements are seen in `requirements.txt`. cd to TDT4225_exercise3 and run the following commands to create the database and populate its tables:

```
docker-compose up -d
docker-compose exec app python main.py
```

# Part 2: Querying the database

Stay in TDT4225_exercise3 and use the following command, which also prints the result for each query:

```
docker-compose up -d
docker-compose exec app python part2.py
```
