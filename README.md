# Trustchain ETL tools
Database migration tools for Trustchain records.

What is implemented:

 - SQLite to Neo4j  
    

## Prerequisites

The dependencies for migration tools can be installed via ``pip``:  

``pip install py2neo``

It is assumed that you have access to local sqlite 
trustchain.db and local/remote connection 
to [Neo4j](https://neo4j.com/docs/operations-manual/current/installation/) database. 

## Running migration tool

To run migration tool from local trustchain sqlite database into neo4j graph run: 

```python database_migrator```

Run `database_migrator` with `-h` to get help on parameters. 
 

