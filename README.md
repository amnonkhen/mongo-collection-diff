# mongo-collection-diff

a utility to compare to mongo collections and generate 2 collections with the different keys in each of them. 

# Usage
The easiest way is to run it from Mongobooster.
It needs Lodas and ShellJS, which are provided in MongoBooster.

# Performance
The currnet benchmark is:
* 2 collections on different DBs
* ~800k documents
* most of them have unique keys
* process  takes 10 minutes

