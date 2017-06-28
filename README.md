# mongo-collection-diff

a utility to compare to mongo collections and generate 2 collections with the different keys in each of them. 

# Usage
The easiest way is to run it from Mongobooster. That is because it needs Lodas and ShellJS, which are provided in MongoBooster.

* set up your DB and collection and key set in the `coll` and `coll2` objects.
* The target collections wil be generated in `coll2.dbname` database
* The target collections will contain the key fields

```javascript
var coll1 = {
    dbname: 'ctml_ak81894_sd-dfc9-2176_2017_06_25_2200',
    coll: 'fxpgEvent',
    label: 'ctml1',
    keys: { usi: 1, tradeType: '$eventType' },
    key_mapping : {tradeType: 'eventType'}
};

var coll2 = {
    dbname: 'QControlData_FX_2606',
    coll: 'tradeEvent',
    label: 'ctml2',
    keys: { usi: 1, tradeType: 1 },
};
```



# Performance
The currnet benchmark is:
* 2 collections on different DBs
* ~800k documents
* most of them have unique keys
* process  takes 10 minutes

