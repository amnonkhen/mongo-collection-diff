// diff 2 collections


function logger(target) {
  return function() {
    console.log('starting '+ target.name);
    target.apply(this);
    console.log('ending '+ target.name);
  };
};


class MongoCollection {
    dbname: string;
    coll: string;
    label: string;
    key_mapping:Object;
    
    constructor(dbname: string,
        coll: string,
        label: string,
        key_mapping: Object) {
        this.dbname = dbname;
        this.coll = coll;
        this.label = label;
        this.key_mapping = key_mapping;
    }
}


var build_unique_records_collection = function (coll: MongoCollection, keys:Object):void {
    var _db = db.getSiblingDB(coll.dbname);
    var unique_coll = buildUniqueCollName(coll);
    var _id = _.mapValues(_.defaults(coll.key_mapping, keys), (value, key, object) => '$' + key);
    _.assign(_id, { side: coll.side })
    var cursor = _db.getCollection(coll.coll).aggregate(
        [
            { $project: keys },
            { $group: { _id: _id } },
            { $out: unique_coll },
        ],
        {
            allowDiskUse: true
        }
    );
    cursor.toArray();
    console.log(_.template('unique record count for ${coll.dbname}.${coll.coll}: ${count}')
        ({
            'dbname': coll.dbname,
            'coll': coll.coll,
            'count': _db.getCollection(unique_coll).count()
        }));
}
build_unique_records_collection = logger(build_unique_records_collection);

function buildUniqueCollName(coll: MongoCollection): string {
    return 'unique_' + coll.coll + '_' + coll.label;
}


var do_export = function(coll:MongoCollection):void {
    var work_dir = 'c:\\temp';
    var export_cmd = _.template('"${mongoexport}" --verbose --host ${mongo_server} -d "${dbname}" -c "${unique_coll}" --out "${work_dir}\\${unique_coll}.json"  --type json')
        ({
            mongoexport: 'C:\\Program Files\\MongoDB\\Server\\3.2\\bin\\mongoexport',
            mongo_server: 'sd-dfc9-2176:27017',
            dbname: coll.dbname,
            unique_coll: buildUniqueCollName(coll.coll, coll.label),
            work_dir: work_dir
        });
    console.log('running export command: ' + export_cmd);
    var export_output = shelljs.exec(export_cmd, 
        { async: false, silent: false }
        );
    console.log('after export')
}
do_export = logger(do_export);

var do_import = function(coll1:MongoCollection, coll2:MongoCollection):void {
    var work_dir = 'c:\\temp';
    var import_cmd = _.template('"${mongoimport}" -v --host ${mongo_server} -d "${dbname}" -c "${target_coll}" --file "${work_dir}\\${unique_coll}.json"  --type json')
        ({
            mongoimport: 'C:\\Program Files\\MongoDB\\Server\\3.2\\bin\\mongoimport',
            mongo_server: 'sd-dfc9-2176:27017',
            dbname: target_dbname,
            target_coll: buildUniqueCollName(coll2),
            unique_coll: buildUniqueCollName(coll1),
            work_dir: work_dir
        });
    console.log('running import command: ' + import_cmd);
    shelljs.exec(import_cmd, { async: false, silent: false });
    console.log('after export')
}
do_import = logger(do_import);


var minus = function(dbname, left, right, coll, keys):void {
    console.log('calculating diff of ' + left.label + " and " + right.label);
    var diff_coll = "diff_" + left.label + "_" + right.label;
    var _db = db.getSiblingDB(dbname);
    var cursor;
    console.log('running diff');
    _db.getCollection(diff_coll).remove({})
    var _project = _.mapValues(keys, (value, key, object) => '$_id.' + key);
    _.assign(_project, { side: '$_id.side' })
    var _group = _.mapValues(keys, (value, key, object) => '$' + key);
    
    cursor =
        _db.getSiblingDB(dbname)
            .getCollection(coll)
            .aggregate(
            [
                { $project: _project },
                // { $group: { _id: { usi: '$usi', tradeType: '$tradeType' }, sides: { $addToSet: '$side' } } },
                { $group: { _id: _group, sides: { $addToSet: '$side' } } },
                { $project: { sides: '$sides', s: { $size: '$sides' } } },
                { $match: { s: 1, } },
                { $unwind: "$sides" },
                { $group: { _id: '$sides', usis: { $addToSet: '$_id' } } },
                { $out: diff_coll }
            ],
            { allowDiskUse: true }
            );
    cursor.toArray();

    buildDifferenceCollection(_db, left, right, diff_coll);
    buildDifferenceCollection(_db, right, left, diff_coll);
    console.log('buiding diffrence collections done');
}
minus = logger(minus);

var buildDifferenceCollection = function(_db, left, right, diff_coll):void {
    console.log('building difference collection: ' + left.label + " minus" + right.label);
    var coll_l_minus_r = left.label + "_minus_" + right.label;
    _db.getCollection(coll_l_minus_r).remove({})
    var cursor =
        _db.getCollection(diff_coll).aggregate([
            { $match: { _id: left.label } },
            { $unwind: "$usis" },
            { $group: { _id: { usi: '$usis.usi', tradeType: '$usis.tradeType' } } },
            { $out: coll_l_minus_r }
        ]);
    cursor.toArray();
}
buildDifferenceCollection = logger(buildDifferenceCollection);

var clean = function(dbname, left, right, target_coll):void {
    var diff_coll = "diff_" + left.label + "_" + right.label;
    var _db = db.getSiblingDB(dbname);
    var collectionsToClean = [
        diff_coll,
        left.label + "_minus_" + right.label,
        right.label + "_minus_" + left.label,
        buildUniqueCollName(left),
        buildUniqueCollName(right)
        ];
        
    _.forEach(collectionsToClean, c => _db.getCollection(c).drop());
}
clean = logger(clean);

var diff = function (target_dbname, coll1, coll2, target_coll, keys) {
    clean(target_dbname, coll1, coll2, target_coll);
    build_unique_records_collection(coll1, keys);
    build_unique_records_collection(coll2, keys);
    db.getSiblingDB(target_dbname).getCollection(target_coll).createIndex({ "_id.usi": 1 })
    do_export(coll1);
    do_import(coll1, coll2);
    minus(target_dbname, coll1, coll2, target_coll, keys);
}

diff = logger(diff);

var coll1 = new MongoCollection('ctml_ak81894_sd-dfc9-2176_2017_06_25_2200', 'fxpgEvent', 'ctml1', {tradeType: 'eventType'});
var coll2 = new MongoCollection('QControlData_FX_2606', 'tradeEvent', 'ctml2', {});

var target_dbname:string = coll2.dbname;
var target_coll = buildUniqueCollName(coll2);
var keys = { usi: 1, tradeType: 1 };

diff(target_dbname, coll1, coll2, target_coll, keys);
