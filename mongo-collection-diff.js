// diff 2 collections

var coll1 = {
    dbname: 'ctml_ak81894_sd-dfc9-2176_2017_06_25_2200',
    coll: 'fxpgEvent',
    label: 'ctml1',
    keys: { usi: 1, tradeType: '$eventType' },
};

var coll2 = {
    dbname: 'QControlData_FX_2606',
    coll: 'tradeEvent',
    label: 'ctml2',
    keys: { usi: 1, tradeType: 1 },
};

var target_dbname = coll2.dbname;
var target_coll = buildUniqueCollName(coll2.coll, coll2.label);
var work_dir = 'c:\\temp';

function find_unique(dbname: string, coll: string, side: string, keys) {
    var _db = db.getSiblingDB(dbname);
    var unique_coll = buildUniqueCollName(coll, side);
    var _id = _.mapValues(keys, (value, key, object) => '$' + key);
    _.assign(_id, { side: side })
    var cursor = _db.getCollection(coll).aggregate(
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
    console.log(_.template('unique record count for ${dbname}.${coll}: ${count}')
        ({
            'dbname': dbname,
            'coll': coll,
            'count': _db.getCollection(unique_coll).count()
        }));
}

function buildUniqueCollName(coll: string, side: string): string {
    return 'unique_' + coll + '_' + side;
}


function do_export() {
    var export_cmd = _.template('"${mongoexport}" --verbose --host ${mongo_server} -d "${dbname}" -c "${unique_coll}" --out "${work_dir}\\${unique_coll}.json"  --type json')
        ({
            mongoexport: 'C:\\Program Files\\MongoDB\\Server\\3.2\\bin\\mongoexport',
            mongo_server: 'sd-dfc9-2176:27017',
            dbname: coll1.dbname,
            unique_coll: buildUniqueCollName(coll1.coll, coll1.label),
            work_dir: work_dir
        });
    console.log('running export command: ' + export_cmd);
    var export_output = shelljs.exec(export_cmd, 
        { async: false, silent: false }
        );
    console.log('after export')
}

function do_import() {
    var import_cmd = _.template('"${mongoimport}" -v --host ${mongo_server} -d "${dbname}" -c "${target_coll}" --file "${work_dir}\\${unique_coll}.json"  --type json')
        ({
            mongoimport: 'C:\\Program Files\\MongoDB\\Server\\3.2\\bin\\mongoimport',
            mongo_server: 'sd-dfc9-2176:27017',
            dbname: target_dbname,
            target_coll: buildUniqueCollName(coll2.coll, coll2.label),
            unique_coll: buildUniqueCollName(coll1.coll, coll1.label),
            work_dir: work_dir
        });
    console.log('running import command: ' + import_cmd);
    shelljs.exec(import_cmd, { async: false, silent: false });
    console.log('after export')
}



function minus(dbname, left, right, coll) {
    console.log('calculating diff of ' + left.label + " and " + right.label);
    var diff_coll = "diff_" + left.label + "_" + right.label;
    var _db = db.getSiblingDB(dbname);
    var cursor;
    console.log('running diff');
    _db.getCollection(diff_coll).remove({})
    cursor =
        _db.getSiblingDB(dbname)
            .getCollection(coll)
            .aggregate(
            [
                { $project: { usi: '$_id.usi', tradeType: '$_id.tradeType', side: '$_id.side' } },
                { $group: { _id: { usi: '$usi', tradeType: '$tradeType' }, sides: { $addToSet: '$side' } } },
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
    console.log('done');
}


function buildDifferenceCollection(_db, left, right, diff_coll) {
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

function clean(dbname, left, right, target_coll) {
    var diff_coll = "diff_" + left.label + "_" + right.label;
    var _db = db.getSiblingDB(dbname);
    _db.getCollection(diff_coll).drop();
    _db.getCollection(left.label + "_minus_" + right.label).drop();
    _db.getCollection(right.label + "_minus_" + left.label).drop();
    _db.getCollection(target_coll).drop();
}

console.log('started');
clean(target_dbname, coll1, coll2, target_coll);
// find_unique(coll1.dbname, coll1.coll, coll1.label, coll1.keys);
db.getSiblingDB(target_dbname).getCollection(target_coll).createIndex({ "_id.usi": 1 })
do_export();
do_import();
minus(target_dbname, coll1, coll2, target_coll);find_unique(coll2.dbname, coll2.coll, coll2.label, coll2.keys);
console.log('done');
