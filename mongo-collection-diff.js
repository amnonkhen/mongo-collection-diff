// diff 2 collections

var coll1 = {
    dbname: 'ctml_ak81894_sd-dfc9-2176_2017_06_25_2200',
    coll: 'fxpgEvent',
    label: 'ctml1',
    keys: { usi: 1, tradeType: '$eventType' },
};

var coll2 = {
    dbname: 'QControlData_FX_2506_fix',
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

function log_process_output(code, stdout, stderr) {
    console.log('Exit code:', code);
    console.log('Program output:', stdout);
    console.log('Program stderr:', stderr);
}
// find_unique('ctml_ak81894_sd-dfc9-2176_2017_06_25_2200', 'fxpgEvent', 'ctml1', { usi: 1, tradeType: '$eventType' });
// find_unique('QControlData_FX_2506_fix', 'tradeEvent', 'ctml2', { usi: 1, tradeType: 1 });
// db.getSiblingDB(target_dbname).getCollection(target_coll).createIndex({ "_id.usi": 1 })

var export_cmd = _.template('"${mongoexport}" -v --host ${mongo_server} -d "${dbname}" -c "${unique_coll}" --out "${work_dir}\\${unique_coll}.json"  --type json')
    ({
        mongoexport: 'C:\\Program Files\\MongoDB\\Server\\3.2\\bin\\mongoexport',
        mongo_server: 'sd-dfc9-2176:27017',
        dbname: coll1.dbname,
        unique_coll: buildUniqueCollName(coll1.coll, coll1.label),
        work_dir: work_dir
    });
console.log('running export command: ' + export_cmd);
// shelljs.exec(export_cmd, {async:true, silent:false}, log_process_output );

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
// shelljs.exec(import_cmd, {async:true, silent:false},  log_process_output);



minus(target_dbname, coll1, coll2, target_coll);


function minus(dbname, left, right, coll) {
    console.log('calculating ' + left.label + "_minus_" + right.label);
    var diff_coll = "diff_" + left.label + "_" + right.label;
    var cursor;
    console.log('running diff');
    // cursor =
    //     db.getSiblingDB(dbname)
    //         .getCollection(coll)
    //         .aggregate(
    //         [
    //             { $project: { usi: '$_id.usi', tradeType: '$_id.tradeType', side: '$_id.side' } },
    //             { $group: { _id: { usi: '$usi', tradeType: '$tradeType' }, sides: { $addToSet: '$side' } } },
    //             { $project: { sides: '$sides', s: { $size: '$sides' } } },
    //             { $match: { s: 1, } },
    //             { $unwind: "$sides" },
    //             { $group: { _id: '$sides', usis: { $addToSet: '$_id' } } },
    //             { $out: diff_coll }
    //         ],
    //         { allowDiskUse: true }
    //         );
    // cursor.toArray();

    console.log('building difference collections');
    cursor =
         db.getSiblingDB(dbname).getCollection(diff_coll).aggregate([
            { $match: { _id: left.label } },
            { $unwind: "$usis" },
            { $group: { _id: { usi: '$usis.usi', tradeType: '$usis.tradeType' } } },
            { $out: left.label + "_minus_" + right.label }
        ]);
    cursor.toArray();

    cursor =
         db.getSiblingDB(dbname).getCollection(diff_coll).aggregate([
            { $match: { _id: right.label } },
            { $unwind: "$usis" },
            { $group: { _id: { usi: '$usis.usi', tradeType: '$usis.tradeType' } } },
            { $out: right.label + "_minus_" + left.label }
        ]);
    cursor.toArray();
    console.log('done');
}

