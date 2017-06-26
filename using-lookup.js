function minus(dbname, left, right) {
    console.log('calculating ' + left.label + "_minus_" + right.label);
    var cursor = db.getSiblingDB(dbname).getCollection(buildUniqueCollName(left.coll, left.label)).aggregate(
            [
                // {
                //     "$lookup": {
                //         "from": buildUniqueCollName(right.coll, right.label),
                //         "localField": "_id.usi",
                //         "foreignField": "_id.usi",
                //         "as": 'right'
                //     }
                // },
                // {
                //     "$match": { 'right': [] }
                // },
                // {
                //     "$project": {
                //         "usi": 1,
                //         "tradeType": 1,
                //     }
                // },
                // { $out: left.label + "_minus_" + right.label }
            ],
            {allowDiskUse: true},
        );
    cursor.toArray();
}
