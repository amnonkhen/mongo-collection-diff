db.runCommand(
    {
        aggregate: 'c',
        pipeline: [
            { $project: { usi: '$a', tradeType:'$b', side: '$c' } },
            { $group: { _id: {usi:'$usi', tradeType:'$tradeType'}, sides: { $addToSet: '$side' } } },
            { $project: {sides: '$sides', s: {$size: '$sides'}}},
            { $match: { s:1,}},
            { $unwind: "$sides" },
            { $group: { _id: '$sides', usis: { $addToSet: '$_id' } } },
            { $out: 'ctml_fpml_diff' }
        ],
         
        allowDiskUse: true,
    }
);

db.getCollection('ctml_fpml_diff').aggregate([
    { $match: { _id:3}},
    { $unwind: "$usis" },
    { $group: {_id:{ usi: '$usis.usi', tradeType:'$usis.tradeType'}}},
    { $out: 'left_minus_right' }
]);

db.getCollection('ctml_fpml_diff').aggregate([
    { $match: { _id:3}},
    { $unwind: "$usis" },
    { $group: {_id:{ usi: '$usis.usi', tradeType:'$usis.tradeType'}}},
    { $out: 'right_minus_left' }
]);
