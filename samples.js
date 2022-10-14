db.friends.aggregate([
  { $unwind: '$hobbies' },
  { $group: { _id: { age: '$age' }, hobbies: { $addToSet: '$hobbies' } } },
]);

db.friends
  .aggregate([
    {
      $project: { _id: 1, name: 1, examScores: { $slice: ['$examScores', 1] } },
    },
  ])
  .pretty();

db.friends
  .aggregate([
    {
      $project: {
        _id: 1,
        name: 1,
        examScores: {
          $filter: {
            input: '$examScores',
            as: 'sx',
            cond: { $gt: ['$$sx.score', 60] },
          },
        },
      },
    },
  ])
  .pretty();

db.friends.aggregate([
  { $unwind: '$examScores' },
  { $project: { _id: 1, name: 1, age: 1, score: '$examScores.score' } },
  {
    $group: {
      _id: '$_id',
      name: { $first: '$name' },
      maxScore: { $max: '$score' },
    },
  },
  { $sort: { maxScore: -1 } },
]);
