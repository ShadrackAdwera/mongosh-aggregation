/*
1. import the json to your mongodb collection using the command:

$ mongoimport persons.json -d dbName -c collectionName --jsonArray --drop

2. Fire up mongo shell

$ mongosh

3. Use the db created

test> use dbName

*/

// Group users based on gender and location

db.persons
  .aggregate([
    { $match: { gender: 'female' } },
    { $group: { _id: { state: '$location.state' }, count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ])
  .pretty();

/*
1. Find persons whose average age is less than 30
2. Group by gender (M/F), show the total counter per age and the average for each gender below 30
3. Sort based on the number 
*/

db.persons
  .aggregate([
    { $match: { 'dob.age': { $lt: 30 } } },
    {
      $group: {
        _id: { gender: '$gender' },
        count: { $sum: 1 },
        averageAge: { $avg: '$dob.age' },
      },
    },
    { $sort: { count: -1 } },
  ])
  .pretty();

/*
1. Return the ID and name (First Letter of name autocapitalized)
*/

db.persons
  .aggregate([
    {
      $project: {
        _id: 1,
        gender: 1,
        fullName: {
          $concat: [
            { $toUpper: { $substrCP: ['$name.title', 0, 1] } },
            {
              $substrCP: [
                '$name.title',
                1,
                { $subtract: [{ $strLenCP: '$name.title' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.first', 0, 1] } },
            {
              $substrCP: [
                '$name.first',
                1,
                { $subtract: [{ $strLenCP: '$name.first' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.last', 0, 1] } },
            {
              $substrCP: [
                '$name.last',
                1,
                { $subtract: [{ $strLenCP: '$name.last' }, 1] },
              ],
            },
          ],
        },
      },
    },
  ])
  .pretty();

/*
COMBINE PROJECTIONS 
1. Transform location data to GeoJSON and convert it to a number
2. Project the data alongside other fields passed from projection 1
3. Group people based on the year they were born and show the count
4. Sort in descending order
*/

db.persons
  .aggregate([
    {
      $project: {
        _id: 1,
        name: 1,
        email: 1,
        dob: 1,
        location: {
          type: 'Point',
          coordinates: [
            {
              $convert: {
                input: '$location.coordinates.longitude',
                to: 'double',
                onError: 0.0,
                onNull: 0.0,
              },
            },
            {
              $convert: {
                input: '$location.coordinates.latitude',
                to: 'double',
                onError: 0.0,
                onNull: 0.0,
              },
            },
          ],
        },
      },
    },
    {
      $project: {
        _id: 1,
        fullname: {
          $concat: [
            { $toUpper: { $substrCP: ['$name.title', 0, 1] } },
            {
              $substrCP: [
                '$name.title',
                1,
                { $subtract: [{ $strLenCP: '$name.title' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.first', 0, 1] } },
            {
              $substrCP: [
                '$name.first',
                1,
                { $subtract: [{ $strLenCP: '$name.first' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.last', 0, 1] } },
            {
              $substrCP: [
                '$name.last',
                1,
                { $subtract: [{ $strLenCP: '$name.last' }, 1] },
              ],
            },
          ],
        },
        email: 1,
        gender: 1,
        dateofbirth: { $toDate: '$dob.date' },
        age: '$dob.age',
        location: 1,
      },
    },
    {
      $group: {
        _id: { yearofbirth: { $isoWeekYear: '$dateofbirth' } },
        count: { $sum: 1 },
      },
    },
    { $sort: { count: -1 } },
  ])
  .pretty();

/*
COMBINE PROJECTIONS 
1. Transform location data to GeoJSON and convert it to a number
2. Project the data alongside other fields passed from projection 1
3. Sort in descending order
4. Write the data into a collection called transformedPersons
*/

db.persons
  .aggregate([
    {
      $project: {
        _id: 1,
        name: 1,
        email: 1,
        dob: 1,
        gender: 1,
        location: {
          type: 'Point',
          coordinates: [
            {
              $convert: {
                input: '$location.coordinates.longitude',
                to: 'double',
                onError: 0.0,
                onNull: 0.0,
              },
            },
            {
              $convert: {
                input: '$location.coordinates.latitude',
                to: 'double',
                onError: 0.0,
                onNull: 0.0,
              },
            },
          ],
        },
      },
    },
    {
      $project: {
        _id: 1,
        fullname: {
          $concat: [
            { $toUpper: { $substrCP: ['$name.title', 0, 1] } },
            {
              $substrCP: [
                '$name.title',
                1,
                { $subtract: [{ $strLenCP: '$name.title' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.first', 0, 1] } },
            {
              $substrCP: [
                '$name.first',
                1,
                { $subtract: [{ $strLenCP: '$name.first' }, 1] },
              ],
            },
            ' ',
            { $toUpper: { $substrCP: ['$name.last', 0, 1] } },
            {
              $substrCP: [
                '$name.last',
                1,
                { $subtract: [{ $strLenCP: '$name.last' }, 1] },
              ],
            },
          ],
        },
        email: 1,
        gender: 1,
        dateofbirth: { $toDate: '$dob.date' },
        age: '$dob.age',
        location: 1,
      },
    },
    { $sort: { count: -1 } },
    { $out: 'transformedPersons' },
  ])
  .pretty();

/*
  1. Buckets to have a feel on data distribution
  */
db.persons.aggregate([
  {
    $bucket: {
      groupBy: '$dob.age',
      boundaries: [0, 18, 30, 50, 80, 120],
      output: {
        persons: { $sum: 1 },
        averageage: { $avg: '$dob.age' },
      },
    },
  },
]);

/*
Mongo looks at the data and draws up the boundaries based on the number of buckets specified
*/
db.persons
  .aggregate([
    {
      $bucketAuto: {
        groupBy: '$dob.age',
        buckets: 5,
        output: {
          persons: { $sum: 1 },
          averageage: { $avg: '$dob.age' },
        },
      },
    },
  ])
  .pretty();

db.persons.aggregate([
  {
    $project: {
      _id: 1,
      name: { $concat: ['$name.first', ' ', '$name.last'] },
      dateofbirth: { $toDate: '$dob.date' },
    },
  },
  { $sort: { dateofbirth: 1 } },
  { $skip: 10 },
  { $limit: 10 },
]);

/*
1. For geo location data created, in the transformedPersons pipeline, create a 2dsphere index on location document.
2. $geoNear finds elements in the collection near the current position
3. In order to use geoNear, it has to be the first element in the pipeline
4. Find the first 10 females closest to the location provided, max distance 10km - age > 23 and less than 32
*/
db.transformedPersons.createIndex({ location: '2dsphere' });

db.transformedPersons.aggregate([
  {
    $geoNear: {
      near: {
        type: 'Point',
        coordinates: [-18.4, -42.8],
      },
      maxDistance: 10000,
      query: { gender: 'female', age: { $lt: 32, $gt: 23 } },
      distanceField: 'distance',
    },
  },
  { $limit: 10 },
]);
