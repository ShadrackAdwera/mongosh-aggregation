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
