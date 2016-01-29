/**
 * Created by mitsos on 1/13/16.
 */
var should = require('chai').should()
  , aggregation = require('../lib/aggregation')
  ;

describe('Aggregation', function () {
  var dataset, dataCopy;
  before(function () {
    dataset = [
      {
        _id: 1,
        username: 'Mitsos',
        gender: 'male',
        isIt:'same',
        posts: [
          {
            title: 'foo',
            content: 'bar'
          },
          {
            title: 'some',
            content: 'thing'
          }
        ],
        someArr: [
          {
            prop: 'tsiki'
          },
          {
            prop: 'baila'
          }
        ]
      },
      {
        _id: 2,
        username: 'Kate',
        gender: 'female',
        isIt:'same',
        subdoc: {
          tika: 'taka'
        },
        posts: [
          {
            title: 'sek',
            content: 'lana'
          },
          {
            title: 'rou',
            content: 'fouses'
          }
        ],
        emptyArr: []
      }
    ];
    // Also create the copy
    dataCopy = JSON.parse(JSON.stringify(dataset));
  });
  describe('Error condition', function () {
    it('Should provide an error when unknown aggregation operator used', function (done) {
      var op = {
        $bla: 'blabla'
      };
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(docs);
        err.should.be.instanceof(Error);
        err.should.have.property('message', 'Unknown aggregation operator $bla used.');
        done();
      });
    });
  });

  describe('$match operator', function () {
    it('Should not affect the original dataset', function (done) {
      var op = {$match: {username: 'Kate'}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        dataset.should.deep.equal(dataCopy);
        done();
      })
    });

    it('Should provide all docs when no empty match is provided', function (done) {
      var op = {$match: {}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.deep.equal(dataset);
        done();
      });
    });
    it('Should match single properties', function (done) {
      var op = {$match: {username: 'Mitsos'}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs.should.deep.have.members([dataset[0]]);
        done();
      });
    });

    it('Should match embedded document properties', function (done) {
      var op = {$match: {'subdoc.tika': 'taka'}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs[0].should.deep.equal(dataset[1]);
        done();
      });
    });

    it('Should match embedded document array properties', function (done) {
      var op = {$match: {'posts.title': 'sek'}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs[0].should.deep.equal(dataset[1]);
        done();
      });
    });
  });

  describe('$unwind', function () {
    it('Should return an error when used on a non array field', function (done) {
      var op = {$unwind: '$gender'};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(docs);
        err.should.be.instanceof(Error);
        err.should.have.property('message', '$unwind operator used on non-array field: gender');
        done();
      });
    });
    it('Should leave the original dataset intact', function (done) {
      var op = {$unwind: '$posts'};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(4);
        dataset.should.deep.equal(dataCopy);
        done();
      })
    });
    it('Should properly unwind array field elements', function (done) {
      var op = {$unwind: '$posts'};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(4);
        docs.should.deep.equal([
          {
            _id: 1,
            username: 'Mitsos',
            isIt:'same',
            gender: 'male',
            posts: {
              title: 'foo',
              content: 'bar'
            },
            someArr: [
              {
                prop: 'tsiki'
              },
              {
                prop: 'baila'
              }
            ]
          },
          {
            _id: 1,
            username: 'Mitsos',
            isIt:'same',
            gender: 'male',
            posts: {
              title: 'some',
              content: 'thing'
            },
            someArr: [
              {
                prop: 'tsiki'
              },
              {
                prop: 'baila'
              }
            ]
          },
          {
            _id: 2,
            username: 'Kate',
            isIt:'same',
            gender: 'female',
            subdoc: {
              tika: 'taka'
            },
            posts: {
              title: 'sek',
              content: 'lana'
            },
            emptyArr: []
          },
          {
            _id: 2,
            username: 'Kate',
            isIt:'same',
            gender: 'female',
            subdoc: {
              tika: 'taka'
            },
            posts: {
              title: 'rou',
              content: 'fouses'
            },
            emptyArr: []
          }
        ]);
        done();
      });
    });

    it('Should skip non existing fields on docs', function (done) {
      var op = {$unwind: '$someArr'};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(2);
        docs.should.deep.equal([
          {
            _id: 1,
            username: 'Mitsos',
            isIt:'same',
            gender: 'male',
            posts: [
              {
                title: 'foo',
                content: 'bar'
              },
              {
                title: 'some',
                content: 'thing'
              }
            ],
            someArr: {
              prop: 'tsiki'
            }
          },
          {
            _id: 1,
            username: 'Mitsos',
            isIt:'same',
            gender: 'male',
            posts: [
              {
                title: 'foo',
                content: 'bar'
              },
              {
                title: 'some',
                content: 'thing'
              }
            ],
            someArr: {
              prop: 'baila'
            }
          }
        ]);
        done();
      });
    })
  });

  describe('$sort', function () {
    it('Should successfully sort on selected field', function (done) {
      var op = {$sort: {username: 1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(2);
        docs[0].should.deep.equal(dataset[1]);
        docs[1].should.deep.equal(dataset[0]);
        done();
      });
    });

    it('Should successfully sort on selected field with reverse order', function (done) {
      var op = {$sort: {username: -1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(2);
        docs[0].should.deep.equal(dataset[0]);
        docs[1].should.deep.equal(dataset[1]);
        done();
      });
    });

    it('Should sort without modifying the positions in original dataset', function (done) {
      var op = {$sort: {username: 1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(2);
        dataCopy[0].should.deep.equal(dataset[0]);
        dataCopy[1].should.deep.equal(dataset[1]);
        done();
      });
    });
  });

  describe('$skip', function () {
    it('Should skip the correct number of result docs', function (done) {
      var op = {$skip: 1};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs[0].should.deep.equal(dataset[1]);
        done();
      });
    });

    it('Should skip the correct of result docs even if skip surpasses the length of input array', function (done) {
      var op = {$skip: 3};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(0);
        done();
      });
    });

    it('Should skip not affect the original dataset', function (done) {
      var op = {$skip: 3};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(0);
        dataCopy.should.deep.equal(dataset);
        done();
      });
    });
  });

  describe('$limit', function () {
    it('Should return maximum number of results selected', function (done) {
      var op = {$limit: 1};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs[0].should.deep.equal(dataset[0]);
        done();
      });
    });

    it('Should not affect the original dataset', function (done) {
      var op = {$limit: 1};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(1);
        docs[0].should.deep.equal(dataset[0]);
        dataCopy.should.deep.equal(dataset);
        done();
      });
    });
  });

  describe('$group', function () {
    it('Should return an error when no _id field in group object provided in operator', function (done) {
      var op = {$group: {count: 1}};
      aggregation.exec(dataset, [op], function (err, results) {
        should.not.exist(results);
        err.should.be.instanceof(Error);
        err.should.have.property('message', 'Group Object in operator does not contain an _id field');
        done();
      });
    });

    it('Should return an error when not supported accumulator used', function (done) {
      var op = {$group: {_id: null, sth: {$sss: 1}}};
      aggregation.exec(dataset, [op], function (err, results) {
        should.not.exist(results);
        err.should.be.instanceof(Error);
        err.should.have.property('message', 'Unsupported accumulator $sss used in $group');
        done();
      });
    });
    
    it('Should match complex _ids of object instead of single literals', function (done) {
      var op = {$group: {_id: {match:'$isIt'}}};
      aggregation.exec(dataset, [op], function (err, results) {
        should.not.exist(err);
        results.should.have.length(1);
        results[0].should.deep.equal({_id:{match:'same'}});
        done();
      });
    });

    describe('$sum accumulator', function () {
      it('Should work correctly when _id provided to filter docs', function (done) {
        var op = {$group: {_id: '$username', count: {$sum: 1}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(2);
          results.should.deep.have.members([
            {
              _id: 'Mitsos',
              count: 1
            },
            {
              _id: 'Kate',
              count: 1
            }
          ]);
          done();
        });
      });

      it('Should work correctly when _id provided to filter docs and different operator value', function (done) {
        var op = {$group: {_id: '$username', count: {$sum: 2}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(2);
          results.should.deep.have.members([
            {
              _id: 'Mitsos',
              count: 2
            },
            {
              _id: 'Kate',
              count: 2
            }
          ]);
          done();
        });
      });

      it('Should work correctly when null _id provided to compute all docs', function (done) {
        var op = {$group: {_id: null, count: {$sum: 2}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(1);
          results[0].should.deep.equal({
            _id: null,
            count: 4
          });
          done();
        });
      });

    });

    describe('$push accumulator', function () {

      it('Should correctly be used on literal values to push', function (done) {
        var op = {$group: {_id: null, foo: {$push: 10}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(1);
          results[0].should.deep.equal({
            _id: null,
            foo:[10,10]
          });
          done();
        });
      });

      it('Should correctly be used on doc field Paths to push', function (done) {
        var op = {$group: {_id: null, users: {$push: '$username'}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(1);
          results[0].should.deep.equal({
            _id: null,
            users:['Mitsos', 'Kate']
          });
          done();
        });
      });

      it('Should correctly be used on doc field Paths to push for new objects', function (done) {
        var op = {$group: {_id: null, users: {$push: {userGenders:'$gender'}}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(1);
          results[0].should.deep.equal({
            _id: null,
            users:[{userGenders:'male'}, {userGenders:'female'}]
          });
          done();
        });
      });

      it('Should correctly skip non existent properties and only provide exiting ones', function (done) {
        var op = {$group: {_id: null, foo: {$push: '$subdoc.tika'}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(err);
          results.should.have.length(1);
          results[0].should.deep.equal({
            _id: null,
            foo:['taka']
          });
          done();
        });
      });

      it('Should throw error when numeric value provided in $push expression', function (done) {
        var op = {$group: {_id: null, foo: {$push: {some:1}}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(results);
          err.should.be.instanceof(Error);
          done();
        });
      });

      it('Should throw error when boolean value provided in $push expression', function (done) {
        var op = {$group: {_id: null, foo: {$push: {some:true}}}};
        aggregation.exec(dataset, [op], function (err, results) {
          should.not.exist(results);
          err.should.be.instanceof(Error);
          done();
        });
      });

    });

  });

  describe('$project', function () {
    it('Should correctly include only existing fields along with _id', function (done) {
      var op = {$project: {username: 1, gender: 1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            _id: 1,
            gender: 'male',
            username: 'Mitsos'
          },
          {
            _id: 2,
            gender: 'female',
            username: 'Kate'
          }
        ]);
        done();
      });
    });

    it('Should correctly include only existing fields excluding _id', function (done) {
      var op = {$project: {username: 1, gender: 1, _id: 0}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            gender: 'male',
            username: 'Mitsos'
          },
          {
            gender: 'female',
            username: 'Kate'
          }
        ]);
        done();
      });
    });

    it('Should correctly include only existing fields (embedded documents) along with _id, excluding field if not exists', function (done) {
      var op = {$project: {username: 1, 'subdoc.tika': 1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            _id: 1,
            username: 'Mitsos'
          },
          {
            _id: 2,
            username: 'Kate',
            subdoc: {
              tika: 'taka'
            }
          }
        ]);
        done();
      });
    });

    it('Should correctly include only existing fields (embedded documents) excluding _id, exluding field if not exists', function (done) {
      var op = {$project: {username: 1, 'subdoc.tika': 1, _id: 0}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            username: 'Mitsos'
          },
          {
            username: 'Kate',
            subdoc: {
              tika: 'taka'
            }
          }
        ]);
        done();
      });
    });

    it('Should correctly include only existing fields (embedded array document fields) along with _id', function (done) {
      var op = {$project: {username: 1, 'posts.title': 1}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            _id: 1,
            username: 'Mitsos',
            posts: [
              {
                title: 'foo'
              },
              {
                title: 'some'
              }
            ]
          },
          {
            _id: 2,
            username: 'Kate',
            posts: [
              {
                title: 'sek'
              },
              {
                title: 'rou'
              }
            ]
          }
        ]);
        done();
      });
    });

    it('Should correctly include only existing fields (embedded array document fields) excluding _id', function (done) {
      var op = {$project: {username: 1, 'posts.title': 1, _id: 0}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            username: 'Mitsos',
            posts: [
              {
                title: 'foo'
              },
              {
                title: 'some'
              }
            ]
          },
          {
            username: 'Kate',
            posts: [
              {
                title: 'sek'
              },
              {
                title: 'rou'
              }
            ]
          }
        ]);
        done();
      });
    });

    it('Should include computed fields from existing document fields', function (done) {
      var op = {$project: {username: 1, 'newField': '$subdoc.tika'}};
      aggregation.exec(dataset, [op], function (err, docs) {
        should.not.exist(err);
        docs.should.have.length(dataset.length);
        docs.should.deep.equal([
          {
            _id: 1,
            username: 'Mitsos'
          },
          {
            _id: 2,
            username: 'Kate',
            newField: 'taka'
          }
        ]);
        done();
      });
    });
  });
});