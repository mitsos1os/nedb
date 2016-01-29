/**
 * Created by mitsos on 1/28/16.
 */
var should = require('chai').should()
  , expression = require('../lib/expression')
;

describe('expression', function () {
  var sourceDoc = {
    num:4,
    foo:'bar',
    blean:false,
    addresses:{
      home:{
        number:56,
        address:'somewhere'
      }
    },
    elems:[{poso:'akoma',test:'thelei'},{poso:'ligo', test:'emeine'}]
  };

  it('Should return directly the expression in case of primitive literal', function() {
    expression.apply('foo').should.equal('foo');
    expression.apply(42).should.equal(42);
    expression.apply(false).should.equal(false);
  });

  it('Should return field path provided', function () {
    expression.apply('$num', sourceDoc).should.equal(4);
  });

  it('Should return field path provided from embedded doc', function () {
    expression.apply('$addresses.home', sourceDoc).should.deep.equal({number:56, address:'somewhere'});
  });

  it('Should return directly the expression in case of object literal', function() {
    var obj = {foo:'bar', some:{thing:'else'}};
    expression.apply(obj).should.deep.equal(obj);
  });

  it('Should correctly return fieldPaths for expression objects', function () {
    var now = new Date()
      , obj = {foo:now, number:'$addresses.home.number'}
      ;
    expression.apply(obj,sourceDoc).should.deep.equal({foo:now,number:56});
  });

  it('Should correctly return array elements by index', function () {
    expression.apply({element:'$elems.1'},sourceDoc).should.deep.equal({element:{poso:'ligo',test:'emeine'}});
  });

  it('Should throw an error when expression object has boolean / numeric and not in $project stage', function () {
    (function(){expression.apply({foo:false})}).should.throw(/not allowed/);
    (function(){expression.apply({foo:5})}).should.throw(/not allowed/);
  });

  it('Should allow expression object that has boolean / numeric when in $project stage', function () {
    var isProjectStage = true
      , expr = {addresses:{home:1}}
      ;
    (function(){expression.apply(expr, sourceDoc, isProjectStage)}).should.not.throw(/not allowed/);
  });

  it('Should correctly apply projection when boolean / numeric value and in $project stage', function () {
    var isProjectStage = true
      , expr = {addresses:{home:1}}
      ;
    expression.apply(expr, sourceDoc, isProjectStage).should.deep.equal({addresses:{
      home:{
        number:56,
        address:'somewhere'
      }
    }});
  });

  it('Should correctly apply projection on array when boolean / numeric value and in $project stage', function () {
    var isProjectStage = true
      , expr = {elems:{poso:1}}
      ;
    expression.apply(expr, sourceDoc, isProjectStage).should.deep.equal({elems:[{poso:'akoma'},{poso:'ligo'}]});
  });

  it('Should correctly compute expression with a mix of projection and fieldPaths in $project stage', function () {
    var isProjectStage = true
      , expr = {whatIsNum:'$num', elems:{poso:1}}
      ;
    expression.apply(expr, sourceDoc, isProjectStage).should.deep.equal({whatIsNum:4, elems:[{poso:'akoma'},{poso:'ligo'}]});
  });

});