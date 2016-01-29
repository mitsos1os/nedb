/**
 * Created by mitsos on 1/26/16.
 * This file will provide the expression module that will apply mongodb Aggregation Expressions
 * Based on [Aggregation Quick Reference](https://docs.mongodb.org/manual/meta/aggregation-quick-reference)
 */
var model = require('./model')
  , utils = require('./customUtils')
  ;

/**
 * This function will handle the execution of the expression upon a source a document
 * NOTICE For not only Field Paths in Expression are supported
 *
 * @param {Object} expression The expression object with one of the available forms
 * @param {Object} doc The doc that the expression is to be applied on
 * @param {Boolean} [isProjectStage=false] This variable will flag if we are in a $project stage, so if to accept boolean key values
 * @param {String} [exprPathStr] This string will hold in dot notation form the origin (parents if there, in case we are in a recursive call) of the current expression object
 */
function apply (expression, doc, isProjectStage, exprPathStr) {
  var key
    , keyValue
    , valueType
    , result   // What will be provided as the result of the expression evaluation
    , tmpObj
    ;
  // Check for expression type
  if (model.isPrimitiveType(expression)) {
    if (typeof expression === 'string' && expression.charAt(0) === '$') { // Field Path
      expression = expression.substr(1);   // Remove $ character
      return utils.getFieldPath(doc,expression);
    }
    else return expression;
  }
  // Expression Object - Operator
  var keys = Object.keys(expression);   // Get expression keys to traverse
  result = {};   // Initialize result object
  for (var i = 0, len = keys.length; i<len;i++) {
    key = keys[i];
    if (key.charAt(0) === '$') {
      // TODO: This is an expression operator.. not supported currently
      continue;
    }
    // Expression Object since key does not start with $ character
    keyValue = expression[key];
    valueType = typeof keyValue;
    if (valueType === 'boolean' || valueType === 'number') {
      if (typeof isProjectStage === 'undefined') isProjectStage = false;
      if (!!isProjectStage) {   // It is actually projection, apply it
        tmpObj = {};   // Create dot notation object from expression current path
        tmpObj[exprPathStr+'.'+key] = 1;   // Append current expression key to whole path
        tmpObj = utils.pick(doc, utils.convertDotToObj(tmpObj));
        tmpObj = utils.getFieldPath(tmpObj, exprPathStr);
        if (Array.isArray(tmpObj)) return tmpObj;   // Recursive call on array projection, return it
        else if (!!tmpObj && tmpObj.hasOwnProperty(key)) result[key] = tmpObj[key];
      }
      else throw new Error('Field inclusion is not allowed inside of $expressions');
    }
    else if (valueType === 'string') {
      if (keyValue.charAt(0) === '$') {
        keyValue = keyValue.substr(1);   // Remove $ character
        tmpObj = utils.getFieldPath(doc,keyValue);
        if (tmpObj !== undefined) result[key] = tmpObj;
      }
      else result[key] = keyValue;   // Literal of string value
    }
    else if (Array.isArray(keyValue) || (keyValue instanceof Date && keyValue.toString() !== 'Invalid Date')) result[key] = keyValue;   // Array or Date
    else {   // Composite Object
      if (typeof exprPathStr === 'undefined') exprPathStr = key;
      else exprPathStr+= '.'+key;
      tmpObj = apply(keyValue, doc, isProjectStage, exprPathStr);
      if (Object.keys(tmpObj).length > 0) result[key] = tmpObj;
    }
  }   // End of expression object keys loop

  return result;
}

   // Interface
module.exports.apply = apply;