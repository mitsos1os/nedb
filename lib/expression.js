/**
 * Created by mitsos on 1/26/16.
 * This file will provide the expression module that will apply mongodb Aggregation Expressions
 * Based on [Aggregation Quick Reference](https://docs.mongodb.org/manual/meta/aggregation-quick-reference)
 */
var model = require('./model');
var utils = require('./customUtils');

/**
 * This function will handle the execution of the expression upon a source a document
 * NOTICE For not only Field Paths in Expression are supported
 *
 * @param {Object} expression
 * @param {Object} doc The doc that the expression is to be applied on
 * @param {Boolean} [isProjectStage=false] This variable will flag if we are in a $project stage, so if to accept boolean key values
 */
function applyExpression (expression, doc, isProjectStage) {
  var key
    , keyValue
    , keyType
    , result   // What will be provided as the result of the expression evaluation
    , keys = Object.keys(expression);   // Get expression keys to traverse
  // Check for expression type
  if (model.isPrimitiveType(expression)) {
    if (typeof expression === 'string' && expression.charAt(0) === '$') { // Field Path
      expression = expression.substr(1);   // Remove $ character
      return utils.getFieldPath(doc,expression);
    }
    else return expression;
  }
  // Expression Object - Operator
  result = {};   // Initialize result object
  for (var i = 0, len = keys.length; i<len;i++) {
    key = keys[i];
    if (key.charAt(0) === '$') {
      // TODO: This is an expression operator.. not supported currently
      continue;
    }
    // Expression Object since key does not start with $ character
    keyValue = expression[key];
    keyType = typeof keyValue;
    if (keyType === 'boolean' || keyType === 'number') {
      if (typeof isProjectStage === 'undefined') isProjectStage = false;
      if (!!isProjectStage) result[key] = keyValue;   // Assign the literal
      else throw new Error('Field inclusion is not allowed inside of $expressions');
    }
    else if (keyType === 'string') {
      if (keyValue.charAt(0) === '$') {
        keyValue = keyValue.substr(1);   // Remove $ character
        result[key] = utils.getFieldPath(doc,keyValue);
      }
      else result[key] = keyValue;   // Literal of string value
    }
    else if (Array.isArray(keyValue) || (keyValue instanceof Date && keyValue.toString() !== 'Invalid Date')) result[key] = keyValue;   // Array or Date
    else {   // Composite Object
      result[key] = applyExpression(keyValue, doc, isProjectStage);
    }
  }   // End of expression object keys loop

  return result;
}

   // Interface
module.exports.apply = applyExpression;