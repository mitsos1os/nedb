/**
 * Created by mitsos on 1/11/16.
 */

var model = require('./model')
  , async = require('async')
  , aggregationOperators = {}
  , accumulators = {}
  , Cursor = require('./cursor')
  , utils = require('./customUtils')
  , expression = require('./expression')
  ;

/**
 * Supported Aggregators for $group functionality
 */
accumulators.$sum = function (sumExpression, previousValue, doc) {
  // Initialize it if not initialized yet
  if (!previousValue) previousValue = 0;
  var sumValue = expression.apply(sumExpression, doc);
  previousValue += parseInt(sumExpression);
  return previousValue;
};

accumulators.$push = function (pushExpression, previousArray, doc) {
  if (!previousArray || !Array.isArray(previousArray)) previousArray = [];   // Initialize the array property in case it is not
  var pushValue = expression.apply(pushExpression, doc);   // Call expression handler to get value that is to be pushed
  if (typeof pushValue !== 'undefined') previousArray.push(pushValue);
  return previousArray;
};

/**
 * Aggregation Operators that will be supported in aggregation pipeline
 */

aggregationOperators.$match = function (docs, matchObj, callback) {
  var newDataSet = [];
  for (var i = 0, docsLen = docs.length;i<docsLen;i++){
    if (model.match(docs[i],matchObj)) newDataSet.push(docs[i]);   // Add if meets query criteria
  }
  callback(null, newDataSet);   // Return the filtered set
};

aggregationOperators.$unwind = function (docs, unwindPath, callback) {
  if (unwindPath.charAt(0) === '$') unwindPath = unwindPath.substr(1);   // Remove first dollar character if there
  else throw new Error('$unwind operator used without field path input');
  // Browse results and unwind them
  var newDataSet = []
    , unwindElemParts = unwindPath.split('.')   // Check to see if unwind is embedded so it needs to be converted to object and assigned to root property
    , unwindHead = unwindElemParts[0]
    , tmpDoc
    ;
  var doc, docUnwindEl, newUnwindDoc, refDoc;
  for (var i = 0, docLen = docs.length; i < docLen; i++) {
    doc = docs[i];
    docUnwindEl = utils.getFieldPath(doc, unwindPath);   // Get the doc element that should be unwinded
    if (typeof doc[unwindHead] === 'undefined') {
      continue;   // Does not exist, continue
    }
    else if (Array.isArray(docUnwindEl)) {   // Manually unwind
      var unwindElemLen = docUnwindEl.length;
      if (unwindElemLen === 0) continue;   // Array element without any content, skip
      refDoc = {};   // Initialize starting point of reference doc to be used in unwinded elements
      for (var key in doc){
        if (key !== unwindHead) refDoc[key] = doc[key];   // Assign all except for element to unwind
      }
      for (var j = 0; j < unwindElemLen; j++) {   // Loop array field element from original doc
        newUnwindDoc = model.deepCopy(refDoc);   // Copy original doc
        tmpDoc = {};
        tmpDoc[unwindPath] = docUnwindEl[j];   // Assign it to an object as key, to be able to convert it to an object
        newUnwindDoc[unwindHead] = utils.convertDotToObj(tmpDoc)[unwindHead];   // The current doc of the array element that's unwinded
        newDataSet.push(newUnwindDoc);
      }   // End loop of doc field
    }
    else {   // Not an array element, error
      return callback(new Error('$unwind operator used on non-array field: ' + unwindPath));
    }
  }   // End loop for docs, new Dataset created
  callback(null, newDataSet);
};

aggregationOperators.$sort = function (docs, sortObj, callback) {
  var tmpDb = {
    getCandidates: function (query, cb) {
      cb(null,docs.slice(0));   // Return copy of docs
    }
  }
    , tmpCursor = new Cursor(tmpDb)   // Construct cursor to use already built in functionality for sorting in cursor prototype
    ;
  tmpCursor.sort(sortObj);   // Apply sort object
  tmpCursor._exec(callback);   // Call internal functionality of cursor to provide results
};

aggregationOperators.$skip = function (docs, skip, callback) {
  // if (skip > docs.length) {
  //  return callback(new Error('Skip option provided ' + skip + ' greater than actual length of provided dataset'));
  //}
  callback(null, docs.slice(skip));
};

aggregationOperators.$limit = function (docs, limit, callback) {
  return callback(null, docs.slice(0, limit));
};

aggregationOperators.$group = function (docs, groupObj, callback) {
  if (typeof groupObj._id === 'undefined') {
    return callback(new Error('Group Object in operator does not contain an _id field'));
  }
  // Group object ok.. continue
  var newDataSet = []
    , idExpression = groupObj._id
    , currentDoc
    , docId
    , j
    , accumulator
    , accumulatorOp
    , docToModify = null
    ;
  for (var i = 0, docsLen = docs.length; i < docsLen; i++) {
    currentDoc = docs[i];
    docId = expression.apply(idExpression, currentDoc);   // Get doc's _id expression
    // Find existing or create new doc
    docToModify = null;   // Initialize selection of doc
    j = newDataSet.length;
    while (j--) {
      if (newDataSet[j]._id !== docId) continue;
      // Doc found
      docToModify = newDataSet[j];
      break;   // Found no need to iterate
    }
    // Check if docToModify was not found in order to create it
    if (!docToModify) {
      docToModify = {_id: docId};   // Add the group match _id
      newDataSet.push(docToModify);
    }
    // Apply aggregators
    for (var property in groupObj) {
      if (property === '_id') continue;   // Skip these properties
      accumulator = groupObj[property];   // Get accumulator object
      accumulatorOp = Object.keys(accumulator)[0];
      if (!accumulators[accumulatorOp]) return callback(new Error('Unsupported accumulator ' + accumulatorOp + ' used in $group'));
      try{
        docToModify[property] = accumulators[accumulatorOp](accumulator[accumulatorOp], docToModify[property], currentDoc);
      }
      catch(err){
        return callback(err);
      }
    }
    // Appliance of operators finished
  }   // End of iteration of result set
  callback(null, newDataSet);
};

aggregationOperators.$project = function (docs, projectObj, callback) {
  var newDataSet = []
    , doc
    , newDoc
    , projectKeys
    , projectKey
    , projectValue
    , projectType
    , tmpObj
    ;
  // Check for exclusion of _id
  var excludeId = false;
  if (typeof projectObj._id !== 'undefined' && !projectObj._id) {
    excludeId = true;
    delete projectObj._id;   // Remove it from projection obj
  }
  projectObj = utils.convertDotToObj(projectObj);   // Convert to normal Doc in case dot notated fields are there
  for (var i = 0, docsLen = docs.length; i < docsLen; i++) {
    doc = docs[i];
    newDoc = {};   // Initialize new doc
    projectKeys = Object.keys(projectObj);
    for (var j = 0, keysLen = projectKeys.length; j<keysLen; j++) {
      projectKey = projectKeys[j];
      projectValue = projectObj[projectKey];
      projectType = typeof projectValue;
      if (!projectValue) return callback(new Error('The top-level _id field is the only field supported for exclusion'));
      else if (projectType === 'number' || projectValue === true) newDoc[projectKey] = doc[projectKey];   // Direct inclusion
      else if (projectType === 'string') {
        if (projectValue.charAt(0) !== '$') return callback(new Error('FieldPath '+projectKey+' doesn\'t start with $'));
        //   Field Path
        tmpObj = utils.getFieldPath(doc, projectValue.substr(1));
        if (typeof tmpObj !== 'undefined') newDoc[projectKey] = tmpObj;
      }
      else {   // Expression Object
        tmpObj = {};
        tmpObj[projectKey] = projectValue;   // Construct temporary expression object
        try{
          tmpObj = expression.apply(tmpObj, doc, true);
        }
        catch(err){
          return callback(err);
        }
        if (tmpObj.hasOwnProperty(projectKey)) newDoc[projectKey] = tmpObj[projectKey];
      }
    }
    if (!excludeId && typeof newDoc._id === 'undefined') newDoc._id = doc._id;
    // newDoc constructed
    newDataSet.push(newDoc);
  }
  callback(null, newDataSet);
};

/**
 * Run the Aggregation pipeline of operators upon the provided dataset
 * @param {Array} dataset The array of objects that will have the aggregation operations applied on
 * @param {Array} pipeline The array that will hold the aggregation operators that should be applied on the provided dataset
 * @param {Function} cb The callback function that will accept the result with signature (err,resultDocsArray)
 */
function exec(dataset, pipeline, cb) {
  async.reduce(pipeline, dataset, function (docs, operator, callback) {
    // Check to find which operator to use
    var operation = Object.keys(operator)[0];   // Get the key of the operator in pipeline to determine operation
    if (!aggregationOperators[operation]) {
      return callback(new Error('Unknown aggregation operator ' + operation + ' used.'));
    }
    else {   // Operation exists, call it
      aggregationOperators[operation](docs, operator[operation], callback);
    }
  }, cb);
}




exports.exec = exec;