var DB = require('sharedb').DB;
var pg = require('pg');

// Postgres-backed ShareDB database

function PostgresDB(options) {
  if (!(this instanceof PostgresDB)) return new PostgresDB(options);
  DB.call(this, options);

  this.closed = false;

  this.pg_config = options;
  this.pool = new pg.Pool(this.pg_config)
  this.allowJSQueries = false;
  this.allowAggregateQueries = true;

};
module.exports = PostgresDB;

PostgresDB.prototype = Object.create(DB.prototype);

PostgresDB.prototype.close = function(callback) {
  this.closed = true;
  this.pool.end()
  
  if (callback) callback();
};


// Persists an op and snapshot if it is for the next version. Calls back with
// callback(err, succeeded)
PostgresDB.prototype.commit = function(collection, id, op, snapshot, options, callback) {
  /*
   * op: CreateOp {
   *   src: '24545654654646',
   *   seq: 1,
   *   v: 0,
   *   create: { type: 'http://sharejs.org/types/JSONv0', data: { ... } },
   *   m: { ts: 12333456456 } }
   * }
   * snapshot: PostgresSnapshot
   */
    this.pool.connect((err, client, done) => {
      if (err) {
        done(client);
        callback(err);
        return;
      }

    /*
    * This query uses common table expression to upsert the snapshot table 
    * (iff the new version is exactly 1 more than the latest table or if
    * the document id does not exists)
    *
    * It will then insert into the ops table if it is exactly 1 more than the 
    * latest table or it the first operation and iff the previous insert into
    * the snapshot table is successful.
    *
    * This result of this query the version of the newly inserted operation
    * If either the ops or the snapshot insert fails then 0 rows are returned
    *
    * If 0 zeros are return then the callback must return false   
    *
    * Casting is required as postgres thinks that collection and doc_id are
    * not varchar  
    */  
    const query = {
      name: 'sdb-commit-op-and-snap',
      text: `WITH snapshot_id AS (
  INSERT INTO snapshots (collection, doc_id, doc_type, version, data)
  SELECT $1::varchar collection, $2::varchar doc_id, $4 doc_type, $3 v, $5 d
  WHERE $3 = (
    SELECT version+1 v
    FROM snapshots
    WHERE collection = $1 AND doc_id = $2
    FOR UPDATE
  ) OR NOT EXISTS (
    SELECT 1
    FROM snapshots
    WHERE collection = $1 AND doc_id = $2
    FOR UPDATE
  )
  ON CONFLICT (collection, doc_id) DO UPDATE SET version = $3, data = $5, doc_type = $4
  RETURNING version
)
INSERT INTO ops (collection, doc_id, version, operation)
SELECT $1::varchar collection, $2::varchar doc_id, $3 v, $6 operation
WHERE (
  $3 = (
    SELECT max(version)+1
    FROM ops
    WHERE collection = $1 AND doc_id = $2
  ) OR NOT EXISTS (
    SELECT 1
    FROM ops
    WHERE collection = $1 AND doc_id = $2
  )
) AND EXISTS (SELECT 1 FROM snapshot_id)
RETURNING version`,
      values: [collection,id,snapshot.v, snapshot.type, snapshot.data,op]
    }
    client.query(query, (err, res) => {
      if (err) {
        callback(err)
      } else if(res.rows.length === 0) {
        done(client);
        callback(null,false)
      } 
      else {
        done(client);
        callback(null,true)
      }
    })
    
    })
};

// Get the named document from the database. The callback is called with (err,
// snapshot). A snapshot with a version of zero is returned if the docuemnt
// has never been created in the database.
PostgresDB.prototype.getSnapshot = function(collection, id, fields, options, callback) {
  this.pool.connect(function(err, client, done) {
    if (err) {
      done(client);
      callback(err);
      return;
    }
    client.query(
      'SELECT version, data, doc_type FROM snapshots WHERE collection = $1 AND doc_id = $2 LIMIT 1',
      [collection, id],
      function(err, res) {
        done();
        if (err) {
          callback(err);
          return;
        }
        if (res.rows.length) {
          var row = res.rows[0]
          var snapshot = new PostgresSnapshot(
            id,
            row.version,
            row.doc_type,
            row.data,
            undefined // TODO: metadata
          )
          callback(null, snapshot);
        } else {
          var snapshot = new PostgresSnapshot(
            id,
            0,
            null,
            undefined,
            undefined
          )
          callback(null, snapshot);
        }
      }
    )
  })
};

// Get operations between [from, to) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);
PostgresDB.prototype.getOps = function(collection, id, from, to, options, callback) {
  this.pool.connect(function(err, client, done) {
    if (err) {
      done(client);
      callback(err);
      return;
    }
    client.query(
      'SELECT version, operation FROM ops WHERE collection = $1 AND doc_id = $2 AND version >= $3 AND version < $4',
      [collection, id, from, to],
      function(err, res) {
        done();
        if (err) {
          callback(err);
          return;
        }
        callback(null, res.rows.map(function(row) {
          return row.operation;
        }));
      }
    )
  })
};

function PostgresSnapshot(id, version, type, data, meta) {
  this.id = id;
  this.v = version;
  this.type = type;
  this.data = data;
  this.m = meta;
}

//Query support
//most of this is stolen from https://github.com/share/sharedb-mongo

//given the ShareDB Query object generate the equivalent sql
function convertToSQL(parsedQuery) {
  //todo: make sure it escapes correctly
  console.log(parsedQuery)
  var table_name = 'snapshots'
  var sql = 'SELECT collection,doc_id,doc_type,version,data from '+ table_name + ' where ';
  var where = 'collection = $1 and '

  for (var j = 0; j < parsedQuery['query']['$or'].length; j++){
    if(j === 0) {
      where += '('
    }
    where += 'data @> \'' + JSON.stringify(parsedQuery['query']['$or'][j]) + '\''
    if(j != parsedQuery['query']['$or'].length-1) {
     where += ' OR ' 
    } else 
     {
      where += ')' 
     }
  }
  
  return sql + where;
}


PostgresDB.prototype.query = function(collectionName, inputQuery, fields, options, callback) {
	var query = this._getSafeParsedQuery(inputQuery,(err, results, extra) => {
		callback(err, results, extra)
	})
	// TODO: this should be refracted and shared with getSnapshot
	this.pool.connect(function(err, client, done) {
    if (err) {
      done(client);
      callback(err);
      return;
    }
    client.query(
      convertToSQL(query),
      [collectionName],
      function(err, res) {
        done();
        if (err) {
          callback(err);
          return;
        }
        var results = []
        if (res.rows.length) {
         for(var i = 0; i < res.rows.length; i++) {
          var row = res.rows[i]
          results.push(new PostgresSnapshot(
            row.doc_id,
            row.version,
            row.doc_type,
            row.data,
            undefined // TODO: metadata
          ))
     	  }
          callback(null, results);
        } else {
          
          callback(null, results);
        }
      }
    )
  })

}

PostgresDB.prototype.checkQuery = function(query) {
  if (query.$query) {
    return PostgresDB.$queryDeprecatedError();
  }

  var validMongoErr = checkValidMongo(query);
  if (validMongoErr) return validMongoErr;

  if (!this.allowJSQueries) {
    if (query.$where != null) {
      return PostgresDB.$whereDisabledError();
    }
    if (query.$mapReduce != null) {
      return PostgresDB.$mapReduceDisabledError();
    }
  }

  if (!this.allowAggregateQueries && query.$aggregate) {
    return PostgresDB.$aggregateDisabledError();
  }
};

// Check that any keys starting with $ are valid Mongo methods. Verify
// that:
// * There is at most one collection operation like $mapReduce
// * If there is a collection operation then there are no cursor methods
// * There is at most one cursor operation like $count
//
// Return {code: ..., message: ...} on error.
function checkValidMongo(query) {
  var collectionOperationKey = null; // only one allowed
  var foundCursorMethod = false; // transform or operation
  var cursorOperationKey = null; // only one allowed

  for (var key in query) {
    if (key[0] === '$') {
      if (collectionOperationsMap[key]) {
        // Found collection operation. Check that it's unique.

        if (collectionOperationKey) {
          return PostgresDB.onlyOneCollectionOperationError(
            collectionOperationKey, key
          );
        }
        collectionOperationKey = key;
      } else if (cursorOperationsMap[key]) {
        if (cursorOperationKey) {
          return PostgresDB.onlyOneCursorOperationError(
            cursorOperationKey, key
          );
        }
        cursorOperationKey = key;
        foundCursorMethod = true;
      } else if (cursorTransformsMap[key]) {
        foundCursorMethod = true;
      }
    }
  }

  if (collectionOperationKey && foundCursorMethod) {
    return PostgresDB.cursorAndCollectionMethodError(
      collectionOperationKey
    );
  }

  return null;
}

function ParsedQuery(
  query,
  collectionOperationKey,
  collectionOperationValue,
  cursorTransforms,
  cursorOperationKey,
  cursorOperationValue
) {
  this.query = query;
  this.collectionOperationKey = collectionOperationKey;
  this.collectionOperationValue = collectionOperationValue;
  this.cursorTransforms = cursorTransforms;
  this.cursorOperationKey = cursorOperationKey;
  this.cursorOperationValue = cursorOperationValue;
}

// Parses a query and makes it safe against deleted docs. On error,
// call the callback and return null.
PostgresDB.prototype._getSafeParsedQuery = function(inputQuery, callback) {
  var err = this.checkQuery(inputQuery);
  if (err) {
    callback(err);
    return null;
  }

  try {
    var parsed = parseQuery(inputQuery);
  } catch (err) {
    err = ShareDbMongo.parseQueryError(err);
    callback(err);
    return null;
  }

  makeQuerySafe(parsed.query);
  return parsed;
};

function parseQuery(inputQuery) {
  // Parse sharedb-mongo query format into an object with these keys:
  // * query: The actual mongo query part of the input query
  // * collectionOperationKey, collectionOperationValue: Key and value of the
  //   single collection operation (eg $mapReduce) defined in the input query,
  //   or null
  // * cursorTransforms: Map of all the cursor transforms in the input query
  //   (eg $sort)
  // * cursorOperationKey, cursorOperationValue: Key and value of the single
  //   cursor operation (eg $count) defined in the input query, or null
  //
  // Examples:
  //
  // parseQuery({foo: {$ne: 'bar'}, $distinct: {field: 'x'}}) ->
  // {
  //   query: {foo: {$ne: 'bar'}},
  //   collectionOperationKey: '$distinct',
  //   collectionOperationValue: {field: 'x'},
  //   cursorTransforms: {},
  //   cursorOperationKey: null,
  //   cursorOperationValue: null
  // }
  //
  // parseQuery({foo: 'bar', $limit: 2, $count: true}) ->
  // {
  //   query: {foo: 'bar'},
  //   collectionOperationKey: null,
  //   collectionOperationValue: null
  //   cursorTransforms: {$limit: 2},
  //   cursorOperationKey: '$count',
  //   cursorOperationValue: 2
  // }

  var query = {};
  var collectionOperationKey = null;
  var collectionOperationValue = null;
  var cursorTransforms = {};
  var cursorOperationKey = null;
  var cursorOperationValue = null;

  if (inputQuery.$query) {
    throw new Error("unexpected $query: should have called checkQuery");
  } else {
    for (var key in inputQuery) {
      if (collectionOperationsMap[key]) {
        collectionOperationKey = key;
        collectionOperationValue = inputQuery[key];
      } else if (cursorTransformsMap[key]) {
        cursorTransforms[key] = inputQuery[key];
      } else if (cursorOperationsMap[key]) {
        cursorOperationKey = key;
        cursorOperationValue = inputQuery[key];
      } else {
        query[key] = inputQuery[key];
      }
    }
  }

  return new ParsedQuery(
    query,
    collectionOperationKey,
    collectionOperationValue,
    cursorTransforms,
    cursorOperationKey,
    cursorOperationValue
  );
};
PostgresDB._parseQuery = parseQuery;

function makeQuerySafe(query) {
  // Don't modify the query if the user explicitly sets _type already
  if (query.hasOwnProperty('_type')) return;
  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. When docs are deleted, their data
  // properties are cleared and _type is set to null. Filter out deleted docs
  // by requiring that _type is a string if the query does not naturally
  // restrict the results with other keys
  if (deletedDocCouldSatisfyQuery(query)) {
    query._type = {$type: 2};
  }
};
PostgresDB._makeQuerySafe = makeQuerySafe; // for tests

function deletedDocCouldSatisfyQuery(query) {
  // Any query with `{foo: value}` with non-null `value` will never
  // match deleted documents (that are empty other than the `_type`
  // field).
  //
  // This generalizes to additional classes of queries. Here’s a
  // recursive description of queries that can't match a deleted doc:
  // In general, a query with `{foo: X}` can't match a deleted doc
  // if `X` is guaranteed to not match null or undefined. In addition
  // to non-null values, the following clauses are guaranteed to not
  // match null or undefined:
  //
  // * `{$in: [A, B, C]}}` where all of A, B, C are non-null.
  // * `{$ne: null}`
  // * `{$exists: true}`
  // * `{$gt: not null}`, `{gte: not null}`, `{$lt: not null}`, `{$lte: not null}`
  //
  // In addition, some queries that have `$and` or `$or` at the
  // top-level can't match deleted docs:
  // * `{$and: [A, B, C]}`, where at least one of A, B, C are queries
  //   guaranteed to not match `{_type: null}`
  // * `{$or: [A, B, C]}`, where all of A, B, C are queries guaranteed
  //   to not match `{_type: null}`
  //
  // There are more queries that can't match deleted docs but they
  // aren’t that common, e.g. ones using `$type` or bit-wise
  // operators.
  if (query.hasOwnProperty('$and')) {
    if (Array.isArray(query.$and)) {
      for (var i = 0; i < query.$and.length; i++) {
        if (!deletedDocCouldSatisfyQuery(query.$and[i])) {
          return false;
        }
      }
      return true;
    } else {
      // Malformed? Play it safe.
      return true;
    }
  }

  if (query.hasOwnProperty('$or')) {
    if (Array.isArray(query.$or)) {
      for (var i = 0; i < query.$or.length; i++) {
        if (deletedDocCouldSatisfyQuery(query.$or[i])) {
          return true;
        }
      }
      return false;
    } else {
      // Malformed? Play it safe.
      return true;
    }
  }

  for (var prop in query) {
    // Ignore fields that remain set on deleted docs
    if (
      prop === '_id' ||
      prop === '_v' ||
      prop === '_o' ||
      prop === '_m' || (
        prop[0] === '_' &&
        prop[1] === 'm' &&
        prop[2] === '.'
      )
    ) {
      continue;
    }
    // When using top-level operators that we don't understand, play
    // it safe
    if (prop[0] === '$') {
      return true;
    }
    if (!couldMatchNull(query[prop])) {
      return false;
    }
  }

  return true;
}

function couldMatchNull(clause) {
  if (
    typeof clause === 'number' ||
    typeof clause === 'boolean' ||
    typeof clause === 'string'
  ) {
    return false;
  } else if (clause === null) {
    return true;
  } else if (isPlainObject(clause)) {
    // Mongo interprets clauses with multiple properties with an
    // implied 'and' relationship, e.g. {$gt: 3, $lt: 6}. If every
    // part of the clause could match null then the full clause could
    // match null.
    for (var prop in clause) {
      var value = clause[prop];
      if (prop === '$in' && Array.isArray(value)) {
        var partCouldMatchNull = false;
        for (var i = 0; i < value.length; i++) {
          if (value[i] === null) {
            partCouldMatchNull = true;
            break;
          }
        }
        if (!partCouldMatchNull) {
          return false;
        }
      } else if (prop === '$ne') {
        if (value === null) {
          return false;
        }
      } else if (prop === '$exists') {
        if (value) {
          return false;
        }
      } else if (prop === '$gt' || prop === '$gte' || prop === '$lt' || prop === '$lte') {
        if (value !== null) {
          return false;
        }
      } else {
        // Not sure what to do with this part of the clause; assume it
        // could match null.
      }
    }

    // All parts of the clause could match null.
    return true;
  } else {
    // Not a POJO, string, number, or boolean. Not sure what it is,
    // but play it safe.
    return true;
  }
}


var collectionOperationsMap = {
  '$distinct': function(collection, query, value, cb) {
    //collection.distinct(value.field, query, cb);
  },
  '$aggregate': function(collection, query, value, cb) {
    //collection.aggregate(value, cb);
  },
  '$mapReduce': function(collection, query, value, cb) {
   /* if (typeof value !== 'object') {
      var err = ShareDbMongo.malformedQueryOperatorError('$mapReduce');
      return cb(err);
    }
    var mapReduceOptions = {
      query: query,
      out: {inline: 1},
      scope: value.scope || {}
    };
    collection.mapReduce(
      value.map, value.reduce, mapReduceOptions, cb);
  */}
};

var cursorOperationsMap = {
  '$count': function(cursor, value, cb) {
  },
  '$explain': function(cursor, verbosity, cb) {
  },
  '$map': function(cursor, fn, cb) {
  }
};

var cursorTransformsMap = {
  '$batchSize': function(cursor, size) {  },
  '$comment': function(cursor, text) { },
  '$hint': function(cursor, index) { },
  '$max': function(cursor, value) {  },
  '$maxScan': function(cursor, value) {  },
  '$maxTimeMS': function(cursor, milliseconds) {
    
  },
  '$min': function(cursor, value) {  },
  '$noCursorTimeout': function(cursor) {
    // no argument to cursor method
    
  },
  '$orderby': function(cursor, value) {
    console.warn('Deprecated: $orderby; Use $sort.');
    
  },
  '$readConcern': function(cursor, level) {
    
  },
  '$readPref': function(cursor, value) {

    
  },
  '$returnKey': function(cursor) {
    // no argument to cursor method
    
  },
  '$snapshot': function(cursor) {
   },
  '$sort': function(cursor, value) {  },
  '$skip': function(cursor, value) { },
  '$limit': function(cursor, value) { },
  '$showDiskLoc': function(cursor, value) {
    console.warn('Deprecated: $showDiskLoc; Use $showRecordId.');
   
  },
  '$showRecordId': function(cursor) {
    // no argument to cursor method
    
  }
};

PostgresDB.$queryDeprecatedError = function() {
  return {code: 4106, message: '$query property deprecated in queries'};
};
PostgresDB.malformedQueryOperatorError = function(operator) {
  return {code: 4107, message: "Malformed query operator: " + operator};
};
PostgresDB.onlyOneCollectionOperationError = function(operation1, operation2) {
  return {
    code: 4108,
    message: 'Only one collection operation allowed. ' +
      'Found ' + operation1 + ' and ' + operation2
  };
};
PostgresDB.onlyOneCursorOperationError = function(operation1, operation2) {
  return {
    code: 4109,
    message: 'Only one cursor operation allowed. ' +
      'Found ' + operation1 + ' and ' + operation2
  };
};
PostgresDB.cursorAndCollectionMethodError = function(collectionOperation) {
  return {
    code: 4110,
    message: 'Cursor methods can\'t run after collection method ' +
      collectionOperation
  };
};
