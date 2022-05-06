const {getConnection} = require("./connector");
const {transformIdToObjectId} = require("./util");
const {ObjectID} = require("mongodb");


/**
 * @class Mongo
 * @description Handles database operations, called via handler. This is like a generic that
 * is used through the project to perform basic operations on the database.
 */
class Mongo {
  /**
   * Runs the MongoDB find() method to fetch documents.
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of the collection to run operation on
   * @param {document} query Specifies selection filter using query operators.
   * To return all documents in a collection, omit this parameter or pass an empty document ({}).
   * @param {boolean} [transform=false] check to transform the IDs to ObjectID in response
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/ Read MongoDB Reference}
   */
  static async find(database, collectionName, query, transform = true, projection) {
    try {
      query = transformIdToObjectId(query);

      const collection = getConnection(database).collection(collectionName);

      const data = projection ? await collection.find(query, projection).toArray():await collection.find(query).toArray();

      /** allow caller method to toggle response transformation */
      if (transform) {
        data.forEach((x) => {

            x["_id"] = {
            $oid: x["_id"],
          };
        });
      }

      return data;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }
  /**
   * Runs a distinct find operation based on given query
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of the collection to run operations on
   * @param {string} upon Field for which to return distinct values.
   * @param {Document} query A query that specifies the documents from
   * which to retrieve the distinct values.
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.distinct Read MongoDB Reference}
   */
  static async distinct(database, collectionName, upon, query) {
    try {
      const collection = getConnection(database).collection(collectionName);
      const data = await collection.distinct(upon, query);
      return data;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

  /**
   * Runs insertion operation to create an array of new documents
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of collection to run operation on
   * @param {Array<document>} data Array of documents to insert into collection
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/  Read MongoDB Reference}
   */
  static async add(database, collectionName, data) {
    /** if not an array, transform into array */
    if (!Array.isArray(data)) {
      data = [data];
    }

    try {
      const collection = getConnection(database).collection(collectionName);
      const res = await collection.insertMany(data);
      return res;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

 /**
   * Runs the MongoDB find() method to fetch specific slide documents and uses information 
   * from those slides to run an insertion operation to create copies as new documents.
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of the collection to run operation on
   * @param {document} query Specifies selection filter using query operators. Will refer to a specific slide id,
   * but will also have information on the names of the batches that will be used to create a new query.
   * @param {boolean} [transform=false] check to transform the IDs to ObjectID in response
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/ Read MongoDB Reference}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/  Read MongoDB Reference}
   *
   */
  static async duplicateSlide(database, collectionName, query, transform = true, projection) {
    try {

      query = transformIdToObjectId(query);

      // Extract the name of the batch to which duplicate will be added.
      var batch =  query.batch;
      // Extract the name of the batch to which all slides belong, our "All Slides" batch
      var all_batch = query.all_batch;

      // Create query that will be used in find operation to find our desired slide document
      query = {
        '_id' : query._id,
      }

      const collection = getConnection(database).collection(collectionName);

      // execute find to retrieve slide document
      const data = projection ? await collection.find(query, projection).toArray():await collection.find(query).toArray();
      
      // Using document of the slide returned by the find, process document to create the duplicate 
      await Promise.all(data.map( async(x) => {
        // create a soft copy of the document (essentially just the json info)
        var newDoc = x;
        // save old id
        var old_id = x._id;

        // Create new field that keeps track from which slide the duplicate was copied from
        newDoc.prev_slide_id = old_id;
        // Delete id in the new document so the mongoDB will assign a new unique ID
        delete newDoc._id
        
        newDoc.create_date = new Date();
        // replace the previous collections (batches) field with our new relevant values
        newDoc.collections = [batch, all_batch]; 
        // make the doc an array (necessary for the insertion operation)
        var arr_newDoc = [newDoc]
        var new_id;
        // execute insertion operation
        await collection.insertMany(arr_newDoc, function (err,arr_newDoc) {
            if(err!=null){
                return console.log(err);
            }
        });

      }));

      return data;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

 /**
   * Runs the MongoDB find() method to fetch specific ROI documents and uses information 
   * from those documents to run an insertion operation to create copies as new documents.
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of the collection to run operation on
   * @param {document} query Specifies selection filter using query operators. Will refer to a specific slide id
   * that will be used to find ROI documents that refer to that Id, but will also have information on the names 
   * of the batches that will be used to create a new query.
   * @param {boolean} [transform=false] check to transform the IDs to ObjectID in response
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/ Read MongoDB Reference}
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/  Read MongoDB Reference}
   *
   */
  static async duplicateROIs(database, collectionName, query, transform = true, projection) {
    try {

      query = transformIdToObjectId(query);

      // id of the new slide that was created and to which ROIs will be assigned
      var new_id =  query.new_slide;
      // id of the batch to which slides will belong. Could be currently misattributed 
      var creator = query.batch_name;

      // extracting id of original slide that has ROIs and creating query to find those ROIs
      query = {
        'provenance.image.slide' : query.prev_slide_id,
      }

      const collection = getConnection(database).collection(collectionName);

      // executing find operation
      const data = projection ? await collection.find(query, projection).toArray():await collection.find(query).toArray();

      // Using documents of the ROIs returned by the find, process documents to create the duplicates 
      await Promise.all(data.map( async(roi_doc) => {
        // soft copy of ROI document
        var new_roi_doc = roi_doc;
        // current format has IDs of ROIs as Strings and not ObjectID's, so assinging that ID here
        new_roi_doc._id = String(new ObjectID());
        // updating data
        new_roi_doc.create_date = new Date(); 
        // updating slide id of new ROI to new slide id
        new_roi_doc.provenance.image.slide = new_id;
        // updating creator, again, could be currently missattributed 
        new_roi_doc.creator = creator;
        // new ROI should now have any annotations
        new_roi_doc.annotations = []
        // making document an array as needed by insertion operation
        new_roi_doc = [new_roi_doc]
        // executing insertion operation
        await collection.insertMany(new_roi_doc, function (err,new_roi_doc) {
            if(err!=null){
                return console.log(err);
            }
        });

      }));
      
      return data;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

  

  /**
   * Runs the delete operation on the first document that satisfies the filter conditions
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of collection to run operation on
   * @param {document} query Specifies deletion criteria using query operators
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.deleteOne/ Read MongoDB Reference}
   */
  static async delete(database, collectionName, filter) {
    try {
      filter = transformIdToObjectId(filter);

      const collection = getConnection(database).collection(collectionName);
      const result = await collection.deleteMany(filter);
      delete result.connection;

      return result;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

  /**
   * Runs aggregate operation on given pipeline
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName Name of collection to run operation on
   * @param {Array} pipeline Array containing all the aggregation framework commands for the execution.
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.aggregate/ Read MongoDB Reference}
   */
  static async aggregate(database, collectionName, pipeline) {
    try {
      const collection = getConnection(database).collection(collectionName);
      const result = await collection.aggregate(pipeline).toArray();
      return result;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }

  /**
   * Runs updateOne operation on documents that satisfy the filter condition.
   *
   * @async
   * @param {string} database Name of the database
   * @param {string} collectionName name of collection to run operation on
   * @param {document} filter selection criteria for the update
   * @param {document|pipeline} updates modifications to apply to filtered documents,
   * can be a document or a aggregation pipeline
   *
   * {@link https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/ Read MongoDB Reference}
   */
  static async update(database, collectionName, filter, updates) {
    try {
      filter = transformIdToObjectId(filter);

      const collection = await getConnection(database).collection(collectionName);
      const result = await collection.updateMany(filter, updates);
      delete result.connection;
      return result;
    } catch (e) {
      console.error(e);
      throw e;
    }
  }
}

/** export to be import using the destructuring syntax */
module.exports = {
  add: Mongo.add,
  find: Mongo.find,
  update: Mongo.update,
  delete: Mongo.delete,
  aggregate: Mongo.aggregate,
  distinct: Mongo.distinct,
  duplicateSlide: Mongo.duplicateSlide,
  duplicateROIs: Mongo.duplicateROIs,
};
