package com.ideal.flume.stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;

@SuppressWarnings("deprecation")
public class MongoStatWriter extends StatWriter implements Closeable {
  private final Mongo mongo;
  private final DBCollection dbCollection;

  /**
   * @param mongodbUrl mongodb://user:password@hostname:port/db.collection
   */
  public MongoStatWriter(String mongodbUrl) {
    int i = mongodbUrl.lastIndexOf('/');
    if (i == -1) {
      throw new IllegalArgumentException("error mongodb url.");
    }
    String mongoUri = mongodbUrl.substring(0, i);

    String str = mongodbUrl.substring(i + 1);
    i = str.indexOf('.');
    if (i == -1) {
      throw new IllegalArgumentException("error mongodb url.");
    }

    String dbName = str.substring(0, i);
    String tableName = str.substring(i + 1);

    this.mongo = new MongoClient(new MongoClientURI(mongoUri));
    this.dbCollection = mongo.getDB(dbName).getCollection(tableName);
    this.dbCollection.setWriteConcern(WriteConcern.SAFE);
  }

  @Override
  public void write(List<Stat> stats) {
    List<DBObject> dbObjs = Lists.newArrayList();
    for (Stat stat : stats) {
      DBObject dbObj = stat2DbObj(stat);
      if (null != dbObj) {
        dbObjs.add(dbObj);
      }
    }
    this.dbCollection.insert(dbObjs);
  }

  @Override
  public void write(Stat stat) {
    DBObject dbObj = stat2DbObj(stat);
    if (null != dbObj) {
      this.dbCollection.insert(dbObj);
    }
  }

  @Override
  public void write(String stat) {
    DBObject dbObj = stat2DbObj(stat);
    if (null != dbObj) {
      this.dbCollection.insert(dbObj);
    }
  }

  private DBObject stat2DbObj(Stat stat) {
    if (null == stat) {
      return null;
    }
    DBObject ret = stat2DbObj(stat2Json(stat));
    ret.put("timestamp", stat.getTimestamp());
    return ret;
  }

  private DBObject stat2DbObj(String stat) {
    if (null == stat || stat.isEmpty()) {
      return null;
    }
    return BasicDBObject.parse(stat);
  }

  @Override
  public void close() throws IOException {
    this.mongo.close();
  }
}
