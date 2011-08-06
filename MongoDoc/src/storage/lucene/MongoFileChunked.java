package storage.lucene;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoFileChunked extends MongoFile {
	DBCollection chunkColl;
	protected MongoFileChunked() {}
	
	MongoFileChunked(MongoDirectoryChunked directory, String name) {
		this.directory = directory;
		this.name = name;
		this.idQuery = new BasicDBObject("_id", name);
		this.chunkColl = directory.getChunkCollection();
		
		DBCollection files = directory.getCollection();
		DBObject results = new BasicDBObject("_id", 1);
		DBObject doc = files.findOne(idQuery, results);
		
		if (doc == null) {
			doc = new BasicDBObject("_id", name);
			doc.put("lastModified", System.currentTimeMillis());
			doc.put("length", 0L);
			doc.put("sizeInBytes", 0L);
			doc.put("numBuffers", 0);
			//doc.put("chunks", new Object[0]);
			files.insert(doc, WriteConcern.SAFE);
			return;
		}
		//initialize(doc);
	}
	
	MongoFileChunked(MongoDirectoryChunked directory, DBObject doc) {
		super(directory, doc);
		chunkColl = directory.getChunkCollection();
	}
	
	protected byte[] addBuffer(int size) {
		byte[] buffer = newBuffer(size);
		synchronized(this) {
			DBObject chunk = new BasicDBObject("_id", name + "/" + numBuffers());
			chunk.put("data", buffer);
			chunkColl.insert(chunk, WriteConcern.SAFE);
			
			DBObject sizeFields = new BasicDBObject("sizeInBytes", size);
			sizeFields.put("numBuffers", 1);
			DBObject setter = new BasicDBObject("$inc", sizeFields);

			directory.getCollection().update(idQuery, setter, false, false, WriteConcern.SAFE);
			
		}
		return buffer;
	}
	
	protected synchronized byte[] getBuffer(int index) {
		DBObject query = new BasicDBObject("_id", name + "/" + index);
		DBObject result = chunkColl.findOne(query);
		return (byte[]) result.get("data");
		
	}
	
	synchronized void writeBuffer(int index, byte[] data) {
		DBObject replacement = new BasicDBObject("_id", name + "/" + index);
		replacement.put("data", data);
		chunkColl.save(replacement, WriteConcern.SAFE);
	}
}
