package storage.lucene;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoFile {
	protected MongoDirectory directory;
	protected String name;
	
	protected DBObject idQuery;
	
	protected MongoFile() {}
	
	MongoFile( MongoDirectory directory, String name) {
		this.directory = directory;
		this.name = name;
		this.idQuery = new BasicDBObject("_id", name);
		
		DBCollection files = directory.getCollection();
		DBObject results = new BasicDBObject("_id", 1);
		DBObject doc = files.findOne(idQuery, results);
		
		if (doc == null) {
			doc = new BasicDBObject("_id", name);
			doc.put("lastModified", System.currentTimeMillis());
			doc.put("length", 0L);
			doc.put("sizeInBytes", 0L);
			doc.put("numBuffers", 0);
			doc.put("chunks", new Object[0]);
			files.insert(doc, WriteConcern.SAFE);
			return;
		}
	}
	
	MongoFile(MongoDirectory directory, DBObject doc) {
		this.directory = directory;
		name = (String) doc.get("_id");
		this.idQuery = new BasicDBObject("_id", name);
	}
	
	MongoDirectory getDirectory() { return directory; }
	String getName() { return name; }
	
	public synchronized long getLength() { 
		DBObject result = directory.getCollection().findOne(idQuery, new BasicDBObject("length", 1));
		return (Long) result.get("length");

	}
	protected synchronized void setLength (long length) {
		DBObject lengthField = new BasicDBObject("length", length);
		DBObject setter = new BasicDBObject("$set", lengthField);
		directory.getCollection().update(idQuery, setter, false, false, WriteConcern.SAFE);
	}
	
	public synchronized long getLastModified() { 
		DBObject result = directory.getCollection().findOne(idQuery, new BasicDBObject("lastModified", 1));
		return (Long) result.get("lastModified");
	}
	protected synchronized void setLastModified(long lastModified) {
		DBObject lastModifiedField = new BasicDBObject("lastModified", lastModified);
		DBObject setter = new BasicDBObject("$set", lastModifiedField);
		directory.getCollection().update(idQuery, setter, false, false, WriteConcern.SAFE);
	}
	
	protected byte[] addBuffer(int size) {
		byte[] buffer = newBuffer(size);
		synchronized(this) {
			DBObject sizeFields = new BasicDBObject("sizeInBytes", size);
			sizeFields.put("numBuffers", 1);
			DBObject setter = new BasicDBObject("$inc", sizeFields);

			DBObject chunkField = new BasicDBObject("chunks", buffer);
			setter.put("$push", chunkField);
			
			directory.getCollection().update(idQuery, setter, false, false, WriteConcern.SAFE);
		}
		return buffer;
	}
	protected byte[] newBuffer(int size) {
		return new byte[size];
	}
	
	protected synchronized byte[] getBuffer(int index) {
		DBCollection files = directory.getCollection();
		// Explicitly return ID so that only the slice is returned
		DBObject results = new BasicDBObject("_id", 1);
		int[] sliceArray = new int[] {index, 1};
		DBObject slice = new BasicDBObject("$slice", sliceArray);
		results.put("chunks", slice);
		
		@SuppressWarnings("rawtypes")
		List chunk = (List) files.findOne(idQuery, results).get("chunks");
		return (byte[]) chunk.get(0);
	}
	
	protected final synchronized int numBuffers() {
		DBObject field = new BasicDBObject("numBuffers", 1);
		DBObject result = directory.getCollection().findOne(idQuery, field);
		return (Integer) result.get("numBuffers");
	}
	
	protected final synchronized long getSizeInBytes() {
		DBObject field = new BasicDBObject("sizeInBytes", 1);
		DBObject result = directory.getCollection().findOne(idQuery, field);
		return (Long) result.get("sizeInBytes");
	}
	
	public synchronized void delete() {
		directory.getCollection().remove(idQuery);
	}
	

	synchronized void writeBuffer(int index, byte[] data) {
		DBCollection files = directory.getCollection();
		BasicDBObject chunk = new BasicDBObject();
		chunk.put("chunks." + index, data);
		DBObject setter = new BasicDBObject("$set", chunk);
		files.update(idQuery, setter, false, false, WriteConcern.SAFE);
	}
}
