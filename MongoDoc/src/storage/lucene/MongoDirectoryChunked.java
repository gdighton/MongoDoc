package storage.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDirectoryChunked extends MongoDirectory {
	private DBCollection chunkColl;
	
	DBCollection getChunkCollection() {
		return chunkColl;
	}

	public MongoDirectoryChunked(DB db, String dirname) throws IOException {
		super(db, dirname);
		chunkColl = db.getCollection("lucene." + dirname + ".chunks");
	}

	protected MongoFile getFile(String name) throws IOException {
		DBObject doc = getDoc(name);
		if (doc == null) {
			throw new FileNotFoundException(name);
		}
		return new MongoFileChunked(this, doc);		
	}

	protected MongoFile createFile(String name) throws IOException {
		return new MongoFileChunked(this, name);
	}
}
