package storage.lucene;

import java.io.IOException;

import org.apache.lucene.store.Lock;

import storage.MongoStore;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoLock extends Lock {
	String lockName;
	DB db;
	
	
	public MongoLock(String name, DB db) {
		this.db = db;
		this.lockName = name;
	}

	@Override
	public boolean obtain() throws IOException {
		return (Boolean) db.eval("obtainLock(\"" + lockName+ "\");", new Object[0] );
		//return false;
	}

	@Override
	public void release() throws IOException {
		db.eval("releaseLock(\"" + lockName + "\");", new Object[0]);
	}

	@Override
	public boolean isLocked() throws IOException {
		return (Boolean) db.eval("isLocked(\"" + lockName + "\")", new Object[0]);
	}
	
	// Testing
	public static void main(String[] args) throws IOException {
		MongoStore store = new MongoStore();
		DB db = store.getDB();
		DBCollection lockCollection = db.getCollection("lucenelocks");
		MongoLockFactory factory = new MongoLockFactory(db);
		Lock lock = factory.makeLock("test");
		lock.obtain();
		System.out.println("After Locking");
		DBCursor cur = lockCollection.find();
		while(cur.hasNext()) {
			DBObject obj = cur.next();
			System.out.println("\t" + obj.toString());
		}
		if (lock.isLocked()) System.out.println("isLocked: true");
		else System.out.println("isLocked: false");
		lock.release();
		System.out.println("After Release");
		cur = lockCollection.find();
		while(cur.hasNext()) {
			DBObject obj = cur.next();
			System.out.println("\t" + obj.toString());
		}
		if (lock.isLocked()) System.out.println("isLocked: true");
		else System.out.println("isLocked: false");
		
	}

}
