package storage.lucene;

import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoLockFactory extends LockFactory {
	private static final String obtainJS = 
		"db.system.js.save({ _id: \"obtainLock\", " +
		"	value: function(name) {" +
		"		var lock = { _id : name };" +
		"		var isLocked = (db.luceneLocks.findOne(lock)) != null; " +
		"		if (isLocked == false) {" +
		"			db.luceneLocks.save(lock);" +
		"			return true;" +
		"		}" +
		"		return false;" +
		"	}" +
		"});";
	private static final String releaseJS =
		"db.system.js.save({ _id: \"releaseLock\", " +
		"	value: function (name) {" +
		"		var lock = { _id : name };" +
		"		db.luceneLocks.remove(lock);" +
		"	}" +
		"});";
	private static final String isLockedJS = 
		"db.system.js.save({ _id: \"isLocked\", " +
		"	value: function(name) {" +
		"		var lock = { _id : name };" +
		"		var isLocked = (db.luceneLocks.findOne(lock)) != null;" +
		"		return isLocked;" +
		"	}" +
		"});";

	private static void initializeJS(DB db) {
		DBCollection jsCollection = db.getCollection("system.js");
		BasicDBObject jsFunc = new BasicDBObject("_id", "obtainLock");
		if (jsCollection.findOne(jsFunc) != null) return;
		db.eval(obtainJS, new Object[0]);
		db.eval(releaseJS, new Object[0]);
		db.eval(isLockedJS, new Object[0]);
	}
	
	DB db;
	String prefix;
	
	MongoLockFactory(DB db, String prefix) {
		this.db = db;
		this.prefix = prefix +"/";
		initializeJS(db);
	}

	@Override
	public Lock makeLock(String lockName) {
		return new MongoLock(prefix + lockName, db);
	}

	@Override
	public void clearLock(String lockName) throws IOException {
		db.eval("releaseLock(" + prefix + lockName + ");", new Object[0]);
	}

}
