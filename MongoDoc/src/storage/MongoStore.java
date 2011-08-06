package storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Scanner;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoStore {
	Mongo mongo;
	DB db;
	GridFS gfs;
	
	public MongoStore() {
		try {
			mongo = new Mongo("dbh23.mongolab.com:27237");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		db = mongo.getDB("mongodoc");
		System.out.println("Authenticating...");
		if (!db.authenticate("gdighton", new String("3400doc").toCharArray())) {
			throw new RuntimeException("Could not authenticate!");
		}
		System.out.println("Authenticated");
	}
	
	public DB getDB() { return db; }
	public GridFS getFilesystem() {
		if (gfs == null) gfs = new GridFS(db);
		return gfs; 
	}
	
	public void store(String filename, String data) {
		if (gfs == null) gfs = new GridFS(db);
		gfs.remove(filename);
		ByteArrayInputStream bais = new ByteArrayInputStream(data.getBytes());
		GridFSInputFile file = gfs.createFile(bais, filename);
		file.save();
	}
	
	public void store(String filename, InputStream data) {
		if (gfs == null) gfs = new GridFS(db);
		gfs.remove(filename);
		GridFSInputFile file = gfs.createFile(data, filename);
		file.save();
	}
	
	public String retrieveString(String filename) {
		if (gfs == null) gfs = new GridFS(db);
		GridFSDBFile file = gfs.findOne(filename);
		return new Scanner(file.getInputStream()).useDelimiter("\\A").next();
	}
	
	public InputStream retrieveStream(String filename) {
		if (gfs == null) gfs = new GridFS(db);
		return gfs.findOne(filename).getInputStream();
	}
	
	

}
