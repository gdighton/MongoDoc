package storage.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoDirectory extends Directory {
	private String dirname;
	private DBCollection fileColl;
	private IndexWriter _writer;
	
	public String getName() { return dirname; }
	DBCollection getCollection() {
		return fileColl;
	}

	public MongoDirectory(DB db, String dirname) throws IOException {
		this.dirname = dirname;
		fileColl = db.getCollection("lucene." + dirname);
		fileColl.setWriteConcern(WriteConcern.SAFE);
		
		setLockFactory(new MongoLockFactory(db));
	}

	@Override
	public String[] listAll() throws IOException {
		ensureOpen();
		DBCursor cur = fileColl.find(new BasicDBObject(), new BasicDBObject("_id", 1));
		ArrayList<String> list = new ArrayList<String>();
		while (cur.hasNext()) {
			list.add((String)cur.next().get("_id"));
		}
		return list.toArray(new String[list.size()]);
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		ensureOpen();
		return (getDoc(name) != null);
	}
	
	protected DBObject getDoc(String name) {
		DBObject query = new BasicDBObject("_id", name);
		DBObject result = new BasicDBObject("_id", 1);
		return fileColl.findOne(query, result);
	}
	
	protected MongoFile getFile(String name) throws IOException {
		DBObject doc = getDoc(name);
		if (doc == null) {
			throw new FileNotFoundException(name);
		}
		return new MongoFile(this, doc);		
	}
	
	protected MongoFile createFile(String name) throws IOException {
		return new MongoFile(this, name);
	}

	@Override
	public long fileModified(String name) throws IOException {
		ensureOpen();
		return getFile(name).getLastModified();
	}

	@Override
	public void touchFile(String name) throws IOException {
		ensureOpen();
		MongoFile file = getFile(name);
		file.setLastModified(System.currentTimeMillis());
	}

	@Override
	public void deleteFile(String name) throws IOException {
		ensureOpen();
		getFile(name).delete();

	}

	@Override
	public long fileLength(String name) throws IOException {
		ensureOpen();
		return getFile(name).getLength();
	}

	@Override
	public IndexOutput createOutput(String name) throws IOException {
		ensureOpen();
		
		if (fileExists(name)) {
			deleteFile(name);
		}
		MongoFile file = createFile(name);
		return new MongoOutputStream(file);
	}

	@Override
	public IndexInput openInput(String name) throws IOException {
		ensureOpen();
		MongoFile file = getFile(name);
		return new MongoInputStream(file);
	}
	
	public IndexWriter openIndexWriter() throws CorruptIndexException, LockObtainFailedException, IOException {
		// Return cached writer if available
		if (_writer != null) return _writer;
    	IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_33, 
    			new StandardAnalyzer(Version.LUCENE_33));
    	LogByteSizeMergePolicy mpolicy= new LogByteSizeMergePolicy();
    	mpolicy.setMaxMergeMB(4);
    	mpolicy.setMaxMergeMBForOptimize(4);
    	mpolicy.setUseCompoundFile(false);
    	cfg.setMergePolicy(mpolicy);
        _writer = new IndexWriter(this, cfg);
        return _writer;
		
	}

	@Override
	public void close() throws IOException {
		isOpen = false;
		_writer = null;

	}

}
