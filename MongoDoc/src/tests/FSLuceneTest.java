package tests;
/**
 * A simple example of an in-memory search using Lucene.
 */
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.mongodb.Mongo;

import storage.lucene.MongoDirectory;

public class FSLuceneTest {

    public static void main(String[] args) throws IOException {
    	Mongo mongo = new Mongo("liorna");

    	long startTime = System.currentTimeMillis();
        try {
            MongoDirectory idx = new MongoDirectory(mongo.getDB("LuceneTest"), "FSTest");
            IndexWriter writer = idx.openIndexWriter();
        	
        	/*
        	FSDirectory idx = FSDirectory.open(new File("lucene-idx"));
        	IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_33, 
        			new StandardAnalyzer(Version.LUCENE_33));
        	IndexWriter writer = new IndexWriter(idx, cfg);
            */
            
            File dataDir = new File("data");
            for (File f: dataDir.listFiles()) {
            	FileReader reader = new FileReader(f);
            	System.out.println("Indexing: " + f.getName());
            	writer.addDocument(createDocument(f.getName(), reader));
            }


            // Optimize and close the writer to finish building the index
            long startOptimize = System.currentTimeMillis();
            System.out.println("Indexing Time: " + (startOptimize - startTime));

            writer.optimize();
            writer.close();
            System.out.println("Close Time: " + (System.currentTimeMillis() - startOptimize));
            

            // Build an IndexSearcher using the index
            IndexSearcher searcher = new IndexSearcher(idx);

            // Run some queries
            search(searcher, "freedom");
            search(searcher, "free");
            search(searcher, "progress or achievements");

            searcher.close();
            System.out.println("Total time: " + (System.currentTimeMillis() - startTime));

        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        catch(ParseException pe) {
            pe.printStackTrace();
        }
    }

    /**
     * Make a Document object with an un-indexed title field and an
     * indexed content field.
     */
    private static Document createDocument(String title, Reader content) {
        Document doc = new Document();

        // Add the title as an unindexed field...
        doc.add(new Field("title", title, Field.Store.YES, Field.Index.NO));

        // ...and the content as an indexed field. Note that indexed
        // Text fields are constructed using a Reader. Lucene can read
        // and index very large chunks of text, without storing the
        // entire content verbatim in the index. In this example we
        // can just wrap the content string in a StringReader.
        doc.add(new Field("content", content));

        return doc;
    }

    /**
     * Searches for the given string in the "content" field
     */
    private static void search(IndexSearcher searcher, String queryString)
        throws ParseException, IOException {

        // Build a Query object
    	QueryParser parser = new QueryParser(Version.LUCENE_33, "content", 
    			new StandardAnalyzer(Version.LUCENE_33));
        Query query = parser.parse(queryString);

        // Search for the query
        TopDocs hits = searcher.search(query, 100);

        // Examine the Hits object to see if there were any matches
        int hitCount = hits.totalHits;
        if (hitCount == 0) {
            System.out.println(
                "No matches were found for \"" + queryString + "\"");
        }
        else {
            System.out.println("Hits for \"" +
                queryString + "\" were found in files:");

            // Iterate over the Documents in the Hits object
            for (int i = 0; i < hitCount; i++) {
                Document doc = searcher.doc(hits.scoreDocs[i].doc);

                // Print the value that we stored in the "title" field. Note
                // that this Field was not indexed, but (unlike the
                // "contents" field) was stored verbatim and can be
                // retrieved.
                System.out.println("  " + (i + 1) + ". " + doc.get("title"));
            }
        }
        System.out.println();
        
    }
}
