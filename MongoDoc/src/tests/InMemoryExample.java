package tests;
/**
 * A simple example of an in-memory search using Lucene.
 */
import java.io.File;
import java.io.IOException;
import java.io.StringReader;

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

import storage.MongoStore;
import storage.lucene.MongoDirectory;

public class InMemoryExample {

    public static void main(String[] args) throws IOException {
    	MongoStore mongo = new MongoStore();

    	long startTime = System.currentTimeMillis();
        try {
            // Make an writer to create the index
            MongoDirectory idx = new MongoDirectory(mongo.getDB(), "test");
            IndexWriter writer = idx.openIndexWriter();

            // Add some Document objects containing quotes
            writer.addDocument(createDocument("Theodore Roosevelt",
                "It behooves every man to remember that the work of the " +
                "critic, is of altogether secondary importance, and that, " +
                "in the end, progress is accomplished by the man who does " +
                "things."));
            writer.addDocument(createDocument("Friedrich Hayek",
                "The case for individual freedom rests largely on the " +
                "recognition of the inevitable and universal ignorance " +
                "of all of us concerning a great many of the factors on " +
                "which the achievements of our ends and welfare depend."));
            writer.addDocument(createDocument("Ayn Rand",
                "There is nothing to take a man's freedom away from " +
                "him, save other men. To be free, a man must be free " +
                "of his brothers."));
            writer.addDocument(createDocument("Mohandas Gandhi",
                "Freedom is not worth having if it does not connote " +
                "freedom to err."));

            // Optimize and close the writer to finish building the index
            writer.optimize();
            writer.close();

            // Build an IndexSearcher using the in-memory index
            IndexSearcher searcher = new IndexSearcher(idx);

            // Run some queries
            search(searcher, "freedom");
            search(searcher, "free");
            search(searcher, "progress or achievements");

            searcher.close();
            System.out.println("Total time: " + (System.currentTimeMillis() - startTime));

        }
        catch(IOException ioe) {
            // In this example we aren't really doing an I/O, so this
            // exception should never actually be thrown.
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
    private static Document createDocument(String title, String content) {
        Document doc = new Document();

        // Add the title as an unindexed field...
        doc.add(new Field("title", title, Field.Store.YES, Field.Index.NO));

        // ...and the content as an indexed field. Note that indexed
        // Text fields are constructed using a Reader. Lucene can read
        // and index very large chunks of text, without storing the
        // entire content verbatim in the index. In this example we
        // can just wrap the content string in a StringReader.
        doc.add(new Field("content", new StringReader(content)));

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
                queryString + "\" were found in quotes by:");

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
