import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class dataBaseIO {

    public static void main(String[] args) {
        List<Document> documents = new ArrayList<Document>(); // Creating a Document ArrayList named "documents" where we will save our documents.

        MongoClient mongoClient = MongoClients.create();    // Creating a mongoClient to connect with Mongodb
        MongoDatabase database = mongoClient.getDatabase("lab3"); // Creating our new database through our mongoClient (database : "lab3")
        MongoCollection<Document> coll = database.getCollection("restaurants"); // Creating our new collection in our new database (collection : "restaurants")
        Block<Document> printBlock = new Block<Document>() { // Creating a "printBlock" method that will identify and print out our documents on blocks in Json format.
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };


        Document doc1 = new Document("_id", "5c39f9b5df831369c19b6bca")   //Creating our new documents and appending their fields and values for our collection("restaurants").
                .append("name"," Sun Bakery Trattoria")                   //With other words, creating our database of "restaurants" and their details on our collection.
                .append("stars", 4)
                .append("categories", Arrays.asList("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches"));

        Document doc2 = new Document("_id", "5c39f9b5df831369c19b6bcb")
                .append("name"," Blue Bagels Grill ")
                .append("stars", 3)
                .append("categories", Arrays.asList("Bagels", "Cookies", "Sandwiches"));

        Document doc3 = new Document("_id", "5c39f9b5df831369c19b6bcc")
                .append("name"," Hot Bakery Cafe")
                .append("stars", 4)
                .append("categories", Arrays.asList("Bakery", "Cafe", "Coffee", "Dessert"));

        Document doc4 = new Document("_id", "5c39f9b5df831369c19b6bcd")
                .append("name", " XYZ Coffee Bar")
                .append("stars", 5)
                .append("categories", Arrays.asList("Coffee", "Cafe", "Bakery", "Chocolates"));

        Document doc5 = new Document("_id", "5c39f9b5df831369c19b6bce")
                .append("name", " 456 Cookies Shop")
                .append("stars", 4)
                .append("categories", Arrays.asList("Bakery","Cookies","Cake", "Coffee"));


        documents.add(doc1);
        documents.add(doc2);
        documents.add(doc3);        // Adding our created documents to our "documents" ArrayList.
        documents.add(doc4);
        documents.add(doc5);


        coll.insertMany(documents); // Inserting our documents on our ArrayList to our created collection of documents ("restaurants") : 'coll', using the "insertMany()" mongodb command.
        System.out.println("");
        System.out.println("ALL DOCUMENTS:\n");
        coll.find().forEach(printBlock); // Printing all block of documents in  our collection using "find()"  mongodb command.
        System.out.println("");

        System.out.println("ALL DOCUMENTS WITH  \"categories\":\"Coffee\":\n ");
        coll.find(eq("categories", "Cafe"))     // Finding documents with "categories":"Coffee"  from our collection and projecting from them only "name" field, using "find()" mongodb command.
                .projection(fields(include("name"),excludeId()))
                .forEach(printBlock); // Printing documents with the matched criteria.
        System.out.println("");

        System.out.println("\"stars\" FIELD INCREASED WITH 1 ON \"XYZ Coffee Bar\":\n ");
        coll.updateOne(                                         // Updating document with "name":" XYZ Coffee Bar " by increasing its number of "stars" by one, using "updateOne" mongodb command.
                eq("name"," XYZ Coffee Bar"),
                Updates.inc("stars",1)
        );
        coll.find().forEach(printBlock); // Printing all documents in our collection to show the update we just did.
        System.out.println("");

        System.out.println("UPDATED DOCUMENT (REPLACED \"name\": \"456 Cookies Shop\" TO \"123 Cookies Heaven\"):\n");
        coll.updateOne(                                         // Updating document with "name":"456 Cookies Shop" by renaming it to "123 Cookies Heaven", using "updateOne" mongodb command.
                eq("name"," 456 Cookies Shop"),
                Updates.set("name"," 123 Cookies Heaven")
        );
        coll.find().forEach(printBlock); // Printing all documents in our collection to show the update we just did.
        System.out.println("");

        System.out.println("GROUPED DOCUMENTS WITH \"stars\" FIELD VALUE GREATER OR EQUAL TO 4:\n");
        coll.aggregate(    // Creating a list by aggregation with the documents that have a "stars" value greater than 4 and projecting from them their "name" field and "stars" field, using "aggregate()" mongodb command.
                Arrays.asList(
                        Aggregates.match(Filters.gte("stars", 4)),
                        Aggregates.project((fields(include("name","stars"),excludeId())))
                )
        ).forEach(printBlock); // Printing aggregated list.
        System.out.println("\n");
   }


}
