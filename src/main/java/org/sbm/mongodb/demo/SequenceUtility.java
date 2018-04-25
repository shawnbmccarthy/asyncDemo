package org.sbm.mongodb.demo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.BsonObjectId;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/*
 * utility to load initial dataset as well as gather various UUIDs to build our tests
 */
public class SequenceUtility {
    private static Logger log = LoggerFactory.getLogger(SequenceUtility.class);

    private MongoClient client;
    private MongoClientURI mongoUri;
    private String collection;
    private Random random;
    private List<String> uuidsCache;

    /*
     * just setup the connection junk for now
     */
    public SequenceUtility(String uri, String coll){
        log.debug("creating sequence utility");
        mongoUri = new MongoClientURI(uri);
        collection = coll;
        random = new Random();
        uuidsCache = new ArrayList<String>();
    }

    public void loadData(int noOfDocuments, boolean drop) {
        log.debug("load data: {}", noOfDocuments);
        if(client == null){
            client = new MongoClient(mongoUri);
        }
        MongoDatabase db = client.getDatabase(mongoUri.getDatabase());
        MongoCollection<Document> coll = db.getCollection(collection);

        if(drop){
            coll.drop();
            coll.createIndex(new Document("uuid", 1));
        }
        List<Document> toInsert = new ArrayList<Document>();
        int j;
        for(int i = 0; i < noOfDocuments; i++){
            List<String> uuids = new ArrayList<String>();
            List<Document> sTtlSegs = new ArrayList<Document>();
            List<Document> lTtlSegs = new ArrayList<Document>();

            Document d = new Document("_id", new BsonObjectId());
            d.put("pid", random.nextInt(noOfDocuments)); // not really worried about the idea of the pid right now

            /* no more than 5 uuids per doc right now */
            for(j = 1; j < (random.nextInt(5)+1); j++){
                uuids.add(UUID.randomUUID().toString());
            }

            for(j = 1; j < (random.nextInt(10)+1); j++){
                sTtlSegs.add(new Document("sts" + j, new Date()));
            }

            for(j = 1; j < (random.nextInt(10)+1); j++){
                lTtlSegs.add(new Document("lts" + j, new Date()));
            }

            d.put("shortTtlSegs", sTtlSegs);
            d.put("longTtlSegs", lTtlSegs);
            d.put("expireAt", new Date());
            d.put("wasUpdated", true);
            d.put("createDate", new Date());
            d.put("uuid", uuids);

            toInsert.add(d);
            if(toInsert.size() >= 1000){
                log.debug("inserting 1k worth of docs");
                coll.insertMany(toInsert);
                toInsert.clear();
            }
        }

        if(toInsert.size() > 0){
            log.debug("inserting the rest of the docs");
            coll.insertMany(toInsert);
            toInsert.clear();
        }
    }

    public List<String> getUuidCache(){
        if(uuidsCache.size() == 0) {
            if (client == null) {
                client = new MongoClient(mongoUri);
            }
            MongoDatabase db = client.getDatabase(mongoUri.getDatabase());
            MongoCollection<Document> coll = db.getCollection(collection);

            MongoCursor<Document> cursor = coll.find().projection(new Document("_id", 0).append("uuid", 1)).iterator();
            while (cursor.hasNext()) {
                Document d = cursor.next();
                uuidsCache.addAll((ArrayList<String>)cursor.next().get("uuid"));
            }
            cursor.close();
        }
        return uuidsCache;
    }

    public void close(){
        client.close();
        client = null;
    }
}
