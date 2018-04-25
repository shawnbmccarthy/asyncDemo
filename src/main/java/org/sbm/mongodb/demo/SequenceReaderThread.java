package org.sbm.mongodb.demo;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;

import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import static com.mongodb.client.model.Filters.eq;

public class SequenceReaderThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SequenceReaderThread.class);

    private ConcurrentLinkedQueue<Document> queue;
    private MongoCollection<Document> collection;
    private List<String> uuidCache;
    private Random random;

    public SequenceReaderThread(MongoCollection<Document> coll, ConcurrentLinkedQueue<Document> q, List<String> cache){
        log.debug("starting reader thread");
        queue = q;
        collection = coll;
        uuidCache = cache;
        random = new Random();
    }



    public void run() {
        final SingleResultCallback<Document> collectionCallback = new SingleResultCallback<Document>() {
            public void onResult(Document document, Throwable throwable) {
                if(throwable != null){
                    log.warn("found throwable: {}", throwable.getLocalizedMessage());
                    return;
                }
                log.debug("read: _id={}", document.getObjectId("_id"));
                queue.add(document);
            }
        };

        while(true){
            try{
                Thread.sleep(10);
            } catch(InterruptedException ie){
                log.error("trying so hard to just sleep");
            }
            collection.find(
                    eq("uuid", uuidCache.get(random.nextInt(uuidCache.size())))
            ).first(collectionCallback);
            try {
                Thread.sleep(10);
            } catch(InterruptedException ie){
                log.warn("IE: {}", ie.getLocalizedMessage());
            }
        }
    }
}
