package org.sbm.mongodb.demo;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.push;

public class SequenceWriterThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SequenceWriterThread.class);

    private ConcurrentLinkedQueue<Document> queue;
    private MongoCollection<Document> collection;

    public SequenceWriterThread(MongoCollection<Document> coll, ConcurrentLinkedQueue<Document> q){
        log.debug("sequence writer starting");
        queue = q;
        collection = coll;
    }

    public void run() {
        log.debug("sequence writer running");
        final long start = System.currentTimeMillis();
        final SingleResultCallback<UpdateResult> resultCallback = new SingleResultCallback<UpdateResult>() {
            public void onResult(UpdateResult updateResult, Throwable throwable) {
                /* todo: change the log.error stuff */
                if(throwable != null){
                    log.error("error: {}", throwable.getLocalizedMessage());
                } else {
                    long end = System.currentTimeMillis() - start;
                    log.error("found result: {}, took: {}(ms)", updateResult.toString(), end);
                }
            }
        };

        while(true) {
            if(queue.size() > 0) {
                log.debug("queue size: {}", queue.size());

                try {
                    Document d = queue.remove();
                    log.debug("retrieved: _id={}", d.getObjectId("_id"));
                    List<Document> sSegs = (List<Document>) d.get("shortTtlSegs");
                    List<Document> lSegs = (List<Document>) d.get("longTtlSegs");

                    if (sSegs.size() > 0) {
                        for (int i = 0; i < sSegs.size(); i++) {
                            Document sd = sSegs.get(i);
                            Iterator<String> iKeys = sd.keySet().iterator();
                            while (iKeys.hasNext()) {
                                Date date = d.getDate(iKeys.next());
                            }
                        }
                    }

                    if (lSegs.size() > 0) {
                        for (int i = 0; i < lSegs.size(); i++) {
                            Document sd = lSegs.get(i);
                            Iterator<String> iKeys = sd.keySet().iterator();
                            while (iKeys.hasNext()) {
                                String k = iKeys.next();
                                Date date = d.getDate(k);
                                if (k.equals("sbmSeg")) {
                                    d.remove(k);
                                }
                            }
                        }
                    }

                    collection.updateOne(
                            eq("_id", d.getObjectId("_id")),
                            combine(push("longTtlSegs", lSegs.add(new Document("sbmSeg", new Date())))),
                            new UpdateOptions().upsert(true),
                            resultCallback
                    );
                } catch (NoSuchElementException nsee) {
                    log.warn("E: nothing on the queue");
                }
            }
        }
    }
}
