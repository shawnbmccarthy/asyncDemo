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
import static com.mongodb.client.model.Updates.set;

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
        final SingleResultCallback<UpdateResult> resultCallback = new SingleResultCallback<UpdateResult>() {
            public void onResult(UpdateResult updateResult, Throwable throwable) {
                /* todo: change the log.error stuff */
                if(throwable != null){
                    log.error("error: {}", throwable.getLocalizedMessage());
                } else {
                    log.info("found result: {}", updateResult.toString());
                }
            }
        };

        Document document;
        Document shortSegDoc;
        Document longSegDoc;
        List<Document> sSegs;
        List<Document> lSegs;
        while(true) {
            if(queue.size() > 0) {
                log.debug("queue size: {}", queue.size());

                try {
                    document = queue.remove();
                    log.debug("retrieved: _id={}", document.getObjectId("_id"));
                    sSegs = (List<Document>) document.get("shortTtlSegs");
                    lSegs = (List<Document>) document.get("longTtlSegs");

                    if (sSegs.size() > 0) {
                        for (int i = 0; i < sSegs.size(); i++) {
                            shortSegDoc = sSegs.get(i);
                            Iterator<String> iKeys = shortSegDoc.keySet().iterator();
                            while (iKeys.hasNext()) {
                                Date date = shortSegDoc.getDate(iKeys.next());
                            }
                        }
                    } else {
                        sSegs.add(new Document("sts1", new Date()));
                    }

                    if (lSegs.size() > 0) {
                        for (int i = 0; i < lSegs.size(); i++) {
                            longSegDoc = lSegs.get(i);
                            Iterator<String> iKeys = longSegDoc.keySet().iterator();
                            while (iKeys.hasNext()) {
                                String k = iKeys.next();
                                Date date = longSegDoc.getDate(k);
                                if (k.equals("sbmSeg")) {
                                    lSegs.remove(i);
                                }
                            }
                        }
                    }

                    lSegs.add(new Document().append("sbmSeg", new Date()));

                    collection.updateOne(
                            eq("_id", document.getObjectId("_id")),
                            combine(
                                    set("longTtlSegs", lSegs),
                                    set("shortTtlSegs", sSegs)
                            ),
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
