package org.sbm.mongodb.demo;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;

import com.mongodb.async.client.MongoCollection;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceProcessService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SequenceProcessService.class);

    private MongoCollection<Document> coll;
    private ExecutorService rPool;
    private ExecutorService wPool;
    private int runtime;
    private ConcurrentLinkedQueue<Document> workQueue;
    private List<String> uuidCache;
    private boolean isRunning;

    public SequenceProcessService(String muri, String collection, List<String> cache, int rthreads, int wthreads, int runtimeMinutes){
        log.debug("Starting SqequenceProcessService");
        ConnectionString u = new ConnectionString(muri);
        MongoClient client = MongoClients.create(u);

        coll = client.getDatabase(u.getDatabase()).getCollection(collection);
        rPool = Executors.newFixedThreadPool(rthreads);
        wPool = Executors.newFixedThreadPool(wthreads);
        workQueue = new ConcurrentLinkedQueue<Document>();
        uuidCache = cache;
        this.runtime = runtimeMinutes;
        isRunning = true;
    }

    public void startExecution(){
        log.debug("starting execution");
        while(isRunning){
            rPool.execute(new SequenceReaderThread(coll, workQueue, uuidCache));
            wPool.execute(new SequenceWriterThread(coll, workQueue));
        }
    }

    /* TODO: not used in thread context right now */
    public void run() {
        log.debug("starting run");
        startExecution();
    }
}