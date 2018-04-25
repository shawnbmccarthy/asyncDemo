package org.sbm.mongodb;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.sbm.mongodb.demo.SequenceProcessService;
import org.sbm.mongodb.demo.SequenceUtility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static CommandLine parserArgs(String[] args) throws ParseException {
        log.debug("parsing command line args");
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption(Option.builder("u")
                .longOpt("uri")
                .required(false)
                .hasArg()
                .desc("mongodb uri")
                .type(String.class)
                .build()
        );

        options.addOption(Option.builder("c")
                .longOpt("collection")
                .required(false)
                .hasArg()
                .desc("collection to use")
                .type(String.class)
                .build()
        );

        options.addOption(Option.builder("d")
                .longOpt("drop")
                .required(false)
                .hasArg()
                .desc("drop collection")
                .type(Boolean.class)
                .build()
        );

        options.addOption(Option.builder("n")
                .longOpt("nodocs")
                .required(false)
                .hasArg()
                .desc("number of documents to insert")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder("r")
                .longOpt("runtime")
                .required(false)
                .hasArg()
                .desc("runtime of test (in minutes)")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder("t")
                .longOpt("readThreads")
                .required(false)
                .hasArg()
                .desc("number of reader threads to use")
                .type(Integer.class)
                .build()
        );

        options.addOption(Option.builder("w")
                .longOpt("writeThreads")
                .required(false)
                .hasArg()
                .desc("number of writer threads to use (default *2 of reader)")
                .build()
        );


        return parser.parse(options, args);
    }

    public static void main(String[] args) {
        log.debug("starting program");
        CommandLine cl = null;
        try {
            cl = parserArgs(args);

            String uri = cl.getOptionValue("uri", "mongodb://localhost:27017/test");
            String coll = cl.getOptionValue("collection", "rpid");
            boolean drop = Boolean.getBoolean(cl.getOptionValue("drop", Boolean.toString(false)));
            int noOfDocs = Integer.parseInt(cl.getOptionValue("nodocs", Integer.toString(100000)));
            int runtime = Integer.parseInt(cl.getOptionValue("runtime", Integer.toString(5)));
            int readersThreads = Integer.parseInt(cl.getOptionValue("readThreads", Integer.toString(4)));
            int writerThreads = Integer.parseInt(cl.getOptionValue("writeThreads", Integer.toString(readersThreads * 2)));

            log.debug(
                    "processed args: url={}, coll={}, drop={}, noOfDocs={}, runtime={}, rthreads={}, wthreads={}",
                    uri, coll, drop, noOfDocs, runtime, readersThreads, writerThreads
            );

            SequenceUtility util = new SequenceUtility(uri, coll);
            util.loadData(noOfDocs, drop);
            List<String> cache = util.getUuidCache();
            util.close();
            log.debug("retrieved cache of uuids: {}", cache.size());

            SequenceProcessService sps = new SequenceProcessService(uri, coll, cache, readersThreads, writerThreads, runtime);
            sps.startExecution();
        } catch(ParseException pe){
            System.out.println("error parsing args: " + pe.getLocalizedMessage());
        }
    }

}
