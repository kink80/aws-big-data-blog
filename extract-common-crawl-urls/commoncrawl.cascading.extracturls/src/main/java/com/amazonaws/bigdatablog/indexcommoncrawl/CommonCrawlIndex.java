package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.flow.FlowDef;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import org.apache.log4j.Logger;
import org.elasticsearch.hadoop.cascading.EsTap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CommonCrawlIndex {

	private static Logger logger = Logger.getLogger(CommonCrawlIndex.class);
	
    public static FlowDef buildFlowDef(Properties properties){
    	logger.debug("Building new flow");
        String inPath =  properties.getProperty("inPath");
        Tap source = new WARCTap(inPath);

        // create the "sink" (output) tap that will export the data to Elasticsearch
        Tap sink = new EsTap(properties.getProperty("es.target.index"));

        //Build the Cascading Flow Definition
        return CommonCrawlIndex.createCommonCrawlFlowDefWet(source, sink);
    }
    
    public static FlowDef buildMultiFlowDef(Properties properties){
    	logger.debug("Building new flow");
        String inPath =  properties.getProperty("inPath");
        String wetPathPrefix = properties.getProperty("wetPathPrefix");
        // InPath 
        final List<Tap<?, ?, ?>> taps = new ArrayList<Tap<?, ?, ?>>();
        
        try {
        	Path indexFile = Paths.get(inPath);
        	 
        	//Construct BufferedReader from InputStreamReader
        	BufferedReader br = null;
        	try {
        		br = new BufferedReader(new InputStreamReader(new FileInputStream(indexFile.toFile())));
        		String line = null;
            	while ((line = br.readLine()) != null) {
            		taps.add(new WARCTap(wetPathPrefix + line));
            	}
        	} finally {
        		if ( br != null ) {
        			br.close();
        		}
        	}
        	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        Tap source = new MultiSourceTap(taps.toArray(new Tap[] {}));

        // create the "sink" (output) tap that will export the data to Elasticsearch
        Tap sink = new EsTap(properties.getProperty("es.target.index"));

        //Build the Cascading Flow Definition
        return CommonCrawlIndex.createCommonCrawlFlowDefWet(source, sink);
    }
    
    public static FlowDef buildFlowDefFiles(Properties properties){
        // create the Cascading "source" (input) tap to read the commonCrawl WAT file(s)
        //check if we're running locally or on HDFS
        String inPath =  properties.getProperty("inPath");

        Tap inTap = new Hfs( new TextDelimited( false, "\n" ), inPath);
        
        Pipe copyPipe = new Pipe( "copy" );
        Tap outTap = new Hfs( new TextDelimited( false, "," ), "file:///tmp/output");

        //Build the Cascading Flow Definition
        return FlowDef.flowDef()
                .addSource( copyPipe, inTap )
                .addTailSink( copyPipe, outTap );
    }

    public static FlowDef createCommonCrawlFlowDef(Tap source, Tap sink) {
        Pipe parsePipe = new Pipe( "exportCommonCrawlWATPipe" );

        //Add a Regular Expression to collect the envelope json field from each line in the file
        RegexGenerator splitter=new RegexGenerator(new Fields("json"),"^\\{\"Envelope\".*$");
        parsePipe = new Each( parsePipe, new Fields( "line" ), splitter, Fields.RESULTS );

        // connect the taps, pipes, etc., into a flow
        return FlowDef.flowDef()
                .addSource( parsePipe, source )
                .addTailSink( parsePipe, sink );
    }
    
    public static FlowDef createCommonCrawlFlowDefWet(Tap source, Tap sink) {
        Pipe parsePipe = new Pipe( "exportCommonCrawlWETPipe" );
        
        //Add a Regular Expression to collect the envelope json field from each line in the file
        // RegexGenerator splitterUri=new RegexGenerator(new Fields("json"),"^WARC-Target-URI: .*$");
        parsePipe = new Each( parsePipe, new TestFilter());
        parsePipe = new Each( parsePipe, new TestFunction());

        // connect the taps, pipes, etc., into a flow
        return FlowDef.flowDef()
                .addSource( parsePipe, source )
                .addTailSink( parsePipe, sink );
    }

}

