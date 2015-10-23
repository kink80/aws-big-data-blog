package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class HadoopJob {
    public static void main(String args[]) {
        Properties properties = null;
        
        if ( args != null && args.length > 0 ) {
        	try {
				properties = new ConfigReader().renderProperties(HadoopJob.class, new FileInputStream(args[0]));
			} catch (FileNotFoundException e) {
				System.out.println("Could not read your config.properties file");e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Could not read your config.properties file");e.printStackTrace();
			}
        	
        	if (args[1] != null && args[1].length() > 1){
                properties.put("inPath", args[1]);
            }
        }

        FlowDef flowDef = CommonCrawlIndex.buildMultiFlowDef(properties);
        new HadoopFlowConnector(properties).connect(flowDef).complete();
    }

}
