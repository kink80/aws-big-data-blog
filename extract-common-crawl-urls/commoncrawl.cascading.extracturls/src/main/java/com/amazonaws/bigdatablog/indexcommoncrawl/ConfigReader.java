package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.property.AppProps;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

public class ConfigReader {

    public Properties renderProperties(Object caller) throws IOException {
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        
        return renderProperties(caller, inputStream);
    }
    
    public JobConf renderJobConf(Object caller) throws IOException {
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        
        Properties props = renderProperties(caller, inputStream);
        for ( String propertyName : props.stringPropertyNames()) {
        	
        }
        JobConf jobConf = new JobConf();
        
        
        return jobConf;
    }
    
    public Properties renderProperties(Object caller, InputStream propStream) throws IOException {
        Properties properties = new Properties();

        if (propStream != null) {
            properties.load(propStream);
            AppProps.setApplicationJarClass(properties, caller.getClass());
        } else {
            throw new FileNotFoundException("property file not found");
        }

        return properties;
    }
}