package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.property.AppProps;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {

    public Properties renderProperties(Object caller) throws IOException {
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        
        return renderProperties(caller, inputStream);
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