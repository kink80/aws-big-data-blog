package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntryCollector;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.OutputCollector;

public class WARCTap extends Tap<JobConf, RecordReader, OutputCollector> {

	private static final long serialVersionUID = 1L;

	public final String id = java.util.UUID.randomUUID().toString();

    public WARCScheme scheme;
    
    private static Logger logger = Logger.getLogger(WARCTap.class);


    public WARCTap(WARCScheme scheme) {
        super(scheme);
        this.scheme = scheme;
        
        logger.debug("Init WARCTAP");
    }

    public WARCTap(String path) {
        this(new WARCScheme(path));
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader recordReader) throws IOException {
    	logger.debug("Open for read");
    	return new HadoopTupleEntrySchemeIterator(flowProcess, this, recordReader);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector outputCollector) throws IOException {
        WARCCollector warcCollector = new WARCCollector(flowProcess, this);
        warcCollector.prepare();
        return warcCollector;
    }


    @Override
    public boolean createResource(JobConf jobConf) throws IOException {
        return true;
    }


    @Override
    public boolean deleteResource(JobConf jobConf) throws IOException {
        return true;
    }


    @Override
    public boolean resourceExists(JobConf jobConf) throws IOException {
        return true;
    }


    @Override
    public long getModifiedTime(JobConf jobConf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof WARCTap))
            return false;
        if (!super.equals(other))
            return false;

        WARCTap otherTap = (WARCTap) other;

        return otherTap.getIdentifier().equals(getIdentifier());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getIdentifier().hashCode();

        return result;
    }
}
