package com.amazonaws.bigdatablog.indexcommoncrawl;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.bigdatablog.indexcommoncrawl.parser.TikaParser;
import com.amazonaws.bigdatablog.indexcommoncrawl.parser.ResponseParser;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TikaParserFunction extends BaseOperation<WARCRecord> implements Function<WARCRecord> {
	
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(TikaParserFunction.class);
	
	public TikaParserFunction() {
		super(new Fields("json"));
	}

	public void operate(FlowProcess flowProcess, FunctionCall<WARCRecord> functionCall) {
		logger.debug("Operating ...");
		
		TupleEntry entry = functionCall.getArguments();
		Tuple tuple = entry.getTuple();
		WARCRecord record = (WARCRecord) tuple.getObject(0);
		
		String uri = record.getHeader().getTargetURI();
		
		String content = null;
		try {
			ResponseParser conn = new ResponseParser(record.getContent());
			HttpResponse resp = conn.receiveResponseWithEntity();
			
			HttpEntity entity = resp.getEntity();
			
		    if ( entity != null ) {
	    		content = TikaParser.parse(entity.getContent());
	    	}
		    
		} catch (Exception e) {
			e.printStackTrace();
		}

		TestData data = new TestData(uri, content);
		ObjectMapper mapper = new ObjectMapper();
		
		String serialized;
		try {
			serialized = mapper.writeValueAsString(data);
			
			Tuple result = new Tuple(serialized);
			functionCall.getOutputCollector().add( result );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
