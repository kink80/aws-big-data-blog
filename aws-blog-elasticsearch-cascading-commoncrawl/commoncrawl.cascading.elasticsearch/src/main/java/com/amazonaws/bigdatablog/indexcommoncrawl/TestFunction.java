package com.amazonaws.bigdatablog.indexcommoncrawl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TestFunction extends BaseOperation<WARCRecord> implements Function<WARCRecord> {
	
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(TestFunction.class);
	
	public TestFunction() {
		super(new Fields("json"));
	}

	public void operate(FlowProcess flowProcess, FunctionCall<WARCRecord> functionCall) {
		// TODO Auto-generated method stub
		logger.debug("Operating ...");
		
		TupleEntry entry = functionCall.getArguments();
		Tuple tuple = entry.getTuple();
		WARCRecord record = (WARCRecord) tuple.getObject(0);
		
		String uri = record.getHeader().getTargetURI();
		String content = null;
		try {
			content = new String(record.getContent(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		TestData data = new TestData(uri, content);
		ObjectMapper mapper = new ObjectMapper();
		
		String serialized;
		try {
			serialized = mapper.writeValueAsString(data);
			
			Tuple result = new Tuple(serialized);
			functionCall.getOutputCollector().add( result );
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
