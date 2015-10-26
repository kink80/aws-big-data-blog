package com.amazonaws.bigdatablog.indexcommoncrawl;


import org.apache.log4j.Logger;
import org.apache.tools.ant.filters.StringInputStream;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.bigdatablog.indexcommoncrawl.parser.TikaParser;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TikaParserFunction extends BaseOperation<WARCRecord> implements Function<WARCRecord> {
	
	private static final long serialVersionUID = 1L;
	
	private static Logger LOGGER = Logger.getLogger(TikaParserFunction.class);
	
	public TikaParserFunction() {
		super(new Fields("json"));
	}

	public void operate(FlowProcess flowProcess, FunctionCall<WARCRecord> functionCall) {
		TupleEntry entry = functionCall.getArguments();
		Tuple tuple = entry.getTuple();
		UrlEntity record = (UrlEntity) tuple.getObject(0);
		
		String content = null;
		try {
			
			try {
				content = TikaParser.parse(new StringInputStream(record.getContent()));
				content = content.replaceAll("[ ]+", " ").
						replaceAll("\n+", "\n").
						replaceAll("\t+", "\t").
						replaceAll("[\n\t]+", "\n\t").
						replaceAll("[\n\t ]+", "\n\t ");
				
				UrlContent data = new UrlContent(record.getUri(), content);
				ObjectMapper mapper = new ObjectMapper();
				
				String serialized = mapper.writeValueAsString(data);
					
				Tuple result = new Tuple(serialized);
				functionCall.getOutputCollector().add( result );
				
			} catch (Exception e) {
				if ( record.getContent() != null ) {
	    			LOGGER.error( record.getContent() );
	    		}
				
				throw e;
			}
		} catch (Exception e) {
			LOGGER.error("Document parsing failed with the following error: ", e);
		}

	}
	
}
