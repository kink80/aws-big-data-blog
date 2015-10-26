package com.amazonaws.bigdatablog.indexcommoncrawl;


import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;

import com.amazonaws.bigdatablog.indexcommoncrawl.parser.ResponseParser;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class Warc2EntityFunction extends BaseOperation<HttpEntity> implements Function<HttpEntity> {
	
	private static final long serialVersionUID = 1L;
	
	private static Logger LOGGER = Logger.getLogger(Warc2EntityFunction.class);
	
	public Warc2EntityFunction() {
		super(new Fields("json"));
	}

	public void operate(FlowProcess flowProcess, FunctionCall<HttpEntity> functionCall) {
		TupleEntry entry = functionCall.getArguments();
		Tuple tuple = entry.getTuple();
		WARCRecord record = (WARCRecord) tuple.getObject(0);
		
		String uri = record.getHeader().getTargetURI();
		
		try {
			ResponseParser conn = new ResponseParser(record.getContent());
			HttpResponse resp = conn.receiveResponseWithEntity();
			
			HttpEntity entity = resp.getEntity();
			if ( entity != null) {
				Header contentType = entity.getContentType();
				if ( contentType != null ) {
					String raw = "";
					try {
						raw = IOUtils.toString(entity.getContent());
						
						UrlEntity data = new UrlEntity(uri, raw, contentType.getValue());
						
						Tuple result = new Tuple(data);
						functionCall.getOutputCollector().add(result);
					} catch (Exception e) {
						LOGGER.error("Conversion to entity failed for " + uri);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Document conversion failed with the following error: ", e);
		}

	}
	
}
