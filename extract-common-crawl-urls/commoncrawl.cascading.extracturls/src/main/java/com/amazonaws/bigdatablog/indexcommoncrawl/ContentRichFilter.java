package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ContentRichFilter extends BaseOperation implements Filter {
	
	private static final long serialVersionUID = 1L;

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
		TupleEntry entry = filterCall.getArguments();
		Tuple tuple = entry.getTuple();
		UrlEntity record = (UrlEntity) tuple.getObject(0);
		
		if ( record.getContentType().startsWith("text") || 
			 record.getContentType().startsWith("application/msword") || 
			 record.getContentType().startsWith("application/pdf") ||
			 record.getContentType().startsWith("application/rtf") ||
			 record.getContentType().startsWith("application/vnd.openxmlformats-officedocument.wordprocessingml.document") ||
			 record.getContentType().startsWith("application/vnd.oasis.opendocument.text") ||
			 record.getContentType().startsWith("application/vnd.ms-powerpoint") ||
			 record.getContentType().startsWith("application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
			return false;
		}
		
		return true;
	}

}
