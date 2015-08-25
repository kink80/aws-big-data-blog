package com.amazonaws.bigdatablog.indexcommoncrawl;

import com.amazonaws.bigdatablog.indexcommoncrawl.WARCRecord.Header;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TestFilter extends BaseOperation implements Filter {

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
		TupleEntry entry = filterCall.getArguments();
		Tuple tuple = entry.getTuple();
		WARCRecord record = (WARCRecord) tuple.getObject(0);
		
		Header h = record.getHeader();
		String recType = h.getRecordType();
		
		if ("warcinfo".equals(recType)) {
			return true;
		}
		return false;
	}

}
