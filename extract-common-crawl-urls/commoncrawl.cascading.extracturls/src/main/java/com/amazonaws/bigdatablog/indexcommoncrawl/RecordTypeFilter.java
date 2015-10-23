package com.amazonaws.bigdatablog.indexcommoncrawl;

import com.amazonaws.bigdatablog.indexcommoncrawl.WARCRecord.Header;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class RecordTypeFilter extends BaseOperation implements Filter {
	
	private final String recordType;
	
	public RecordTypeFilter(String recordType) {
		this.recordType = recordType;
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
		TupleEntry entry = filterCall.getArguments();
		Tuple tuple = entry.getTuple();
		WARCRecord record = (WARCRecord) tuple.getObject(0);
		
		Header h = record.getHeader();
		String recType = h.getRecordType();
		
		if (recordType.equals(recType)) {
			return false;
		}
		return true;
	}

}
