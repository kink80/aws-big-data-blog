package com.amazonaws.bigdatablog.indexcommoncrawl.parser;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

public class TikaParser {
	
	private static final Tika tika = new Tika();
	
	public static String parse(InputStream is) throws IOException, TikaException {
		return tika.parseToString(is);
	}

}
