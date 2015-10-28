package com.amazonaws.bigdatablog.indexcommoncrawl.parser;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.tika.Tika;


public class TikaParser {

	private static Logger LOGGER = Logger.getLogger(TikaParser.class);
	
	private static final Tika tika = new Tika();

	private static final ExecutorService executor = Executors.newSingleThreadExecutor();
	
	public static String parse(final InputStream is)  {
		Callable<String> task = new Callable<String>() {

			@Override
			public String call() throws Exception {
				return tika.parseToString(is);
			}

		};

		Future<String> result = executor.submit(task);

		String extractedText = "";
		try {
			extractedText = result.get();
		} catch (Exception ee) {
			LOGGER.error("Thread interrupted while extracting the text.", ee);
		}

		return extractedText;
	}

}
