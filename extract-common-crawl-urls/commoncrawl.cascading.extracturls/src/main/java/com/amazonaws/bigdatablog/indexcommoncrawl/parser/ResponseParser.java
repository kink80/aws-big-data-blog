package com.amazonaws.bigdatablog.indexcommoncrawl.parser;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.impl.entity.LaxContentLengthStrategy;
import org.apache.http.impl.io.ChunkedInputStream;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.EmptyInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.IdentityInputStream;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.protocol.HTTP;

public class ResponseParser {
	
	private byte[] content;
	private ContentLengthStrategy incomingStrategy = LaxContentLengthStrategy.INSTANCE;
	private MessageConstraints messageConstraints = MessageConstraints.DEFAULT;
	private SessionInputBufferImpl inbuffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 8 * 1024);
	
	
	public ResponseParser(byte[] content) {
		this.content = content;
	}

	public HttpResponse receiveResponseHeader() throws HttpException, IOException {
    	inbuffer.bind(new BufferedInputStream(new ByteArrayInputStream(content)));
    	
    	HttpResponse httpResponse = new DefaultHttpResponseParser(inbuffer).parse();
        return httpResponse;
	}

	public void receiveResponseEntity(HttpResponse response) throws HttpException, IOException {
		final HttpEntity entity = prepareInput(response);
        response.setEntity(entity);
	}
	
	private boolean canResponseHaveBody(final HttpResponse response) {

		final int status = response.getStatusLine().getStatusCode();
		return status >= HttpStatus.SC_OK
				&& status != HttpStatus.SC_NO_CONTENT
				&& status != HttpStatus.SC_NOT_MODIFIED
				&& status != HttpStatus.SC_RESET_CONTENT;
	}
	
	public HttpResponse receiveResponseWithEntity() throws HttpException, IOException {
		HttpResponse resp = receiveResponseHeader();
		if ( canResponseHaveBody(resp) ) {
			receiveResponseEntity(resp);
		}
		
		return resp;
	}

	protected InputStream createInputStream(
            final long len,
            final SessionInputBuffer inbuffer) {
        if (len == ContentLengthStrategy.CHUNKED) {
            return new ChunkedInputStream(inbuffer, this.messageConstraints);
        } else if (len == ContentLengthStrategy.IDENTITY) {
            return new IdentityInputStream(inbuffer);
        } else if (len == 0L) {
            return EmptyInputStream.INSTANCE;
        } else {
            return new ContentLengthInputStream(inbuffer, len);
        }
    }
	
	protected HttpEntity prepareInput(final HttpMessage message) throws HttpException {
        final BasicHttpEntity entity = new BasicHttpEntity();

        final long len = this.incomingStrategy.determineLength(message);
        final InputStream instream = createInputStream(len, this.inbuffer);
        if (len == ContentLengthStrategy.CHUNKED) {
            entity.setChunked(true);
            entity.setContentLength(-1);
            entity.setContent(instream);
        } else if (len == ContentLengthStrategy.IDENTITY) {
            entity.setChunked(false);
            entity.setContentLength(-1);
            entity.setContent(instream);
        } else {
            entity.setChunked(false);
            entity.setContentLength(len);
            entity.setContent(instream);
        }

        final Header contentTypeHeader = message.getFirstHeader(HTTP.CONTENT_TYPE);
        if (contentTypeHeader != null) {
            entity.setContentType(contentTypeHeader);
        }
        final Header contentEncodingHeader = message.getFirstHeader(HTTP.CONTENT_ENCODING);
        if (contentEncodingHeader != null) {
            entity.setContentEncoding(contentEncodingHeader);
        }
        return entity;
    }

}
