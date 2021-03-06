package com.amazonaws.bigdatablog.indexcommoncrawl;

public class UrlEntity {
	
	private String uri;
	private byte[] rawContent;
	private String contentType;
	
	public UrlEntity(String uri, byte[] rawContent, String contentType) {
		this.uri = uri;
		this.rawContent = rawContent;
		this.contentType = contentType;
	}
	
	public String getUri() {
		return uri;
	}
	
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public byte[] getContent() {
		return rawContent;
	}
	
	public void setContent(byte[] content) {
		this.rawContent = content;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	
	@Override
	public String toString() {
		return contentType + ":" + uri;
	}

}
