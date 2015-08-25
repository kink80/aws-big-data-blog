package com.amazonaws.bigdatablog.indexcommoncrawl;

public class TestData {
	
	private String uri;
	private String content;
	
	public TestData(String uri, String content) {
		this.uri = uri;
		this.content = content;
	}
	
	public String getUri() {
		return uri;
	}
	
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public String getContent() {
		return content;
	}
	
	public void setContent(String content) {
		this.content = content;
	}
	
	

}
