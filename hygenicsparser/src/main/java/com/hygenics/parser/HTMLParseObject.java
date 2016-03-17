package com.hygenics.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores the Individual HTML and results for parsing as an attempt to reduce
 * memory usage.
 */

public class HTMLParseObject {

	// HTML information
	private String html = null;
	private String link = null;

	private HTMLParseObject next = null;

	// the result map
	private Map<String, String> results = new HashMap<String, String>();

	public HTMLParseObject() {

	}

	public HTMLParseObject getNext() {
		return next;
	}

	public void setNext(HTMLParseObject next) {
		this.next = next;
	}

	public String getHtml() {
		return html;
	}

	public void setHtml(String html) {
		this.html = html;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public Map<String, String> getResults() {
		return results;
	}

	public void setResults(Map<String, String> results) {
		this.results = results;
	}

}
