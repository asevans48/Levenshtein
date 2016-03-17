package com.hygenics.parser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.vfs.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * "This class accounts for the seeming inability of the Kettle Java API to work
 * appropriately with Fedora to replace database trasnformations. The API seems
 * to work fine with Windows but when opening transformations in Linux after
 * running RepConn in Linux, nothing really changes in the XML (the data appears
 * to be returned though). This also eradicates the Pentaho APIs issue of
 * changing encoding and eradicating the XML header leading to a content in the
 * prolog error.
 * 
 * Pentaho v 5.4
 * 
 * @author asevans48
 *
 */
public class ManualReplacement {
	private boolean log_to_table = false;
	private HashMap<String, HashMap<String, String>> loggingTables = new HashMap<String, HashMap<String, String>>();
	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private Map<String, Map<String, String>> databaseAttributes;
	private List<String> fpaths;
	private List<String> remove;
	private String mainConnection;
	private Map<String, Map<String, String>> replacements = null;

	public ManualReplacement() {

	}// ManualReplacement

	public Map<String, Map<String, String>> getReplacements() {
		return replacements;
	}

	public void setReplacements(Map<String, Map<String, String>> replacements) {
		this.replacements = replacements;
	}

	public HashMap<String, HashMap<String, String>> getLoggingTables() {
		return loggingTables;
	}

	public void setLoggingTables(
			HashMap<String, HashMap<String, String>> loggingTables) {
		this.loggingTables = loggingTables;
	}

	public boolean isLog_to_table() {
		return log_to_table;
	}

	public void setLog_to_table(boolean log_to_table) {
		this.log_to_table = log_to_table;
	}

	public String getMainConnection() {
		return mainConnection;
	}// getMainConnection

	public void setMainConnection(String mainConnection) {
		this.mainConnection = mainConnection;
	}// setMainConnection

	public List<String> getRemove() {
		return remove;
	}// getRemove

	public void setRemove(List<String> remove) {
		this.remove = remove;
	}// setRemove

	public Map<String, Map<String, String>> getDatabaseAttributes() {
		return databaseAttributes;
	}// getDatabaseAttributes

	public void setDatabaseAttributes(
			Map<String, Map<String, String>> databaseAttributes) {
		this.databaseAttributes = databaseAttributes;
	}// setDatabaseAttributes

	public List<String> getFpaths() {
		return fpaths;
	}// getFpaths

	public void setFpaths(List<String> fpaths) {
		this.fpaths = fpaths;
	}// setFpaths

	private void transform() {
		log.info("Transforming");

		DocumentBuilderFactory docFactory = DocumentBuilderFactory
				.newInstance();
		for (String fpath : fpaths) {
			log.info("FILE: " + fpath);
			try {
				// TRANSFORM
				DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
				Document doc = docBuilder.parse(new File(fpath));
				Node root = doc.getFirstChild();
				XPathFactory xfactory = XPathFactory.newInstance();
				XPath xpath = xfactory.newXPath();
				String database = null;
				String path = "//transformation/connection";

				log.info("Removing");
				for (String dbname : remove) {
					log.info("XPATH:" + path
							+ "[descendant::name[contains(text(),'"
							+ dbname.trim() + "')]]");
					XPathExpression xepr = xpath.compile(path
							+ "[descendant::name[contains(text(),'"
							+ dbname.trim() + "')]]");
					Node conn = (Node) xepr.evaluate(doc, XPathConstants.NODE);
					if (conn != null) {
						root.removeChild(conn);
					}
				}

				log.info("Transforming");
				for (String key : databaseAttributes.keySet()) {
					database = key;
					log.info("XPATH:" + path
							+ "[descendant::name[contains(text(),'"
							+ database.trim() + "')]]");
					XPathExpression xepr = xpath.compile(path
							+ "[descendant::name[contains(text(),'"
							+ database.trim() + "')]]");
					Node conn = (Node) xepr.evaluate(doc, XPathConstants.NODE);

					if (conn != null) {
						if (remove.contains(key)) {
							root.removeChild(conn);
						} else {
							Map<String, String> attrs = databaseAttributes
									.get(database);
							NodeList nl = conn.getChildNodes();
							Set<String> keys = databaseAttributes.get(key)
									.keySet();

							for (int i = 0; i < nl.getLength(); i++) {
								org.w3c.dom.Node n = nl.item(i);

								if (keys.contains(n.getNodeName().trim())) {
									n.setNodeValue(attrs.get(n.getNodeName()));
								}
							}
						}
					}

					if (!this.log_to_table
							|| (this.log_to_table && this.loggingTables != null)) {
						log.info("Logging Manipulation");
						log.info("PERFORMING LOGGING MANIPULATION: "
								+ (!this.log_to_table) != null ? "Removing Logging Data"
								: "Adding Logging data");
						String[] sections = new String[] { "trans-log-table",
								"perf-log-table", "channel-log-table",
								"step-log-table", "metrics-log-table" };
						for (String section : sections) {
							log.info("Changing Settings for " + section);
							xepr = xpath.compile("//" + section
									+ "/field/enabled");
							NodeList nodes = (NodeList) xepr.evaluate(doc,
									XPathConstants.NODESET);
							log.info("Nodes Found: "
									+ Integer.toString(nodes.getLength()));
							for (int i = 0; i < nodes.getLength(); i++) {
								if (!this.log_to_table) {
									nodes.item(i).setNodeValue("N");
								} else {
									nodes.item(i).setNodeValue("Y");
								}
							}

							for (String nodeName : new String[] { "schema",
									"connection", "table", "size_limit_lines",
									"interval", "timeout_days" }) {
								if (!this.log_to_table) {
									log.info("Changing Settings for Node: "
											+ nodeName);
									xepr = xpath.compile("//" + section + "/"
											+ nodeName);
									Node node = (Node) xepr.evaluate(doc,
											XPathConstants.NODE);
									if (node != null) {
										if (!this.log_to_table) {
											node.setNodeValue(null);
										} else if (this.loggingTables
												.containsKey(section)
												&& this.loggingTables.get(
														section).containsKey(
														nodeName)) {
											node.setNodeValue(this.loggingTables
													.get(section).get(nodeName));
										}
									}
								}
							}
						}
					}
				}

				// SET MAIN CONNECTION
				if (mainConnection != null) {
					XPathExpression xepr = xpath.compile(path);
					NodeList conns = (NodeList) xepr.evaluate(doc,
							XPathConstants.NODESET); // NodeSet is not a part of
														// org.w3c it is
														// actually a NodeList
					for (int i = 0; i < conns.getLength(); i++) {
						if (!conns.item(i).hasChildNodes()) {// only connection
																// elements
																// without child
																// nodes have
																// text content
							conns.item(i).setNodeValue(mainConnection);
						}
					}
				}

				if (this.replacements != null) {
					for (String key : this.replacements.keySet()) {
						XPathExpression xepr = xpath.compile(key);
						Node node = (Node) xepr.evaluate(doc,
								XPathConstants.NODE);
						if (node != null) {
							for (String attrVal : this.replacements.get(key).keySet()) {
								log.info("Replacing Information at " + key+ "at " + attrVal);
								log.info("Replacement Will Be: "+ StringEscapeUtils.escapeXml11(this.replacements.get(key).get(attrVal)));

								if (attrVal.toLowerCase().trim().equals("text")) {
									node.setNodeValue(StringEscapeUtils.escapeXml11(this.replacements.get(key).get(attrVal)));
									if(node.getNodeValue() == null){
										node.setTextContent(StringEscapeUtils.escapeXml11(this.replacements.get(key).get(attrVal)));
									}
									
								} else {
									NamedNodeMap nattrs = node.getAttributes();
									Node n = nattrs.getNamedItem(attrVal);
									if (n != null) {
										n.setNodeValue(StringEscapeUtils.escapeXml11(this.replacements.get(key).get(attrVal)));
									} else {
										log.warn("Attribute Not Found "+ attrVal);
									}
								}
							}
						} else {
							log.warn("Node not found for " + key);
						}
					}
				}

				// WRITE TO FILE
				log.info("Writing to File");
				TransformerFactory tfact = TransformerFactory.newInstance();
				Transformer transformer = tfact.newTransformer();
				DOMSource source = new DOMSource(doc);
				try(FileOutputStream stream = new FileOutputStream(new File(fpath))){
					StreamResult result = new StreamResult(stream);
					transformer.transform(source, result);
					stream.flush();
				}catch(FileNotFoundException e){
					e.printStackTrace();
				}catch(IOException e){
					e.printStackTrace();
				}catch (TransformerException e) {
					e.printStackTrace();
				}catch(Exception e){
					e.printStackTrace();
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				e.printStackTrace();
			} catch (TransformerConfigurationException e) {
				e.printStackTrace();
			}
		}
	}

	public void run() {
		transform();
	}// run

}
