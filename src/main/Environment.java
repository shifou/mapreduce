package main;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class Environment {
	public static int TIME_LIMIT=5;

	public static class Dfs {
		public static String NAME_NODE_IP;
		public static String NAMENODE_SERVICENAME = "namenode";
		public static int NAME_NODE_REGISTRY_PORT = 11111;
		public static int DATA_NODE_REGISTRY_PORT = 22222;
		public static String DIRECTORY = "HDFS";
		public static int NAME_NODE_CHECK_PERIOD = 5000;
		public static int REPLICA_NUMS = 2;
		public static int BUF_SIZE = 2048;
	}

	public static boolean configure() {
		System.out.println("----------check conf----------\n");
		String[] str = { "NAMENODE_SERVICENAME", "DIRECTORY", "NAME_NODE_IP" };
		String[] num = { "REPLICA_NUMS", "BUF_SIZE", "NAME_NODE_CHECK_PERIOD",
				"DATA_NODE_REGISTRY_PORT", "NAME_NODE_REGISTRY_PORT" };
		File fXmlFile = new File("conf/hdfs.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();

			Document doc = dBuilder.parse(fXmlFile);

			// optional, but recommended
			// read this -
			// http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("dfs");
			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					for (int i = 0; i < str.length; i++) {
						System.out.print("read <" + str[i] + "> : ");
						if (eElement.getElementsByTagName(str[i]) != null) {
							String hold = eElement.getElementsByTagName(str[i])
									.item(0).getTextContent();
							switch (str[i]) {
							case "NAMENODE_SERVICENAME":
								Dfs.NAMENODE_SERVICENAME = hold;
								break;
							case "DIRECTORY":
								Dfs.DIRECTORY = hold;
								break;
							case "NAME_NODE_IP":
								Dfs.NAME_NODE_IP = hold;
								break;
							}
							System.out.println(hold);
						} else {
							System.out.println(" error! exit...");
							return false;
						}
					}
					for (int i = 0; i < num.length; i++) {
						System.out.print("read <" + num[i] + "> : ");
						if (eElement.getElementsByTagName(num[i]) != null) {
							int hold = Integer.valueOf(eElement
									.getElementsByTagName(num[i]).item(0)
									.getTextContent());
							switch (num[i]) {
							case "NAME_NODE_REGISTRY_PORT":
								Dfs.NAME_NODE_REGISTRY_PORT = hold;
								break;
							case "DATA_NODE_REGISTRY_PORT":
								Dfs.DATA_NODE_REGISTRY_PORT = hold;
								break;
							case "REPLICA_NUMS":
								Dfs.REPLICA_NUMS = hold;
								break;
							case "BUF_SIZE":
								Dfs.BUF_SIZE = hold;
								break;
							case "NAME_NODE_CHECK_PERIOD":
								Dfs.NAME_NODE_CHECK_PERIOD = hold;
								break;
							}
							System.out.println(hold);
						} else {
							System.out.println(" error! exit...");
							return false;
						}
					}
				}
			}
			System.out.println("----------------------------");
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			System.out.println(" error! exit...");
			return false;
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			System.out.println(" error! exit...");
			e.printStackTrace();
			return false;
		} catch (IOException e) {

			System.out.println(" error! exit...");
			e.printStackTrace();
			return false;
		} catch (Exception e) {

			System.out.println(" error! exit...");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean createDirectory(String name) {

		File folder = new File(Environment.Dfs.DIRECTORY + "/" + name);
		if (!folder.exists()) {
			if (folder.mkdir()) {
				System.out.println("Directory created");
			} else {
				System.out
						.println("Warning Directory already used please change directory name or delete the directory first");
				return false;
			}
		}
		return true;
	}

}
