package main;

import java.io.File;
import java.io.FileNotFoundException;
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
	
	public static class MapReduceInfo {
		
		public static String JOBTRACKER_SERVICENAME = "jobtracker";
		public static int JOBTRACKER_PORT = 12222;
		public static int JOBTRACKER_CHECK_PERIOD = 5000;
		//public static String JOBTRACKERFOLDER= Dfs.DIRECTORY+"/"+JOBTRACKER_SERVICENAME;
		public static int SLOTS=10;
		public static int TASKTRACKER_PORT= 23333;
	}

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

	public static boolean configure() throws FileNotFoundException {
		System.out.println("----------check hdfs conf----------\n");
		String[] str1 = { "NAMENODE_SERVICENAME", "DIRECTORY", "NAME_NODE_IP" };
		String[] num1 = { "REPLICA_NUMS", "BUF_SIZE", "NAME_NODE_CHECK_PERIOD",
				"DATA_NODE_REGISTRY_PORT", "NAME_NODE_REGISTRY_PORT" };
		File fXmlFile1 = new File("conf/hdfs.xml");
		DocumentBuilderFactory dbFactory1 = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder1;
		try {
			dBuilder1 = dbFactory1.newDocumentBuilder();

			Document doc1 = dBuilder1.parse(fXmlFile1);

			doc1.getDocumentElement().normalize();
			NodeList nList1 = doc1.getElementsByTagName("dfs");
			for (int temp = 0; temp < nList1.getLength(); temp++) {

				Node nNode = nList1.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					for (int i = 0; i < str1.length; i++) {
						System.out.print("read <" + str1[i] + "> : ");
						if (eElement.getElementsByTagName(str1[i]) != null) {
							String hold = eElement.getElementsByTagName(str1[i])
									.item(0).getTextContent();
							switch (str1[i]) {
							case "NAMENODE_SERVICENAME":
								Dfs.NAMENODE_SERVICENAME = hold;
								break;
							case "DIRECTORY":
								Dfs.DIRECTORY = hold;
								break;
							case "NAME_NODE_IP":
								Dfs.NAME_NODE_IP = hold;
								break;
							default:
								break;
							
							}
							System.out.println(hold);
						} else {
							System.out.println(" error! exit...");
							return false;
						}
					}
					for (int i = 0; i < num1.length; i++) {
						System.out.print("read <" + num1[i] + "> : ");
						if (eElement.getElementsByTagName(num1[i]) != null) {
							int hold = Integer.valueOf(eElement
									.getElementsByTagName(num1[i]).item(0)
									.getTextContent().trim());
							switch (num1[i]) {
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
							default:
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
			
			
			
			System.out.println("----------check mapred conf----------\n");
			String[] str = { "JOBTRACKER_SERVICENAME"};
			String[] num = { "JOBTRACKER_CHECK_PERIOD","SLOTS","JOBTRACKER_PORT","TASKTRACKER_PORT" };
			File fXmlFile = new File("conf/mapred.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder;
				dBuilder = dbFactory.newDocumentBuilder();

				Document doc = dBuilder.parse(fXmlFile);

				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("MapReduceInfo");
				
				
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
								case "JOBTRACKER_SERVICENAME":
									MapReduceInfo.JOBTRACKER_SERVICENAME = hold;
									break;
								//case "JOBFOLDER":
								//	MapReduceInfo.JOBFOLDER = hold;
								//	break;
								default:
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
										.getTextContent().trim());
								switch (num[i]) {
								case "JOBTRACKER_CHECK_PERIOD":
									MapReduceInfo.JOBTRACKER_CHECK_PERIOD = hold;
									break;
								case "SLOTS":
									MapReduceInfo.SLOTS=hold;
									break;
								case "JOBTRACKER_PORT":
									MapReduceInfo.JOBTRACKER_PORT=hold;
									break;
								case "TASKTRACKER_PORT":
									MapReduceInfo.TASKTRACKER_PORT=hold;
									break;
								default:
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
		}
		return true;
	}

	public static boolean createDirectory(String name) {

		File folder = new File(Environment.Dfs.DIRECTORY + "/" + name);
		System.out.println("trying to create: "+folder.getAbsolutePath());
		if (!folder.exists()) {
			if (folder.mkdir()) {
				System.out.println("Directory created");
			} else {
				System.out
						.println("Warning Directory already used please change directory name or delete the directory first");
				return false;
			}
		}
		else
			System.out.println("Warning Directory already used please change directory name or delete the directory first");
		return true;
	}

}
