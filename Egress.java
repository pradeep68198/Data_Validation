package Utilities;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.base.Strings;

public class Egress {
	
	public String ML_Username;
	public String ML_Pwd;
	public String URL;
	
	
	public void Rundate_Updater(String Type,String User,String Pwd,String url,String date) throws IOException, ClassNotFoundException, SQLException, URISyntaxException, TransformerFactoryConfigurationError, TransformerException, ParserConfigurationException, SAXException, InterruptedException, AuthenticationException, ParseException  {
		String FinalDate;
		
		if(Strings.isNullOrEmpty(date)) {
			FinalDate=null;
		}
		else {
		

		FinalDate=date;
    	
		}

    	
	    ML_Username=User;
	    ML_Pwd=Pwd;
	    URL=url;
	    
	   
		
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials
		 = new UsernamePasswordCredentials(ML_Username, ML_Pwd);
		provider.setCredentials(AuthScope.ANY, credentials);
		  
		HttpClient client = HttpClientBuilder.create()
		  .setDefaultCredentialsProvider(provider)
		  .build();
		 
		HttpResponse response = client.execute(new HttpGet(URL));
		int statusCode = response.getStatusLine().getStatusCode();
		
		//System.out.println("Base64 encoded auth string: " + statusCode);
		
		
		HttpEntity entity = response.getEntity();
		String responseString = EntityUtils.toString(entity, "UTF-8");
		//System.out.println(responseString);
		
		
		String data;
		
		if(Type.equalsIgnoreCase("EGRESS"))
		{
		data=XML_Update_Egress(responseString,FinalDate);
		}
		else {
			
			data=XML_Update_Ingest(responseString,FinalDate);
			
		}
		
		HTTP_POST_Final(data);
		
	
		
		//System.out.println("Done");
		
		
}


public  String XML_Update_Egress(String doc1,String date) throws ParserConfigurationException, SAXException, IOException, TransformerFactoryConfigurationError, TransformerException, InterruptedException {
	
	
	
	Document doc = toXmlDocument(doc1);
	Node SourceData = doc.getElementsByTagName("egressConfiguration").item(0);
	
	NodeList list = SourceData.getChildNodes();

	for (int i = 0; i < list.getLength(); i++) {
		
               Node node = list.item(i);

	 
	   if ("runDate".equals(node.getNodeName())) {
		node.setTextContent(date);

		
	   }

      

	}
	
	 String str = toXmlString(doc);
	
	
    
    
    
    
    return str;
}

public  String XML_Update_Ingest(String doc1,String date) throws ParserConfigurationException, SAXException, IOException, TransformerFactoryConfigurationError, TransformerException, InterruptedException {
	
	
	
	Document doc = toXmlDocument(doc1);
	Node SourceData = doc.getElementsByTagName("IngestionConfiguration").item(0);
	
	NodeList list = SourceData.getChildNodes();

	for (int i = 0; i < list.getLength(); i++) {
		
               Node node = list.item(i);

	 
	   if ("RunDate".equals(node.getNodeName())) {
		node.setTextContent(date);

		
	   }

      

	}
	
	 String str = toXmlString(doc);
	
	
    //System.out.println(str);
    
    
    
    return str;
}



 public  Document toXmlDocument(String str) throws ParserConfigurationException, SAXException, IOException{
        
     DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
     DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
     Document document = docBuilder.parse(new InputSource(new StringReader(str)));
    
     return document;
}
 
 
 public  String toXmlString(Document document) throws TransformerException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(document);
        StringWriter strWriter = new StringWriter();
        StreamResult result = new StreamResult(strWriter);
    
        transformer.transform(source, result);
        
        return strWriter.getBuffer().toString();
        
    }
 

 
public  void HTTP_POST_Final(String data) throws ClientProtocolException, IOException, AuthenticationException {
	 
	 //String url2="http://dev.hrhub.aetna.com:8070/LATEST/documents?uri=/harmonize/hrhub/consultant/97bc4f59-5acb-4a26-bcb1-aa83b6615e32.xml";
	    
	   
	 
	        CloseableHttpClient client = HttpClients.createDefault();
	        HttpUriRequest request = RequestBuilder.put()
	  			  .setUri(URL)
	  			  .setHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
	  			  .setEntity(new StringEntity(data))
	  			  .build();
		    
		    UsernamePasswordCredentials creds
		      = new UsernamePasswordCredentials(ML_Username, ML_Pwd);
		    request.addHeader(new BasicScheme().authenticate(creds, request, null));
		 
		    CloseableHttpResponse response = client.execute(request);
		  
		    
		    int statusCode = response.getStatusLine().getStatusCode();
			
			//System.out.println("POST Status code : " + statusCode);
			
		    client.close();
		}





}
