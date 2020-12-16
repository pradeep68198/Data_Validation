package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HR_HUB {
	
	
	
	 public  HashMap<String, String> Status =new HashMap<String, String>();
	 public HashMap<String, String> Link =new HashMap<String, String>();
public String[][] readXLSXFile(String Filepath,String sheet) throws IOException {
		
		
		
		File excel = new File(Filepath);
		FileInputStream fis = new FileInputStream(excel);
		String value=null;
		XSSFWorkbook workbook= new XSSFWorkbook(fis);
		XSSFSheet ws = workbook.getSheet(sheet);
		XSSFCell cell;
		
		DataFormatter formatter = new DataFormatter();
		int rowNum = ws.getLastRowNum()+1;
	  int colNum = ws.getRow(0).getLastCellNum();
	 
		  String[][] parameterarray = new String[(colNum)][rowNum];
		  for (int i = 1; i < rowNum; i++) {
			  XSSFRow row = ws.getRow(i);
			  for (int j = 0; j < colNum; j++) {
				  
				  cell = row.getCell(j);
				
				 
				  if(!(cell==null)){
					  
					  
					 
					 value = formatter.formatCellValue(cell);
				  }
				  else{
					  value = null; 
				  }
	           
	            
	            parameterarray[j][i] = value;
	          
			  }
			  
		  }
		  workbook.close();
		  return parameterarray;
		
	}
	
public String writeXLSX(String Filepath,String Sheet,String Value,int col,int row) throws IOException{
  		
  		String val=Value;
  		String main=Sheet;
  		FileInputStream fis=new FileInputStream(Filepath);
  		XSSFWorkbook book=new XSSFWorkbook(fis);
  		XSSFSheet ws=book.getSheet(main);
  		ws.createRow(row).createCell(col).setCellValue(Value);
  		fis.close();
  		FileOutputStream fo=new FileOutputStream(Filepath);
  		book.write(fo);
  		book.close();
  		fo.flush();
  		fo.close();
  		return val;
  		
  	}
	

	public void XML_Read(String Sname,String Type)throws IOException, ParserConfigurationException, TransformerException, InterruptedException{
		
		PrintWriter writer=new PrintWriter("\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\HRHUB_Script\\HRHUB_Automation\\Testng.xml");
		writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		writer.println("<!DOCTYPE suite SYSTEM \"http://testng.org/testng-1.0.dtd\" >");
		writer.println("<suite name=\"HRHUB Automation\">");	
		writer.println("<test name=\""+Sname+"\" >");
		writer.println("<parameter name=\"Excelsheet\" value=\""+Sname+"\" />");
		writer.println("<parameter name=\"Scenario\" value=\""+Type+"\" />");
		writer.println(" <classes>");
		writer.println(" <class name=\"com.HRHUB.Tests.HR_HUB_Test\" /> ");
		writer.println("</classes>");
		writer.println("</test>");
		writer.println("</suite>");
		writer.close();
		Thread.sleep(3000);
	}
	
	public void Create_bat(String Path) throws FileNotFoundException, UnsupportedEncodingException, InterruptedException{
		
		
		PrintWriter writer = new PrintWriter(Path, "UTF-8");
	    writer.println("cd\\");
	    writer.println("pushd \\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\HRHUB_Script\\HRHUB_Automation");
	    writer.println("set path=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\jdk18\\bin;");
	    writer.println("set classpath=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;");
	    writer.println("java -cp \".\\bin;\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;\" org.testng.TestNG Testng.xml");
	    writer.println("exit");
	    writer.close();
	   

	}
	
	public void Execution(String Fname) throws Exception{
		Kill_Bat();
		Thread.sleep(2000);
		Runtime runtime = Runtime.getRuntime();
		System.out.println(runtime);
		Process p1 = runtime.exec("cmd /c start "+Fname);
		System.out.println(runtime);
		
	
	}
	
public void Execution_Wait() throws Exception{
		
	 String TASKLIST = "tasklist";
	 String KILL = "taskkill /F /IM ";
	 String processName = "cmd.exe";
	 for (int second = 0;second<300000; second++) {
			
		 if (isProcessRunning(processName,TASKLIST)) {

			 Thread.sleep(1000);
			 
		 }
		 else{
			 break;
		 }
		
		}
	   
		
	
	}

public void Kill_Bat() throws Exception{
	
	 Process p = Runtime.getRuntime().exec("taskkill /F /IM cmd.exe");
	   
		
	
	}

public static boolean isProcessRunning(String serviceName,String TASKLIST) throws Exception {

	 Process p = Runtime.getRuntime().exec(TASKLIST);
	 BufferedReader reader = new BufferedReader(new InputStreamReader(
	   p.getInputStream()));
	 String line;
	 while ((line = reader.readLine()) != null) {

	 // System.out.println(line);
	  if (line.contains(serviceName)) {
	   return true;
	  }
	 }

	 return false;

	}



public void File_Copy(String Src,String Desc) throws InterruptedException, IOException{
	File f1= new File(Src);
	File f2= new File(Desc);
	FileUtils.copyFile(f1, f2);

}

public void Report_File(String Report,String Report_Path) throws InterruptedException, IOException{
	String Src=Report;
	String Dest= Report_Path;
	File_Copy(Src,Dest);
System.out.println("Report Copy done");
Thread.sleep(3000);
}

public void XML_Read_Rapid(String[] Sname,String[] type,String code)throws IOException, ParserConfigurationException, TransformerException, InterruptedException{
	
	PrintWriter writer=new PrintWriter(code+"\\Testng.xml");
	writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
	writer.println("<!DOCTYPE suite SYSTEM \"http://testng.org/testng-1.0.dtd\" >");
	writer.println("<suite name=\"RAPID Automation\" parallel=\"methods\" thread-count=\"1\">");
	for(int i=0;i<Sname.length;i++) {
		
			String Script = Sname[i];
			String Script1=type[i];
	writer.println("<test name=\""+Script+"\" >");
	writer.println(" <classes>");
	writer.println(" <class name=\"com.rapid.Tests."+Script1+"\"/> ");
	writer.println("</classes>");
	writer.println("</test>");
		
	}
	writer.println("</suite>");
	writer.close();
	Thread.sleep(3000);
}

public void XML_Read_JE(String env,String[] Sname,String[] type)throws IOException, ParserConfigurationException, TransformerException, InterruptedException{
	PrintWriter writer;
	
	if(env.equalsIgnoreCase("Path1")) {
		writer=new PrintWriter("\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\JE_Integration_Script\\Path1\\JE_Path1_Final\\Testng.xml");
	}
	
	else {
		writer=new PrintWriter("\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\JE_Integration_Script\\Path2\\JE_Integration_Path2\\Testng.xml");
		
	}
	
	writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
	writer.println("<!DOCTYPE suite SYSTEM \"http://testng.org/testng-1.0.dtd\" >");
	writer.println("<suite name=\"JE Integration Automation\" >");
	for(int i=0;i<Sname.length;i++) {
		
			String Script = Sname[i];
			String Script1=type[i];
			
	writer.println("<parameter name=\"Excelsheet\" value=\""+Script+"\" />");
	writer.println("<test name=\""+Script+"\" >");
	writer.println(" <classes>");
	writer.println(" <class name=\"com.JE.Tests."+Script1+"\"/> ");
	writer.println("</classes>");
	writer.println("</test>");
		
	}
	writer.println("</suite>");
	writer.close();
	Thread.sleep(3000);
}

public void Create_bat_Rapid(String Path,String code) throws FileNotFoundException, UnsupportedEncodingException, InterruptedException{
	
	
	PrintWriter writer = new PrintWriter(Path, "UTF-8");
    writer.println("cd\\");
    writer.println("pushd "+code);
    writer.println("set path=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\jdk18\\bin;");
    writer.println("set classpath=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;");
    writer.println("java -cp \".\\bin;\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;\" org.testng.TestNG Testng.xml");
    writer.println("exit");
    writer.close();
   

}

public void Create_bat_JE(String env,String Path) throws FileNotFoundException, UnsupportedEncodingException, InterruptedException{
	
	
	PrintWriter writer = new PrintWriter(Path, "UTF-8");
    writer.println("cd\\");
    
    if(env.equalsIgnoreCase("Path1")) {
    	writer.println("pushd \\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\JE_Integration_Script\\Path1\\JE_Path1_Final");
    }
    else {
    	writer.println("pushd \\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\JE_Integration_Script\\Path2\\JE_Integration_Path2");
    }
    
    writer.println("set path=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\jdk18\\bin;");
    writer.println("set classpath=\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;");
    writer.println("java -cp \".\\bin;\\\\MIDP-SFS-010\\checogdata\\SIT Data\\SIT-Informatics\\006-General\\Automation_Utilities\\CVS_JE_lib\\*;\" org.testng.TestNG Testng.xml");
    writer.println("exit");
    writer.close();
   

}

public void XML_Rapid_Status(String path) throws SAXException, IOException, ParserConfigurationException {
	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(path);

    NodeList nList = doc.getElementsByTagName("Dashboard");
    
    for (int i = 0; i < nList.getLength(); i++) {
        System.out.println("Processing element " + (i+1) + "/" + nList.getLength());
        Node node = nList.item(i);
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            Element element = (Element) node;
            
            String s1 = element.getElementsByTagName("Name").item(0).getTextContent().trim();
            String s2 = element.getElementsByTagName("Status").item(0).getTextContent().trim();
			 String s3 = element.getElementsByTagName("ReportPath").item(0).getTextContent().trim();
            Status.put(s1, s2);
			Link.put(s1,s3);
        }
    }

 
    
    
	System.out.println("Done ");
    
}


public int Query_Finder(String val,String[][] data) {
	
	int kk=0;
	String[] que=new String[2];
	
	for(int i=1;i<data[0].length;i++) {
		
		if(data[0][i].equalsIgnoreCase(val)) {
			
			kk=i;
			break;
		}
		
	}
	
	
	return kk;
	
}


public String writeXLSX_exist(String Filepath,String Sheet,String Value,int col,int row) throws IOException{
	
	String val=Value;
	String main=Sheet;
	FileInputStream fis=new FileInputStream(Filepath);
	XSSFWorkbook book=new XSSFWorkbook(fis);
	XSSFSheet ws=book.getSheet(main);
	ws.getRow(row).createCell(col).setCellValue(Value);
	fis.close();
	FileOutputStream fo=new FileOutputStream(Filepath);
	book.write(fo);
	book.close();
	fo.flush();
	fo.close();
	return val;
	
}






}
