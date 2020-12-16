package Utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Strings;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class File_Transfer {
	
	public  String Username=null;
	public  String Password=null;
	public  String host=null;
	public  String val="";
	public String Unix_path,filename,local_path;
	public   String driverName_Hive = "org.apache.hive.jdbc.HiveDriver";
	public   String driverName_Sql = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public  String driverName_DB2="com.ibm.db2.jcc.DB2Driver";
	public  String driverName_Nete="org.netezza.Driver";
	public  String driverName_Mysql="com.mysql.jdbc.Driver";
    public   Connection con;
    public   Statement stmt;
    public ResultSet res,res1,res2, res3,res4, res5,res6, res7,res8, res9,res10;
	
	
	
	public void setup(String user,String pwd,String hos,String upath,String name,String local,String type) throws ClassNotFoundException, SQLException, JSchException, SftpException {
		
		 Username=user;
		 Password=pwd;
		 host=hos;
		 String test=upath;
		 int dd=test.length();
		 char fin=test.charAt(dd-1);
		 
		 if(fin=='/') {
			 Unix_path= test;
		 }
		 else {
			 Unix_path= test+"/";
		 }
		
		 
		 filename=name;
		 local_path=local+"\\";
		 
		 if(type.equalsIgnoreCase("U_L")) {
			 Transfer_U_L(Unix_path+filename,local_path+filename) ;
			 
		 }
		 else {
			 Transfer_L_U(local_path+filename,Unix_path+filename);
			 
		 }
		
		
	}
	
	
	public void setup_ML(String user,String pwd,String hos,String name,String local,String FTP_File,String FTP_bat_path,String Result_Path) throws ClassNotFoundException, SQLException, JSchException, SftpException, IOException, InterruptedException {
		
		 Username=user;
		 Password=pwd;
		 host=hos;
		 filename=name;
		 local_path=local+"\\"+"'"+filename+"'";
		 String Src_File_mainframe_path=Result_Path+"'"+filename+"'";
	    	PrintWriter writer = new PrintWriter(FTP_File, "UTF-8");
		    writer.println(Username);
		    writer.println(Password);
		    writer.println("get '"+filename+"'");
		    writer.println("quit");
		    writer.close();
		    
		    PrintWriter writer1 = new PrintWriter(FTP_bat_path, "UTF-8");
		    writer1.println("cd\\");
		    writer1.println("C:");
		    writer1.println("cd "+Result_Path);
		    writer1.println("ftp -s:"+FTP_File+" "+host);
		    writer1.println("exit(0)");
		    writer1.close();
		    
		    Runtime runtime = Runtime.getRuntime();
			
			Process p1 = runtime.exec("cmd /c start "+FTP_bat_path);
			
			for(int i=0;i<50;i++) {
				
				if(new File(Result_Path).exists()) {
					break;
				}
				Thread.sleep(2000);
			}
			
		 
			File_Copy(Src_File_mainframe_path,local_path);
		
	}
	
	public  void Transfer_L_U(String Src,String path) throws ClassNotFoundException, SQLException, JSchException, SftpException {
		 
		
		 ChannelSftp channelSftp;
		 
		 java.util.Properties config = new java.util.Properties();
		    config.put("StrictHostKeyChecking", "no");
		    JSch ssh = new JSch();
		    com.jcraft.jsch.Session ses = ssh.getSession(Username, host, 22);
		    ses.setConfig(config);
		    ses.setPassword(Password);
		    ses.connect();
			    channelSftp = (ChannelSftp) ses.openChannel("sftp");
			    channelSftp.connect();
			   // channelSftp.get(Src,path);
			    channelSftp.put(Src, path);
			    channelSftp.disconnect();
			    ses.disconnect();

		}
	
	public  void Transfer_U_L(String Src,String path) throws ClassNotFoundException, SQLException, JSchException, SftpException {
		 
		
		 ChannelSftp channelSftp;
		 
		 java.util.Properties config = new java.util.Properties();
		    config.put("StrictHostKeyChecking", "no");
		    JSch ssh = new JSch();
		    com.jcraft.jsch.Session ses = ssh.getSession(Username, host, 22);
		    ses.setConfig(config);
		    ses.setPassword(Password);
		    ses.connect();
			    channelSftp = (ChannelSftp) ses.openChannel("sftp");
			    channelSftp.connect();
			    channelSftp.get(Src,path);
			    //channelSftp.put(Src, path);
			    channelSftp.disconnect();
			    ses.disconnect();

		}
	
	
     public  void XML_Reader(String XMl,String Target,String Tag,String Symbol) throws ParserConfigurationException, SAXException, IOException {
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(XMl);

        NodeList nList = doc.getElementsByTagName(Tag);
        
       PrintWriter writer=new PrintWriter(Target);
		

        for (int i = 0; i < nList.getLength(); i++) {
          
            Node node = nList.item(i);
            NodeList First_Parent= node.getChildNodes();
            
            val="";
            
            for (int j = 0; j < First_Parent.getLength(); j++) {
            	
            	Node node1 = First_Parent.item(j);
            	NodeList Second_Parent= node1.getChildNodes();
            	
            	if(node1.getChildNodes().getLength() >1) {
            		
              for (int k = 0; k < Second_Parent.getLength(); k++) {
            			
            			Node n1 = Second_Parent.item(k);
            			if(n1.getChildNodes().getLength() >1) {
            				
            				XML_Caller(n1);
            			}
            			else {
            			
            			if(Second_Parent.item(k).getNodeType() == Node.ELEMENT_NODE){
                            Element ele = (Element) Second_Parent.item(k);
                    	 String s1 = ele.getTextContent();
                    	 
                    	 if(val.length()==0) {
                    		 val=val+s1;
                    	 }
                    	 else {
                    		 
                    		 val=val+" | "+s1;
                    		 
                    	 }
                    	 
                    	}
            			}
            			
            		}
            		
            		
            		
            	}
            	
            	else {
            	
            	
            	
            	if(First_Parent.item(j).getNodeType() == Node.ELEMENT_NODE){
                    Element element = (Element) First_Parent.item(j);
            	 String s1 = element.getTextContent();
            	 
            	 if(val.length()==0) {
            		 val=val+s1;
            	 }
            	 else {
            		 
            		 val=val+Symbol+s1;
            		 
            	 }
            	 
            	}
            	
            }
            	
            }
            
                
                
                writer.println(val);
               
            }
        

        writer.close();
        
        
		System.out.println("Done ");

	}
     
     public  void Clean_Up(String s1,String s2,String s3,String s4,String s5,String s6,String s7) throws ClassNotFoundException, SQLException, JSchException, IOException, InterruptedException {
    	 
    	 String hive_path = null;
    	 String Cleanup = null;
    	 Class.forName(driverName_Hive);
     	 con = DriverManager.getConnection("jdbc:hive2://hiveldap.aetna.com:10000/default;AuthMech=3;",s1,s2);
     	 stmt = con.createStatement();
     	 
     	 res= stmt.executeQuery("describe formatted "+s3);
     	 
     	while(res.next())
		{
			
					String aa=res.getString(1);
					
					if(aa.contains("Location:")) {
						hive_path="hdfs dfs -rm "+res.getString(2);
						break;
						
					}
		}
     	
     	if(s4.equalsIgnoreCase("Drop")) {
     		
     		 res1= stmt.executeQuery("drop table "+s3);
     	}
     	
     	
     	
     	con.close();
     	
     	Cleanup_putty(hive_path,s1,s2);
     	
     	
     	if (s7.equalsIgnoreCase("DSPF_QA1_FDR"))
     	{
     		Cleanup="mysql -h xdspfdbm2q --port 50031 -u fdrmysq1_usr -pFdr#50031 -e 'delete from DSPF_QA1_FDR.AUDIT_BALANCE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA1_FDR.AUDIT_CYCLE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA1_FDR.AUDIT_TARGET_LOAD where batch_id="+s5+" and proc_id="+s6+"'";
     	}
     	else if(s7.equalsIgnoreCase("DSPF_QA2_FDR")) {
     		
     		Cleanup="mysql -h xdspfdbm2q --port 50032 -u fdrmysq2_usr -pFdr#50032 -e 'delete from DSPF_QA2_FDR.AUDIT_BALANCE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA2_FDR.AUDIT_CYCLE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA2_FDR.AUDIT_TARGET_LOAD where batch_id="+s5+" and proc_id="+s6+"'";
     	}else 
     	{
     		Cleanup="mysql -h xdspfdbm2q --port 50031 -u fdrmysq1_usr -pFdr#50031 -e 'delete from DSPF_QA3_FDR.AUDIT_BALANCE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA3_FDR.AUDIT_CYCLE_CONTROL where batch_id="+s5+" and proc_id="+s6+" ; delete from DSPF_QA3_FDR.AUDIT_TARGET_LOAD where batch_id="+s5+" and proc_id="+s6+"'";
     	}
     	
     	
     	Cleanup_putty(Cleanup,s1,s2);
     	
    	 
     }
     
     
     public String Cleanup_putty(String hive_path,String s1,String s2) throws JSchException, IOException, InterruptedException{
 		
 		String val=null;
 		try{
 	    	
 	    	java.util.Properties config = new java.util.Properties(); 
 	    	config.put("StrictHostKeyChecking", "no");
 	    	JSch jsch = new JSch();
 	    	Session session=jsch.getSession(s1,"Xhadstgm2p.aetna.com", 22);
 	    	session.setPassword(s2);
 	    	session.setConfig(config);
 	    	session.connect();
 	    	System.out.println("Connected");
 	    	
 	    	Channel channel=session.openChannel("exec");
 	        ((ChannelExec)channel).setCommand(hive_path);
 	        channel.setInputStream(null);
 	        ((ChannelExec)channel).setErrStream(System.err);
 	        
 	        InputStream in=channel.getInputStream();
 	        channel.connect();
 	        byte[] tmp=new byte[1024];
 	        while(true){
 	          while(in.available()>0){
 	            int i=in.read(tmp, 0, 1024);
 	            if(i<0)break;
 	            System.out.print(new String(tmp, 0, i));
 	            val=new String(tmp, 0, i);
 	          }
 	          if(channel.isClosed()){
 	            System.out.println("exit-status: "+channel.getExitStatus());
 	            break;
 	          }
 	          try{Thread.sleep(1000);}catch(Exception ee){}
 	        }
 	        channel.disconnect();
 	        session.disconnect();
 	  
 	       
 	        System.out.println("DONE");
 	        
 	     return val;
 	    }catch(Exception e){
 	    	
 	    	 
 	    	e.printStackTrace();
 	    	 return val;
 	    	 
 	    }
 		
 	}
     
     public  void XML_Caller(Node node1) {
 		
 		NodeList Second_Parent= node1.getChildNodes();
     	
     	if(node1.getChildNodes().getLength() >1) {
     		
     		for (int k = 0; k < Second_Parent.getLength(); k++) {
     			
     			Node n1 = Second_Parent.item(k);
     			if(n1.getChildNodes().getLength() >1) {
     				
     				XML_Caller(n1);
     			}
     			else {
     			
     			if(Second_Parent.item(k).getNodeType() == Node.ELEMENT_NODE){
                     Element ele = (Element) Second_Parent.item(k);
             	 String s1 = ele.getTextContent();
             	 
             	 if(val.length()==0) {
             		 val=val+s1;
             	 }
             	 else {
             		 
             		 val=val+" | "+s1;
             		 
             	 }
             	 
             	}
     			}
     		}
     	}
 		
 	}
     
     public void File_Copy(String Src,String Desc) throws InterruptedException, IOException{
 		File f1= new File(Src);
 		File f2= new File(Desc);
 		FileUtils.copyFile(f1, f2);

 	}
 	
 	

	

}
