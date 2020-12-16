package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.testng.util.Strings;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;



public class Layout {
	
	public   String driverName_Hive = "org.apache.hive.jdbc.HiveDriver";
	public   String driverName_Sql = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public  String driverName_DB2="com.ibm.db2.jcc.DB2Driver";
	public  String driverName_Nete="org.netezza.Driver";
	public  String driverName_Mysql="com.mysql.jdbc.Driver";
    public   Connection con;
    public   Statement stmt;
    public ResultSet res,res1,res2, res3,res4, res5,res6, res7,res8, res9,res10;
    public String Src_DB,Src_Username,Src_Password,Src_Server_name,Src_Port,Src_DB_Name,Src_Host;
	public  String Src_EOR;
	public  String Src_Symbol,Src_Header,Src_Footer;
	public  String Src_Col_Name;
	public  String Input_File_Path_Source;
	public String Src_Copy_book;
	public  String[] Src_Column_Name,Tgt_Column_Name;
	
	
	public  String Condition;
	public  String TB_Name,count;
	public  String Result_path,File_Path,Error_desc;
	public String[] Col_name,Keyword,Action;
	public int total_count=0;
	public int total_Print=0;
	public int[] index;
	public int[] Error_Count;
	public String[] Keyword_index;
	public String[] Action_index;
	public int Source_id_index;
	
	   
	    public   HashMap<Integer, String> Column_Index =new HashMap<Integer, String>();
	   
	    public SimpleDateFormat formatter = new SimpleDateFormat("dd_MM_yyyy_hh_mm_ss");
	    

	     
	    
	    public void Setup(String Result_File,String file) throws ClassNotFoundException, SQLException {
	    	
	    	
			Result_path=Result_File;
			File_Path=file;
			
			if(Src_DB.equalsIgnoreCase("Hive")) {
		    	
		    	Hive_HDBC(Src_Username,Src_Password,Src_Host);
		    	}

		    	if(Src_DB.equalsIgnoreCase("SQL Server")) {
		    	
		    		SQL_Server(Src_Server_name,Src_DB_Name);
		    	}
		    	
		    	if(Src_DB.equalsIgnoreCase("DB2")) {
		        	
		    		DB2_jDBC(Src_Username,Src_Password,Src_DB_Name,Src_Port,Src_Host);
		    	}
		    	
		          if(Src_DB.equalsIgnoreCase("Netezza")) {
		        	
		    		Netezza(Src_Username,Src_Password,Src_DB_Name,Src_Server_name);
		        	}
		          
		          if(Src_DB.equalsIgnoreCase("MySQL")) {
		          	
		        	  My_Sql(Src_Username,Src_Password,Src_DB_Name,Src_Port,Src_Host);
		          	}
		          
			
	    }
	    
	    public void execution() throws SQLException, IOException {
	    	
	    	String query="";
	    	
	    	if(Strings.isNullOrEmpty(Condition)) {
	    		query="select * from "+TB_Name;
	    		
	    	}
	    	else {
	    		
	    		query="select * from "+TB_Name+" where "+Condition;
	    	}
	    	
	    	
	    	
	    	  res= stmt.executeQuery(query);
	     	  ResultSetMetaData rsmd=res.getMetaData();
	    	  int Src_Column_Count=rsmd.getColumnCount();
	    	  
	    	  for(int i=1;i<=Src_Column_Count;i++) {
	    		  
	    		  Column_Index.put(i, rsmd.getColumnName(i).toLowerCase());
	    		  
	    	  }
	    	  
	    	  
	    	  String[][] data=readXLSXFile(File_Path,"Sheet1");
	    	  
	    	  int len=data[0].length-1;
	    	  
	    	  Col_name=new String[len];
	    	  Keyword=new String[len];
	    	  Action=new String[len];
	    	  index=new int[len];
	    	  Error_Count=new int[len];
	    	  int mm=0;
	    	  
	    	  for(int i=1;i<data[0].length;i++) {
	    		  
	    		  for (int j=1;j<=Column_Index.size();j++)  {
	    			  
	    			 
	    			  
	    			  if(Column_Index.get(j).equals(data[0][i].toLowerCase().trim())) {
	    				  
	    				  index[mm]=j;
	    				  Col_name[mm]=data[0][i].toLowerCase().trim();
	    				  Keyword[mm]=data[1][i].toLowerCase().trim();
	    				  Action[mm]=data[2][i];
	    				  Error_Count[mm]=0;
	    			
	    				  mm++;
	    				  break;
	    				  
	    			  }
	    			  
	    			  
	    		  }
	    		  
	    	  }
	    	  

	    	    PrintWriter writer = new PrintWriter(Result_path, "UTF-8");
				 writer.println("Error Description--------------Record");
	    	  
	    	  

			 while(res.next())
				{
				 
				 total_count++;
				 
				 Error_desc="";
				 
				 for(int i=0;i<index.length;i++) {
					 
					 String val=res.getString(index[i]);
					 
					 Validation(val,Keyword[i],Action[i],i,Col_name[i]);
					 
					

				 }
				 
				
				 
				 if(!(Error_desc.length()==0)) {
				 
				 String key_val="";
					
	             for(int k=0;k<Src_Column_Count;k++){
					 
						 String v=res.getString(k+1); 
						 if(k==0) {
							
							 if (res.wasNull()) {
								  key_val=key_val+v;
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (res.wasNull()) {
								  key_val=key_val+" | "+v;
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						
						 
					 }
	             
	             total_Print++;
	             
				 if(total_Print<=1000) {
	             writer.println(Error_desc+" --------------> "+key_val);
				 }
				 }
	 			
	 	    }

	    	 
	    	writer.close();

	    	
	    }
	    
	    
	    
public void execution_File() throws SQLException, IOException {
	    	
	          Src_Column_Name=Src_Col_Name.split(",");
	    	
	    	  
	    	  for(int i=0;i<Src_Column_Name.length;i++) {
	    		  
	    		  Column_Index.put(i, Src_Column_Name[i].trim().toLowerCase());
	    		  
	    	  }
	    	  
	    	  System.out.println(Column_Index);
	    	  String[][] data=readXLSXFile(File_Path,"Sheet1");
	    	  
	    	  int len=data[0].length-1;
	    	  
	    	  Col_name=new String[len];
	    	  Keyword=new String[len];
	    	  Action=new String[len];
	    	  index=new int[len];
	    	  Error_Count=new int[len];
	    	  int mm=0;
	    	  
	    	  for(int i=1;i<data[0].length;i++) {
	    		  
	    		  for (int j=0;j<Column_Index.size();j++)  {
	    			  
	    			 
	    			  
	    			  if(Column_Index.get(j).equals(data[0][i].toLowerCase().trim())) {
	    				  
	    				  index[mm]=j;
	    				  Col_name[mm]=data[0][i].toLowerCase().trim();
	    				  Keyword[mm]=data[1][i].toLowerCase().trim();
	    				  Action[mm]=data[2][i];
	    				  Error_Count[mm]=0;
	    			
	    				  mm++;
	    				  break;
	    				  
	    			  }
	    			  
	    			  
	    		  }
	    		  
	    	  }
	    	  

	    	    PrintWriter writer = new PrintWriter(Result_path, "UTF-8");
				 writer.println("Error Description--------------Record");
	    	  
	    	  

				 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
				 
			     String line1;
			     String temp_val = null;
			     
			     if(Src_Header.equalsIgnoreCase("Yes") ) {
					  line1 = reader1.readLine();  
					  line1 = reader1.readLine();
					 
					   }
				 
					   else {
						   line1 = reader1.readLine(); 
					   }
			     
			     
			     while (line1 != null)
			     {
				
			     
				 total_count++;
				 Error_desc="";
				 
				
				 
				 String[] vv=line1.split(Src_Symbol) ;
				  
				 
				 for(int i=0;i<index.length;i++) {
					 
					String temp=vv[index[i]].replace("\"", "");
					 String val=temp;
					 
					 Validation(val,Keyword[i],Action[i],i,Col_name[i]);
					 
					

				 }

				 if(!(Error_desc.length()==0)) {
				 
				 String key_val="";
					
	             for(int k=0;k<vv.length;k++){
					 
						 String v=vv[k]; 
						 if(k==0) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+v;
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+v;
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						
						 
					 }
	             
	             total_Print++;
	             
				 if(total_Print<=1000) {
	             writer.println(Error_desc+" --------------> "+key_val);
				 }
				 }
				 
				 line1 = reader1.readLine();
	 			
	 	    }
			     
			     reader1.close();

	    	 
	    	writer.close();

	    	
	    }


public void execution_File_unix() throws SQLException, IOException {
	
    Src_Column_Name=Src_Col_Name.split(",");
	
	  
	  for(int i=1;i<=Src_Column_Name.length;i++) {
		  
		  Column_Index.put(i, Src_Column_Name[i].trim().toLowerCase());
		  
	  }
	  
	  
	  String[][] data=readXLSXFile(File_Path,"Sheet1");
	  
	  int len=data[0].length-1;
	  
	  Col_name=new String[len];
	  Keyword=new String[len];
	  Action=new String[len];
	  index=new int[len];
	  Error_Count=new int[len];
	  int mm=0;
	  
	  for(int i=1;i<data[0].length;i++) {
		  
		  for (int j=1;j<=Column_Index.size();j++)  {
			  
			 
			  
			  if(Column_Index.get(j).equals(data[0][i].toLowerCase().trim())) {
				  
				  index[mm]=j;
				  Col_name[mm]=data[0][i].toLowerCase().trim();
				  Keyword[mm]=data[1][i].toLowerCase().trim();
				  Action[mm]=data[2][i];
				  Error_Count[mm]=0;
			
				  mm++;
				  break;
				  
			  }
			  
			  
		  }
		  
	  }
	  

	    PrintWriter writer = new PrintWriter(Result_path, "UTF-8");
		 writer.println("Error Description--------------Record");
	  
		 String[][] mydata=readXLSXFile(Src_Copy_book,"Copybook");
		 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
		 String line1;
	      String temp_val = null;
	     
	      
	      if(Src_Header.equalsIgnoreCase("Yes") ) {
	  		  line1 = reader1.readLine();  
	  		  line1 = reader1.readLine();
	  		 
	  		   }
	  	 
	  		   else {
	  			   line1 = reader1.readLine(); 
	  		   }
	    
	    while (line1 != null)
	    {
		 total_count++;
		 Error_desc="";
		
		 
		 for(int i=0;i<index.length;i++) {
			 
			  int start=Integer.parseInt(mydata[1][index[i]])-1;
	  		  int end=start + Integer.parseInt(mydata[2][index[i]]);
	  		  String val=line1.substring(start, end);
			 
			 Validation(val,Keyword[i],Action[i],i,Col_name[i]);
			 
			

		 }

		 if(!(Error_desc.length()==0)) {
		 
		 String key_val="";
			
       for(int k=1;k<mydata[0].length;k++){
			 
    	      int start=Integer.parseInt(mydata[1][k])-1;
	  		  int end=start + Integer.parseInt(mydata[2][k]);
	  		  String v=line1.substring(start, end);
				 if(k==0) {
					
					 if (Strings.isNullOrEmpty(v)) {
						  key_val=key_val+v;
					  }
					  else {
						  key_val=key_val+v.trim();
					  }
					 
				 }
				 else {
					 
					 if (Strings.isNullOrEmpty(v)) {
						  key_val=key_val+" | "+v;
					  }
					  else {
						  key_val=key_val+" | "+v.trim();
					  }
					 
				 }
				
				 
			 }
       
       total_Print++;
       
		 if(total_Print<=1000) {
       writer.println(Error_desc+" --------------> "+key_val);
		 }
		 }
		 
		 line1 = reader1.readLine();
		
   }
	     
	     reader1.close();

	 
	writer.close();

	
}
	    
	    
	    public void Validation(String val,String key,String Action,int index,String Col_Name)  {
	    	
	    	
	    	if(key.equalsIgnoreCase("digit")) {
	    		
	    		String[] ff=Action.split("or");
	    		
	    		boolean flag=true;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			int exe=Integer.parseInt(ff[i].trim());
	    			
	    			if(val.trim().length()==exe) {
	    				flag=false;
	    				break;
	    				
	    			}
	    			
	    		}
	    		
	    		if(flag) {
	    			
                        if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with Digit length "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with Digit length "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}
	    		
	    		
	    	}
	    	
	    	
             if(key.equalsIgnoreCase("StringLength")) {
	    		
	    		String[] ff=Action.split("or");
	    		
	    		boolean flag=true;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			int exe=Integer.parseInt(ff[i].trim());
	    			
	    			if(val.trim().length()==exe) {
	    				flag=false;
	    				break;
	    				
	    			}
	    			
	    		}
	    		
	    		if(flag) {
	    			
                        if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with String length "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with String length "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}
	    		
	    		
	    	}
             
             if(key.equalsIgnoreCase("MaxLength")) {
 	    		
 	    		String[] ff=Action.split("or");
 	    		
 	    		boolean flag=true;
 	    		
 	    		for(int i=0;i<ff.length;i++) {
 	    			
 	    			int exe=Integer.parseInt(ff[i].trim());
 	    			
 	    			if(val.trim().length()<=exe) {
 	    				flag=false;
 	    				break;
 	    				
 	    			}
 	    			
 	    		}
 	    		
 	    		if(flag) {
 	    			
                         if(Error_desc.length()==0) {
 	    				
 	    				Error_desc =Col_Name+" is not met with expectation with maximum length "+Action+" - "+val;
 	    				
 	    			}
 	    			else {
 	    				
 	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with maximum length "+Action+" - "+val;
 	    				
 	    			}
 	    			
 	    			
 	    			 Error_Count[index]=Error_Count[index]+1;
 	    			
 	    		}
 	    		
 	    		
 	    	}
	    	
            if(key.equalsIgnoreCase("default")) {
            	
            	if(Action.trim().equalsIgnoreCase("Space")) {
            		
            		if(!(val.trim().length()==0)) {
    	    			
    	    			if(Error_desc.length()==0) {
    	    				
    	    				Error_desc =Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			else {
    	    				
    	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			
    	    			
    	    			 Error_Count[index]=Error_Count[index]+1;
    	    			
    	    		}
            		
            	}
            	
            	else if(Action.trim().equalsIgnoreCase("Blank")) {
            		
            		if(!(val.length()==0)) {
    	    			
    	    			if(Error_desc.length()==0) {
    	    				
    	    				Error_desc =Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			else {
    	    				
    	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			
    	    			
    	    			 Error_Count[index]=Error_Count[index]+1;
    	    			
    	    		}
            		
            	}
            	else if(Action.trim().equalsIgnoreCase("null")) {
            		
                         if(!(Strings.isNullOrEmpty(val))) {
    	    			
    	    			if(Error_desc.length()==0) {
    	    				
    	    				Error_desc =Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			else {
    	    				
    	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with default value "+Action;
    	    				
    	    			}
    	    			
    	    			
    	    			 Error_Count[index]=Error_Count[index]+1;
    	    			
    	    		}
            		
            	}
            	
            	else {

	    		if(!(val.trim().equals(Action))) {
	    			
	    			if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with default value "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with default value "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}
            	}
	    		
	    	}
            
            if(key.equalsIgnoreCase("startwith")) {
            	
            	
                 String[] ff=Action.split("or");
	    		
	    		boolean flag=true;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			   if(val.trim().startsWith(ff[i].trim())) {
	    				   flag=false;
	    				   break;
	    			   }
	    			
	    			
	    			
	    		}
	    		
	    		
            	
               if(flag) {
	    			
	    			if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with Start with text "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with Start with text "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}

	    	}
            
            if(key.equalsIgnoreCase("endswith")) {
            	
            	
                String[] ff=Action.split("or");
	    		
	    		boolean flag=true;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			   if(val.trim().endsWith(ff[i].trim())) {
	    				   flag=false;
	    				   break;
	    			   }
	    			
	    			
	    			
	    		}
	    		
	    		
           	
              if(flag) {
	    			
	    			if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with End with text "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with End with text "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}

	    	}
           
            
            
            if(key.equalsIgnoreCase("contains")) {
            	
            	
                String[] ff=Action.split("or");
	    		
	    		boolean flag=true;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			if(ff[i].trim().equalsIgnoreCase("Blank"))   {
	    				
	    				if(val.length()==0) {
	    					flag=false;
		    				   break;
	    					
	    				}
	    			}
	    			else if(ff[i].trim().equalsIgnoreCase("Space")) {
	    				if(val.trim().length()==0) {
	    					flag=false;
		    				   break;
	    					
	    				}
	    				
	    			}
	    			
	    			else if(val.trim().contains(ff[i].trim())) {
	    				   flag=false;
	    				   break;
	    			   }
	    			
	    			
	    			
	    		}
	    		
	    		
           	
              if(flag) {
	    			
	    			if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with Contains text "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with Contains text "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}

	    	}
            
                  if(key.equalsIgnoreCase("NOT contains")) {
            	
            	
                String[] ff=Action.split("and");
	    		
	    		boolean flag=false;
	    		
	    		for(int i=0;i<ff.length;i++) {
	    			
	    			   
                   if(val.equalsIgnoreCase("Blank"))   {
	    				
	    				if(val.length()==0) {
	    					flag=true;
		    				
	    					
	    				}
	    			}
                   
                   else if(val.trim().equalsIgnoreCase("Space"))   {
	    				
	    				if(val.length()==0) {
	    					flag=true;
		    				
	    					
	    				}
	    			}
                   
                   else if(val.trim().contains(ff[i].trim())) {
	    				   flag=true;
	    				  
	    			   }
	    			   
	    			   
	    			
	    			
	    			
	    		}
	    		
	    		
           	
              if(flag) {
	    			
	    			if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with Contains text "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with Contains text "+Action+" - "+val;
	    				
	    			}
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
	    			
	    		}

	    	}
            
            if(key.equalsIgnoreCase("flag")) {
            	
            	String[] text=Action.split("or");
            	
            	if(val.trim().equals(text[0].trim()) ||val.trim().equals(text[1].trim())) {
            		
            		
            		
            		
            	}
            	else {
            		
                    if(Error_desc.length()==0) {
	    				
	    				Error_desc =Col_Name+" is not met with expectation with flag value "+Action+" - "+val;
	    				
	    			}
	    			else {
	    				
	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with flag value "+Action+" - "+val;
	    				
	    			}
            	
	    			
	    			
	    			 Error_Count[index]=Error_Count[index]+1;
            	
            		
            	}
            	
            	
            }
            
              if(key.equalsIgnoreCase("date")) {
            	  
            	  SimpleDateFormat sdf=new SimpleDateFormat(Action);
            	  sdf.setLenient(false);
            	  
            	  try {
            	  
            	  Date dd=sdf.parse(val);
            	  }
            	  catch(ParseException e) {
            		  
            		  if(Error_desc.length()==0) {
  	    				
  	    				Error_desc =Col_Name+" is not met with expectation with Date Format "+val;
  	    				
  	    			}
  	    			else {
  	    				
  	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with expectation with Date Format "+val;
  	    				
  	    			}
  	    			
  	    			
  	    			 Error_Count[index]=Error_Count[index]+1;
            		  
            	  }
              }
              
                if(key.equalsIgnoreCase("numeric")) {
            	  
            	  
                	try {
                        Double num = Double.parseDouble(val);
                    } catch (NumberFormatException e) {
                       
            		  
            		  if(Error_desc.length()==0) {
  	    				
  	    				Error_desc =Col_Name+" is not met with expectation with numeric value "+val;
  	    				
  	    			}
  	    			else {
  	    				
  	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with numeric value "+val;
  	    				
  	    			}
  	    			
  	    			
  	    			 Error_Count[index]=Error_Count[index]+1;
            		  
            	  }
              }
                
                if(key.equalsIgnoreCase("alphanumeric")) {
              	  
              	  
                	if(val.matches("[a-zA-Z0-9]+")) {
                       
                    } else{
                       
            		  
            		  if(Error_desc.length()==0) {
  	    				
  	    				Error_desc =Col_Name+" is not met with expectation with Alphanumeric value "+val;
  	    				
  	    			}
  	    			else {
  	    				
  	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with Alphanumeric value "+val;
  	    				
  	    			}
  	    			
  	    			
  	    			 Error_Count[index]=Error_Count[index]+1;
            		  
            	  }
              }
                
                
                if(key.equalsIgnoreCase("lowercase")) {
                	  
                	  
                	if(StringUtils.isAllLowerCase(val.trim())) {
                       
                    } else{
                       
            		  
            		  if(Error_desc.length()==0) {
  	    				
  	    				Error_desc =Col_Name+" is not met with expectation with Lower case "+val;
  	    				
  	    			}
  	    			else {
  	    				
  	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with Lower case "+val;
  	    				
  	    			}
  	    			
  	    			
  	    			 Error_Count[index]=Error_Count[index]+1;
            		  
            	  }
              }
                
                if(key.equalsIgnoreCase("uppercase")) {
              	  
              	  
                	if(StringUtils.isAllUpperCase(val.trim())) {
                       
                    } else{
                       
            		  
            		  if(Error_desc.length()==0) {
  	    				
  	    				Error_desc =Col_Name+" is not met with expectation with upper case "+val;
  	    				
  	    			}
  	    			else {
  	    				
  	    				Error_desc =Error_desc+ " , "+Col_Name+" is not met with upper case "+val;
  	    				
  	    			}
  	    			
  	    			
  	    			 Error_Count[index]=Error_Count[index]+1;
            		  
            	  }
              }
	    	
	    }
	    
	    
       
	    
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
	    
	    public void File_Copy(String Src,String Desc) throws InterruptedException, IOException{
	    	File f1= new File(Src);
	    	File f2= new File(Desc);
	    	FileUtils.copyFile(f1, f2);

	    }
	    
	    
	    
	    public void Unix_Caller(String Src,String path,String user,String Pwd,String Host) throws ClassNotFoundException, SQLException, JSchException, SftpException {
	   	 
	    	
	   	 ChannelSftp channelSftp;
	   	 
	   	 java.util.Properties config = new java.util.Properties();
	   	    config.put("StrictHostKeyChecking", "no");
	   	    JSch ssh = new JSch();
	   	    com.jcraft.jsch.Session ses = ssh.getSession(user,Host, 22);
	   	    ses.setConfig(config);
	   	    ses.setPassword(Pwd);
	   	    ses.connect();
	   		    channelSftp = (ChannelSftp) ses.openChannel("sftp");
	   		    channelSftp.connect();
	   		    channelSftp.get(Src,path);
	   		    
	   		    channelSftp.disconnect();
	   		    ses.disconnect();

	   	}
	    
	    
	    public void setvalue_Src_Hive(String Src_DB1,String Src_Username1,String Src_Password1,String host,String TB,String conn) throws IOException {
	    	
	    	Src_DB=Src_DB1;
	    	
	    	Src_Username=Src_Username1;
	    	Src_Password=Src_Password1;
	    	Src_Host=host;
	    	TB_Name=TB;
			Condition=conn;
	    	
	    	
	    }
	     
	     public void setvalue_Src_Sql_Server(String Src_DB1,String Src_Username1,String Src_Password1,String Server1,String DBname,String TB,String conn) throws IOException {
	     	
	     	Src_DB=Src_DB1;
	     	
	     	Src_Username=Src_Username1;
	     	Src_Password=Src_Password1;
	     	Src_Server_name=Server1;
	    	Src_DB_Name=DBname;
	    	TB_Name=TB;
			Condition=conn;
	     	
	     	
	     }
	     
	     public void setvalue_Src_DB2(String Src_DB1,String Src_Username1,String Src_Password1,String DBname,String port,String host,String TB,String conn) throws IOException {
	      	
	      	Src_DB=Src_DB1;
	      	
	      	Src_Username=Src_Username1;
	      	Src_Password=Src_Password1;
	      	Src_DB_Name=DBname;
	      	Src_Port=port;
	      	Src_Host=host;
	      	TB_Name=TB;
			Condition=conn;
	      	
	      	
	      }
	     
	     public void setvalue_Src_Mysql(String Src_DB1,String Src_Username1,String Src_Password1,String DBname,String port,String host,String TB,String conn) throws IOException {
	     
	    	 Src_DB=Src_DB1;
	       	
	       	Src_Username=Src_Username1;
	       	Src_Password=Src_Password1;
	       	Src_DB_Name=DBname;
	       	Src_Port=port;
	       	Src_Host=host;
	       	TB_Name=TB;
			Condition=conn;
	       	
	       	
	       }
	    	 
	     public void setvalue_Src_Netezza(String Src_DB1,String Src_Username1,String Src_Password1,String DBname,String server,String TB,String conn) throws IOException {
	       	
	       	Src_DB=Src_DB1;
	       	
	       	Src_Username=Src_Username1;
	       	Src_Password=Src_Password1;
	       	Src_DB_Name=DBname;
	       	Src_Server_name=server;
	       	TB_Name=TB;
			Condition=conn;
	       	
	       	
	       }
	     
	     
	     public void setvalue_Src_CSV(String Src_DB1,String input_file,String Symbol,String header,String footer,String col) throws IOException {
	        	
	        	Src_DB=Src_DB1;
	        	Input_File_Path_Source=input_file;
	        	Src_Symbol=Symbol;
	        	Src_Header=header;
	         	Src_Footer=footer;
	         	Src_Col_Name=col;
	        	
	        
	        	
	        }
	     
	     public void setvalue_Src_Unix(String Src_DB1,String User,String Pwd,String host,String input_file,String final_path,String Sym,String Head,String foot,String Copy,String col) throws IOException, ClassNotFoundException, SQLException, JSchException, SftpException {
	     	
	     	Src_DB=Src_DB1;
	     	Src_Username=User;
	       	Src_Password=Pwd;
	    	Src_Host=host;
	    	String filename=input_file;
	     	Input_File_Path_Source=final_path;
	     	Unix_Caller(filename,Input_File_Path_Source,Src_Username,Src_Password,Src_Host);
	     	Src_Header=Head;
	     	Src_Footer=foot;
	     	Src_Copy_book=Copy;
	     	Src_Symbol=Sym;
	     	Src_Col_Name=col;
	     	
	     	
	     
	     	
	     }
	     
	     

	     public  void Hive_HDBC(String Username,String Pwd,String host) throws ClassNotFoundException, SQLException{
	      	  
	    	   Class.forName(driverName_Hive);
	    	  
	    	con = DriverManager.getConnection("jdbc:hive2://"+host+":10000/default;AuthMech=3;",Username,Pwd);
	    	   
	    	   stmt = con.createStatement();
	    	   System.out.println("Hive Connection Success");
	        }
	       
	       public  void DB_Close() throws ClassNotFoundException, SQLException{
	       	con.close();

	        }
	       
	       public  void DB2_jDBC(String Username,String Pwd,String DB,String port ,String host) throws ClassNotFoundException, SQLException{
	      	  

	     	  Class.forName(driverName_DB2);
	       	   
	       	// db2ctqa1
	       	 //db2ctprd1
	       	 con = DriverManager.getConnection("jdbc:db2://"+host+":"+port+"/"+DB,Username,Pwd);
	       	stmt = con.createStatement();
	       	
	       	   System.out.println("DB2 Connection Success");
	           }
	        
	       
	       public  void SQL_Server(String Server,String DB) throws ClassNotFoundException, SQLException{
	    	  
	    	   Class.forName(driverName_Sql);
	    	   if(Strings.isNullOrEmpty(DB)) {
	    		con = DriverManager.getConnection("jdbc:sqlserver://"+Server+":1433;integratedSecurity=true");  
	    	   }
	    	   else {
	    	   con = DriverManager.getConnection("jdbc:sqlserver://"+Server+":1433;DatabaseName="+DB+";integratedSecurity=true");
	    	   }
	    	  stmt = con.createStatement();
	    	
	    	  System.out.println("Sql Server Connection Success");
	    	  

	        }
	       
	       public  void Netezza(String Username,String Pwd,String DB ,String server) throws ClassNotFoundException, SQLException{
	      	  
	      	   Class.forName(driverName_Nete);
	      	   
	    
	      	 con = DriverManager.getConnection("jdbc:netezza://"+server+"/"+DB,Username,Pwd);
	      	stmt = con.createStatement();
	      	
	      	   System.out.println("Netezza Connection Success");
	          }
	       
	       public  void My_Sql(String Username,String Pwd,String DB,String port ,String host) throws ClassNotFoundException, SQLException{
	      	  
	     	  Class.forName(driverName_Mysql);
	     	  con = DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+DB,Username,Pwd);
	         	stmt = con.createStatement();
	       	   System.out.println("MYSQL Connection Success");
	           }
	      
	       




	    
	   

}
