package Utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.testng.util.Strings;



public class Claim_Diagnosis {
	
	public  String DB2_Username;
	public  String DB2_Password;
	public  String DB2_DB_Name;
	public  String DB2_Port,Query1,Query2,Final_Query,count;
	public  String DB2_Host,Result_path,Miss_Path,Sequence_Mismatch_path;
	
	    public    Connection con;
	    public    Statement stmt;
	    public  ResultSet res,res1,res2;
	    public   HashMap<String, HashSet<String>> data =new HashMap<String, HashSet<String>>();
	    public   HashMap<String, HashSet<String>> Final_Source =new HashMap<String, HashSet<String>>();
	    public   HashMap<String, String> Primary_key =new HashMap<String, String>();
	    //public   HashMap<String, Integer> Aetna_Count =new HashMap<String, Integer>();
	   // public   HashMap<String, Integer> CSV_Count =new HashMap<String, Integer>();
	    public   HashMap<String, Integer> Final_Count =new HashMap<String, Integer>();
	    public   HashMap<String, HashSet<String>> result =new HashMap<String, HashSet<String>>();
	    public  HashMap<String, Integer> Aetna_Tot_Count=new  HashMap<String, Integer>();
	    public  HashMap<String, Integer> CSV_Tot_Count=new  HashMap<String, Integer>();
	    public HashSet<String> Aetna_Source_ID=new HashSet<String>();
	    public HashSet<String> CSV_Source_ID=new HashSet<String>();
	    public  String driverName_DB2="com.ibm.db2.jcc.DB2Driver";
	    public int Src_Missing_Count=0;
	    public int Tgt_Missing_Count=0;
	    public int Mismatch_Count=0;
	    public int Source_Count=0;
	    public int Target_Count=0;
	    
	    public int Total_Aetna_Diagnosis=0;
	    public int Total_CSV_Diagnosis=0;
	    public SimpleDateFormat formatter = new SimpleDateFormat("dd_MM_yyyy_hh_mm_ss");
	    
	    public void Setup(String Result_File,String Src_Username,String Src_Password,String Src_Port,String Src_DB_Name,String Src_Host,String q_count,String Q1,String Q2,String Final_Q) throws ClassNotFoundException, SQLException {
	    	
	    	DB2_Username=Src_Username;
			DB2_Password=Src_Password;
			DB2_DB_Name=Src_DB_Name;
			DB2_Port=Src_Port;
			DB2_Host=Src_Host;
			Query1=Q1;
			Query2=Q2;
			Final_Query=Final_Q;
			count=q_count;
			Result_path=Result_File;
			Miss_Path=Result_path+"Missing records_"+formatter.format(new Date())+".txt";
			Sequence_Mismatch_path=Result_path+"Sequence_Mismatch_"+formatter.format(new Date())+".txt";
			
			Class.forName(driverName_DB2);
	     	con = DriverManager.getConnection("jdbc:db2://"+DB2_Host+":"+DB2_Port+"/"+DB2_DB_Name,DB2_Username,DB2_Password);
	     	stmt = con.createStatement();
			
	    	
	    }
	    
	    public void execution() throws SQLException, FileNotFoundException, UnsupportedEncodingException {
	    	
	    	  res= stmt.executeQuery(Query1);
	     	  ResultSetMetaData rsmd=res.getMetaData();
	    	  int Src_Column_Count=rsmd.getColumnCount();

			 while(res.next())
				{
				 
				 HashSet<String> test=new HashSet<String>();
				 String key=res.getString(1).trim();
				 Aetna_Source_ID.add(res.getString(2).trim());

	             
				 for(int k=3;k<=Src_Column_Count;k++){
					 String v=res.getString(k);
					 if(k==3) {
						 
						 if (!(Strings.isNullOrEmpty(v))) {
							 
							 Primary_key.put(key, "1");
						 }else {
							 
							 Primary_key.put(key, "0");
							 
						 }
						 
						 
					 }

					 if (!(Strings.isNullOrEmpty(v))) {
						 String r=v.replace(".", "");
						 test.add(r.trim().toLowerCase());
	                    }
					 
					 

					 
				 }
				 
				 data.put(key,test);
				 //Aetna_Count.put(key, test.size());
				 
				 System.out.println("1111111111");
				 System.out.println(data);
				 
				 if(Aetna_Tot_Count.containsKey(res.getString(2).trim())) {
				 Aetna_Tot_Count.put(res.getString(2).trim(),Aetna_Tot_Count.get(res.getString(2).trim())+test.size());
				 }
				 else {
					 Aetna_Tot_Count.put(res.getString(2).trim(),test.size());
				 }
				 
				 
				 
				}
			 
			 
			 //Total_Aetna_Diagnosis=Aetna_Tot_Count.size();
			
			 if(count.equalsIgnoreCase("2")) {
				 
				 
				 
				 res1= stmt.executeQuery(Query2);
		     	 ResultSetMetaData rsmd1=res1.getMetaData();
		    	 int Src_Column_Count1=rsmd1.getColumnCount();
		    	 
		    	 while(res1.next())
					{
					 
					 String key=res1.getString(1).trim();
					 CSV_Source_ID.add(res1.getString(2).trim());
					 HashSet<String> test=new HashSet<String>();
					 if(data.containsKey(key)){
						
						 for(int k=3;k<=Src_Column_Count1;k++){ 
							 
							 String v=res1.getString(k);
							 if (!(Strings.isNullOrEmpty(v))) {
								 
								 String r=v.replace(".", "");
									
								 test.add(r.trim().toLowerCase());
			                    }
						 }
						
						
						 int size=data.get(key).size();
						 
						 data.get(key).addAll(test);
						 
						 System.out.println("1111111111");
						 System.out.println(data);
						 
						 int size1=data.get(key).size();
						 
						 int tot=size1-size;
						 
						 //CSV_Count.put(key,tot);
						 
						
						 
						 if(CSV_Tot_Count.containsKey(res1.getString(2).trim())) {
							 CSV_Tot_Count.put(res1.getString(2).trim(),CSV_Tot_Count.get(res1.getString(2).trim())+tot);
							 }
							 else {
								 CSV_Tot_Count.put(res1.getString(2).trim(),tot);
							 }
					 }
					
						 
					 }
		    	 
			 }
			 
             //Total_CSV_Diagnosis=CSV_Tot_Count.size();
			 
			 System.out.println("*********");
	    	 System.out.println(CSV_Tot_Count);
			 
			 
			 
			 for (String key : data.keySet())  
		  	    { 
		    		 HashSet<String> test=new HashSet<String>(data.get(key));
		    		 
		    		 if(Primary_key.get(key).equalsIgnoreCase("1")) {
		    			 
		    			 HashSet<String> test1=new HashSet<String>();
		    			 for(int k=1;k<=test.size();k++) {
		    				 
		    				 test1.add(String.valueOf(k));
		    				 
		    			 }
		    			 
		    			
		    			 Final_Source.put(key, test1) ;
		    		 }
		    		 else {
		    			 
		    			 HashSet<String> test1=new HashSet<String>();
		    			 for(int k=2;k<=test.size()+1;k++) {
		    				 
		    				 test1.add(String.valueOf(k));
		    				 
		    			 }
		    			 
		    			 Final_Source.put(key, test1) ;
		    			 
		    		 }
		    		 
		    		 
		  	    }
			 
			 
			 System.out.println("222222");
			 System.out.println(Final_Source);
			 
			 
			 
			 data.clear(); 
			 System.out.println(Final_Source.size()); 
			 
			 Source_Count=Final_Source.size(); 
			 
			 Iterator value = Aetna_Source_ID.iterator(); 
			 
		        while (value.hasNext()) { 
		        	Final_Count.put(value.next().toString(), 0); 
		        } 
		        
		        System.out.println("----------------------");
		    	 System.out.println(Final_Count);
		        
		        Iterator value1 = CSV_Source_ID.iterator(); 
				 
		        while (value1.hasNext()) { 
		        	Final_Count.put(value1.next().toString(), 0); 
		        } 
		        
		        System.out.println("----------------------");
		    	 System.out.println(Final_Count);
			 
			 res2= stmt.executeQuery(Final_Query);
		     	
	    	 while(res2.next())
				{
	    		 
	    		
	    		 String key=res2.getString(1).trim();
	    		 String val=res2.getString(2).trim();
	    		 String dia=res2.getString(3).trim();
	    		 
	    		 //Final_Count
	    		 
	    		 if(result.containsKey(key)) {
	    			 
	    			 HashSet<String> test=new HashSet<String>(result.get(key));
	    			 
	    			 test.add(val);
	    			 
	    			 result.put(key, test);
	    			 
	    			 if(Final_Count.containsKey(dia)) {
	    			 Final_Count.put(dia, Final_Count.get(dia)+1);
	    			 }
	    			 else {
	    				 Final_Count.put(dia, 0);
	    				 
	    			 }
	    			 
	    		 }
	    		 else {
	    			 
	    			 HashSet<String> test=new HashSet<String>();
	    			 test.add(val);
	    			 result.put(key, test);
	    			 if(Final_Count.containsKey(dia)) {
		    			 Final_Count.put(dia, Final_Count.get(dia)+1);
		    			 }
		    			 else {
		    				 Final_Count.put(dia, 0);
		    				 
		    			 }
	    			 
	    		 }
	    		
	    		
				}
	    	 
	    	 con.close();
	    	 System.out.println(result.size());
	    	 
	    	 System.out.println("----------------------");
	    	 System.out.println(Final_Count);
	    	 Target_Count=result.size();
	    	 PrintWriter writer = new PrintWriter(Sequence_Mismatch_path, "UTF-8");
			 writer.println("SGK Number--------------Target Sequence--------------------Source Sequence");
			 PrintWriter writer1 = new PrintWriter(Miss_Path, "UTF-8");
			 writer1.println("SGK Number--------------Error Description");
	    	 
	    	 
	    	 for (String key : Final_Source.keySet())  
	 	    { 
	 			
	 			if(result.containsKey(key)){
	 				
	 				if(!(result.get(key).equals(Final_Source.get(key)))) {
	 					
	 					writer.println(key+"--------------"+result.get(key)+"--------------------"+Final_Source.get(key));
	 					
	 				
	 					Mismatch_Count++;
	 				}
	 				
	 				
	 			}
	 			
	 			else {
	 				
	 				writer1.println(key+"--------------Present in Source but NOT in Target");
	 				Src_Missing_Count++;
	 			}
	 			
	 	    }
	    	 
	    	 

	    	 for (String key : result.keySet())  
	 	    { 
	 			
	 			if(!(Final_Source.containsKey(key))){
	 				
	 				writer1.println(key+"--------------Present in Target but NOT in Source");
	 				
	 				Tgt_Missing_Count++;
	 			}
	 			
	 	    }

	    	 
	    	 writer.close();
	    	 
	    	 writer1.close();

	    	
	    }
	    
	    public void File_Copy(String Src,String Desc) throws InterruptedException, IOException{
	    	File f1= new File(Src);
	    	File f2= new File(Desc);
	    	FileUtils.copyFile(f1, f2);

	    }

}
