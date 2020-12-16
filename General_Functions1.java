package Utilities;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.common.base.Strings;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.monitorjbl.xlsx.StreamingReader;

public class General_Functions1 {
	
	public   String driverName_Hive = "org.apache.hive.jdbc.HiveDriver";
	public   String driverName_Sql = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public  String driverName_DB2="com.ibm.db2.jcc.DB2Driver";
	public  String driverName_Nete="org.netezza.Driver";
	public  String driverName_Mysql="com.mysql.jdbc.Driver";
    public   Connection con;
    public   Statement stmt;
    public ResultSet res,res1,res2, res3,res4, res5,res6, res7,res8, res9,res10;
    public String Src_DB,Src_Query,Src_Username,Src_Password,Src_Server_name,Src_Port,Src_DB_Name,Src_Host;
	public  String Src_EOR;
	public  String Src_Symbol,Src_Header,Src_Footer;
	public  String Src_Col_Name;
    public String Tgt_DB,Tgt_Query,Tgt_Username,Tgt_Password,Tgt_Server_name,Tgt_Port,Tgt_DB_Name,Tgt_Host,Hive_Key;
	public  String Tgt_EOR,Tgt_Header,Tgt_Footer;
	public  String Tgt_Symbol;
	public String Tgt_Col_Name;
    public  String Input_File_Path_Source,Default_Key,Partition_Value;
	public  String Input_File_Path_Target;
	public  String Tgt_Derived="No";
	
	public  String Result_File;
	public String Key_Column;
	public  Cell cell_final;
	public  String Src_FTP_file,Tgt_FTP_file;
	public  String Src_FTP_bat,Tgt_FTP_bat;
	public String Src_Copy_book,Tgt_Copy_book;
	public  String Src_File_Name,Tgt_File_Name;
	public  String Result_Folder;
	public  String Src_File_mainframe_path,Tgt_File_mainframe_path;
	
	public  HashMap<String, String> Src_Column_index =new HashMap<String, String>();
	public  HashMap<String, String> Tgt_Column_index =new HashMap<String, String>();
	
	public ArrayList<String> DDL_Src = new ArrayList<String>();
	
	public   HashSet<String> Src_data = new HashSet<String>(); 
	public   HashSet<String> Tgt_data = new HashSet<String>(); 
	public   HashSet<String> Src_duplicate = new HashSet<String>(); 
	public   HashSet<String> Tgt_duplicate = new HashSet<String>(); 
	public   HashSet<String> Temp_Src_data; 
	public   HashSet<String> Temp_Tgt_data; 
	public   HashMap<String, String> Src_Map =new HashMap<String, String>();
	public   HashMap<String, String> Tgt_Map =new HashMap<String, String>();
	
	public   HashMap<String, String> Query_Log =new HashMap<String, String>();
	
	public   HashMap<Integer, Long> Column_Mismatch =new HashMap<Integer, Long>();

	public   HashMap<Integer, String> Table_column =new HashMap<Integer, String>();
	public   HashMap<Integer, String> Table_column_key =new HashMap<Integer, String>();
   
   public  boolean Non_Primary=true;
    public  boolean DDL_Status=false;
    
    public  long Total_Src_Count,Total_Tgt_Count,Total_Src_Count_no_dup,Total_Tgt_Count_no_dup,Src_duplicate_count,Tgt_duplicate_count;
  
    public  String Source_Missing="Source Missing";
    public  String Target_Missing="Target Missing";
    public  String Count_Report="Count Summary";
    public  String Source_Duplicate="Source Duplicate";
    public  String Target_Duplicate="Target Duplicate";
    public  String Query_List="Query List";
    public static String Source_Columns="Source Columns";
    public static String Target_Columns="Target Columns";
    
    public static String Mismatches="Mismatches";
  
    public  String Data_Mismatch="Data Mismatch";
    public  String Summary="Summary";
    public  boolean Source_Missing_Flag=false;
    public  boolean Target_Missing_Flag=false;
    
    public  boolean Source_Duplicate_Flag=false;
    public  boolean Target_Duplicate_Flag=false;
    public  boolean Data_Mismatch_Flag=false;
    public  boolean Count_Mismatch_Flag=false;
    public  String[] Src_Column_Name,Tgt_Column_Name;
    public  int Src_Column_Count,Tgt_Column_Count;
    public  int Record_Count,Src_missing_count,Tgt_missing_count,Data_mismatch_count;
    public  int[] Keyvalue_Column;
    
    public   Row row_final;
	public  XSSFCellStyle lock_Final;
    
   
    public General_Functions1() {
    	
    	
    	Tgt_Derived="No";
    	Src_DB=null;
    	Src_Query=null;
    	Src_Username=null;
    	Src_Password=null;
    	Src_Server_name=null;
    	Src_Port=null;
    	Src_DB_Name=null;
    	Src_Host=null;
    	Src_EOR=null;
    	Src_Symbol=null;
    	Src_Header=null;
    	Src_Footer=null;
    	Src_Col_Name=null;
    	Tgt_DB=null;
    	Tgt_Query=null;
    	Tgt_Username=null;
    	Tgt_Password=null;
    	Tgt_Server_name=null;
    	Tgt_Port=null;
    	Tgt_DB_Name=null;
    	Tgt_Host=null;
    	Hive_Key=null;
    	Tgt_EOR=null;
    	Tgt_Symbol=null;
    	Tgt_Col_Name=null;
    	Input_File_Path_Source=null;
    	Default_Key=null;
    	Input_File_Path_Target=null;
    	Result_File=null;
    	Key_Column=null;
    	Src_FTP_file=null;
    	Tgt_FTP_file=null;
    	Src_FTP_bat=null;
    	Tgt_FTP_bat=null;
    	Src_Copy_book=null;
    	Tgt_Copy_book=null;
    	Src_File_Name=null;
    	Tgt_File_Name=null;
    	Result_Folder=null;
    	Src_File_mainframe_path=null;
    	Tgt_File_mainframe_path=null;
    	Source_Missing_Flag=false;
        Target_Missing_Flag=false;
       Source_Duplicate_Flag=false;
       Target_Duplicate_Flag=false;
        Data_Mismatch_Flag=false;
      Count_Mismatch_Flag=false;
    	
    	
    	Src_Column_index.clear();
    	Tgt_Column_index.clear(); 
    	Src_data.clear();
    	Tgt_data.clear();
    	Src_duplicate.clear();
    	Tgt_duplicate.clear();
    	//Temp_Src_data.clear();
    	//Temp_Tgt_data.clear();
    	Non_Primary=true;
        DDL_Status=false;
    	
    	DDL_Src.clear();
    	
    	
    	Src_Map.clear();
    	Tgt_Map.clear();
    	
    	Column_Mismatch.clear();

    	Table_column.clear();
    	Table_column_key.clear();
    	 Src_Column_Name=null;
         Tgt_Column_Name=null;

         Total_Src_Count=0;
         Total_Tgt_Count=0;
         Total_Src_Count_no_dup=0;
         Total_Tgt_Count_no_dup=0;
         Src_duplicate_count=0;
         Tgt_duplicate_count=0;
         Src_Column_Count=0;
         Tgt_Column_Count=0;
         Record_Count=0;
         Src_missing_count=0;
         Tgt_missing_count=0;
         Data_mismatch_count=0;
        Keyvalue_Column=null;
        
       
    }
  
    
     public void setvalue_Src_Hive(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String host) throws IOException {
    	
    	Src_DB=Src_DB1;
    	Src_Query=Src_Query1;
    	Src_Username=Src_Username1;
    	Src_Password=Src_Password1;
    	Src_Host=host;
    	
    	
    }
     
     public void setvalue_Src_Sql_Server(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String Server1,String DBname) throws IOException {
     	
     	Src_DB=Src_DB1;
     	Src_Query=Src_Query1;
     	Src_Username=Src_Username1;
     	Src_Password=Src_Password1;
     	Src_Server_name=Server1;
    	Src_DB_Name=DBname;
     	
     	
     }
     
     public void setvalue_Src_DB2(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String DBname,String port,String host) throws IOException {
      	
      	Src_DB=Src_DB1;
      	Src_Query=Src_Query1;
      	Src_Username=Src_Username1;
      	Src_Password=Src_Password1;
      	Src_DB_Name=DBname;
      	Src_Port=port;
      	Src_Host=host;
      	
      	
      }
     
     public void setvalue_Src_Mysql(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String DBname,String port,String host) throws IOException {
     
    	 Src_DB=Src_DB1;
       	Src_Query=Src_Query1;
       	Src_Username=Src_Username1;
       	Src_Password=Src_Password1;
       	Src_DB_Name=DBname;
       	Src_Port=port;
       	Src_Host=host;
       	
       	
       }
    	 
     public void setvalue_Src_Netezza(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String DBname,String server) throws IOException {
       	
       	Src_DB=Src_DB1;
       	Src_Query=Src_Query1;
       	Src_Username=Src_Username1;
       	Src_Password=Src_Password1;
       	Src_DB_Name=DBname;
       	Src_Server_name=server;
       	
       	
       }
     
     public void setvalue_Src_Excel(String Src_DB1,String input_file) throws IOException {
       	
       	Src_DB=Src_DB1;
       	Input_File_Path_Source=input_file;
       	
       
       	
       }
     
     public void setvalue_Src_CSV(String Src_DB1,String input_file,String Symbol,String header,String footer) throws IOException {
        	
        	Src_DB=Src_DB1;
        	Input_File_Path_Source=input_file;
        	
        	Src_Symbol=Symbol;
        	Src_Header=header;
         	Src_Footer=footer;
        	
        
        	
        }
     
     public void setvalue_Src_Unix(String Src_DB1,String User,String Pwd,String host,String input_file,String final_path,String Sym,String Head,String foot,String Copy) throws IOException, ClassNotFoundException, SQLException, JSchException, SftpException {
     	
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
     	
     	
     
     	
     }
     
     public void setvalue_Src_Mainframe(String Src_DB1,String user,String pwd,String host,String f3,String FF,String f1,String f2,String f4,String f5) throws IOException, InterruptedException {
     	
     	Src_DB=Src_DB1;
     	Src_Username=user;
       	Src_Password=pwd;
       	Src_Host=host;
       	Src_File_Name=f3;
        Src_FTP_file=FF;
    	Src_FTP_bat=f1;
    	Src_Copy_book=f2;
    	Result_Folder=f4;
    	Input_File_Path_Source=f5;
    	Src_File_mainframe_path=Result_Folder+"'"+Src_File_Name+"'";
    	
    	PrintWriter writer = new PrintWriter(Src_FTP_file, "UTF-8");
	    writer.println(Src_Username);
	    writer.println(Src_Password);
	    writer.println("get '"+Src_File_Name+"'");
	    writer.println("quit");
	    writer.close();
	    
	    PrintWriter writer1 = new PrintWriter(Src_FTP_bat, "UTF-8");
	    writer1.println("cd\\");
	    writer1.println("C:");
	    writer1.println("cd "+Result_Folder);
	    writer1.println("ftp -s:"+Src_FTP_file+" "+Src_Host);
	    writer1.println("exit(0)");
	    writer1.close();
	    
	    Runtime runtime = Runtime.getRuntime();
		
		Process p1 = runtime.exec("cmd /c start "+Src_FTP_bat);
		
		for(int i=0;i<50;i++) {
			
			if(new File(Src_File_mainframe_path).exists()) {
				break;
			}
			Thread.sleep(2000);
		}
		
     	
     }
     
     
     public void setvalue_Tgt_Mainframe(String Src_DB1,String user,String pwd,String host,String f3,String FF,String f1,String f2,String f4,String f5) throws IOException {
      	
      	Tgt_DB=Src_DB1;
      	Tgt_Username=user;
        	Tgt_Password=pwd;
        	Tgt_Host=host;
        	Tgt_File_Name=f3;
         Tgt_FTP_file=FF;
     	Tgt_FTP_bat=f1;
     	Tgt_Copy_book=f2;
     	Result_Folder=f4;
     	Input_File_Path_Target=f5;
     	Tgt_File_mainframe_path=Result_Folder+"'"+Tgt_File_Name+"'";
     	
     	PrintWriter writer = new PrintWriter(Src_FTP_file, "UTF-8");
 	    writer.println(Src_Username);
 	    writer.println(Src_Password);
 	    writer.println("get '"+Tgt_File_Name+"'");
 	    writer.println("quit");
 	    writer.close();
 	    
 	    PrintWriter writer1 = new PrintWriter(Tgt_FTP_bat, "UTF-8");
 	    writer1.println("cd\\");
 	    writer1.println("C:");
 	    writer1.println("cd "+Result_Folder);
 	    writer1.println("ftp -s:"+Tgt_FTP_file+" "+Tgt_Host);
 	    writer1.println("exit(0)");
 	    writer1.close();
 	    
 	    Runtime runtime = Runtime.getRuntime();
 		
 		Process p1 = runtime.exec("cmd /c start "+Tgt_FTP_bat);
 		
      	
      }
      
     
     public void setvalue_Tgt_CSV(String Src_DB1,String input_file,String Symbol,String head,String foot) throws IOException {
     	
     	Tgt_DB=Src_DB1;
     	Input_File_Path_Target=input_file;
     	
     	Tgt_Symbol=Symbol;
     	
     	Tgt_Header=head;
       	Tgt_Footer=foot;
     	
     
     	
     }
     
     public void setvalue_Tgt_Hive(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String host) throws IOException {
     	
     	Tgt_DB=Src_DB1;
     	Tgt_Query=Src_Query1;
     	Tgt_Username=Src_Username1;
     	Tgt_Password=Src_Password1;
     	Tgt_Host=host;
     	
     	
     }
      
      public void setvalue_Tgt_Sql_Server(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String Server1,String DBname) throws IOException {
      	
      	Tgt_DB=Src_DB1;
      	Tgt_Query=Src_Query1;
      	Tgt_Username=Src_Username1;
      	Tgt_Password=Src_Password1;
      	Tgt_Server_name=Server1;
    	Tgt_DB_Name=DBname;
      
      	
      }
      
      public void setvalue_Tgt_DB2(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String DBname,String port,String host) throws IOException {
       	
       	Tgt_DB=Src_DB1;
       	Tgt_Query=Src_Query1;
       	Tgt_Username=Src_Username1;
       	Tgt_Password=Src_Password1;
       	Tgt_DB_Name=DBname;
       	Tgt_Port=port;
    	Tgt_Host=host;
      
       	
       }
      
      public void setvalue_Tgt_Netezza(String Src_DB1,String Src_Query1,String Src_Username1,String Src_Password1,String DBname,String server) throws IOException {
         	
         	Tgt_DB=Src_DB1;
         	Tgt_Query=Src_Query1;
         	Tgt_Username=Src_Username1;
         	Tgt_Password=Src_Password1;
         	Tgt_DB_Name=DBname;
        	Tgt_Server_name=server;
        
         	
         }
      
      public void setvalue_Tgt_Unix(String Src_DB1,String User,String Pwd,String host,String input_file,String final_path,String Sym,String Head,String foot,String Copy) throws IOException, ClassNotFoundException, SQLException, JSchException, SftpException {
       	
       	Tgt_DB=Src_DB1;
       	Tgt_Username=User;
         	Tgt_Password=Pwd;
      	Tgt_Host=host;
      	String filename=input_file;
       	Input_File_Path_Target=final_path;
       	Unix_Caller(filename,Input_File_Path_Target,Tgt_Username,Tgt_Password,Tgt_Host);
       	Tgt_Header=Head;
       	Tgt_Footer=foot;
       	Tgt_Copy_book=Copy;
       	Tgt_Symbol=Sym;
       	//Tgt_Col_Name=col;
       	
       
       	
       }
       
      
      public void setvalue_Tgt_Excel(String Src_DB1,String input_file,String data) throws IOException {
        	
        	Tgt_DB=Src_DB1;
        	Input_File_Path_Target=input_file;
        	Tgt_Derived=data;
       	
        }
      
      public void set_Key_Value(String key,String Result_File1,HashMap<Integer, Long> Column_Mismatch1, HashMap<Integer, String> Table_column1,HashMap<Integer, String> Table_column_key1,String col,String[] CC) throws IOException {
    	  
    	     
    		 
    		 
    	  Result_File=Result_File1;
    	  Column_Mismatch=Column_Mismatch1;
    	  Table_column=Table_column1;
    	  Table_column_key=Table_column_key1;
    	  Key_Column=key;
      	  Key_Value_Split();
      	  Create_workbook_Sheets(); 
      	  Src_Col_Name=col;
      	  Tgt_Col_Name=col;
      	  Src_Column_Count=CC.length;
      	  Tgt_Column_Count=CC.length;
      	  Src_Column_Name=CC;
      	  Tgt_Column_Name=CC;
      	  
      	 if(Src_Column_Count==Keyvalue_Column.length) {
    		 Non_Primary=false; 
    	 }
    	 
      	  
      	
      }
      
      public void Source_caller() throws ClassNotFoundException, SQLException, IOException {
      	Source_query_Exe();
      	
      	
      }
      
      public void Source_caller_Excel() throws ClassNotFoundException, SQLException, IOException {
    	  Source_Excel_Exe();
      	
      }
      
      public void Target_caller() throws ClassNotFoundException, SQLException, IOException {
    	  Target_query_Exe();
        	
        	
        }
        
        public void Target_caller_Excel() throws ClassNotFoundException, SQLException, IOException {
      	  Target_Excel_Exe();
        	
        }
        
        public void Common_caller() throws ClassNotFoundException, SQLException, IOException {
        	Missing_Record();
        	Data_Validation();
        	Count_Validation();
        	Summary_sheet();
          	
          	
          }
        
        

    
    public void Source_query_Exe() throws SQLException, ClassNotFoundException, IOException {
    	
    	System.out.println("Source Query Begin");
    	
    	Query_Log.put("Source Query", Src_Query);
    	long startTime = System.currentTimeMillis();
    	
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
          
          String[] Temp_Src_Query=Src_Query.split(";");
     	 
 	     
 	     int  src_len=Temp_Src_Query.length;
 	   
 	     
 	     if(Temp_Src_Query.length==1) {
 	    	 Src_Query=Temp_Src_Query[0];
 	     }
 	     
 	     else {
 	    	 Src_Query=Temp_Src_Query[src_len-1];
 	    
 	    	 for(int l=0;l<src_len-1;l++) {
 	    		 stmt.execute(Temp_Src_Query[l]);
 	    	 }
 	     }
 	     
          
    	
    	 res= stmt.executeQuery(Src_Query);
    	 
    	
    	 int gg=0;
    	 int jj=0;
		
		 
		 long Src_count=0;
		 while(res.next())
			{
			 
			 Src_count++;
			 
			
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
             
             
			   int temp=Src_data.size();
				 Src_data.add(key_val);
				 if(temp==Src_data.size()) {
					 Src_duplicate.add(key_val);
				 }
			 
			 
			 
			} 
		 DB_Close();
		 System.out.println("Source Query End");
		 
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 
		 Source_dup();
		 Src_duplicate.clear();
		 System.out.println("Source completed");
		 long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
		 System.out.println("Source time taken :"+elapsedTime/1000+ " Seconds");

    }
    
    
    
    
    
public void Target_query_Exe() throws SQLException, ClassNotFoundException, IOException {
	System.out.println("Target Started");
	
	Query_Log.put("Target Query", Tgt_Query);
	long startTime = System.currentTimeMillis();	
      if(Tgt_DB.equalsIgnoreCase("Hive")) {
    	
    	Hive_HDBC(Tgt_Username,Tgt_Password,Tgt_Host);
    	}

    	if(Tgt_DB.equalsIgnoreCase("SQL Server")) {
    	
    		SQL_Server(Tgt_Server_name,Tgt_DB_Name);
    	}
    	
    	if(Tgt_DB.equalsIgnoreCase("DB2")) {
        	
    		DB2_jDBC(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Port,Tgt_Host);
    	}
         if(Tgt_DB.equalsIgnoreCase("Netezza")) {
        	
    		Netezza(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Server_name);
        	}
         
         String[] Temp_Tgt_Query=Tgt_Query.split(";");
   	 
	     
	     int  tgt_len=Temp_Tgt_Query.length;
	    
	     
	     if(Temp_Tgt_Query.length==1) {
	    	 Tgt_Query=Temp_Tgt_Query[0];
	     }
	     
	     else {
	    	 Tgt_Query=Temp_Tgt_Query[tgt_len-1];
	    
	    	 for(int l=0;l<tgt_len-1;l++) {
	    		 stmt.execute(Temp_Tgt_Query[l]);
	    	 }
	     }
	     
    	
         
    	res= stmt.executeQuery(Tgt_Query);
    	
    	 System.out.println(Tgt_Column_Count);
		 long Tgt_count=0;
		 while(res.next())
			{
			 
			 Tgt_count++;
			 //System.out.println(Tgt_count);
			 

			 String key_val="";
			
             for(int k=0;k<Tgt_Column_Count;k++){
				 
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
             
             
			   int temp=Tgt_data.size();
			   //System.out.println(key_val);
			   Tgt_data.add(key_val);
				 if(temp==Tgt_data.size()) {
					 Tgt_duplicate.add(key_val);
				 }
			 
			 
			 
			} 
		 DB_Close();
		 System.out.println("Target Query End");
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Tgt_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 Target_dup();
		
		 Tgt_duplicate.clear();
		 System.out.println("Target completed");
		
		 long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
		 System.out.println("Target time taken :"+elapsedTime/1000+ " Seconds");
    	
    }

  public boolean Key_Column_identifier(int i) {
	
	 boolean test = false; 
    for (int element : Keyvalue_Column) { 
        if (element == i) { 
            test = true; 
            break; 
        } 
    }
    
    if(test) {
   	 return true;
   	
    }
    else
   	 return false;
	
}
  
  public boolean Key_Column_identifier1(int i,int[] data) {
		
		 boolean test = false; 
	    for (int element : data) { 
	        if (element == i) { 
	            test = true; 
	            break; 
	        } 
	    }
	    
	    if(test) {
	   	 return true;
	   	
	    }
	    else
	   	 return false;
		
	}
  
  
  public  void Source_dup() throws IOException{
	  long startTime = System.currentTimeMillis();	
	  System.out.println("Source duplicate begin");
	  ArrayList<String> record = new ArrayList<String>(Src_duplicate);
	
	  int h=0;
	  Header_Duplicate(Result_File,Source_Duplicate,Src_Column_Name);
	
	  
	  if(record.size()>0) {
		  Source_Duplicate_Flag=true;
	  Duplicate_Bulk_Writer(Result_File,Source_Duplicate,record);
	  }
	 // System.out.println("Source Duplicate end");
	  record.clear();
	
	  long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
		 System.out.println("Source Duplicate time Taken:"+elapsedTime/1000+ " Seconds");
	  
  }
  
public void Target_dup() throws IOException{
	 long startTime = System.currentTimeMillis();	
	 System.out.println("Target Duplicate begin");
	  ArrayList<String> record = new ArrayList<String>(Tgt_duplicate);
	 
	  int h=0;
	  
	  Header_Duplicate(Result_File,Target_Duplicate,Tgt_Column_Name);
	  
	  
	 
	  
	  if(record.size()>0) {
		  Target_Duplicate_Flag=true;
	   Duplicate_Bulk_Writer(Result_File,Target_Duplicate,record);
	  }
		
		 record.clear();
		
		  long stopTime = System.currentTimeMillis();
			 long elapsedTime = stopTime - startTime;
			 System.out.println("Target Duplicate time Taken:"+elapsedTime/1000+ " Seconds");
		  
  }




public  void Count_Validation() throws IOException{
	 long startTime = System.currentTimeMillis();	
	  
	System.out.println("Count validation begin");
    
    if(Total_Src_Count==Total_Tgt_Count){
		Count_Mismatch_Flag=false;
	}
	else{
		Count_Mismatch_Flag=true;
	}
    
    Count_Write(Result_File,Count_Report);
  
  
	System.out.println("count validation end");
	 long stopTime = System.currentTimeMillis();
	 long elapsedTime = stopTime - startTime;
	 System.out.println("Target Duplicate time Taken:"+elapsedTime/1000+ " Seconds");
}

public void Missing_Record() throws IOException{
	 long startTime = System.currentTimeMillis();
	 
	 if(Tgt_Derived.equalsIgnoreCase("No")) {
	
	System.out.println("Missing record begin");
	int row=0;
	ArrayList<String> record_Target = new ArrayList<String>();
	ArrayList<String> record_Source = new ArrayList<String>();
	
	ArrayList<String> Dele_Src = new ArrayList<String>();
	ArrayList<String> Dele_Tgt = new ArrayList<String>();
	
	Header_Missing_tgt(Result_File,Target_Missing);
	Header_Missing_Src(Result_File,Source_Missing);
	
    
    Temp_Src_data = new  HashSet<String>(Src_data);
    Temp_Tgt_data = new  HashSet<String>(Tgt_data);
   
    Temp_Src_data.removeAll(Tgt_data);
    Temp_Tgt_data.removeAll(Src_data);
    
    Src_data.clear();
	Tgt_data.clear();
	
	spliter1();
	

    Temp_Src_data.clear();
    Temp_Tgt_data.clear();
    
    int count=0;
    int count1=0;
    		
    
    for (String key : Src_Map.keySet())  
    { 
         
    	if(!(Tgt_Map.containsKey(key))){
			  Source_Missing_Flag=true;
			  
			  count++;
			  Dele_Src.add(key);
			 
			  if(count<=200) {
			  record_Source.add("Present in Source But not in Target | "+key);
    	      }  
			 
		  }
    	
    } 
    
    for (String key : Tgt_Map.keySet())  
    { 
         
    	if(!(Src_Map.containsKey(key))){
			  Target_Missing_Flag=true;
			  count1++;
			  Dele_Tgt.add(key);
				 
			  if(count1<=200) {
			 
			  record_Target.add("Present in Target But not in Source | "+key);
			  }
			 
		  }
    	
    } 
    
    for (String key : Dele_Src)  {
    	
    	Src_Map.remove(key);
    	
    }
    

    for (String key : Dele_Tgt)  {
    	
    	Tgt_Map.remove(key);
    	
    }
    
    

    
		  System.out.println("Target Missing Count : "+count1); 
		  System.out.println("Source Missing Count : "+count); 
		  Tgt_missing_count=count1;
		 
		
		  Src_missing_count=count;
		
		Missing_Record_Bulk_Writer(Result_File,Source_Missing,record_Source);
		Missing_Record_Bulk_Writer(Result_File,Target_Missing,record_Target);
	      
		System.out.println("Missing record end");
		
		record_Target.clear();
		record_Source.clear();
		
		
		 long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
		 System.out.println("Missing time Taken:"+elapsedTime/1000+ " Seconds");
	 }
	 
	 else {
		 
		 Tgt_missing_count=0;
		 
			
		  Src_missing_count=0;;
		 
	 }
	
	
}



public void spliter1() {
	
	  Iterator value = Temp_Src_data.iterator(); 
	  
      while (value.hasNext()) { 
    	  
    	  String[] act=value.next().toString().split(" \\| ");
		  
			String key_val="";
			String val="";
			int key_start=0;
			int nonkey_start=0;
			
			 for(int j=0;j<act.length;j++) {
					
				  if(Key_Column_identifier(j)) {
					  
					  key_start++;
					  
					  String v=act[j]; 
						 if(key_start==1) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+"";
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+"";
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						 
					 }
						 else {
							 nonkey_start++;
							 String v=act[j]; 
							 if(nonkey_start==1) {
								
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+"";
								  }
								  else {
									  val=val+v.trim();
								  }
								 
							 }
							 else {
								 
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+" | "+"";
								  }
								  else {
									  val=val+" | "+v.trim();
								  }
								 
							 }
							 
					  
				  }
				  
			 }
			 
    	  
			 Src_Map.put(key_val,val);
    	  
      
      }
      
      
   Iterator value1 = Temp_Tgt_data.iterator(); 
	  
      while (value1.hasNext()) { 
    	  
    	  String[] act=value1.next().toString().split(" \\| ");
		  
			String key_val="";
			String val="";
			int key_start=0;
			int nonkey_start=0;
			
			 for(int j=0;j<act.length;j++) {
					
				  if(Key_Column_identifier(j)) {
					  key_start++;
					  
					  String v=act[j]; 
						 if(key_start==1) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+"";
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+"";
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						 
					 }
						 else {
							 nonkey_start++;
							 
							 String v=act[j]; 
							 if(nonkey_start==1) {
								
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+"";
								  }
								  else {
									  val=val+v.trim();
								  }
								 
							 }
							 else {
								 
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+" | "+"";
								  }
								  else {
									  val=val+" | "+v.trim();
								  }
								 
							 }
							 
					  
				  }
				  
			 }
			 
    	  
			 Tgt_Map.put(key_val,val);
    	  
      
      }
	
	
	
	
}




   public void Data_Validation() throws IOException{
	   
	   //Data_Validation_Derived
	   
	   if(Tgt_Derived.equalsIgnoreCase("No")) { 
	  
	int count=0;
		
	boolean Sta;
	
	if(Non_Primary) {

	
	ArrayList<String> data_write = new ArrayList<String>();
	
	
	
	Header_data_mismatch(Result_File,Data_Mismatch);
	
		
		        for (String key : Tgt_Map.keySet()) {
		        	 Sta=false;
		        	 
		        	
		 //System.out.println(Tgt_Map.get(key)+"-----"+Src_Map.get(key));

				if(Tgt_Map.get(key).equals(Src_Map.get(key))) {
					
				}
				else {
					
					count++;
			
					Data_Mismatch_Flag=true;
					
					String[] Tgt_val=Tgt_Map.get(key).split(" \\| ");
					String[] Src_val=Src_Map.get(key).split(" \\| ");
					
					String val="";
					
					
					for(int i=0;i<Tgt_val.length;i++) {
						
						if(!(Tgt_val[i].trim().equalsIgnoreCase(Src_val[i].trim()))) {
							
		
							 if(val.length()==0) {
							
								val=val+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
							 }
							 else {
								 
								 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
								 
							 }
							
							
							Column_Mismatch.put(i+1, Column_Mismatch.get(i+1)+1);
							
							if(Column_Mismatch.get(i+1)<=10) {
								
								Sta=true;
							}
						}
						
						else {
							
							 if(val.length()==0) {
									
									val=val+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
								 }
								 else {
									 
									 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
									 
								 }
						}
						
						
						
					}
					
					if(Sta){
						
						data_write.add(key+" | "+val);
						
					}
					
					
					
				}
			}
		
		        Data_mismatch_count=count;
		        
		      
		   
		       Data_validation_Bulk_Writer(Result_File,Data_Mismatch,data_write);
	}
	else {
		
		Header_No_pri(Result_File,Data_Mismatch);
		
		
		
	}
	
	   }
	   else {
		   
		   Data_Validation_Derived();
	   }
		
		   	  
}





    
    public void Key_Value_Split() {
    	
    	 String[] Keyvalue_Split=Key_Column.split(",");
		  Keyvalue_Column=new int[Keyvalue_Split.length];
		  for(int x=0;x<Keyvalue_Split.length;x++){
		    	
		    	Keyvalue_Column[x]=Integer.parseInt(Keyvalue_Split[x]);
	
		    	
		    }
		 
    	
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
     
      
      public  String[][] readXLSX(String Filepath,String Sheet) throws IOException{
   		File excel=new File(Filepath);
   		FileInputStream fis=new FileInputStream(excel);
   		String Value=null;
   		XSSFWorkbook book=new XSSFWorkbook(fis);
   		XSSFSheet ws=book.getSheet(Sheet);
   		XSSFCell cell;
   		
   		DataFormatter format=new DataFormatter();
   		long rowNum=ws.getLastRowNum()+1;
   		long colNum=ws.getRow(0).getLastCellNum();
   		
   		String[][] Parameter=new String[(int)colNum][(int)rowNum];
   		for(int i=0;i<rowNum;i++){
   			XSSFRow row=ws.getRow(i);
   			for(int j=0;j<colNum;j++){
   				
   				cell=row.getCell(j);
   				if(!(cell==null)){
   					Value=format.formatCellValue(cell);
   				}
   				else{
   					Value="";
   				}
   				Parameter[j][i]=Value;
   			}
   			
   		}
   		book.close();
   		return Parameter;
   		
   	}
       
      
      public  boolean isStringEmpty(String input){
          if(input.trim().length() == 0){
              return true;
          }
          return false;
      }
  	
  	public  boolean isStringNull(String input){
  	
          if(input == null ){
              return true;
          }
          return false;
      }
  	
public  void Create_Sheet(String Filepath,String Sheet) throws IOException{
 		
 		FileInputStream fis=new FileInputStream(Filepath);
 		XSSFWorkbook book=new XSSFWorkbook(fis);
 		XSSFSheet ws=book.createSheet(Sheet);
 		
 		if(Sheet.equalsIgnoreCase("Summary")){
 			book.setSheetOrder("Summary", 0);
 		}
 		fis.close();
 		FileOutputStream fo=new FileOutputStream(Filepath);
 		book.write(fo);
 		book.close();
 		fo.flush();
 		fo.close();
 		
 	}

public  void Create_workbook_Sheets() throws IOException{
	  
    Create_Book(Result_File);
	Create_Sheet(Result_File,Count_Report);
	Create_Sheet(Result_File,Source_Duplicate);
	Create_Sheet(Result_File,Target_Duplicate);
	Create_Sheet(Result_File,Source_Missing);
	Create_Sheet(Result_File,Target_Missing);
	Create_Sheet(Result_File,Data_Mismatch);
	Create_Sheet(Result_File,Summary);
	Create_Sheet(Result_File,Query_List);
  
  
}
 	

 	
 public  String Create_Book(String Filepath) throws IOException{
 	
 		String FP=Filepath;
 		XSSFWorkbook workbook = new XSSFWorkbook();
 	    FileOutputStream out = new FileOutputStream(new File(FP));
 	      workbook.write(out);
 	      out.close();
 	      return FP;
 			
 		}
 
 
 public void File_Copy(String Src,String Desc) throws InterruptedException, IOException{
		File f1= new File(Src);
		File f2= new File(Desc);
		FileUtils.copyFile(f1, f2);

	}
 
 public void Restart_server(String url) throws InterruptedException, IOException{
		Runtime runtime = Runtime.getRuntime();
		System.out.println(runtime);
		Process p1 = runtime.exec("cmd /c start chrome "+url);
		Thread.sleep(10000);
		p1.destroy();
	}
 
 
 public void Source_Excel_Exe() throws SQLException, ClassNotFoundException, IOException {
	 	
	 
	  //HashSet<String> Coll = new HashSet<String>(); 
	  String[] ll = null;
		InputStream is = new FileInputStream(new File(Input_File_Path_Source));
		Workbook workbook = StreamingReader.builder()
		        .rowCacheSize(100)   
		        .bufferSize(4096)    
		        .open(is); 
		
		 Sheet s = workbook.getSheet("Sheet1");
		 
		 long Src_count=0;
		 int jj=0;
		 int rr=0;
		
		 for (Row r : s) {
			 
			 String key_val="";
			
			 int i=0;
			 int gg=0;
			 int ff=0;
			 if(rr==0) {
				
			 }
			 
			 for(int kk=0;kk<Src_Column_Count;kk++) {
			    	
			         if(jj==0) {
			        	
					        //Cell c=r.getCell(kk);
			        	
						 //Src_Column_Count=r.getLastCellNum();
						// Coll.add(c.getStringCellValue());
			        	
			        	 //ll[kk]=c.getStringCellValue();
			        	
			        	 i++;
			        	 
			         }
			         else {
			        	 
			        	 String v; 
					      Cell c=r.getCell(kk);
	
					      if(!(c==null)){
			 					v=c.getStringCellValue();
			 				}
			 				else{
			 					v="";
			 				} 
						 if(kk==0) {
							
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
			         }
			    
			   
			   
			    if(jj==0){
			    	 jj++;
			    }
			    else {
			    	 Src_count++;
			    	 int temp=Src_data.size();
					   Src_data.add(key_val);
					 if(temp==Src_data.size()) {
						 Src_duplicate.add(key_val);
					 }
			    	
			    }
			   
				 
				 
					}
		 
		 
		 
		 
		 //Src_Column_Name =ll;
		
			  
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Src_Count);
		 System.out.println("Srctotal count no dup : "+Total_Src_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Src_duplicate_count);
		 
		 Source_dup();
		
		 System.out.println("Source completed");
	 }
 
 
 
 public void Target_Excel_Exe() throws SQLException, ClassNotFoundException, IOException {
	 
	 HashSet<String> Coll = new HashSet<String>(); 
	 String[] ll = null;
		InputStream is = new FileInputStream(new File(Input_File_Path_Target));
		Workbook workbook = StreamingReader.builder()
		        .rowCacheSize(100)   
		        .bufferSize(4096)    
		        .open(is); 
		
		 Sheet s = workbook.getSheet("Sheet1");
		 
		 long Tgt_count=0;
		 int jj=0;
		 int rr=0;
		
		 for (Row r : s) {
			 
			 String key_val="";
			
			 int i=0;
			 int gg=0;
			 if(rr==0) {
				 //Tgt_Column_Count=r.getLastCellNum();
				// ll=new String[Tgt_Column_Count];
				// rr++;
			 }
			 
			 for(int kk=0;kk<Tgt_Column_Count;kk++) {
			    	
			         if(jj==0) {
			        	
					        //Cell c=r.getCell(kk);
			        	
						 //Tgt_Column_Count=r.getLastCellNum();
						 //Coll.add(c.getStringCellValue());
						 //ll[kk]=c.getStringCellValue();
			        	 
			        
			        	 //i++;
			        	 
			         }
			         else {
			        	 
			        	 String v; 
					      Cell c=r.getCell(kk);
	
					      if(!(c==null)){
			 					v=c.getStringCellValue();
			 				}
			 				else{
			 					v="";
			 				} 
						 if(kk==0) {
							
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
			         }
			    
			   
			   
			    if(jj==0){
			    	 jj++;
			    }
			    else {
			    	 Tgt_count++;
			    	 int temp=Tgt_data.size();
					   Tgt_data.add(key_val);
					 if(temp==Tgt_data.size()) {
						 Tgt_duplicate.add(key_val);
					 }
			    	
			    }
			   
				 
				 
					}
		 
		 
		 
		
		 //Tgt_Column_Name =ll;
		
		
		
			  
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Tgt_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 System.out.println("Tgttotal count : "+Total_Tgt_Count);
		 System.out.println("Tgttotal count no dup : "+Total_Tgt_Count_no_dup);
		 System.out.println("Tgt Duplicate Record count : "+Tgt_duplicate_count);
		 
		 Target_dup();
		
 }
 
 public void Summary_sheet() throws IOException{	
	 
	 Summary_Write(Result_File,Summary);
	 Query_Bulk_Writer(Result_File,Query_List);
	
	
}
 
 
 public String[] Uncommon(String src,String type) throws FileNotFoundException{
		String[] FinalData = null;
		
		String[] temp=src.split(";");
		int k=temp.length-1;
		
		if(!(type.equalsIgnoreCase("Excel"))) {
		String data=temp[k].replaceAll("\\s+", " ").trim();
		String[] data1=data.split("\\bfrom\\b");
		System.out.println(data1[0]);
		String dd2=data1[0].replace("select", "");
		dd2=dd2.trim();
		System.out.println("-----------------------");
		System.out.println(dd2);
		String[] dd3=dd2.split(",");
		FinalData=new String[dd3.length];
		
		for(int i=0;i<dd3.length;i++){
			
			String bb=dd3[i].trim();
			//if(bb.split(" ").length==1){
				
				FinalData[i]=bb;
				
				
			//}
			//else{
				
				//String[] dd4=bb.split("\\bas\\b");
				//String cc=dd4[1].trim();
				//FinalData[i]=cc;
			
				
			//}
		}
		}
		else {
			
			InputStream is = new FileInputStream(new File(src));
			Workbook workbook = StreamingReader.builder()
			        .rowCacheSize(100)   
			        .bufferSize(4096)    
			        .open(is); 
			
			 Sheet s = workbook.getSheet("Sheet1");
			 
		
			 int rr=0;
			  int ll=0;
			 for (Row r : s) {
				 
				 if(rr==0) {
					 ll =r.getLastCellNum();
					 FinalData=new String[ll];
					 rr++;
				 }
				 
				 else {
					 break;
				 }
				 
				 for(int kk=0;kk<ll;kk++) {
				    	
				       
				        	
						        Cell c=r.getCell(kk);
						        FinalData[kk]=c.getStringCellValue();
							
				        	 
				        
				 }
				 
				
			 }
			
			
			
		}
		
		return FinalData;
		
		
		
	}



 public String[] Uncommon_csv(String src) throws FileNotFoundException{
	 
	 String[] FinalData = null;
	 
	 
     String[] data1=src.split(",");
     FinalData=new String[data1.length];
     
     for(int i=0;i<data1.length;i++) {
    	 
    	 FinalData[i]=data1[i].trim();
     }
	 
	 
	 
	 
	 return FinalData;
 }
 
 public String[] Uncommon_Mainframe(String src) throws IOException{
	 String[] FinalData = null;
	 
	 String[][] data=readXLSX(src,"Copybook");
	 int j=0;
	 
	 for(int i=1;i<data[0].length;i++) {
		 
		 FinalData[j]=data[0][i];
		 j++;
	 }
	 
	 
	 
	 
	 return FinalData;
	 
	 
 }
 
 
 public void Source_Delimiter_Exe() throws IOException{
	 	
		
	  BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
	  long Src_count=0;
	  int len=0;
    
     
     int gg=0;
     int ff=0;
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
   	  String key_val="";
   	
   	 String[] v=line1.split(Src_Symbol) ;
   	
   		   len=v.length;
   	  
   
   	  for(int i=0;i<len;i++)
         {
   		 
   		  if(i==0) {
   			  
   			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v[i].trim();
				  }
				 
   			  
   		  }
   		  else {
   			  
   			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v[i].trim();
				  }
   			   
         }
   	  
         }
   	  
   	  
   	 Src_count++;
   	 
   	 //System.out.println(key_val);
	 int temp=Src_data.size();
	   Src_data.add(key_val);
	   temp_val=key_val;
	 if(temp==Src_data.size()) {
		 Src_duplicate.add(key_val);
	 }


line1 = reader1.readLine();

 
}

if(Src_Footer.equalsIgnoreCase("Yes")) {
Src_data.remove(temp_val);
Src_count=Src_count-1;
 }
	
     
     reader1.close();
		
		   
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Src_Count);
		 System.out.println("Srctotal count no dup : "+Total_Src_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Src_duplicate_count);
		 
		 Source_dup();
		
		 System.out.println("Source completed");
	 }
 
 public void Source_Delimiter_Unix_Exe() throws IOException{
	 	
		
	  BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
	  long Src_count=0;
	  int len=0;
   
    
    int gg=0;
    int ff=0;
    String line1;
    String temp_val = null;
    
   // String line1 = reader1.readLine();
    
    System.out.println(Src_Symbol);
    
    if(Src_Header.equalsIgnoreCase("Yes") ) {
		  line1 = reader1.readLine();  
		  line1 = reader1.readLine();
		 
		   }
	 
		   else {
			   line1 = reader1.readLine(); 
		   }
		
    
    while (line1 != null)
    {
  	  String key_val="";
  	  
  	  String[] v=line1.split(Src_Symbol) ;

  	  for(int i=0;i<v.length;i++)
        {
  		 
  		  if(i==0) {
  			  
  			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v[i].trim();
				  }
				 
  			  
  		  }
  		  else {
  			  
  			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v[i].trim();
				  }
  			   
        }
  	  
        }
  	  
  	  
  	     Src_count++;
	    	 int temp=Src_data.size();
	    	
			   Src_data.add(key_val);
			   temp_val=key_val;
			 if(temp==Src_data.size()) {
				 Src_duplicate.add(key_val);
			 }
  	  
  	  
  	  line1 = reader1.readLine();
        
         
    }
    
    if(Src_Footer.equalsIgnoreCase("Yes")) {
    	Src_data.remove(temp_val);
    	Src_count=Src_count-1;
 	    }
			
    
    reader1.close();
		 
		
		   
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Src_Count);
		 System.out.println("Srctotal count no dup : "+Total_Src_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Src_duplicate_count);
		 
		 Source_dup();
		
		 System.out.println("Source completed");
	 }
 
 public void Target_Delimiter_Unix_Exe() throws IOException{
	 	
		
	  BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Target));
	  long Tgt_count=0;
	  int len=0;
  
  
   int gg=0;
   int ff=0;
   String line1;
   String temp_val = null;
   
  // String line1 = reader1.readLine();
   
   if(Tgt_Header.equalsIgnoreCase("Yes") ) {
		  line1 = reader1.readLine();  
		  line1 = reader1.readLine();
		 
		   }
	 
		   else {
			   line1 = reader1.readLine(); 
		   }
		
   
   while (line1 != null)
   {
 	  String key_val="";
 	  
 	  String[] v=line1.split(Tgt_Symbol) ;
 	 
 	 
 	  
 	  for(int i=0;i<len;i++)
       {
 		 
 		  if(i==0) {
 			  
 			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v[i].trim();
				  }
				 
 			  
 		  }
 		  else {
 			  
 			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v[i].trim();
				  }
 			   
       }
 	  
       }
 	  
 	  
 	     Tgt_count++;
	    	 int temp=Tgt_data.size();
			   Tgt_data.add(key_val);
			   temp_val=key_val;
			 if(temp==Tgt_data.size()) {
				 Tgt_duplicate.add(key_val);
			 }
 	  
 	  
 	  line1 = reader1.readLine();
       
        
   }
   
   if(Tgt_Footer.equalsIgnoreCase("Yes")) {
   	Tgt_data.remove(temp_val);
   	Tgt_count=Tgt_count-1;
	    }
			
   
   reader1.close();
		 
		
		   
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Tgt_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 System.out.println("Tgttotal count : "+Total_Tgt_Count);
		 System.out.println("Tgttotal count no dup : "+Total_Tgt_Count_no_dup);
		 System.out.println("Tgt Duplicate Record count : "+Tgt_duplicate_count);
		 
		 Target_dup();
		
		 System.out.println("Target completed");
	 }

 
 
 public void Target_Delimiter_Exe() throws IOException{
	 	
		
	  BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Target));
	  long Tgt_count=0;
	  int len=0;
   
    
    String line1;
    String temp_val = null;
    
   // String line1 = reader1.readLine();
    
    if(Tgt_Header.equalsIgnoreCase("Yes") ) {
 		  line1 = reader1.readLine();  
 		  line1 = reader1.readLine();
 		 
 		   }
 	 
 		   else {
 			   line1 = reader1.readLine(); 
 		   }
    
    while (line1 != null)
    {
  	  String key_val="";
  	  
  	  String[] v=line1.split(Tgt_Symbol) ;
  	  
  	 
  		   len=v.length;
  	  
  	 
  	  
  	  for(int i=0;i<len;i++)
        {
  		 
  		  if(i==0) {
  			  
  			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v[i].trim();
				  }
				 
  			  
  		  }
  		  else {
  			  
  			  if (Strings.isNullOrEmpty(v[i])) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v[i].trim();
				  }
  			   
        }
  	  
        }
  	  
  	  
  	 Tgt_count++;
  	 
  	 System.out.println(key_val);
	 int temp=Tgt_data.size();
	   Tgt_data.add(key_val);
	   temp_val=key_val;
	 if(temp==Tgt_data.size()) {
		 Tgt_duplicate.add(key_val);
	 }


line1 = reader1.readLine();


}

if(Tgt_Footer.equalsIgnoreCase("Yes")) {
Tgt_data.remove(temp_val);
Tgt_count=Tgt_count-1;
}
	

			
    
    reader1.close();
		 
		
		   
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Src_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Tgt_Count);
		 System.out.println("Srctotal count no dup : "+Total_Tgt_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Tgt_duplicate_count);
		 
		 Target_dup();
		
		 System.out.println("Target completed");
	 }
 
 
 public void Source_Mainframe_Exe() throws IOException, InterruptedException{
	 
	 
	 File_Copy(Src_File_mainframe_path,Input_File_Path_Source);
	 
	 new File(Src_FTP_file).delete();
	 new File(Src_FTP_bat).delete();
	 new File(Src_File_mainframe_path).delete();
	 
	 String[][] mydata=readXLSX(Src_Copy_book,"Copybook");
		
		
		
		 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
		  long Src_count=0;
		  int len=0;
	  
	      
	      int gg=0,ff=0;
	      
	      
	   
	    
	    String line1 = reader1.readLine();
			
	    
	    while (line1 != null)
	    {
	  	  String key_val="";
	  	  
	  	  
	  	  for(int i=1;i<mydata[0].length;i++)
	        {
	  		  
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		  int end=start + Integer.parseInt(mydata[2][i]);
	  		  String v=line1.substring(start, end);
	  		 
	  		  if(i==1) {
	  			  
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
	  	  
	  	  
	  	     Src_count++;
		    	 int temp=Src_data.size();
				   Src_data.add(key_val);
				 if(temp==Src_data.size()) {
					 Src_duplicate.add(key_val);
				 }
	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
				
	    
	    reader1.close();
	    
	  
		   
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Src_Count);
		 System.out.println("Srctotal count no dup : "+Total_Src_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Src_duplicate_count);
		 
		 Source_dup();
		
			 
	
	 
	 
 }
 
public void Source_Unix_Copybook_Exe() throws IOException, InterruptedException{
	 
	 
	     String[][] mydata=readXLSX(Src_Copy_book,"Copybook");
		 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Source));
		  long Src_count=0;
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
	  	  String key_val="";
	  	  
	  	  
	  	  for(int i=1;i<mydata[0].length;i++)
	        {
	  		  
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		  int end=start + Integer.parseInt(mydata[2][i]);
	  		  String v=line1.substring(start, end);
	  		 
	  		  if(i==1) {
	  			  
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
	  	  
	  	  
	  	     Src_count++;
		    	 int temp=Src_data.size();
				   Src_data.add(key_val);
				   temp_val=key_val;
				 if(temp==Src_data.size()) {
					 Src_duplicate.add(key_val);
				 }
	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
	    
	    if(Src_Footer.equalsIgnoreCase("Yes")) {
	    	Src_data.remove(temp_val);
	    	Src_count=Src_count-1;
	 	    }
				
	    
	    reader1.close();
	   
		   
		 Total_Src_Count=Src_count;
		 Total_Src_Count_no_dup=Src_data.size();
		 Src_duplicate_count=Src_duplicate.size();
		 System.out.println("Srctotal count : "+Total_Src_Count);
		 System.out.println("Srctotal count no dup : "+Total_Src_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Src_duplicate_count);
		 
		 Source_dup();
		
			 
	
	 
	 
 }

public void Target_Unix_Copybook_Exe() throws IOException, InterruptedException{
	 
	 
	 String[][] mydata=readXLSX(Tgt_Copy_book,"Copybook");
		
		
		
		 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Target));
		  long Tgt_count=0;
		 
	   
	      
	      String line1;
	      String temp_val = null;
	     
	      
	      if(Tgt_Header.equalsIgnoreCase("Yes") ) {
	  		  line1 = reader1.readLine();  
	  		  line1 = reader1.readLine();
	  		 
	  		   }
	  	 
	  		   else {
	  			   line1 = reader1.readLine(); 
	  		   }
	    
	    while (line1 != null)
	    {
	  	  String key_val="";
	  	  
	  	  
	  	  for(int i=1;i<mydata[0].length;i++)
	        {
	  		  
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		  int end=start + Integer.parseInt(mydata[2][i]);
	  		  String v=line1.substring(start, end);
	  		 
	  		  if(i==1) {
	  			  
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
	  	  
	  	  
	  	     Tgt_count++;
		    	 int temp=Tgt_data.size();
				   Tgt_data.add(key_val);
				   temp_val=key_val;
				 if(temp==Tgt_data.size()) {
					 Tgt_duplicate.add(key_val);
				 }
	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
	    
	    if(Tgt_Footer.equalsIgnoreCase("Yes")) {
	    	Tgt_data.remove(temp_val);
	    	Tgt_count=Tgt_count-1;
	 	    }
				
	    
	    reader1.close();
	    
	  
		   
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Tgt_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 System.out.println("Tgttotal count : "+Total_Tgt_Count);
		 System.out.println("Srctotal count no dup : "+Total_Tgt_Count_no_dup);
		 System.out.println("Src Duplicate Record count : "+Tgt_duplicate_count);
		 
		 Target_dup();
		
			 
	
	 
	 
}

 
public void Target_Mainframe_Exe() throws IOException, InterruptedException{
	 
	 
	 File_Copy(Tgt_File_mainframe_path,Input_File_Path_Target);
	 String[][] mydata=readXLSX(Tgt_Copy_book,"Copybook");
		
	
		
		 BufferedReader reader1 = new BufferedReader(new FileReader(Input_File_Path_Target));
		  long Tgt_count=0;
		  int len=0;
	   
	     
	    
	    String line1 = reader1.readLine();
			
	    
	    while (line1 != null)
	    {
	  	  String key_val="";
	  	  
	  	  
	  	  for(int i=1;i<mydata[0].length;i++)
	        {
	  		  
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		  int end=start + Integer.parseInt(mydata[2][i]);
	  		  String v=line1.substring(start, end);
	  		 
	  		  if(i==1) {
	  			  
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
	  	  
	  	  
	  	     Tgt_count++;
		    	 int temp=Tgt_data.size();
				   Tgt_data.add(key_val);
				 if(temp==Tgt_data.size()) {
					 Tgt_duplicate.add(key_val);
				 }
	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
				
	    
	    reader1.close();
	   
		   
		 Total_Tgt_Count=Tgt_count;
		 Total_Tgt_Count_no_dup=Tgt_data.size();
		 Tgt_duplicate_count=Tgt_duplicate.size();
		 System.out.println("Tgttotal count : "+Total_Tgt_Count);
		 System.out.println("Tgttotal count no dup : "+Total_Tgt_Count_no_dup);
		 System.out.println("Tgt Duplicate Record count : "+Tgt_duplicate_count);
		 
		 Target_dup();
		
			 
	
	 
	 
 }
 
 
 public  void Header_Duplicate(String Filepath,String Sheet,String[] act) throws IOException{
		
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     
         Row row = sh.createRow(0);
        
         for(int i=0;i<act.length;i++){
        	// sh.autoSizeColumn(i);
        	 Cell cell = row.createCell(i);  
        	 if(Strings.isNullOrEmpty(act[i])){
	        	 cell.setCellValue(act[i]);
	        	 }
	        	 else {
	        		 cell.setCellValue(act[i].trim()); 
	        	 }
        	 cell.setCellStyle(lock);
         }
           

 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}


public  void Header_Missing_Src(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     
       Row row = sh.createRow(0);
       Cell cell1 = row.createCell(0); 
       cell1.setCellValue("Error Desc");
       
         int i=1;
        
         for(int aa=0;aa<Keyvalue_Column.length;aa++){
        	
        	 Cell cell = row.createCell(i); 
        	     
  
	        	 cell.setCellValue(Src_Column_Name[Keyvalue_Column[aa]]);
        	     
	        		
	        	
        	 cell.setCellStyle(lock);
        	 i++;
         }
           

 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Header_Missing_tgt(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     
       Row row = sh.createRow(0);
       Cell cell1 = row.createCell(0); 
       cell1.setCellValue("Error Desc");
         int i=1;
        
         for(int aa=0;aa<Keyvalue_Column.length;aa++){
        	
        	 Cell cell = row.createCell(i); 
        	     
  
	        	 cell.setCellValue(Tgt_Column_Name[Keyvalue_Column[aa]]);
        	     
	        		
	        	
        	 cell.setCellStyle(lock);
        	 i++;
         }
           

 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Header_No_pri(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     
       Row row = sh.createRow(0);
       Cell cell1 = row.createCell(0); 
       cell1.setCellValue("No non Primary key columns");
         
	        		
	        	
        	 cell1.setCellStyle(lock);
        

 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Header_data_mismatch(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     
       Row row = sh.createRow(0);
      
         int i=0;
        
         for(int aa=0;aa<Keyvalue_Column.length;aa++){
        	
        	     Cell cell = row.createCell(i); 
	        	 cell.setCellValue(Tgt_Column_Name[Keyvalue_Column[aa]]);
        	    	
        	 cell.setCellStyle(lock);
        	 i++;
         }
         
         for(int bb=0;bb<Tgt_Column_Name.length;bb++){
        	 
        	 if(!(Key_Column_identifier1(bb))) {
	        	
    	     Cell cell = row.createCell(i); 
        	 cell.setCellValue(Tgt_Column_Name[bb]+"_Target");	
    	     cell.setCellStyle(lock);
    	     i++;
    	     
    	     Cell cell1 = row.createCell(i); 
        	 cell1.setCellValue(Tgt_Column_Name[bb]+"_Source");	
    	     cell1.setCellStyle(lock);
    	     i++;
    	     
    	     Cell cell2 = row.createCell(i); 
        	 cell2.setCellValue(Tgt_Column_Name[bb]+"_Status");	
    	     cell2.setCellStyle(lock);
    	     i++;
    	 
        	 }
     }
           

 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

 public  boolean Key_Column_identifier1(int i) {
		
	 boolean test = false; 
    for (int element : Keyvalue_Column) { 
        if (element == i) { 
            test = true; 
            break; 
        } 
    }
    
    if(test) {
   	 return true;
   	
    }
    else
   	 return false;
	
}




public  void Missing_Record_Bulk_Writer(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(false);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
    
      for(int rownum = 1; rownum <= map.size(); rownum++){
         Row row = sh.createRow(rownum);
         
         String[] act=map.get(rownum-1).split(" \\| ");
        
         for(int i=0;i<act.length;i++){
        	// sh.autoSizeColumn(i);
        	 Cell cell = row.createCell(i);  
        	 if(Strings.isNullOrEmpty(act[i])){
	        	 cell.setCellValue(act[i]);
	        	 }
	        	 else {
	        		 cell.setCellValue(act[i].trim()); 
	        	 }
        	 cell.setCellStyle(lock);
         }
           

 }


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Data_validation_Bulk_Writer(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(false);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
    
      for(int rownum = 1; rownum <= map.size(); rownum++){
         Row row = sh.createRow(rownum);
         
         String[] act=map.get(rownum-1).split(" \\| ");
        
         for(int i=0;i<act.length;i++){
        	// sh.autoSizeColumn(i);
        	 Cell cell = row.createCell(i);  
        	 if(Strings.isNullOrEmpty(act[i])){
	        	 cell.setCellValue(act[i]);
	        	 }
	        	 else {
	        		 cell.setCellValue(act[i].trim()); 
	        	 }
        	 cell.setCellStyle(lock);
         }
           

 }


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Sample_Bulk_Writer(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(false);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
   
      
      for(int rownum = 1; rownum <= 50; rownum++){
    	  if(map.size()>=rownum) {
    	  
         Row row = sh.createRow(rownum);
         String[] act=map.get(rownum-1).split(" \\| ");
     
         for(int i=0;i<act.length;i++){
        	// sh.autoSizeColumn(i);
        	 Cell cell = row.createCell(i);  
        	 if(Strings.isNullOrEmpty(act[i])){
	        	 cell.setCellValue(act[i]);
	        	 }
	        	 else {
	        		 cell.setCellValue(act[i].trim()); 
	        	 }
        	 cell.setCellStyle(lock);
         }
           
    	  }
 }


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}


public  void Query_Bulk_Writer(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
    XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
    inputStream.close();

    SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
    wb.setCompressTempFiles(true);

    SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
    sh.setRandomAccessWindowSize(100);
    
    XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
    lock.setAlignment(HorizontalAlignment.LEFT);
    XSSFFont font= (XSSFFont) wb.createFont();
    font.setFontHeightInPoints((short)10);
    font.setFontName("Verdana");
    font.setColor(IndexedColors.BLACK.getIndex());
    font.setBold(false);
    font.setItalic(false);
    lock.setFont(font);
    lock.setBorderTop(BorderStyle.MEDIUM);
    lock.setBorderRight(BorderStyle.MEDIUM);
    lock.setBorderBottom(BorderStyle.MEDIUM);
    lock.setBorderLeft(BorderStyle.MEDIUM);
    
    int rownum=1;
  
     
    for (String key : Query_Log.keySet()) {
   	 
        Row row = sh.createRow(rownum);
        String val=Query_Log.get(key);
    
        
       
       	 Cell cell = row.createCell(1); 
       	 Cell cell1 = row.createCell(3);
       	 
	     cell.setCellValue(key);
	     if(val.length()<30000) {
	     cell1.setCellValue(val);
	     }
	     else {
	    	 cell1.setCellValue("Query too long.Kindly check Log File"); 
	     }
	        	 
       	 cell.setCellStyle(lock);
       	cell1.setCellStyle(lock);
       	
       	rownum++;
        }
          
   	 


FileOutputStream out = new FileOutputStream(Filepath);
wb.write(out);
out.close();
}



public  void Duplicate_Bulk_Writer(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(false);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
    
   
      for(int rownum = 1; rownum <= 100; rownum++){
    	  if(map.size()>=rownum) {
    	  
         Row row = sh.createRow(rownum);
         
         String[] act=map.get(rownum-1).split(" \\| ");
	     
         for(int i=0;i<act.length;i++){
         
        	// sh.autoSizeColumn(0);
        	 Cell cell = row.createCell(i);  
        	 if(Strings.isNullOrEmpty(act[i])){
	        	 cell.setCellValue(act[i]);
	        	 }
	        	 else {
	        		 cell.setCellValue(act[i].trim()); 
	        	 }
        	 cell.setCellStyle(lock);
         }
           
    	  }
 }


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}

public  void Count_Write(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
     lock.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock.setFont(font);
     lock.setBorderTop(BorderStyle.MEDIUM);
     lock.setBorderRight(BorderStyle.MEDIUM);
     lock.setBorderBottom(BorderStyle.MEDIUM);
     lock.setBorderLeft(BorderStyle.MEDIUM);
     
     int row_count=1;
     
             Row row1 = sh.createRow(row_count);
        	// sh.autoSizeColumn(row_count);
        	 Cell cell1 = row1.createCell(0); 
        	 Cell cell2 = row1.createCell(1); 
        	 cell1.setCellValue("Total Source count");
        	 cell2.setCellValue(String.valueOf(Total_Src_Count));
        	 cell1.setCellStyle(lock);
        	 cell2.setCellStyle(lock);
        	 
        	 
        	 row_count++;
        	 
        	 Row row2 = sh.createRow(row_count);
        	 //sh.autoSizeColumn(row_count);
        	 Cell cell3 = row2.createCell(0); 
        	 Cell cell4 = row2.createCell(1); 
        	 cell3.setCellValue("Total Target count");
        	 cell4.setCellValue(String.valueOf(Total_Tgt_Count));
        	 cell3.setCellStyle(lock);
        	 cell4.setCellStyle(lock);


        	 row_count= row_count+2;

        	 Row row3 = sh.createRow(row_count);
        	 //sh.autoSizeColumn(row_count);
        	 Cell cell5 = row3.createCell(0); 
        	 Cell cell6 = row3.createCell(1); 
        	 cell5.setCellValue("Total Source Duplicate");
        	 cell6.setCellValue(String.valueOf(Src_duplicate_count));
        	 cell5.setCellStyle(lock);
        	 cell6.setCellStyle(lock);
        	 
        	 row_count++;
        	 
        	 Row row4 = sh.createRow(row_count);
        	// sh.autoSizeColumn(row_count);
        	 Cell cell7 = row4.createCell(0); 
        	 Cell cell8 = row4.createCell(1); 
        	 cell7.setCellValue("Total Target Duplicate");
        	 cell8.setCellValue(String.valueOf(Tgt_duplicate_count));
        	 cell7.setCellStyle(lock);
        	 cell8.setCellStyle(lock);
        	 
        	 row_count= row_count+2;
        	 
        	 Row row5 = sh.createRow(row_count);
        	 //sh.autoSizeColumn(row_count);
        	 Cell cell9 = row5.createCell(0); 
        	 Cell cell10 = row5.createCell(1); 
        	 cell9.setCellValue("Source Missing count");
        	 cell10.setCellValue(String.valueOf(Src_missing_count));
        	 cell9.setCellStyle(lock);
        	 cell10.setCellStyle(lock);
        	 
        	 row_count++;
        	 
        	 Row row6 = sh.createRow(row_count);
        	 //sh.autoSizeColumn(row_count);
        	 Cell cell11 = row6.createCell(0); 
        	 Cell cell12 = row6.createCell(1); 
        	 cell11.setCellValue("Target Missing count");
        	 cell12.setCellValue(String.valueOf(Tgt_missing_count));
        	 cell11.setCellStyle(lock);
        	 cell12.setCellStyle(lock);
        	 
        	 row_count= row_count+2;
        	 
        	 Row row7 = sh.createRow(row_count);
        	 //sh.autoSizeColumn(row_count);
        	 Cell cell13 = row7.createCell(0);
        	 Cell cell14 = row7.createCell(1); 
        	 cell13.setCellValue("Total Data Mismatch Record");
        	 cell14.setCellValue(String.valueOf(Data_mismatch_count));
        	 cell13.setCellStyle(lock);
        	 cell14.setCellStyle(lock);
        	 
        	 
        	 row_count++;
        	 
        	 for(int i=1;i<=Column_Mismatch.size();i++) {
        			
        			if(Column_Mismatch.get(i)>0) {
        				
        				 Row row8 = sh.createRow(row_count);
        	        	 //sh.autoSizeColumn(row_count);
        	        	 Cell cell15 = row8.createCell(0);
        	        	 Cell cell16 = row8.createCell(1); 
        	        	 
        	        	 cell15.setCellValue(Table_column.get(i));
        	        	 cell16.setCellValue(String.valueOf(Column_Mismatch.get(i)));
        	        	 cell15.setCellStyle(lock);
        	        	 cell16.setCellStyle(lock);
        			
        	        	 row_count++;
        			}
        		}
        	 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
}


public  void Summary_Write(String Filepath,String Sheet) throws IOException{
	
	 FileInputStream inputStream = new FileInputStream(Filepath);
     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
     inputStream.close();

     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
     wb.setCompressTempFiles(true);

     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
     sh.setRandomAccessWindowSize(100);
     
     lock_Final=(XSSFCellStyle) wb.createCellStyle();
     lock_Final.setAlignment(HorizontalAlignment.LEFT);
     XSSFFont font= (XSSFFont) wb.createFont();
     font.setFontHeightInPoints((short)10);
     font.setFontName("Verdana");
     font.setColor(IndexedColors.BLACK.getIndex());
     font.setBold(true);
     font.setItalic(false);
     lock_Final.setFont(font);
     lock_Final.setBorderTop(BorderStyle.MEDIUM);
     lock_Final.setBorderRight(BorderStyle.MEDIUM);
     lock_Final.setBorderBottom(BorderStyle.MEDIUM);
     lock_Final.setBorderLeft(BorderStyle.MEDIUM);
     
     int row_count=0;
     
             row_final = sh.createRow(row_count);
        
             cell_value("Type",0);
             cell_value("Column",1);
             cell_value("Result",2);
        	 
        	 
        	 
        	 row_count++;
        	 row_final = sh.createRow(row_count);
        	 if(Count_Mismatch_Flag) {
        		 cell_value("Count_Validation",0);
	             cell_value("NA",1);
	             cell_value("Fail",2);
        		 
        	 }
        	
        	 else {
        		 
        		 cell_value("Count_Validation",0);
	             cell_value("NA",1);
	             cell_value("Pass",2);
        		 
        	 }


        	 row_count++;

        	 row_final = sh.createRow(row_count);
        	 if(Source_Missing_Flag) {
        		 cell_value("Source_Missing_Validation",0);
	             cell_value("NA",1);
	             cell_value("Fail",2);
        		 
        	 }
        	
        	 else {
        		 
        		 cell_value("Source_Missing_Validation",0);
	             cell_value("NA",1);
	             cell_value("Pass",2);
        		 
        	 }
        	 
        	 row_count++;
        	 
        	 row_final = sh.createRow(row_count);
        	 if(Target_Missing_Flag) {
        		 cell_value("Target_Missing_Validation",0);
	             cell_value("NA",1);
	             cell_value("Fail",2);
        		 
        	 }
        	
        	 else {
        		 
        		 cell_value("Target_Missing_Validation",0);
	             cell_value("NA",1);
	             cell_value("Pass",2);
        		 
        	 }
        	 
        	 row_count++;
        	 
        	 row_final= sh.createRow(row_count);
        	
        	 if(Source_Duplicate_Flag) {
        		 cell_value("Source_Duplicate_Validation",0);
	             cell_value("NA",1);
	             cell_value("Fail",2);
        		 
        	 }
        	
        	 else {
        		 
        		 cell_value("Source_Duplicate_Validation",0);
	             cell_value("NA",1);
	             cell_value("Pass",2);
        		 
        	 }
        	 
        	 row_count++;
        	 
        	 row_final= sh.createRow(row_count);
	        	
        	 if(Target_Duplicate_Flag) {
        		 cell_value("Target_Duplicate_Validation",0);
	             cell_value("NA",1);
	             cell_value("Fail",2);
        		 
        	 }
        	
        	 else {
        		 
        		 cell_value("Target_Duplicate_Validation",0);
	             cell_value("NA",1);
	             cell_value("Pass",2);
        		 
        	 }
        	 
        	
        	 
        	
        	 for(int i=1;i<=Column_Mismatch.size();i++){
        		 row_count++;
        		 row_final = sh.createRow(row_count);
        		 cell_value("Data_Validation",0);
	             cell_value(Table_column.get(i),1);
	             if(Column_Mismatch.get(i)>0){
	            	 cell_value("Fail",2);
	    		}
	    		else{
	    			 cell_value("Pass",2);
	    		}
	    		
	             
        		}
        	 
        	
        	 
	        	
        	 for(int i=1;i<=Table_column_key.size();i++){
        		 row_count++;
        		 row_final = sh.createRow(row_count);
        		 cell_value("Data_Validation",0);
	             cell_value(Table_column_key.get(i),1);
	             if(Target_Missing_Flag || Source_Missing_Flag ){
	            	 cell_value("Fail",2);
	    		}
	    		else{
	    			 cell_value("Pass",2);
	    		}
	    		  
	             
        		}
        	 


 FileOutputStream out = new FileOutputStream(Filepath);
 wb.write(out);
 out.close();
 
}



public  void cell_value(String val,int cell) {
   
   cell_final=row_final.createCell(cell); 
   cell_final.setCellValue(val);
   cell_final.setCellStyle(lock_Final);
	
   
}


public void Launcher() throws ClassNotFoundException, SQLException, IOException {
	
	Query_Log.put("Target Query", Tgt_Query);
	Query_Log.put("Source Query", Src_Query);
	
	if(Src_DB.equalsIgnoreCase("SQL Server")) {
    	
		SQL_Server(Src_Server_name,Src_DB_Name);
	}
	
	if(Src_DB.equalsIgnoreCase("DB2")) {
    	
		DB2_jDBC(Src_Username,Src_Password,Src_DB_Name,Src_Port,Src_Host);
	}
	
      if(Src_DB.equalsIgnoreCase("Netezza")) {
    	
		Netezza(Src_Username,Src_Password,Src_DB_Name,Src_Server_name);
		System.out.println("11");
    	}
      
      String[] Temp_Src_Query=Src_Query.split(";");
  	 
	     
	     int  src_len=Temp_Src_Query.length;
	   
	     
	     if(Temp_Src_Query.length==1) {
	    	 Src_Query=Temp_Src_Query[0];
	     }
	     
	     else {
	    	 Src_Query=Temp_Src_Query[src_len-1];
	    
	    	 for(int l=0;l<src_len-1;l++) {
	    		 stmt.execute(Temp_Src_Query[l]);
	    	 }
	     }
	     
	   String[] Temp_Tgt_Query=Tgt_Query.split(";");
 	 
     
     int  tgt_len=Temp_Tgt_Query.length;
    
     
     if(Temp_Tgt_Query.length==1) {
    	 Tgt_Query=Temp_Tgt_Query[0];
     }
     
     else {
    	 Tgt_Query=Temp_Tgt_Query[tgt_len-1];
    
    	 for(int l=0;l<tgt_len-1;l++) {
    		 stmt.execute(Temp_Tgt_Query[l]);
    	 }
     }
     
    
     
        //Src_Column_name();
       //Tgt_Column_name();
        Count_Validation_Qtype();
        
        if(Total_Src_Count <= 500000 && Total_Tgt_Count <=500000) {
       	 DB_Close();
       	 System.out.println("11111------------------");
       	Source_caller();
       	Target_caller();
       	Common_caller();
       }
       else {
        Source_query_Exe_Qtype();
        Target_query_Exe_Qtype();
       Missing_Record_Qtype();
       Data_Validation_Qtype();
       TGT_Duplicate_Qtype();
       SRC_Duplicate_Qtype();
        Count_Summary_Qtype();
        DB_Close();
       }
      
     
	
}


public void Count_Validation_Qtype() throws SQLException, IOException{
	
	try{
		
	
	System.out.println("Count Validation Begins");
	System.out.println("------------------------");
	
	
	String SRC_Query="Select count (*) from ("+ Src_Query+" ) src";
	String TGT_Query="Select count (*) from ("+ Tgt_Query+" ) src";
	
	String SC=null;
	String TC=null;
	
	res1 = stmt.executeQuery(SRC_Query);
	System.out.println("Count Validation Processing........");

		
		while(res1.next())
		{
			
			SC=res1.getString(1);
			Total_Src_Count=Long.parseLong(SC);
			
			
		}
	
		//reportStep_only("Source Count : "+Total_Src_Count,"INFO");
		res2 = stmt.executeQuery(TGT_Query);
		
		while(res2.next())
		{
			
			TC=res2.getString(1);
			Total_Tgt_Count=Long.parseLong(TC);
			
		}
		
	
		if(SC.equalsIgnoreCase(TC)){
		
			
			System.out.println("Count Validation Matches");
			
		}
		else{
			
			Count_Mismatch_Flag=true;
			
			
			
		}
		
		System.out.println(Total_Src_Count+"-------------------"+Total_Tgt_Count);
		
		System.out.println("Count Validation Ends");
		System.out.println("------------------------");
		
		
		
}catch(SQLException se){
	
	Error_exception(Result_File,Count_Report,se.getMessage());
	
   
   
 }catch(Exception e){
	 
	 Error_exception(Result_File,Count_Report,e.getMessage());
 }
	
}


public void Source_query_Exe_Qtype() throws SQLException, ClassNotFoundException, IOException {
	
	System.out.println("Source Query Begin");
	long startTime = System.currentTimeMillis();
	
	String Query =Src_Query+" minus "+Tgt_Query;

	System.out.println("Query Processing");
	 res= stmt.executeQuery(Query);
	
	
	 
	
	long count=0;
	 while(res.next())
		{
		 
		 count++;
		 
		 if(count<=100000) {
		 
	
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
         
         
		   
			 Src_data.add(key_val);
			 
		 
		 }
		 else {
			 break;
		 }
		 
		} 
	
	 System.out.println("Source Minus Query End");
	 
	
	 System.out.println("Source Minus completed");
	 long stopTime = System.currentTimeMillis();
	 long elapsedTime = stopTime - startTime;
	 System.out.println("Source time taken :"+elapsedTime/1000+ " Seconds");

}


public void Target_query_Exe_Qtype() throws SQLException, ClassNotFoundException, IOException {
	
	System.out.println("Source Query Begin");
	long startTime = System.currentTimeMillis();
	
	String Query =Tgt_Query+" minus "+Src_Query;
	
	 res= stmt.executeQuery(Query);
	
	long count=0;
	 while(res.next())
		{
		 count++;
		 
		 if(count<=100000) {
	
		 String key_val="";
		
         for(int k=0;k<Tgt_Column_Count;k++){
			 
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
         
         
		   
			 Tgt_data.add(key_val);
			 
		 
		 }
		 else {
			 break;
		 }
		 
		} 
	
	 System.out.println("Source Minus Query End");
	 
	
	 System.out.println("Source Minus completed");
	 long stopTime = System.currentTimeMillis();
	 long elapsedTime = stopTime - startTime;
	 System.out.println("Source time taken :"+elapsedTime/1000+ " Seconds");

}


public void Missing_Record_Qtype() throws IOException{
	 long startTime = System.currentTimeMillis();	
	
	System.out.println("Missing record begin");
	int row=0;
	ArrayList<String> record_Target = new ArrayList<String>();
	ArrayList<String> record_Source = new ArrayList<String>();
	
	ArrayList<String> Dele_Src = new ArrayList<String>();
	ArrayList<String> Dele_Tgt = new ArrayList<String>();
	
	Header_Missing_tgt(Result_File,Target_Missing);
	Header_Missing_Src(Result_File,Source_Missing);
    spliter1_Qtype();
	

  
   int count=0;
   int count1=0;
   		
   
   for (String key : Src_Map.keySet())  
   { 
        
   	if(!(Tgt_Map.containsKey(key))){
			  Source_Missing_Flag=true;
			  
			  count++;
			  Dele_Src.add(key);
			 
			  if(count<=200) {
			  record_Source.add("Present in Source But not in Target | "+key);
   	      }  
			 
		  }
   	
   } 
   
   for (String key : Tgt_Map.keySet())  
   { 
        
   	if(!(Src_Map.containsKey(key))){
			  Target_Missing_Flag=true;
			  count1++;
			  Dele_Tgt.add(key);
				 
			  if(count1<=200) {
			 
			  record_Target.add("Present in Target But not in Source | "+key);
			  }
			 
		  }
   	
   } 
   
   for (String key : Dele_Src)  {
   	
   	Src_Map.remove(key);
   	
   }
   

   for (String key : Dele_Tgt)  {
   	
   	Tgt_Map.remove(key);
   	
   }
   

		  System.out.println("record size of traget msiign : "+count1); 
		  Tgt_missing_count=count1;
		 
		
		  Src_missing_count=count;
				
		  System.out.println("record size of source msiign : "+Src_missing_count); 
	   
		Missing_Record_Bulk_Writer(Result_File,Source_Missing,record_Source);
		Missing_Record_Bulk_Writer(Result_File,Target_Missing,record_Target);
	      
		System.out.println("Missing record end");
		
		record_Target.clear();
		record_Source.clear();
		
		
		 long stopTime = System.currentTimeMillis();
		 long elapsedTime = stopTime - startTime;
		 System.out.println("Missing time Taken:"+elapsedTime/1000+ " Seconds");
	
	
	
}



public void Data_Validation_Qtype() throws IOException{
	  
	int count=0;
		
	boolean Sta;
	
	System.out.println(Tgt_Map.size()+"------"+Src_Map.size());
	
	if(Non_Primary) {

	
	ArrayList<String> data_write = new ArrayList<String>();
	
	
	
	Header_data_mismatch(Result_File,Data_Mismatch);
	
		
		        for (String key : Tgt_Map.keySet()) {
		        	 Sta=false;
		        	 
		        	
		 

				if(Tgt_Map.get(key).equals(Src_Map.get(key))) {
					
				}
				else {
					
					count++;
					
					Data_Mismatch_Flag=true;
					
					String[] Tgt_val=Tgt_Map.get(key).split("\\|");
					String[] Src_val=Src_Map.get(key).split("\\|");
					
					String val="";
					
					
					for(int i=0;i<Tgt_val.length;i++) {
						
						if(!(Tgt_val[i].trim().equalsIgnoreCase(Src_val[i].trim()))) {
							
		
							 if(val.length()==0) {
							
								val=val+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
							 }
							 else {
								 
								 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
								 
							 }
							
							
							Column_Mismatch.put(i+1, Column_Mismatch.get(i+1)+1);
							
							if(Column_Mismatch.get(i+1)<=10) {
								
								Sta=true;
							}
						}
						
						else {
							
							 if(val.length()==0) {
									
									val=val+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
								 }
								 else {
									 
									 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
									 
								 }
						}
						
						
						
					}
					
					if(Sta){
						
						data_write.add(key+" | "+val);
						
					}
					
					
					
				}
			}
		
		        Data_mismatch_count=count;
		        
		      
		   
		       Data_validation_Bulk_Writer(Result_File,Data_Mismatch,data_write);
	}
	else {
		
		Header_No_pri(Result_File,Data_Mismatch);
		
		
		
	}
	
	
		
		   	  
}


public void TGT_Duplicate_Qtype() throws SQLException, IOException{
	
	
	try{
	System.out.println("Target Duplicate Validation Begins");
	System.out.println("----------------------------------");
	
	ArrayList<String> record = new ArrayList<String>();
	String Finalquery=Tgt_Query;
	String[] Finalquery1=Finalquery.split("from");
	String[] FinalData = null;
	String FinalData1 = "";
	String ss1=Finalquery1[0].replace("select", "");
	
	FinalData=Tgt_Column_Name;

	for(int gg=0;gg<FinalData.length;gg++){
		
		
		if(gg==FinalData.length-1){
			FinalData1=FinalData1.concat(FinalData[gg]+"  ");
		}
		else{
		FinalData1=FinalData1.concat(FinalData[gg]+" ,");
		}
	}
	
	
	 String Query="select "+FinalData1+" ,count(*) from ( "+Finalquery+" ) ss group by "+FinalData1+" having count(*)>1";
	
	 System.out.println("Target Duplicate Validation Processing.........");	
	
	 res6 = stmt.executeQuery(Query);
	 ResultSetMetaData rsmd=res6.getMetaData();
	
	 long Count = 0;	
	while(res6.next())
	{
	  
                  Count++;
		
			 String key_val="";
	     for(int jj=0;jj<Tgt_Column_Name.length;jj++){
	    	String v=res6.getString(jj+1); 
			 if(key_val.length()==0) {
				
				 if (res6.wasNull()) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v.trim();
				  }
				 
			 }
			 else {
				 
				 if (res6.wasNull()) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v.trim();
				  }
				 
			 }
	    	
			
		}
	    
	    record.add(key_val);
	   
	}
	
	Tgt_duplicate_count=Count;
	
	//reportStep_only("Total Target Duplicate record Count : "+Tgt_duplicate_count,"INFO");
	
	 if(Tgt_duplicate_count>0) {
		 Target_Duplicate_Flag=true;
		}
	
	if(Tgt_duplicate_count==0) {
		//reportStep_only("TARGET Duplicate VALIDATION ","PASS");
		}
		else {
			//reportStep_only("TARGET Duplicate VALIDATION ","FAIL");
		}
	
	
	Header_Duplicate(Result_File,Target_Duplicate,Tgt_Column_Name);
	
	
	
	Duplicate_Bulk_Writer(Result_File,Target_Duplicate,record);
	
	
	
	System.out.println("Target Duplicate Validation Ends");
	System.out.println("----------------------------------");
	
	
	}catch(SQLException se){
		
		Error_exception(Result_File,Target_Duplicate,se.getMessage());
		

	   
	   
	 }
}


public void SRC_Duplicate_Qtype() throws SQLException, IOException{
	
	
	try{
	System.out.println("Source Duplicate Validation Begins");
	System.out.println("----------------------------------");
	
	ArrayList<String> record = new ArrayList<String>();
	String Finalquery=Src_Query;
	String[] Finalquery1=Finalquery.split("from");
	String[] FinalData = null;
	String FinalData1 = "";
	String ss1=Finalquery1[0].replace("select", "");
	
	FinalData=Src_Column_Name;

	for(int gg=0;gg<FinalData.length;gg++){
		
		
		if(gg==FinalData.length-1){
			FinalData1=FinalData1.concat(FinalData[gg]+"  ");
		}
		else{
		FinalData1=FinalData1.concat(FinalData[gg]+" ,");
		}
	}
	
	
	 String Query="select "+FinalData1+" ,count(*) from ( "+Finalquery+" ) bb group by "+FinalData1+" having count(*)>1";
	
	
	 System.out.println("Source Duplicate Validation Processing.........");	
	
	 res7 = stmt.executeQuery(Query);
	
	 long Count = 0;	
	while(res7.next())
	{
	  
                  Count++;
		
			 String key_val="";
	     for(int jj=0;jj<Src_Column_Name.length;jj++){
	    	String v=res7.getString(jj+1); 
			 if(key_val.length()==0) {
				
				 if (res7.wasNull()) {
					  key_val=key_val+"";
				  }
				  else {
					  key_val=key_val+v.trim();
				  }
				 
			 }
			 else {
				 
				 if (res7.wasNull()) {
					  key_val=key_val+" | "+"";
				  }
				  else {
					  key_val=key_val+" | "+v.trim();
				  }
				 
			 }
	    	
			
		}
	    
	    record.add(key_val);
	   
	}
	
	Src_duplicate_count=Count;
	
	//reportStep_only("Total Source Duplicate record Count : "+Src_duplicate_count,"INFO");
	 if(Src_duplicate_count>0) {
		 Source_Duplicate_Flag=true;
		}
	
	if(Src_duplicate_count==0) {
		//reportStep_only("SOURCE Duplicate VALIDATION","PASS");
		}
		else {
			//reportStep_only("SOURCE Duplicate VALIDATION","FAIL");
		}
	
	
	Header_Duplicate(Result_File,Source_Duplicate,Src_Column_Name);
	
	Duplicate_Bulk_Writer(Result_File,Source_Duplicate,record);
	
	
	
	System.out.println("Source Duplicate Validation Ends");
	System.out.println("----------------------------------");
	
	
	}catch(SQLException se){
		
		Error_exception(Result_File,Source_Duplicate,se.getMessage());
		
	   
	   
	 }
} 

public void Count_Summary_Qtype() throws SQLException, IOException{
	
	Count_Write(Result_File,Count_Report);
	Summary_Write(Result_File,Summary);
	 Query_Bulk_Writer(Result_File,Query_List);
}

public void Count_Summary_Hive() throws SQLException, IOException{
	
	Count_Write(Result_File,Count_Report);
	Summary_Write(Result_File,Summary);
	 Query_Bulk_Writer(Result_File,Query_List);
}












public void spliter1_Qtype() {
	
	  Iterator value = Src_data.iterator(); 
	  
    while (value.hasNext()) { 
  	  
  	  String[] act=value.next().toString().split("\\|");
		  
			String key_val="";
			String val="";
			int key_start=0;
			int nonkey_start=0;
			
			 for(int j=0;j<act.length;j++) {
					
				  if(Key_Column_identifier(j)) {
					  
					  key_start++;
					  
					  String v=act[j]; 
						 if(key_start==1) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+"";
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+"";
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						 
					 }
						 else {
							 nonkey_start++;
							 String v=act[j]; 
							 if(nonkey_start==1) {
								
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+"";
								  }
								  else {
									  val=val+v.trim();
								  }
								 
							 }
							 else {
								 
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+" | "+"";
								  }
								  else {
									  val=val+" | "+v.trim();
								  }
								 
							 }
							 
					  
				  }
				  
			 }
			 
  	  
			 Src_Map.put(key_val,val);
  	  
    
    }
    
    
 Iterator value1 = Tgt_data.iterator(); 
	  
    while (value1.hasNext()) { 
  	  
  	  String[] act=value1.next().toString().split("\\|");
		  
			String key_val="";
			String val="";
			int key_start=0;
			int nonkey_start=0;
			
			 for(int j=0;j<act.length;j++) {
					
				  if(Key_Column_identifier(j)) {
					  key_start++;
					  
					  String v=act[j]; 
						 if(key_start==1) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+"";
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+"";
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						 
					 }
						 else {
							 nonkey_start++;
							 
							 String v=act[j]; 
							 if(nonkey_start==1) {
								
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+"";
								  }
								  else {
									  val=val+v.trim();
								  }
								 
							 }
							 else {
								 
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+" | "+"";
								  }
								  else {
									  val=val+" | "+v.trim();
								  }
								 
							 }
							 
					  
				  }
				  
			 }
			 
  	  
			 Tgt_Map.put(key_val,val);
  	  
    
    }
	
	
	
	
}
/*



public void Src_Column_name(){
	
	int count=0;
	
	String data=Src_Query.replaceAll("\\s+", " ").trim();
	String[] data1=data.split("\\bfrom\\b");
	
	String dd2=data1[0].replace("select", "");
	dd2=dd2.trim();
	String[] dd3=dd2.split(",");
	Src_Column_Name=new String[dd3.length];
	
	System.out.println(dd3.length);
	
	for(int i=0;i<dd3.length;i++){
		String bb=dd3[i].trim();
		
			
		Src_Column_Name[i]=bb;
			System.out.println("Final Data :"+bb);
		
	}
}
	
	public void Tgt_Column_name(){
		
		int count=0;
		
		String data=Tgt_Query.replaceAll("\\s+", " ").trim();
		String[] data1=data.split("\\bfrom\\b");
		
		String dd2=data1[0].replace("select", "");
		dd2=dd2.trim();
		String[] dd3=dd2.split(",");
		Tgt_Column_Name=new String[dd3.length];
		
		System.out.println(dd3.length);
		int cc=1;
		int jj=1;
		
		for(int i=0;i<dd3.length;i++){
			String bb=dd3[i].trim();
			
				
			Tgt_Column_Name[i]=bb;
			 
		}
		
	}
	*/
	
	


    
    public  void Error_exception(String Filepath,String Sheet,String Error) throws IOException{
		
		 FileInputStream inputStream = new FileInputStream(Filepath);
	     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
	     inputStream.close();

	     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
	     wb.setCompressTempFiles(true);

	     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
	     sh.setRandomAccessWindowSize(100);
	     
	     lock_Final=(XSSFCellStyle) wb.createCellStyle();
	     lock_Final.setAlignment(HorizontalAlignment.LEFT);
	     XSSFFont font= (XSSFFont) wb.createFont();
	     font.setFontHeightInPoints((short)10);
	     font.setFontName("Verdana");
	     font.setColor(IndexedColors.BLACK.getIndex());
	     font.setBold(true);
	     font.setItalic(false);
	     lock_Final.setFont(font);
	     lock_Final.setBorderTop(BorderStyle.MEDIUM);
	     lock_Final.setBorderRight(BorderStyle.MEDIUM);
	     lock_Final.setBorderBottom(BorderStyle.MEDIUM);
	     lock_Final.setBorderLeft(BorderStyle.MEDIUM);
	     
	     int row_count=1;
	     
	             row_final = sh.createRow(row_count);
	        
	             cell_value("Error Occured will processing",0);
	             cell_value(Error,1);
	             
	        	 


	 FileOutputStream out = new FileOutputStream(Filepath);
	 wb.write(out);
	 out.close();
	}
    
    
public void Launcher_Hive() throws ClassNotFoundException, SQLException, IOException {
		
	Query_Log.put("Target Query", Tgt_Query);
	Query_Log.put("Source Query", Src_Query);	
	
	if(Src_DB.equalsIgnoreCase("Hive")) {
	    	
			Hive_HDBC(Src_Username,Src_Password,Src_Host);
	    	}
	      String[] Temp_Src_Query=Src_Query.split(";");
	  	 
		     
		     int  src_len=Temp_Src_Query.length;
		   
		     System.out.println("----");
		     stmt.execute("set hive.execution.engine=tez");
		     System.out.println("----");
		     if(Temp_Src_Query.length==1) {
		    	 Src_Query=Temp_Src_Query[0];
		     }
		     
		     else {
		    	 Src_Query=Temp_Src_Query[src_len-1];
		    	 System.out.println(Temp_Src_Query[0]);
		    	 for(int l=0;l<src_len-1;l++) {
		    		 stmt.execute(Temp_Src_Query[l]);
		    	 }
		     }
		     
		   String[] Temp_Tgt_Query=Tgt_Query.split(";");
	 	 
	     
	     int  tgt_len=Temp_Tgt_Query.length;
	    
	     
	     if(Temp_Tgt_Query.length==1) {
	    	 Tgt_Query=Temp_Tgt_Query[0];
	     }
	     
	     else {
	    	 Tgt_Query=Temp_Tgt_Query[tgt_len-1];
	    
	    	 for(int l=0;l<tgt_len-1;l++) {
	    		 stmt.execute(Temp_Tgt_Query[l]);
	    	 }
	     }
	     
	     
	     
	        //Src_Column_name();
	        //Tgt_Column_name();
	        
	      
	        
	        Count_Validation_Hive();
	   
	        
	        if(Total_Src_Count <= 500000 && Total_Tgt_Count <=500000) {
	        	 DB_Close();
	        
	        	Source_caller();
	        	Target_caller();
	        	Common_caller();
	        }
	        else {
	        Data_Validation_Hive();	
			SRC_Missing_Hive();
			TGT_Missing_Hive();
			TGT_Duplicate_Hive();
			SRC_Duplicate_Hive();
			 DB_Close();
			 Count_Summary_Hive();
	        }
	     
		
	}


	
	
	public void Count_Validation_Hive() throws SQLException, IOException{
		
		try{
			
		
		System.out.println("Count Validation Begins");
		System.out.println("------------------------");
	
		
		String SRC_Query="Select count (*) from ("+ Src_Query+" ) src";
		String TGT_Query="Select count (*) from ("+ Tgt_Query+" ) src";
		
		String SC=null;
		String TC=null;
		
		
		
		res1 = stmt.executeQuery(SRC_Query);
		System.out.println("Count Validation Processing........");

			
			while(res1.next())
			{
				
				SC=res1.getString(1);
				Total_Src_Count=Long.parseLong(SC);
				
				
			}
		
			
			System.out.println(Total_Src_Count);
			
			res2 = stmt.executeQuery(TGT_Query);
			
			while(res2.next())
			{
				
				TC=res2.getString(1);
				Total_Tgt_Count=Long.parseLong(TC);
				
			}
			
			System.out.println(Total_Tgt_Count);
			
			if(SC.equalsIgnoreCase(TC)){
			
				
				
			}
			else{
				
				Count_Mismatch_Flag=true;
				
				
				
			}
			
			System.out.println("Count Validation Ends");
			System.out.println("------------------------");
			
			
			
	}catch(SQLException se){
		
		
		
		Error_exception(Result_File,Count_Report,se.getMessage());
	   
	   
	 }catch(Exception e){
		 
		 Error_exception(Result_File,Count_Report,e.getMessage());
	 }
		
	}


	public void Data_Validation_Hive() throws SQLException, IOException{
		
		
		try{
		System.out.println("Data Validation Begins");
		System.out.println("------------------------");
	
		ArrayList<String> data_write = new ArrayList<String>();
	    String FinalSP="";

		for (int i=0;i<Src_Column_Name.length;i++){
			
		    if(Key_Column_identifier(i)) {
			if(FinalSP.length()==0){
				FinalSP =FinalSP.concat("SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim()+" ");
			}
			else{
				FinalSP =FinalSP.concat(" and SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim());
			}
			
		}
		}
		
		 String FinalSU="";
			for (int i=0;i<Src_Column_Name.length;i++){
				
				
				 if(!(Key_Column_identifier(i))) {
					 
					
				if(FinalSU.length()==0){
					FinalSU =FinalSU.concat("SRC."+Src_Column_Name[i].trim()+" <> TGT."+Tgt_Column_Name[i].trim()+" ");
				}
				else{
					FinalSU =FinalSU.concat(" or SRC."+Src_Column_Name[i].trim()+" <> TGT."+Tgt_Column_Name[i].trim());
				}
				
			}
			}
			
			String Query;
			
			if(FinalSU.length()==0){
				Query="select * from ("+Src_Query+")SRC left outer join ("+Tgt_Query+")TGT on "+FinalSP;
				
			}
			else{
		       Query="select * from ("+Src_Query+")SRC left outer join ("+Tgt_Query+")TGT on "+FinalSP+" where "+FinalSU;
			}
	
			
			Query_Log.put("Data Validation Query", Query);
			
		 System.out.println("Data Validation Processing.......");
		

		 
		 res3 = stmt.executeQuery(Query);
		 ResultSetMetaData rsmd=res3.getMetaData();

		 int Columncount = rsmd.getColumnCount()/2;
		 
		int count=0;
		while(res3.next())
		{
			
			String Value="";
			
			String KK_Value="";
			
			boolean Flag=false;
			boolean Flag1=false;
			int ll=0;
		    for(int jj=1;jj<=Columncount;jj++){
		    	
		    	String a=res3.getString(jj); 
				String b=res3.getString(jj+Columncount); 
		    	
		    	if((Key_Column_identifier(jj-1))){
		    		
		    		if(KK_Value.length()==0) {
						
		    			if(Strings.isNullOrEmpty(a)) {
		    				KK_Value=KK_Value+a;
						  }
						  else {
							  KK_Value=KK_Value+a.trim();
						  }
						 
					 }
					 else {
						 
						 if (Strings.isNullOrEmpty(b)) {
							 KK_Value=KK_Value+" | "+a;
						  }
						  else {
							  KK_Value=KK_Value+" | "+b.trim();
						  }
						 
					 }
			    	
		    	}
		    	
		    	 if(!(Key_Column_identifier(jj-1))){
				 ll++;
				 String v="";
				 String r="";
				 
				 if(Strings.isNullOrEmpty(a)) {
					 v="";
				 }
				 else {
					 v=a.trim();
				 }
				 
				 if(Strings.isNullOrEmpty(b)) {
					 r="";
				 }
				 else {
					 r=b.trim();
				 }
				 
				 
				 
				 if(!(v.equals(r))) {
					 
					 Flag1=true;
					 
	                 Column_Mismatch.put(ll, (Column_Mismatch.get(ll))+1);
					 
					 if(Column_Mismatch.get(ll)<=5) {
						 Flag=true; 
					 }
					 
				 if(Value.length()==0) {
					
					
						 Value=Value+r+" | "+v+" | MisMatch";
					
					 
				 }
				 else {
					 
					
						 Value=Value+" | "+r+" | "+v+" | MisMatch";
					  
					 
				 }
				 }
				 
				 else {
					 
					 
					 if(Value.length()==0) {
						
						
							 Value=Value+v+" | "+r+" | Match";
						 
						 
					 }
					 else {
						 
						
							 Value=Value+" | "+r+" | "+v+" | Match";
						 
						 
					 }
					 
					 
				 }
				 }
					 	 
			 }
		    
		    if(Flag) {
		    
		    	data_write.add(KK_Value+" | "+Value);
	    
		   }
		    
		    if(Flag1) {
			    
		    	count++;
	    
		   }
			}
		   

		Data_mismatch_count=count;
	
		
		if(Data_mismatch_count>0) {
			Data_Mismatch_Flag=true;
		}
		Header_data_mismatch(Result_File,Data_Mismatch);
		
		   Data_validation_Bulk_Writer(Result_File,Data_Mismatch,data_write);
		
		System.out.println("Data Validation Ends");
		System.out.println("------------------------");
	
	  	
	 
		
		}catch(SQLException se){
			
			Error_exception(Result_File,Data_Mismatch,se.getMessage());
		   
		   
		 }
		
	}






		
		public void SRC_Missing_Hive() throws SQLException, IOException{
			
			try{
			
			System.out.println("SOURCE Missing Validation Begins");
			System.out.println("------------------------");
			
			String FinalSP="";
			String FinalSU="";
			ArrayList<String> record_Source = new ArrayList<String>();


			for (int i=0;i<Src_Column_Name.length;i++){
				
				 if(Key_Column_identifier(i)) {
					 
					 if(FinalSP.length()==0){
							FinalSP =FinalSP.concat("SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim()+" ");
						}
						else{
							FinalSP =FinalSP.concat(" and SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim());
						}
			
				
			}
			}
			
		
				for (int i=0;i<Src_Column_Name.length;i++){
					
					 if(Key_Column_identifier(i)) {
					if(FinalSU.length()==0){
						FinalSU =FinalSU.concat(" SRC."+Src_Column_Name[i].trim()+" is null ");
					}
					else{
						FinalSU =FinalSU.concat(" and  SRC."+Src_Column_Name[i].trim()+" is null");
					}
					
				}
				}
				
				
			
				String Query;	
			
			 Query="select * from ("+Src_Query+")SRC right join ("+Tgt_Query+")TGT on "+FinalSP+" where "+FinalSU;
			
			 Query_Log.put("Source Missing Query", Query);
			 System.out.println("SOURCE Missing Validation Processing.................");
			
			 res4 = stmt.executeQuery(Query);
			 ResultSetMetaData rsmd=res4.getMetaData();
		
			 int Columncount = rsmd.getColumnCount()/2;
			 int Count = 0;
		
			while(res4.next())
			{
				Count++;
				
				if(Count<=20) {
					
					 String key_val="";
			    for(int jj=1;jj<=Columncount;jj++){
			    	
			    	 if(Key_Column_identifier(jj-1)) {
			    	String v=res4.getString(jj+Columncount); 
					 if(key_val.length()==0) {
						
						 if (res4.wasNull()) {
							  key_val=key_val+v;
						  }
						  else {
							  key_val=key_val+v.trim();
						  }
						 
					 }
					 else {
						 
						 if (res4.wasNull()) {
							  key_val=key_val+" | "+v;
						  }
						  else {
							  key_val=key_val+" | "+v.trim();
						  }
						 
					 }
			    	
			    	 }
				}
			    
			   // System.out.println(key_val);
			    record_Source.add("Present in Target But not in Source | "+key_val);
			}
			   
			}
			
		
			
			Src_missing_count=Count;
			 
			
			
			 if(Src_missing_count>0) {
				 Source_Missing_Flag=true;
				}
			
			
			 Header_Missing_Src(Result_File,Source_Missing);
			
			Missing_Record_Bulk_Writer(Result_File,Source_Missing,record_Source);
			
			System.out.println("SOURCE Missing Validation Ends");
			System.out.println("-------------------------------"); 
			
			
			
			}catch(SQLException se){
				
				Error_exception(Result_File,Source_Missing,se.getMessage());
			   
			   
			 }
		}
		
		
		public void TGT_Missing_Hive() throws SQLException, IOException{
			
			try{
			
			System.out.println("Target Missing Validation Begins");
			System.out.println("----------------------------------");
			

			String FinalSP="";
			String FinalSU="";
			ArrayList<String> record_Target = new ArrayList<String>();


			for (int i=0;i<Tgt_Column_Name.length;i++){
				
				 if(Key_Column_identifier(i)) {
					 
					 if(FinalSP.length()==0){
					FinalSP =FinalSP.concat("SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim()+" ");
				}
				else{
					FinalSP =FinalSP.concat(" and SRC."+Src_Column_Name[i].trim()+" = TGT."+Tgt_Column_Name[i].trim());
				}
				
			}
			}
			
		
			for (int i=0;i<Tgt_Column_Name.length;i++){
				
				 if(Key_Column_identifier(i)) {
				if(FinalSU.length()==0){
						FinalSU =FinalSU.concat(" TGT."+Tgt_Column_Name[i].trim()+" is null ");
					}
					else{
						FinalSU =FinalSU.concat(" and  TGT."+Tgt_Column_Name[i].trim()+" is null");
					}
					
				}
				}
				
		
			 String Query="select * from ("+Src_Query+")SRC left join ("+Tgt_Query+")TGT on "+FinalSP+" where "+FinalSU;
			
			 Query_Log.put("Target Missing Query", Query);
			 System.out.println("TARGET Missing Validation Processing.................");
			
			 res5 = stmt.executeQuery(Query);
			 ResultSetMetaData rsmd=res5.getMetaData();
			 int Columncount = rsmd.getColumnCount()/2;
			 int Count = 0;
			
			
			while(res5.next())
			{
				Count++;
				
				if(Count<=20) {
					
					 String key_val="";
			     for(int jj=1;jj<=Columncount;jj++){
			    	 
			    	 if(Key_Column_identifier(jj-1)) {
			    	String v=res5.getString(jj); 
					 if(key_val.length()==0) {
						
						 if (res5.wasNull()) {
							  key_val=key_val+v;
						  }
						  else {
							  key_val=key_val+v.trim();
						  }
						 
					 }
					 else {
						 
						 if (res5.wasNull()) {
							  key_val=key_val+" | "+v;
						  }
						  else {
							  key_val=key_val+" | "+v.trim();
						  }
						 
					 }
			    	 }
					
				}
			    
			    record_Target.add("Present in Source But not in Target | "+key_val);
			}
			   
			}
			
			
			
			Tgt_missing_count=Count;
		
			 if(Tgt_missing_count>0) {
				 Target_Missing_Flag=true;
				}
				
			
			 Header_Missing_tgt(Result_File,Target_Missing);
			
			Missing_Record_Bulk_Writer(Result_File,Target_Missing,record_Target);
			System.out.println("Target Missing Validation Ends");
			System.out.println("----------------------------------"); 
		
			
			
			}catch(SQLException se){
				
			
				Error_exception(Result_File,Target_Missing,se.getMessage());
			   
			 }
			
		}

		public void TGT_Duplicate_Hive() throws SQLException, IOException{
			
			
			try{
			System.out.println("Target Duplicate Validation Begins");
			System.out.println("----------------------------------");
		
			ArrayList<String> record = new ArrayList<String>();
			String Finalquery=Tgt_Query;
			String[] Finalquery1=Finalquery.split("from");
			String[] FinalData = null;
			String FinalData1 = "";
			String ss1=Finalquery1[0].replace("select", "");
			
			FinalData=Tgt_Column_Name;
		
			for(int gg=0;gg<FinalData.length;gg++){
				
				
				if(gg==FinalData.length-1){
					FinalData1=FinalData1.concat(FinalData[gg]+"  ");
				}
				else{
				FinalData1=FinalData1.concat(FinalData[gg]+" ,");
				}
			}
			
			
			 String Query="select "+FinalData1+" ,count(*) from ( "+Finalquery+" ) ss group by "+FinalData1+" having count(*)>1";
			 Query_Log.put("Target Duplicate Query", Query);
			
			 System.out.println("Target Duplicate Validation Processing.........");	
			
			 res6 = stmt.executeQuery(Query);
			 ResultSetMetaData rsmd=res6.getMetaData();
			
			 long Count = 0;	
			while(res6.next())
			{
			  
	                      Count++;
				
					 String key_val="";
			     for(int jj=1;jj<=Tgt_Column_Name.length;jj++){
			    	String v=res6.getString(jj); 
					 if(key_val.length()==0) {
						
						 if (res6.wasNull()) {
							  key_val=key_val+v;
						  }
						  else {
							  key_val=key_val+v.trim();
						  }
						 
					 }
					 else {
						 
						 if (res6.wasNull()) {
							  key_val=key_val+" | "+v;
						  }
						  else {
							  key_val=key_val+" | "+v.trim();
						  }
						 
					 }
			    	
					
				}
			    
			    record.add(key_val);
			   
			}
			
			Tgt_duplicate_count=Count;
			
			
			
			 if(Tgt_duplicate_count>0) {
				 Target_Duplicate_Flag=true;
				}
			
			
			
			
			 Header_Duplicate(Result_File,Target_Duplicate,Tgt_Column_Name);
			
			Duplicate_Bulk_Writer(Result_File,Target_Duplicate,record);
			
			
			
			System.out.println("Target Duplicate Validation Ends");
			System.out.println("----------------------------------");
			
			
			}catch(SQLException se){
				
				
				Error_exception(Result_File,Target_Duplicate,se.getMessage());
			   
			 }
		}
		
	public void SRC_Duplicate_Hive() throws SQLException, IOException{
			
			
			try{
			System.out.println("Source Duplicate Validation Begins");
			System.out.println("----------------------------------");
		
			ArrayList<String> record = new ArrayList<String>();
			String Finalquery=Src_Query;
			String[] Finalquery1=Finalquery.split("from");
			String[] FinalData = null;
			String FinalData1 = "";
			String ss1=Finalquery1[0].replace("select", "");
			
			FinalData=Src_Column_Name;
		
			for(int gg=0;gg<FinalData.length;gg++){
				
				
				if(gg==FinalData.length-1){
					FinalData1=FinalData1.concat(FinalData[gg]+"  ");
				}
				else{
				FinalData1=FinalData1.concat(FinalData[gg]+" ,");
				}
			}
			
			
			 String Query="select "+FinalData1+" ,count(*) from ( "+Finalquery+" ) bb group by "+FinalData1+" having count(*)>1";
			
			 Query_Log.put("Source Duplicate Query", Query);
			 System.out.println("Source Duplicate Validation Processing.........");	
			
			 res7 = stmt.executeQuery(Query);
			
			 long Count = 0;	
			while(res7.next())
			{
			  
	                      Count++;
				
					 String key_val="";
			     for(int jj=1;jj<=Src_Column_Name.length;jj++){
			    	String v=res7.getString(jj); 
					 if(key_val.length()==0) {
						
						 if (res7.wasNull()) {
							  key_val=key_val+v;
						  }
						  else {
							  key_val=key_val+v.trim();
						  }
						 
					 }
					 else {
						 
						 if (res7.wasNull()) {
							  key_val=key_val+" | "+v;
						  }
						  else {
							  key_val=key_val+" | "+v.trim();
						  }
						 
					 }
			    	
					
				}
			    
			    record.add(key_val);
			   
			}
			
			Src_duplicate_count=Count;
			
			
			 if(Src_duplicate_count>0) {
				 Source_Duplicate_Flag=true;
				}
			
			
			
		    Header_Duplicate(Result_File,Source_Duplicate,Src_Column_Name);
			
			Duplicate_Bulk_Writer(Result_File,Source_Duplicate,record);
			
			
			
			System.out.println("Source Duplicate Validation Ends");
			System.out.println("----------------------------------");
			
			
			}catch(SQLException se){
				
				Error_exception(Result_File,Source_Duplicate,se.getMessage());
			   
			   
			 }
	} 
	
	public void DDL(String Result_File1,String Partition) throws IOException, SQLException {
		
		 Result_File=Result_File1;
		 Partition_Value=Partition;
		 Create_workbook_Sheets1();
		 
		 if(Src_DB.equalsIgnoreCase("DB2")) {
			 Source_Column_Validation_DB2(); 
		 }else {
		 Source_Column_Validation();
		 }
		 if(Tgt_DB.equalsIgnoreCase("DB2")) {
			 Target_Column_Validation_DB2();
			 
		 }else {
		 Target_Column_Validation();
		 }
		 Mismatch_Validation();
		 Summary_Write_DDL(Result_File,Summary);
	}
	
	
	public void Source_Column_Validation() throws SQLException, IOException{
		
		if(Src_DB.equalsIgnoreCase("Excel")) {
			
			String[][] data=readXLSX(Input_File_Path_Source,"Sheet1");
			
			for(int i=1;i<data[0].length;i++) {
				
				if(data[1][i].equalsIgnoreCase("Array")) {
					
					String key="";
					
					String[][] d1=readXLSX(Input_File_Path_Source,data[0][i]);
					
					for(int j=1;j<d1[0].length;j++) {
						
						if(j==1) {
							
							key="array<struct<"+d1[0][j]+":"+d1[1][j];
						}
						
						else {
							
							key=key+","+d1[0][j]+":"+d1[1][j];
						}
						
						
					}
					
					
					key=key+">>";
					
					Src_Column_index.put(data[0][i].toLowerCase().trim(), key.toLowerCase().trim());
					
				}
				else {
				
				Src_Column_index.put(data[0][i].toLowerCase().trim(), data[1][i].toLowerCase().trim());
				}
				
			}
			
		}
		
		else {
		
		try{
			
			
			
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
		          
			
		
		System.out.println(" Validation Begins");
		System.out.println("------------------------");

		res1 = stmt.executeQuery("desc "+Src_Query);
		System.out.println(" Validation Processing........");

			String val=null;
			while(res1.next())
			{
				if(Partition_Value.equalsIgnoreCase("No")) {
					if(!(Strings.isNullOrEmpty(res1.getString(1)))) {
						String aa=res1.getString(1);
						if(aa.contains("etl_cycle_dt") || aa.contains("# Partition Information") ||aa.contains("# col_name") || aa.contains("batch_id")) {
							
							break;
						}
					}
					
				}
				
				if(!(Strings.isNullOrEmpty(res1.getString(1)))) {
					
					if((Strings.isNullOrEmpty(res1.getString(2)))) {
						val="";
					}
					else {
						val=res1.getString(2);
					}
				Src_Column_index.put(res1.getString(1).toLowerCase().trim(), val.toLowerCase().trim());
				}
				
			}
		
			 DB_Close();
			
			
	}catch(SQLException se){
		
		Error_exception(Result_File,Source_Columns,se.getMessage());
	   
	   
	 }catch(Exception e){
		 Error_exception(Result_File,Source_Columns,e.getMessage());
		
	 }
		}
		
		Total_Src_Count=Src_Column_index.size();
		
		System.out.println(" Total_Src_Count........"+Total_Src_Count);
		Column_Writer_Header(Result_File,Source_Columns,Src_Column_index);
		Column_Writer(Result_File,Source_Columns,Src_Column_index);
		
		
	}
	
public void Source_Column_Validation_DB2() throws SQLException, IOException{
		
		
		
		try{
			
			
			
		    	if(Src_DB.equalsIgnoreCase("SQL Server")) {
		    	
		    		SQL_Server(Src_Server_name,Src_DB_Name);
		    	}
		    	
		    	if(Src_DB.equalsIgnoreCase("DB2")) {
		        	
		    		DB2_jDBC(Src_Username,Src_Password,Src_DB_Name,Src_Port,Src_Host);
		    	}
		    	
		          if(Src_DB.equalsIgnoreCase("Netezza")) {
		        	
		    		Netezza(Src_Username,Src_Password,Src_DB_Name,Src_Server_name);
		        	}
		          
			
		
		System.out.println(" Validation Begins");
		System.out.println("------------------------");
		System.out.println(Src_Query);
		
		String[] que=Src_Query.split("\\.");
		System.out.println("select name,coltype,length from sysibm.SYSCOLUMNS where tbname='"+que[1]+"' and tbcreator='"+que[0]+"'");

		res1 = stmt.executeQuery("select name,coltype,length from sysibm.SYSCOLUMNS where tbname='"+que[1]+"' and tbcreator='"+que[0]+"'");
		System.out.println(" Validation Processing........");
		
		//System.out.println("select name,coltype,length from sysibm.SYSCOLUMNS where tbname='"+que[1]+"' and tbcreator='"+que[0]+"'");

			
			while(res1.next())
			{
				
				String Col_name=res1.getString(1).toLowerCase().trim();
				String Col_type=res1.getString(2).toLowerCase().trim();
				String Lenght=res1.getString(3).toLowerCase().trim();
				String fin=Col_type+"_"+Lenght;
				
				Src_Column_index.put(Col_name, fin);
				
				
			}
		
			 DB_Close();
			
			
	}catch(SQLException se){
		
		Error_exception(Result_File,Source_Columns,se.getMessage());
	   
	   
	 }catch(Exception e){
		 Error_exception(Result_File,Source_Columns,e.getMessage());
		
	 }
		
		
		Total_Src_Count=Src_Column_index.size();
		
		System.out.println(" Total_Src_Count........"+Total_Src_Count);
		Column_Writer_Header(Result_File,Source_Columns,Src_Column_index);
		Column_Writer(Result_File,Source_Columns,Src_Column_index);
		
		
	}

public void Target_Column_Validation_DB2() throws SQLException, IOException{
	
	try{
		
		
	    	if(Tgt_DB.equalsIgnoreCase("SQL Server")) {
	    	
	    		SQL_Server(Tgt_Server_name,Tgt_DB_Name);
	    	}
	    	
	    	if(Tgt_DB.equalsIgnoreCase("DB2")) {
	        	
	    		DB2_jDBC(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Port,Tgt_Host);
	    	}
	         if(Tgt_DB.equalsIgnoreCase("Netezza")) {
	        	
	    		Netezza(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Server_name);
	        	}
	         
		
	
	System.out.println(" Validation Begins");
	System.out.println("------------------------");
	String[] que=Tgt_Query.split("\\.");
	System.out.println("select name,coltype,length from sysibm.SYSCOLUMNS where tbname='"+que[1]+"' and tbcreator='"+que[0]+"'");

	res2 = stmt.executeQuery("select name,coltype,length from sysibm.SYSCOLUMNS where tbname='"+que[1]+"' and tbcreator='"+que[0]+"'");
	System.out.println(" Validation Processing........");
		String val=null;
		while(res2.next())
		{
			
			String Col_name=res2.getString(1).toLowerCase().trim();
			String Col_type=res2.getString(2).toLowerCase().trim();
			String Lenght=res2.getString(3).toLowerCase().trim();
			String fin=Col_type+"_"+Lenght;
			Tgt_Column_index.put(Col_name, fin);
			
			
		}
		
		 DB_Close();
	
		Total_Tgt_Count=Tgt_Column_index.size();
		Column_Writer_Header(Result_File,Target_Columns,Tgt_Column_index);
		Column_Writer(Result_File,Target_Columns,Tgt_Column_index);
		
		
}catch(SQLException se){
	
	Error_exception(Result_File,Target_Columns,se.getMessage());
   
   
 }catch(Exception e){
	 
	 Error_exception(Result_File,Target_Columns,e.getMessage());
 }
	
}



	public void Target_Column_Validation() throws SQLException, IOException{
		
		try{
			
			if(Tgt_DB.equalsIgnoreCase("Hive")) {
		    	
				Hive_HDBC(Tgt_Username,Tgt_Password,Tgt_Host);
		    	}

		    	if(Tgt_DB.equalsIgnoreCase("SQL Server")) {
		    	
		    		SQL_Server(Tgt_Server_name,Tgt_DB_Name);
		    	}
		    	
		    	if(Tgt_DB.equalsIgnoreCase("DB2")) {
		        	
		    		DB2_jDBC(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Port,Tgt_Host);
		    	}
		         if(Tgt_DB.equalsIgnoreCase("Netezza")) {
		        	
		    		Netezza(Tgt_Username,Tgt_Password,Tgt_DB_Name,Tgt_Server_name);
		        	}
		         
			
		
		System.out.println(" Validation Begins");
		System.out.println("------------------------");

		res2 = stmt.executeQuery("desc "+Tgt_Query);
		System.out.println(" Validation Processing........");

			String val=null;
			while(res2.next())
			{
				
				if(Partition_Value.equalsIgnoreCase("No")) {
					if(!(Strings.isNullOrEmpty(res2.getString(1)))) {
						String aa=res2.getString(1);
						if(aa.contains("etl_cycle_dt") || aa.contains("# Partition Information") ||aa.contains("# col_name") || aa.contains("batch_id")) {
							
							break;
						}
					}
					
				}
				if(!(Strings.isNullOrEmpty(res2.getString(1)))) {
					if((Strings.isNullOrEmpty(res2.getString(2)))) {
						val="";
					}
					else {
						val=res2.getString(2);
					}
				Tgt_Column_index.put(res2.getString(1).toLowerCase().trim(), val.toLowerCase().trim());
				}
				
			}
			
			 DB_Close();
		
			Total_Tgt_Count=Tgt_Column_index.size();
			Column_Writer_Header(Result_File,Target_Columns,Tgt_Column_index);
			Column_Writer(Result_File,Target_Columns,Tgt_Column_index);
			
			
	}catch(SQLException se){
		
		Error_exception(Result_File,Target_Columns,se.getMessage());
	   
	   
	 }catch(Exception e){
		 
		 Error_exception(Result_File,Target_Columns,e.getMessage());
	 }
		
	}
	
	
	public static void Column_Writer(String Filepath,String Sheet,HashMap<String, String> map) throws IOException{
		
		 FileInputStream inputStream = new FileInputStream(Filepath);
	     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
	     inputStream.close();

	     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
	     wb.setCompressTempFiles(true);

	     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
	     sh.setRandomAccessWindowSize(100);
	     
	     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
	     lock.setAlignment(HorizontalAlignment.LEFT);
	     XSSFFont font= (XSSFFont) wb.createFont();
	     font.setFontHeightInPoints((short)10);
	     font.setFontName("Verdana");
	     font.setColor(IndexedColors.BLACK.getIndex());
	     font.setBold(false);
	     font.setItalic(false);
	     lock.setFont(font);
	     lock.setBorderTop(BorderStyle.MEDIUM);
	     lock.setBorderRight(BorderStyle.MEDIUM);
	     lock.setBorderBottom(BorderStyle.MEDIUM);
	     lock.setBorderLeft(BorderStyle.MEDIUM);
	     
	    
	     
	     int rownum = 1;
	     for (Map.Entry<String,String> entry : map.entrySet()) { 
	           
	            Row row = sh.createRow(rownum);
	            
	          
		        	 Cell cell3 = row.createCell(0);
		        	 Cell cell4 = row.createCell(1);
		        	
			        	cell3.setCellValue(entry.getKey()); 
			        	cell4.setCellValue(entry.getValue()); 
			        	
		        	 cell3.setCellStyle(lock);
		        	 cell4.setCellStyle(lock);
		         
	            
	            rownum++;
	            
	     }
	     

	 FileOutputStream out = new FileOutputStream(Filepath);
	 wb.write(out);
	 out.close();
	}
	
	
	public static void Column_Writer_Header(String Filepath,String Sheet,HashMap<String, String> map) throws IOException{
		
		 FileInputStream inputStream = new FileInputStream(Filepath);
	     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
	     inputStream.close();

	     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
	     wb.setCompressTempFiles(true);

	     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
	     sh.setRandomAccessWindowSize(100);
	     
	     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
	     lock.setAlignment(HorizontalAlignment.LEFT);
	     XSSFFont font= (XSSFFont) wb.createFont();
	     font.setFontHeightInPoints((short)10);
	     font.setFontName("Verdana");
	     font.setColor(IndexedColors.BLACK.getIndex());
	     font.setBold(true);
	     font.setItalic(false);
	     lock.setFont(font);
	     lock.setBorderTop(BorderStyle.MEDIUM);
	     lock.setBorderRight(BorderStyle.MEDIUM);
	     lock.setBorderBottom(BorderStyle.MEDIUM);
	     lock.setBorderLeft(BorderStyle.MEDIUM);
	     
	     int row1=0;
	     Row row11 = sh.createRow(row1);
	     Cell cell1 = row11.createCell(0); 
	     Cell cell2 = row11.createCell(1); 
	     cell1.setCellValue("Column Name");
	     cell2.setCellValue("Data Type");
	     cell1.setCellStyle(lock);
	     cell2.setCellStyle(lock);
	     
	     

	 FileOutputStream out = new FileOutputStream(Filepath);
	 wb.write(out);
	 out.close();
	}
	
	
	
	public void Mismatch_Validation() throws SQLException, IOException{
		
		ArrayList<String> record_Source = new ArrayList<String>();
		
		 for (String key :Src_Column_index.keySet())  
		    { 
			 
			 if(!(Tgt_Column_index.containsKey(key))){
				 
				 record_Source.add(key+" | "+Src_Column_index.get(key)+" |   |   | Fail | Present in Source table But not in Target table");
				 DDL_Status=true;
			 }
			 else {
				 
				 if(!(Src_Column_index.get(key).equalsIgnoreCase(Tgt_Column_index.get(key)))) {
					 
				  record_Source.add(key+" | "+Src_Column_index.get(key)+" | "+ key+" | "+Tgt_Column_index.get(key)+" | Fail | Column Data Types Mismatch");
				  DDL_Status=true;
				 }
				 else {
					 
				 record_Source.add(key+" | "+Src_Column_index.get(key)+" | "+ key+" | "+Tgt_Column_index.get(key)+" | Pass |  ");
					 
				 }
				 
				 Tgt_Column_index.remove(key);
				 
			 }
			 
		    }
		 
		 for (String key :Tgt_Column_index.keySet())  
		    { 
			 
			
				 
				 record_Source.add("  |   | "+key+" | "+Tgt_Column_index.get(key)+" | Fail | Present in Target table But not in Source table");
				 DDL_Status=true;
			 
			 
		    }
		 
		 DDL_Src=record_Source;
		 Missing_Record_header_DDL(Result_File,Mismatches,record_Source);
		 
		 Missing_Record_Bulk_Writer_DDL(Result_File,Mismatches,record_Source);
		
		
	}
	
	public static void Missing_Record_Bulk_Writer_DDL(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
		
		 FileInputStream inputStream = new FileInputStream(Filepath);
	     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
	     inputStream.close();

	     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
	     wb.setCompressTempFiles(true);

	     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
	     sh.setRandomAccessWindowSize(100);
	     
	     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
	     lock.setAlignment(HorizontalAlignment.LEFT);
	     XSSFFont font= (XSSFFont) wb.createFont();
	     font.setFontHeightInPoints((short)10);
	     font.setFontName("Verdana");
	     font.setColor(IndexedColors.BLACK.getIndex());
	     font.setBold(false);
	     font.setItalic(false);
	     lock.setFont(font);
	     lock.setBorderTop(BorderStyle.MEDIUM);
	     lock.setBorderRight(BorderStyle.MEDIUM);
	     lock.setBorderBottom(BorderStyle.MEDIUM);
	     lock.setBorderLeft(BorderStyle.MEDIUM);
	     
	     
	     
	      for(int rownum = 1; rownum <= map.size(); rownum++){
	         Row row = sh.createRow(rownum);
	         
	         String[] act=map.get(rownum-1).split(" \\| ");
	        
	         for(int i=0;i<act.length;i++){
	        	
	        	 Cell cell = row.createCell(i);  
	        	 if(Strings.isNullOrEmpty(act[i])){
		        	 cell.setCellValue(act[i]);
		        	 }
		        	 else {
		        		 cell.setCellValue(act[i].trim()); 
		        	 }
	        	 cell.setCellStyle(lock);
	         }
	           

	 }


	 FileOutputStream out = new FileOutputStream(Filepath);
	 wb.write(out);
	 out.close();
	}
	
	public static void Missing_Record_header_DDL(String Filepath,String Sheet,ArrayList<String> map) throws IOException{
		
		 FileInputStream inputStream = new FileInputStream(Filepath);
	     XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
	     inputStream.close();

	     SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
	     wb.setCompressTempFiles(true);

	     SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
	     sh.setRandomAccessWindowSize(100);
	     
	     XSSFCellStyle lock=(XSSFCellStyle) wb.createCellStyle();
	     lock.setAlignment(HorizontalAlignment.LEFT);
	     XSSFFont font= (XSSFFont) wb.createFont();
	     font.setFontHeightInPoints((short)10);
	     font.setFontName("Verdana");
	     font.setColor(IndexedColors.BLACK.getIndex());
	     font.setBold(true);
	     font.setItalic(false);
	     lock.setFont(font);
	     lock.setBorderTop(BorderStyle.MEDIUM);
	     lock.setBorderRight(BorderStyle.MEDIUM);
	     lock.setBorderBottom(BorderStyle.MEDIUM);
	     lock.setBorderLeft(BorderStyle.MEDIUM);
	     
	     int row1=0;
	     Row row11 = sh.createRow(row1);
	     Cell cell1 = row11.createCell(0); 
	     Cell cell2 = row11.createCell(1);
	     Cell cell3 = row11.createCell(2); 
	     Cell cell4 = row11.createCell(3);
	     Cell cell5 = row11.createCell(4);
	     Cell cell6 = row11.createCell(5);
	     cell1.setCellValue("Source Column Name");
	     cell2.setCellValue("Source Data Type");
	     cell3.setCellValue("Target Column Name");
	     cell4.setCellValue("Target Data Type");
	     cell5.setCellValue("Status");
	     cell6.setCellValue("Comments");
	     cell1.setCellStyle(lock);
	     cell2.setCellStyle(lock);
	     cell3.setCellStyle(lock);
	     cell4.setCellStyle(lock);
	     cell5.setCellStyle(lock);
	     cell6.setCellStyle(lock);
	     
	     
	 FileOutputStream out = new FileOutputStream(Filepath);
	 wb.write(out);
	 out.close();
	}


public  void Create_Sheet1(String Filepath,String Sheet) throws IOException{
 		
 		FileInputStream fis=new FileInputStream(Filepath);
 		XSSFWorkbook book=new XSSFWorkbook(fis);
 		XSSFSheet ws=book.createSheet(Sheet);
 	
 		fis.close();
 		FileOutputStream fo=new FileOutputStream(Filepath);
 		book.write(fo);
 		book.close();
 		fo.flush();
 		fo.close();
 		
 	}

public  void Create_workbook_Sheets1() throws IOException{
	  
    Create_Book(Result_File);
	Create_Sheet1(Result_File,Source_Columns);
	Create_Sheet1(Result_File,Target_Columns);
	Create_Sheet1(Result_File,Mismatches);
	Create_Sheet1(Result_File,Summary);
	
	
  
  
}
 	


public void Target_Excel_Derived_Exe() throws SQLException, ClassNotFoundException, IOException {
	 
	   spliter_Derived_Validation();
		String[][] data=readXLSX(Input_File_Path_Target,"Sheet1");
		
		// Tgt_Column_Count=data.length;
		 String key_val="";
		 
		 //Tgt_Column_Name
		 
		 //String[] ll=new String[Tgt_Column_Count];
		 
		 //for(int i=0;i<data.length;i++) {
			// ll[i]=data[i][0];
			 
		// }
		 
		// Tgt_Column_Name=ll;
		 
		
		for(int i=0;i<data.length;i++) {
			
			String v=data[i][1];
			 
			 if(!(Key_Column_identifier(i))) {
				 
				 if( key_val.length()==0) {
						
					 if (Strings.isNullOrEmpty(v)) {
						  key_val=key_val+"";
					  }
					  else {
						  key_val=key_val+v.trim();
					  }
					 
				 }
				 else {
					 
					 if (Strings.isNullOrEmpty(v)) {
						  key_val=key_val+" | "+"";
					  }
					  else {
						  key_val=key_val+" | "+v.trim();
					  }
					 
				 }
				 
				 
			 }
				 
				 
			
		}
		
		Default_Key=key_val;
		
		 for (String key : Src_Map.keySet())  
	   	    { 
			 
			 Tgt_Map.put(key,key_val);
			 
	   	    }
			
			        	 
			        	
	
		
			  
		 Total_Tgt_Count=Src_Map.size();
		 Total_Tgt_Count_no_dup=Src_Map.size();
		 Tgt_duplicate_count=0;
		 Target_Duplicate_Flag=false;
		
		
}



public void spliter_Derived_Validation() {
	
	  Iterator value = Src_data.iterator(); 
	  
    while (value.hasNext()) { 
  	  
  	  String[] act=value.next().toString().split("\\|");
		  
			String key_val="";
			String val="";
			int key_start=0;
			int nonkey_start=0;
			
			 for(int j=0;j<act.length;j++) {
					
				  if(Key_Column_identifier(j)) {
					  
					  key_start++;
					  
					  String v=act[j]; 
						 if(key_start==1) {
							
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+"";
							  }
							  else {
								  key_val=key_val+v.trim();
							  }
							 
						 }
						 else {
							 
							 if (Strings.isNullOrEmpty(v)) {
								  key_val=key_val+" | "+"";
							  }
							  else {
								  key_val=key_val+" | "+v.trim();
							  }
							 
						 }
						 
					 }
						 else {
							 nonkey_start++;
							 String v=act[j]; 
							 if(nonkey_start==1) {
								
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+"";
								  }
								  else {
									  val=val+v.trim();
								  }
								 
							 }
							 else {
								 
								 if (Strings.isNullOrEmpty(v)) {
									 val=val+" | "+"";
								  }
								  else {
									  val=val+" | "+v.trim();
								  }
								 
							 }
							 
					  
				  }
				  
			 }
			 
  	  
			 Src_Map.put(key_val,val);
  	  
    
    }
    
   
	
	
	
}



public void Data_Validation_Derived() throws IOException{
	  
	int count=0;
		
	boolean Sta;
	
	
	
	if(Non_Primary) {

	
	ArrayList<String> data_write = new ArrayList<String>();
	
	
	
	Header_data_mismatch(Result_File,Data_Mismatch);
	
		
		        for (String key : Src_Map.keySet()) {
		        	 Sta=false;
		        	 
		        	
		 

				if(Default_Key.equals(Src_Map.get(key))) {
					
				}
				else {
					
					count++;
					
					Data_Mismatch_Flag=true;
					
					String[] Tgt_val=Default_Key.split("\\|");
					String[] Src_val=Src_Map.get(key).split("\\|");
					
					String val="";
					
					
					for(int i=0;i<Tgt_val.length;i++) {
						
						if(!(Tgt_val[i].trim().equalsIgnoreCase(Src_val[i].trim()))) {
							
		
							 if(val.length()==0) {
							
								val=val+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
							 }
							 else {
								 
								 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | MisMatch ";
								 
							 }
							
							
							Column_Mismatch.put(i+1, Column_Mismatch.get(i+1)+1);
							
							if(Column_Mismatch.get(i+1)<=10) {
								
								Sta=true;
							}
						}
						
						else {
							
							 if(val.length()==0) {
									
									val=val+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
								 }
								 else {
									 
									 val=val+" | "+Tgt_val[i]+" | "+Src_val[i]+" | Match ";
									 
								 }
						}
						
						
						
					}
					
					if(Sta){
						
						data_write.add(key+" | "+val);
						
					}
					
					
					
				}
			}
		
		        Data_mismatch_count=count;
		        
		      
		   
		       Data_validation_Bulk_Writer(Result_File,Data_Mismatch,data_write);
	}
	else {
		
		Header_No_pri(Result_File,Data_Mismatch);
		
		
		
	}
	
	
		
		   	  
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



public  void Summary_Write_DDL(String Filepath,String Sheet) throws IOException{
	
  	 FileInputStream inputStream = new FileInputStream(Filepath);
       XSSFWorkbook wb_template = new XSSFWorkbook(inputStream);
       inputStream.close();

       SXSSFWorkbook wb = new SXSSFWorkbook(wb_template); 
       wb.setCompressTempFiles(true);

       SXSSFSheet sh = (SXSSFSheet) wb.getSheet(Sheet);
       sh.setRandomAccessWindowSize(100);
       
       lock_Final=(XSSFCellStyle) wb.createCellStyle();
       lock_Final.setAlignment(HorizontalAlignment.LEFT);
       XSSFFont font= (XSSFFont) wb.createFont();
       font.setFontHeightInPoints((short)10);
       font.setFontName("Verdana");
       font.setColor(IndexedColors.BLACK.getIndex());
       font.setBold(true);
       font.setItalic(false);
       lock_Final.setFont(font);
       lock_Final.setBorderTop(BorderStyle.MEDIUM);
       lock_Final.setBorderRight(BorderStyle.MEDIUM);
       lock_Final.setBorderBottom(BorderStyle.MEDIUM);
       lock_Final.setBorderLeft(BorderStyle.MEDIUM);
       
       int row_count=0;
       
               row_final = sh.createRow(row_count);
          
               cell_value("Type",0);
               cell_value("Column",1);
               cell_value("Result",2);
               
               row_count++;
   
               if(DDL_Status)
               {
          		 
          		 row_final = sh.createRow(row_count);
          		 cell_value("DDL",0);
                 cell_value("Fail",2);
  	    		

          		}
               else {
            	   
            	   row_final = sh.createRow(row_count);
            		 cell_value("DDL",0);
                   cell_value("Pass",2);
            	   
               }

   FileOutputStream out = new FileOutputStream(Filepath);
   wb.write(out);
   out.close();
  }
  
  

}

