package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.common.base.Strings;

public class Fixed_Scenario {
	
	public  String Source;
	public  String header;
	public  String footer;
	public  String output;
	public  int[] Key1;
	public  int[] Key2;
	public  String copy;
	public  String Src_Col_Name;
	public  String symbol;
	 public  String[] Src_Column_Name;
	 public  int Src_Column_Count,Tgt_Column_Count;
	
	public ArrayList<String> Src = new ArrayList<String>();

	public void Fixed_Launcher(String a1,String a2,String a3,String a4,String a5,String a6) throws FileNotFoundException, IOException, InterruptedException {
		
		
		Source=a1;
		header=a2;
		footer=a3;
		output=a4;
		copy=a5;
		symbol=a6;
		
		if(symbol.equalsIgnoreCase("Excel")) {
		
		 Create_workbook_Sheets();
		 Source_Unix_Copybook_Exe();
		
		 Missing_Record_Bulk_Writer(output,"Sheet1",Src,Src_Column_Name);
		}
		else {
			Source_Unix_Copybook_delimiter();
		}
		
	}
	
	
public  void Create_Sheet(String Filepath,String Sheet) throws IOException{
 		
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

public  void Create_workbook_Sheets() throws IOException{
	  
    Create_Book(output);
	Create_Sheet(output,"Sheet1");
	
  
  
}
 	

 	
 public  String Create_Book(String Filepath) throws IOException{
 	
 		String FP=Filepath;
 		XSSFWorkbook workbook = new XSSFWorkbook();
 	    FileOutputStream out = new FileOutputStream(new File(FP));
 	      workbook.write(out);
 	      out.close();
 	      return FP;
 			
 		}
 
 
public void Source_Unix_Copybook_Exe() throws IOException, InterruptedException{
	 
	 
	 String[][] mydata=readXLSX(copy,"Copybook");
		
		for(int i=1;i<mydata[0].length;i++) {
			
			if(i==1) {
			
			Src_Col_Name=mydata[0][i];
			}
			else {
			Src_Col_Name=Src_Col_Name+" , "+mydata[0][i];
				
			}
			
		}
		
		 BufferedReader reader1 = new BufferedReader(new FileReader(Source));
		  long Src_count=0;
		 
	   
	      Src_Column_Name=Src_Col_Name.split(",");
	      Src_Column_Count=Src_Column_Name.length;
	      
	   
	      
	      String line1;
	      String temp_val = null;
	     
	      
	      if(header.equalsIgnoreCase("Yes") ) {
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
	  		  int end;
	  		  int len;
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		
	  		  
	  		  if(Strings.isNullOrEmpty(mydata[2][i])) {
	  			  
	  			end=Integer.parseInt(mydata[3][i]);
	  			
	  			  
	  		  }
	  		  else {
	  			  
	  			 end=start + Integer.parseInt(mydata[2][i]);
	  			  
	  		  }
	  		  
	  		len=(end-start)+1;
	  		  
	  		  String v=null;
	  		
	  		    if(!(Strings.isNullOrEmpty(mydata[4][i]))) {
	  		    	
	  		    	if (Strings.isNullOrEmpty(line1.substring(start, end))) {
	  		    		v=line1.substring(start, end);
	  		    	}
	  		    	else {
	  		    	
	  		    	 v=StringUtils.leftPad(line1.substring(start, end), len, mydata[4][i]);
	  		    	}
	  		    }
	  		    else if(!(Strings.isNullOrEmpty(mydata[5][i]))) {
	  		    	if (Strings.isNullOrEmpty(line1.substring(start, end))) {
	  		    		v=line1.substring(start, end);
	  		    	}
	  		    	else {
	  		    	
	  		    	v=StringUtils.rightPad(line1.substring(start, end), len, mydata[5][i]);
	  		    	
	  		    	}
	  		    }
	  		    
	  		    else {
	  	     
	  		   v=line1.substring(start, end);
	  		    }
	  		 
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
		    	 int temp=Src.size();
		    	 Src.add(key_val);
				   temp_val=key_val;

	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
	    
	    if(footer.equalsIgnoreCase("Yes")) {
	    	Src.remove(temp_val);
	    	Src_count=Src_count-1;
	 	    }
				
	    
	    reader1.close();
	    
	
			 
	
	 
	 
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


public  void Missing_Record_Bulk_Writer(String Filepath,String Sheet,ArrayList<String> map,String[] a) throws IOException{
	
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
    
    Row row1 = sh.createRow(0);
    
    for(int i=0;i<a.length;i++){
   	// sh.autoSizeColumn(i);
   	 Cell cell1 = row1.createCell(i);  
   	 if(Strings.isNullOrEmpty(a[i])){
       	 cell1.setCellValue(a[i]);
       	 }
       	 else {
       		 cell1.setCellValue(a[i].trim()); 
       	 }
   	 cell1.setCellStyle(lock);
    }
      

   
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


public void Source_Unix_Copybook_delimiter() throws IOException, InterruptedException{
	 
	 
	 String[][] mydata=readXLSX(copy,"Copybook");
		
		for(int i=1;i<mydata[0].length;i++) {
			
			if(i==1) {
			
			Src_Col_Name=mydata[0][i];
			}
			else {
			Src_Col_Name=Src_Col_Name+" , "+mydata[0][i];
				
			}
			
		}
		
		 BufferedReader reader1 = new BufferedReader(new FileReader(Source));
		  long Src_count=0;
		 
	   
	      Src_Column_Name=Src_Col_Name.split(",");
	      Src_Column_Count=Src_Column_Name.length;
	      
	   
	      
	      String line1;
	      String temp_val = null;
	     
	      
	      if(header.equalsIgnoreCase("Yes") ) {
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
	  		  int end;
	  		  int len;
	  		  int start=Integer.parseInt(mydata[1][i])-1;
	  		  
	  		  if(Strings.isNullOrEmpty(mydata[2][i])) {
	  			  
	  			 end=Integer.parseInt(mydata[3][i]);
	  			  
	  		  }
	  		  else {
	  			  
	  			 end=start + Integer.parseInt(mydata[2][i]);
	  			  
	  		  }
	  		  
	  		len=(end-start)+1;
	  		  
	  		  String v=null;
	  		
	  		    if(!(Strings.isNullOrEmpty(mydata[4][i]))) {
	  		    	
	  		    	if (Strings.isNullOrEmpty(line1.substring(start, end))) {
	  		    		v=line1.substring(start, end);
	  		    	}
	  		    	else {
	  		    	
	  		    	 v=StringUtils.leftPad(line1.substring(start, end), len, mydata[4][i]);
	  		    	}
	  		    }
	  		    else if(!(Strings.isNullOrEmpty(mydata[5][i]))) {
	  		    	if (Strings.isNullOrEmpty(line1.substring(start, end))) {
	  		    		v=line1.substring(start, end);
	  		    	}
	  		    	else {
	  		    	
	  		    	v=StringUtils.rightPad(line1.substring(start, end), len, mydata[5][i]);
	  		    	
	  		    	}
	  		    }
	  		    
	  		    else {
	  	     
	  		   v=line1.substring(start, end);
	  		    }
	  		 
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
						  key_val=key_val+symbol+v;
					  }
					  else {
						  key_val=key_val+symbol+v.trim();
					  }
	  			   
	        }
	  	  
	        }
	  	  
	  	  
	  	     Src_count++;
		    	 int temp=Src.size();
		    	 Src.add(key_val);
				   temp_val=key_val;

	  	  
	  	  
	  	  line1 = reader1.readLine();
	        
	         
	    }
	    
	    if(footer.equalsIgnoreCase("Yes")) {
	    	Src.remove(temp_val);
	    	Src_count=Src_count-1;
	 	    }
				
	    
	    reader1.close();
	    
	
	    PrintWriter writer=new PrintWriter(output);
	    
	    for(String val:Src) {
	    	 writer.println(val);
	    }
	    
	    
	    writer.close();
	 
	 
}


   
  

 
	
	
	
	
	
	 

}
