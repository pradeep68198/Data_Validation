package Utilities;

import java.awt.AWTException;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.KeyEvent;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.imageio.ImageIO;

import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Zeke_Jobs extends General_Functions1{
	
	public  String Username;
	public  String Password;
	public  String Job_ID;
	//public  String Screenshot_Path;
	public  String Imagepath;
	public  char[] alpha = new char[26];
	public   HashMap<String, String> Zeke_Result =new HashMap<String, String>();
	
	
	 public void set_Key_Value(String Result_File1,String path) throws IOException {
   	      Result_File=Result_File1;
   	      Imagepath=path;
     	  Create_workbook_Sheets();
     	  

     }
	 
	
	
	
	public void Zeke_Run(String s1,String s2,String s3) throws Exception {
		
		Username=s1;
		Password=s2;
		Job_ID=s3;
		
         String[] job_split=Job_ID.split(",");
         String instance="";
		
		for(int j=0;j<job_split.length;j++) {
		
		
		Process p = Runtime.getRuntime().exec("cmd.exe /c start C:\\Rumba\\Desktops\\"+alpha[j]+"-Window.rdps");
		Thread.sleep(10000);
		Copy("log tso84,,"+Username);
		Enter();		
		Copy(Password);	
		Enter();	
		Thread.sleep(15000);		
		Enter();
		Thread.sleep(5000);
		Copy("zeketest;ze;5");
		Enter();	
			
			PressTAB(1);
		Copy("*");
		PressTAB(1);
		Copy("1ku");
		PressTAB(3);
		Copy("2");
		Enter();
		
		Thread.sleep(5000);
		for(int i=1;i<=8;i++){
			PressTAB(i+1);
			Copy("c");
			Enter();
			Thread.sleep(2000);
			BufferedImage subimage_Active = ImageIO.read(new File(Imagepath+"New_Value.png"));
			 BufferedImage Main_image = new Robot().createScreenCapture(new Rectangle(Toolkit.getDefaultToolkit().getScreenSize()));
			 boolean flag=true;
			 
			 if(match(subimage_Active, Main_image) != null) {
				break;
			 }
			 
			
		}
		
		PressDEL(6			);	
		Copy(job_split[j].trim());
		Enter();
		PressDEL(4);				
											
		Copy("run");
		Enter();
		Copy("complete");
		Enter();
		
		Thread.sleep(3000);
		Copy("start zeketest;ze;2");
		Enter();
		PressTAB(4);
		Copy(job_split[j]);
		Enter();
		validation(job_split[j]);
		f3(10);
		Copy("3");
		Enter();
		Copy("bye");
		Enter();
		Thread.sleep(8000);
		Kill_Bat();
		}
		
		Summary_Write_Zeke(Result_File,Summary);
	}
	
	
	
	public  void Copy(String text) throws AWTException, InterruptedException{
		Robot r = new Robot();
		StringSelection stringSelection = new StringSelection(text);
		Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
		clipboard.setContents(stringSelection, stringSelection);
		r.keyPress(KeyEvent.VK_CONTROL);
		r.keyPress(KeyEvent.VK_V);
		r.keyRelease(KeyEvent.VK_V);
		r.keyRelease(KeyEvent.VK_CONTROL);
		Thread.sleep(2000);
		
	}
	
	public  void Enter() throws AWTException, InterruptedException{
		Robot r = new Robot();
		r.keyPress(KeyEvent.VK_ENTER);
		r.keyRelease(KeyEvent.VK_ENTER);
		Thread.sleep(2000);
	}
	
	public  void PressTAB(int count) throws AWTException, InterruptedException{
		Robot r = new Robot();
		for(int i=1;i<=count;i++){
		r.keyPress(KeyEvent.VK_TAB);
		r.keyRelease(KeyEvent.VK_TAB);
		Thread.sleep(1000);
		}
	}
	
	public  void Delete() throws AWTException, InterruptedException{
		Robot r = new Robot();
		r.keyPress(KeyEvent.VK_DELETE);
		r.keyRelease(KeyEvent.VK_DELETE);
		Thread.sleep(2000);
	}
	
	public  void f3(int count) throws AWTException, InterruptedException{
		Robot r = new Robot();
		for(int i=1;i<=count;i++){
		r.keyPress(KeyEvent.VK_F3);
		r.keyRelease(KeyEvent.VK_F3);
		Thread.sleep(2000);
	
		}
	}
	public  void PressDEL(int count) throws AWTException, InterruptedException{
		Robot r = new Robot();
		for(int i=1;i<=count;i++){
		r.keyPress(KeyEvent.VK_DELETE);
		r.keyRelease(KeyEvent.VK_DELETE);
		Thread.sleep(1000);
		}
	}
	
	 public void Instance_setup() {
		 
			
		 for(int i = 0; i < 26; i++){
		     alpha[i] = (char)(65 + i);
		 }
 

}

	
	
	
	public  void validation(String job) throws InterruptedException, IOException, AWTException {
		
		 BufferedImage subimage_Active = ImageIO.read(new File(Imagepath+"Active.png"));
		 BufferedImage subimage_Fail = ImageIO.read(new File(Imagepath+"Fail.png"));
		 BufferedImage subimage_Success = ImageIO.read(new File(Imagepath+"Success.png"));
		 
		// BufferedImage Main_image = new Robot().createScreenCapture(new Rectangle(Toolkit.getDefaultToolkit().getScreenSize()));
		 //boolean flag=true;
		
		for(int i=1;i<=120;i++) {
			
				
				 BufferedImage Main_image1 = new Robot().createScreenCapture(new Rectangle(Toolkit.getDefaultToolkit().getScreenSize()));
				
				 if(match(subimage_Active, Main_image1) != null) {
					 loop();
					 }
				 else {
					 
					 if(match(subimage_Success, Main_image1) != null) {
						 
						 Zeke_Result.put(job, "Pass");
						
						 break;
					 }
				 
					 else if(match(subimage_Fail, Main_image1) != null) {
					
					Zeke_Result.put(job, "Fail");
					
					break;
				 }
				 
				 
				 
				 }
				 
				
						
				
			
			
			
			
		//}
		}
		
		
	}
	
	public  void loop() throws InterruptedException, AWTException {
		
		System.out.println("inside loop");
		
		for(int j=1;j<=60;j++) {
			
			Thread.sleep(500);
		}
		
		f3(1);
		PressTAB(4);
		Enter();
		
		
		
		
	}
	
	 public  Point match(BufferedImage subimage, BufferedImage image) {
	        
	        for (int i = 0; i <= image.getWidth() - subimage.getWidth(); i++) {
	            check_subimage:
	            for (int j = 0; j <= image.getHeight() - subimage.getHeight(); j++) {
	                for (int ii = 0; ii < subimage.getWidth(); ii++) {
	                    for (int jj = 0; jj < subimage.getHeight(); jj++) {
	                        if (subimage.getRGB(ii, jj) != image.getRGB(i + ii, j + jj)) {
	                            continue check_subimage;
	                        }
	                    }
	                }
	               
	                return new Point(i, j);
	            }
	        }
	        return null;
	    }

	    
	    public  void Kill_Bat() throws Exception{
	    	
	    	 Process p1 = Runtime.getRuntime().exec("taskkill /F /IM RumbaPage.exe");
	   		
	   	
	   	}
	    
	    
	    public  void Summary_Write_Zeke(String Filepath,String Sheet) throws IOException{
	    	
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
	    
	           	
	                for (String key : Zeke_Result.keySet()) {
	           		 row_count++;
	           		 row_final = sh.createRow(row_count);
	           		 cell_value("ZekeJob",0);
	   	             cell_value(key,1);
	   	             
	   	            	 cell_value(Zeke_Result.get(key),2);
	   	    		

	           		}

	    FileOutputStream out = new FileOutputStream(Filepath);
	    wb.write(out);
	    out.close();
	   }

	    
	    
	   

}
