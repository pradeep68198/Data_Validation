package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import com.google.common.base.Strings;

public class RAPS_Scenario {
	
	public  String Source_1;
	public  String Source_2;
	public  String Source_1_Index;
	public  String Source_2_Index;
	public  int Billed_Amt_index,Paid_Amt_index,ICD_IND_Index,Src_claim_Index,revenue_index;
	public  int[] Key1;
	
	public  String Output_Path,Result_File;
	public  Properties prob;
	public  HashMap<String, String> Src =new HashMap<String, String>();
	public  HashMap<String, String> Tgt =new HashMap<String, String>();
	public  HashSet<String> Src1 =new HashSet<String>();
	
	
	public  HashMap<String, Integer> Src_1 =new HashMap<String, Integer>();
	public  HashMap<String, String> ICD_IND_list =new HashMap<String, String>();


	public void MPP_Launcher(String a1,String a2,String a3) throws FileNotFoundException, IOException, ParseException {
		
		
		Source_1=a1; 
		Source_2=a2;
		Source_1_Index="1,2,3,4,9,11,12,22,23,24,26,27";
		Billed_Amt_index=19;
		Paid_Amt_index=20;
		
		Result_File=a3;
		Key1=Key_Value_Split(Source_1_Index);
		
		Source_Delimiter_Exe2();
		Source_Delimiter_Exe1();
		
				
		 PrintWriter writer=new PrintWriter(Result_File); 
	    for (String key : Src.keySet())  {

	    		 writer.println(Src.get(key));
	    	
	    	
	    
	    }
	    
	    writer.close();

	}
	

	 public  int[] Key_Value_Split(String Key_Column) {
		 
		 int[] Keyvalue_Column;
	    	
   	 String[] Keyvalue_Split=Key_Column.split(",");
		  Keyvalue_Column=new int[Keyvalue_Split.length];
		  for(int x=0;x<Keyvalue_Split.length;x++){
		    	
		    	Keyvalue_Column[x]=Integer.parseInt(Keyvalue_Split[x])-1;
	
		    	
		    }
		 
   	return Keyvalue_Column;
   }
	 
	 
	 public  void Source_Delimiter_Exe1() throws IOException, ParseException{
		 	
		 int len=0;
		  BufferedReader reader1 = new BufferedReader(new FileReader(Source_1));
	       String line1 = reader1.readLine();
	       line1 = reader1.readLine();
			
	     
	     while (line1 != null)
	     {
	   	  String key_val="";
	   	  
	   	  String[] val=line1.split("\\|");
	   	  
	   	  
	   	  if(Src.containsKey(val[1])) {
	   		  
	   		DecimalFormat df2 = new DecimalFormat("#.##");
	   		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	   		String final_start;
	   		String final_stop;
	   		  
	   		double bill = Double.parseDouble(val[Billed_Amt_index]); 
	   		double paid = Double.parseDouble(val[Paid_Amt_index]); 
	   		String start_date=val[2].trim();
	   		String stop_date=val[3].trim();
	   		Date start_date_date = formatter.parse(start_date);
	        Date stop_date_date = formatter.parse(stop_date);
	   		
	   		String res=Src.get(val[1]);
	   		
	   	   String[] res1=res.split(" \\| ");
	   	   
	   	   int i1=res1.length-2;
	   	   int i2=res1.length-1;
	   	   
	   	   String start_date1=res1[2].trim();
  		   String stop_date1=res1[3].trim();
  		   
  		   
  		   if(start_date1.equals("0001-01-01")) {
  			 start_date1="1600-01-01";
  		   }
  		   if(stop_date1.equals("0001-01-01")) {
  			 stop_date1="9999-12-31";
  		   }
  		  
  		   
  		  Date start_date1_date = formatter.parse(start_date1);
         Date stop_date1_date = formatter.parse(stop_date1);
         
         if(start_date_date.before(start_date1_date)) {
       	  
       	  res1[2]=formatter.format(start_date_date);  
         }
         
         if(stop_date_date.after(stop_date1_date)) {
       	  
       	  res1[3]=formatter.format(stop_date_date);  
         }
         
	   	   
	   	 double bill1 = Double.parseDouble(res1[i1].trim()); 
  		 double paid1 = Double.parseDouble(res1[i2].trim()); 
  		
  		String bill_res=String.format("%.2f",(bill+bill1));
  		String paid_res=String.format("%.2f",(paid+paid1));
  		
  		res1[i1]=bill_res;
  		res1[i2]=paid_res;
  		
  		String result=delimiter(res1);
  		
  	 	Src.put(val[1], result);
	   		  
	   		  
	   	  }
	   	  else {
	   
	   	  
	   		 len=val.length;
	   		int key_start=0;
	   	  
	   	  for(int i=0;i<len;i++)
	         {
	   		 
	   		if(Key_Column_identifier(i,Key1)) {
				  
				  key_start++;
				  
				  String v=val[i]; 
					 if(key_start==1) {
						
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
	   	  
	   	String provider="";
	   	  
	   	  if(Tgt.containsKey(val[1].trim())) {
	   		  
	   		provider=Tgt.get(val[1].trim());
	   		
	   		Tgt.remove(val[1].trim());
	   		  
	   	  }
	   	  else {
	   		  
	   		provider="0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0"; 
	   		  
	   	
	   	  }
	   	  
	   	 double bill1 = Double.parseDouble(val[Billed_Amt_index].trim()); 
  		 double paid1 = Double.parseDouble(val[Paid_Amt_index].trim()); 
	   	
	   	  
	   	  String Billed=String.format("%.2f",bill1);
	   	 String Paid=String.format("%.2f",paid1);
	   	 
	   	 key_val=key_val+" | "+provider+" | "+Billed+" | "+Paid;
	   	  
	   	Src.put(val[1], key_val);
	   	  
	         }
	   	  
	   	  
	   	   
	   	  
	   	  
	   	  line1 = reader1.readLine();
	         
	          
	     }
				
	     
	     reader1.close();
			 
			
			
			 System.out.println("Source1 completed");
		 }
	 
	 public  void Source_Delimiter_Exe2() throws IOException{
		 	
		   BufferedReader reader1 = new BufferedReader(new FileReader(Source_2));
	       String line1 = reader1.readLine();
	       line1 = reader1.readLine();
	      
	       String RENDERING;
	       String REFERRING;
	       String BILLING;
	       String ATTENDING;
	       String ORDERING;
	       String OPERATING;
	       String SUPERVISING;
	       String SERVICING;
	       String OTHER_PROVIDER;
	       
	     
	     while (line1 != null)
	     {
	   	 
	   	  
	   	  String[] k=line1.split("\\|");
	   	  
	   	  
	       if(Tgt.containsKey(k[1].trim())) {
	    	   
	    	   String[] k1=Tgt.get(k[1]).split(" \\| ");
	    	   
	    	    RENDERING=k1[0];
		        REFERRING=k1[1];
		        BILLING=k1[2];
		        ATTENDING=k1[3];
		        ORDERING=k1[4];
		        OPERATING=k1[5];
		        SUPERVISING=k1[6];
		        SERVICING=k1[7];
		        OTHER_PROVIDER=k1[8];
		        
		        if(k[4].trim().equalsIgnoreCase("RENDERING")) {
		        	
		        	if(RENDERING.equalsIgnoreCase("0")) {
	   				
	   				RENDERING=k[2].trim();
		        	}
	   				
	   			}
	   			
           if(k[4].trim().equalsIgnoreCase("REFERRING")) {
        	   if(REFERRING.equalsIgnoreCase("0")) {		
           	REFERRING=k[2].trim();
        	   }
	   				
	   			}
         
	   			
	   			if(k[4].trim().equalsIgnoreCase("BILLING")) {
	   				if(BILLING.equalsIgnoreCase("0")) {
	   				BILLING=k[2].trim();
	   				}
	   				
	   			}
	   			
           if(k[4].trim().equalsIgnoreCase("ATTENDING")) {
        	   if(ATTENDING.equalsIgnoreCase("0")) {		
           	ATTENDING=k[2].trim();
        	   }	
	   			}
           
           if(k[4].trim().equalsIgnoreCase("ORDERING")) {
        	   if(ORDERING.equalsIgnoreCase("0")) {	
           	ORDERING=k[2].trim();
        	   }
  				
  			}
  			
         if(k[4].trim().equalsIgnoreCase("OPERATING")) {
        	 if(OPERATING.equalsIgnoreCase("0")) {	
       	OPERATING=k[2].trim();
        	 }	
  			}
       

           if(k[4].trim().equalsIgnoreCase("SUPERVISING")) {
        	   if(SUPERVISING.equalsIgnoreCase("0")) {	
       	SUPERVISING=k[2].trim();
        	   }	
			}
			
            if(k[4].trim().equalsIgnoreCase("SERVICING")) {
            	if(SERVICING.equalsIgnoreCase("0")) {	
   	SERVICING=k[2].trim();
            	}
			}
   
          if(k[4].trim().equalsIgnoreCase("OTHER PROVIDER")) {
        	  if(OTHER_PROVIDER.equalsIgnoreCase("0")) {
   	OTHER_PROVIDER=k[2].trim();
        	  }	
			}
	   			
	   			
       String result=RENDERING+" | "+REFERRING+" | "+BILLING+" | "+ATTENDING+" | "+ORDERING+" | "+OPERATING+" | "+SUPERVISING+" | "+SERVICING+" | "+OTHER_PROVIDER;
        Tgt.put(k[1].trim(), result);

	    	   
	    	   
	    	   
	       }
	       else {
	    	   
	    	    RENDERING="0";
		        REFERRING="0";
		        BILLING="0";
		        ATTENDING="0";
		        ORDERING="0";
		        OPERATING="0";
		        SUPERVISING="0";
		        SERVICING="0";
		        OTHER_PROVIDER="0";
		        
		     
		   			
		        	if(k[4].trim().equalsIgnoreCase("RENDERING")) {
		        		   				
		        		   				RENDERING=k[2].trim();
		        		   				
		        		   			}
		        		   			
		        	            if(k[4].trim().equalsIgnoreCase("REFERRING")) {
		        		   				
		        	            	REFERRING=k[2].trim();
		        		   				
		        		   			}
		        	          
		        		   			
		        		   			if(k[4].trim().equalsIgnoreCase("BILLING")) {
		        		   				
		        		   				BILLING=k[2].trim();
		        		   				
		        		   			}
		        		   			
		        	            if(k[4].trim().equalsIgnoreCase("ATTENDING")) {
		        		   				
		        	            	ATTENDING=k[2].trim();
		        		   				
		        		   			}
		        	            
		        	            if(k[4].trim().equalsIgnoreCase("ORDERING")) {
		        	   				
		        	            	ORDERING=k[2].trim();
		        	   				
		        	   			}
		        	   			
		        	        if(k[4].trim().equalsIgnoreCase("OPERATING")) {
		        	   				
		        	        	OPERATING=k[2].trim();
		        	   				
		        	   			}
		        	        

		        	        if(k[4].trim().equalsIgnoreCase("SUPERVISING")) {
		        					
		        	        	SUPERVISING=k[2].trim();
		        					
		        				}
		        				
		        	    if(k[4].trim().equalsIgnoreCase("SERVICING")) {
		        					
		        	    	SERVICING=k[2].trim();
		        					
		        				}
		        	    
		        	    if(k[4].trim().equalsIgnoreCase("OTHER PROVIDER")) {
		        			
		        	    	OTHER_PROVIDER=k[2].trim();
		        					
		        				}
		        		   			
		        		   			
		        	    String result=RENDERING+" | "+REFERRING+" | "+BILLING+" | "+ATTENDING+" | "+ORDERING+" | "+OPERATING+" | "+SUPERVISING+" | "+SERVICING+" | "+OTHER_PROVIDER;
		        	    Tgt.put(k[1].trim(), result);
	    	   
	       }
	   
	   		
	   			
	   		
	   		
	   		line1 = reader1.readLine();
	   
	         
	          
	     }
				
	     
	     reader1.close();
	     
	   
	     
	    
			 
			
			
			 System.out.println("Source2 completed");
			
		 }
	 
	 
	 public  boolean Key_Column_identifier(int i,int[] Keyvalue_Column) {
			
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
	 
	 public  String fmt(double d) {
		    if(d == (long) d)
		        return String.format("%d",(long)d);
		    else
		        return String.format("%s",d);
		    }
	 
	 
	 public  String delimiter(String[] d) {
		    
		 String key="";
		 
		 for(int i=0;i<d.length;i++) {
			 
			 
			 if(i==0) {
					
				
				 key=key+d[i];
				  
				 
			 }
			 else {
				 
				 
				 key=key+" | "+d[i];
				  
				 
			 }
			 
			 
		 }
		 
		 return key;
		 
		 
		    }
	 
	 
	 public void MPP_Launcher1(String a1,String a3) throws FileNotFoundException, IOException, ParseException {
			
			
			Source_1=a1;
			
			Source_1_Index="2,3,4,5,6,8,10,13,14,15,16,17,18,19,20,21,6";
			Billed_Amt_index=19;
			Paid_Amt_index=20;
			revenue_index=12;
			
			Result_File=a3;
			Key1=Key_Value_Split(Source_1_Index);
			
			Source_Delimiter_Detail();
			
			
					
			 PrintWriter writer=new PrintWriter(Result_File); 
			    for (String key : Src1)  {

			    		 writer.println(key);
			    	
			    	
			    
			    }
			    
			    writer.close();

		}
	 
	 
	 public void Source_Delimiter_Detail() throws IOException, ParseException{
		 	
		 int len=0;
		  BufferedReader reader1 = new BufferedReader(new FileReader(Source_1));
	       String line1 = reader1.readLine();
	       line1 = reader1.readLine();
			
	     
	     while (line1 != null)
	     {
	   	  String key_val="";
	   	  
	   	  String[] val=line1.split("\\|");
	   	 
	   		 len=val.length;
	   		int key_start=0;
	   	  
	   	  for(int i=0;i<Key1.length;i++)
	         {
	   		 
	     
				  key_start++;
				  
				  String v=val[Key1[i]]; 
					 if(i==0) {
						
						 if (Strings.isNullOrEmpty(v)) {
							  key_val=key_val+v;
						  }
						  else {
							  key_val=key_val+v.trim();
						  }
						 
					 }
					 else {
						 
                           if(Key1[i]==Billed_Amt_index || Key1[i]==Paid_Amt_index) {
							 
							 double paid1 = Double.parseDouble(v.trim()); 
						   		
						   	 v=String.format("%.2f",paid1);
						 }
                             if(Key1[i]==revenue_index) {
                            	 
                        	   if (Strings.isNullOrEmpty(v)) {
                        		   v="0000";
                        	   }
                        	   else if(v.length()<4) {
                        		     int b=Integer.parseInt(v);
                        			
                        			v=String.format("%04d", b);
                        	   }
                        	   
                           }
                           
                           if(Key1[i]==7) {
                        	   if (Strings.isNullOrEmpty(v)) {
                        		   v="0";
                        	   }
                           }
                           
                           if(Key1[i]==2) {
                        	   
                        	   if(v.equals("0001-01-01")) {
                        			 v="1600-01-01";
                        		   }
                           }
                           
                             if(Key1[i]==3) {
                        	   
                        	   if(v.equals("0001-01-01")) {
                        			 v="9999-12-31";
                        		   }
                           }
                          
						 
						 if (Strings.isNullOrEmpty(v)) {
							  key_val=key_val+" | "+v;
						  }
						  else {
							  key_val=key_val+" | "+v.trim();
						  }
					 }
				 }
	   	  
	  	   Src1.add(key_val);
	   	  
	   	  line1 = reader1.readLine();
	         }
	   	  
	   
	   	 
	   	 
				
	     
	     reader1.close();
			 
			
			
			 System.out.println("Source1 completed");
		 }
	 
	 
	 public void MPP_Launcher2(String a1,String a2,String a3) throws FileNotFoundException, IOException, ParseException {
			
			
			Source_1=a1; 
			Source_2=a2;
			Source_1_Index="1,2,3,4,5";
			ICD_IND_Index=24;
			Src_claim_Index=1;
			
			Result_File=a3;
			Key1=Key_Value_Split(Source_1_Index);
			
			
			Source_Delimiter_Dia2();
			Source_Delimiter_Dia1();
			
					
			 

		}
	 
	 public void Source_Delimiter_Dia1() throws IOException, ParseException{
		 	
			
		  BufferedReader reader1 = new BufferedReader(new FileReader(Source_1));
	       String line1 = reader1.readLine();
	       line1 = reader1.readLine();
	       PrintWriter writer=new PrintWriter(Result_File); 
	     
	     while (line1 != null)
	     {
	   	  
	   	  
	   	  String[] val=line1.split("\\|");
	   	  
	   	  String one=val[0].trim();
	   	  String two=val[1].trim();
	   	  //String three=String.valueOf(Integer.parseInt(val[2].trim()+1));
	   	  
	   	  String ff=val[2].trim();
	   	  
	   	  int i=Integer.parseInt(ff)+1;
	   	  String three=String.valueOf(i);
	   	  String four=val[3].trim();
	   	  String five=val[4].trim();
	   	  String six=ICD_IND_list.get(two);
	   	  
	   	  String fin=one+" | "+two+" | "+three+" | "+four+" | "+five+" | "+six;

	   	  if(Src_1.containsKey(two+"_"+three)) {
	   		  
	   		Src_1.put(two+"_"+three,Src_1.get(two+"_"+three)+1);
	   		  
	   	  }else {
	   		  
	   		 Src_1.put(two+"_"+three,1);
	   	  }
	   	  
	   	 //String fin1=fin+" | "+Src_1.get(two+"_"+three);	
	   	  String fin1=fin;	
	   	  
	   	  
	   	 writer.println(fin1);
	   	  
	   	  
	   	  
	   	  line1 = reader1.readLine();
	         
	          
	     }
				
	     
	     reader1.close();
			 
	     writer.close();
			
			 System.out.println("Source1 completed");
		 }
	 
	 public  void Source_Delimiter_Dia2() throws IOException{
		 	
		   BufferedReader reader1 = new BufferedReader(new FileReader(Source_2));
	       String line1 = reader1.readLine();
	       line1 = reader1.readLine();
	      
	       
	     
	     while (line1 != null)
	     {
	   	 
	   	  
	   	  String[] k=line1.split("\\|");
	   	  
	   	  
	       if(ICD_IND_list.containsKey(k[Src_claim_Index].trim())) {
	    	   
	    	  
	       }
	       else {
	    	   
	    	   
		   
	    	   ICD_IND_list.put(k[Src_claim_Index].trim(), k[ICD_IND_Index].trim());
	    	   
	       }
	   
	   		
	   			
	   		
	   		
	   		line1 = reader1.readLine();
	   
	         
	          
	     }
				
	     
	     reader1.close();
	     
	   
	     
	    
			 
			
			
			 System.out.println("Source2 completed");
			
		 }
	 
		
	 
	
	 
		

		
	 
	 

}


