package com.mmu;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;


public class ReportGenerator 
{
	Logger log = Logger.getLogger(this.getClass());
	private double dataList[] ={0,0,0,0,0,0,0,0,0,0};
	
	public void generateHistogram(String input) throws IOException
	{

		 File file = new File(Master.localCopiedPathBin);
		 FileInputStream fin = new FileInputStream(file);
         BufferedReader br = new BufferedReader( new InputStreamReader(fin));
         
         String line;
         int tempKey = 0;
         do 
         {
            line=br.readLine();
            String token[] = Master.splitMessage(line, "\t");
            
            if(null!=token[0])
            {
            	tempKey = Integer.parseInt(token[0]);            	         
            	dataList[(tempKey-1)]= Double.parseDouble(token[1]);
            }
         }while(line != null);
         
         printGraph(input); 
         
         br.close();
		
	}

	
	private void printGraph( String histogramData) 
	{
		 DefaultCategoryDataset bardataset = new DefaultCategoryDataset(); 
		 
		 for( int i=0; i<dataList.length;i++)
		 {
			 log.info("This is the datalist" + dataList[i] + " i "+ i);
			 bardataset.setValue(dataList[i], "occurances", String.valueOf((i+1)));
		 }
		 
		 JFreeChart barchart = ChartFactory.createBarChart(  
		         "Product Rating Occurances",      //Title  
		         "Bin",             // X-axis Label  
		         "Occurances",               // Y-axis Label  
		         bardataset,             // Dataset  
		         PlotOrientation.VERTICAL,      //Plot orientation  
		         false,                // Show legend  
		         true,                // Use tooltips  
		         false                // Generate URLs  
		      );  
		 
		 barchart.getTitle().setPaint(Color.BLUE);    // Set the colour of the title  
	     barchart.setBackgroundPaint(Color.WHITE);    // Set the background colour of the chart  
	     CategoryPlot cp = barchart.getCategoryPlot();  // Get the Plot object for a bar graph  
	     cp.setBackgroundPaint(Color.WHITE);       // Set the plot background colour  
	     cp.setRangeGridlinePaint(Color.BLACK);      // Set the colour of the plot gridlines  
	     
	     
	     try 
         {
        	 ChartUtilities.saveChartAsJPEG(new File(Master.histoPath), barchart, 1000, 1000);
         }
         
         catch (IOException e) 
         {
        	 log.error(e);
         }
		
	}
	

}
