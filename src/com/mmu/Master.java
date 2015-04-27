package com.mmu;

import org.apache.commons.lang.StringUtils;

public class Master 
{
	
	public static String localCopiedPathBin;
	public static String localCopiedPathAverage;
	public static String histoPath;
	public final static String fileSequence="/part-r-00000";
	
	public static void main(String args[]) throws Exception
	{
		if( args.length < 4)
		{
			System.out.println("Please run the following command: hadoop jar Assigment2.jar <localDirPath> <hfsinputpath> <hfsoutputpathRating> <hfsoutputpathHistogram>");
			System.exit(0);
		}
		
		localCopiedPathAverage=args[0]+"/average-rating.txt";
		localCopiedPathBin = args[0]+"/bin-data.txt";
		histoPath= args[0]+"/histogram.jpeg";
		
		 new ProductRating().productRatingAvarage(args[1], args[2]);
		 new ProductHistogram().productHistogram(args[2], args[3]);
		 new ReportGenerator().generateHistogram(args[3]);
	}
	
	public static String[] splitMessage(String text, String delimiter)
	{
		if( StringUtils.isNotEmpty(text) )
		{
			return text.split(delimiter);
		}
		
		else 
			return new String[1];
		
	}
	
	public static String formatMessage( String delimiter,String ... tokens)
	{
		StringBuilder temp = new StringBuilder();

		temp.append(tokens[0])
		.append(delimiter)
		.append(tokens[1]);
		
		return temp.toString();
	}

}
