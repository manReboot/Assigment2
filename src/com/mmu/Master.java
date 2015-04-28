/**
 * @author Kalaiselvam ( 113260015)
 * @version 1.0
 * This class with acting as master class which will be calling the other
 *
 *
 */
package com.mmu;

import org.apache.commons.lang.StringUtils;

public class Master 
{
	
	public static String localCopiedPathBin;
	public static String localCopiedPathAverage;
	public static String histoPath;
	public final static String fileSequence="/part-r-00000";
	
	/**
	 * This method will be used to master, which will be calling other class.
	 * 
	 * @param args array of String which accepted the 4 parameter
	 * i) localfilepath - where you want to copied the result from HDS 
	 * ii) inputfilepathInHDS - this location of the input file in the HDS
	 * iii) hfsoutputpathRating - this location of the output file in the HDS for the average-rating
	 * iv) hfsoutputpathHistogram - this location of the of the output of the HDS for the bin data
	 * @throws Exception
	 */
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
	/**
	 * This method utility method which used to split the message
	 * @param text which need to be splited
	 * @param delimiter which need to be used to split it
	 * @return array of string 
	 */
	public static String[] splitMessage(String text, String delimiter)
	{
		if( StringUtils.isNotEmpty(text) )
		{
			return text.split(delimiter);
		}
		
		else 
			return new String[1];
		
	}
	
	/**
	 * This method is use to concat multiple String together using a delimiter
	 * @param delimiter which use to pipe the string 
	 * @param tokens multiple string to be concat
	 * @return the concat string
	 */
	public static String formatMessage( String delimiter,String ... tokens)
	{
		StringBuilder temp = new StringBuilder();

		temp.append(tokens[0])
		.append(delimiter)
		.append(tokens[1]);
		
		return temp.toString();
	}

}
