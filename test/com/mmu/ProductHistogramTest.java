package com.mmu;

import static org.junit.Assert.*;

import org.hamcrest.core.IsEqual;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import com.mmu.ProductHistogram.TokenizerMapperHisto;

public class ProductHistogramTest 
{

	@Test
	public void testReturnBinIdLevel8() 
	{
		String count="3.5";
		int returnId = new TokenizerMapperHisto().returnBinId(count);
		assertEquals(returnId,8);
	}
	
	@Test
	public void testReturnBinIdLevelUpperLimit8() 
	{
		String count="3.99";
		int returnId = new TokenizerMapperHisto().returnBinId(count);
		assertEquals(returnId,8);
	}
	
	@Test
	public void testReturnBinId10() 
	{
		String count="4.51";
		int returnId = new TokenizerMapperHisto().returnBinId(count);
		assertEquals(returnId,10);
	}
	
	@Test
	public void testSplitString()
	{
		String testString="1	3.0";
		String token[]=Master.splitMessage(testString, "\t");
		
		assertThat(token.length,is(4));
	}

}
