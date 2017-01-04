package com.ibm.commerce.copyright;

import static org.junit.Assert.*;

import org.junit.Test;

public class IBMCopyrightTest {

	@Test
	public void testGetLongCopyright() {
		assertEquals(IBMCopyright.LONG_COPYRIGHT, IBMCopyright.getLongCopyright());
	}

	@Test
	public void testGetShortCopyright() {
		assertEquals(IBMCopyright.SHORT_COPYRIGHT, IBMCopyright.getShortCopyright());
	}

}
