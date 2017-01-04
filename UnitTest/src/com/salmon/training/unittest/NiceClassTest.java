package com.salmon.training.unittest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest( { EvilParentClass.class })

public class NiceClassTest {

	@Test
	public void testNiceClass() throws Exception {

		//TODO - uncomment 
		//PowerMockito.suppress(
		//		PowerMockito.method(EvilParentClass.class, "doSomethingEvil"));

		
		NiceClass cutNiceClass = new NiceClass();
		
		cutNiceClass.doSomethingNice();

	}
}
