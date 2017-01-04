package com.salmon.training.unittest;

import static org.powermock.api.support.membermodification.MemberModifier.suppress;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ibm.commerce.command.AbstractECTargetableCommand;

@RunWith(PowerMockRunner.class)
@PrepareForTest( { System.class })

public class MyClassTest {

	@Test
	public void testX() throws Exception {

		PowerMockito.mockStatic(System.class);
		
		MyClass cutMyClass = new MyClass();
		
		cutMyClass.performExecute();
		
		PowerMockito.verifyStatic();
		System.out.println(Matchers.anyString());

	}
}
