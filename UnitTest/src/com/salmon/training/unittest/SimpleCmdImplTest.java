package com.salmon.training.unittest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ibm.commerce.command.AbstractECTargetableCommand;
import com.ibm.commerce.exception.ECException;

@RunWith(PowerMockRunner.class)

@PrepareForTest(
        {
        	AbstractECTargetableCommand.class
        }
    )
    
    
public class SimpleCmdImplTest {

    private final SimpleCmdImpl cutSimpleCmdImpl = new SimpleCmdImpl();
    
    @Test
    public void test_performExecute() throws ECException {

    	//TODO - uncomment
    	//PowerMockito.suppress( PowerMockito.method(AbstractECTargetableCommand.class, 
    	//		"performExecute" ));
                
        ///////////////////////////////////////////////
        // run the target method
        cutSimpleCmdImpl.performExecute(); 
    }        
}

