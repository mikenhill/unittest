package com.salmon.training.unittest;

import java.util.logging.Logger;

import com.ibm.commerce.command.ControllerCommandImpl;
import com.ibm.commerce.exception.ECException;

public class SimpleCmdImpl extends ControllerCommandImpl  {
    
	private static final long serialVersionUID = 1L;
	private static final String CLASSNAME = SimpleCmdImpl.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASSNAME);


    @Override
    public void performExecute() throws ECException {
        String methodName = "performExecute";
        LOGGER.entering(CLASSNAME, methodName);

        
        super.performExecute();
        
        LOGGER.exiting(CLASSNAME, methodName);        
    }
    
}

