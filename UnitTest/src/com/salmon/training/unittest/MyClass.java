package com.salmon.training.unittest;

import com.ibm.commerce.command.ControllerCommandImpl;
import com.ibm.commerce.exception.ECException;

public class MyClass extends ControllerCommandImpl {

	public void performExecute() throws ECException {

		super.performExecute();
		
		System.out.println ("MyClass.performExecute");

	}
	
}
