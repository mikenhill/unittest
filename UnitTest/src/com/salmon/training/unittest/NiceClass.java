package com.salmon.training.unittest;

import java.io.FileNotFoundException;

public class NiceClass extends EvilParentClass {
	
	public void doSomethingNice() throws FileNotFoundException {
		
		super.doSomethingEvil();
		
		System.out.println ("NiceClass.doSomethingNice");
	}

}
