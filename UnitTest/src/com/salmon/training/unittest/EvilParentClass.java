package com.salmon.training.unittest;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class EvilParentClass {

	protected void doSomethingEvil() throws FileNotFoundException {
		FileReader evilFileReader = new FileReader("evil file spec");
	}
	
}
