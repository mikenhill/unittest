package com.salmon.training.unittest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.salmon.commerce.lowstock.dao.LowStockDao;
import com.salmon.commerce.lowstock.dto.LowStockDto;

@RunWith(PowerMockRunner.class)

@PrepareForTest(
        {        	        	
        	LowStockDao.class,
        	AdvancedCmdImpl.class
        }
    )
public class MyAdvancedTest {

	//Simple test - wrong result caused by the fact that we are in default mode but have not set any behaviour for the
	//method being called upon the mock.
	@Test
	public void test_getDefaultLowStockThreshold_withMock() {
		
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		//lowStockDefaultThreshold will not be 100 because we are in 'default' mode and the real "getDefaultLowStockThreshold" is not 
		//called hence we get null/0
		assertFalse(lowStockDefaultThreshold == 100);
		
		assertTrue(lowStockDefaultThreshold == 0);
	}
	
	
	//Simple test - right result. Now we get the right result as we are in default mode but have specified
	//we call the methods for real.
	@Test
	public void test_getDefaultLowStockThreshold_withDoCallReal() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		PowerMockito.doCallRealMethod().when(mockAdvancedCmdImpl).getDefaultLowStockThreshold();
		
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 100);
	}
	
	//Simple test - right result. This time we instantiate the mock in CALLS_REAL_METHODS mode so there is no need
	//to connect the method separately.
	@Test
	public void test_getDefaultLowStockThreshold_withCallsRealMethods() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in CALLS_REAL_METHODS (all methods connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class,
				Mockito.CALLS_REAL_METHODS);
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 100);
	}
	
	
	//Simple test - forced results via Whitebox. Here set both a static and non-static class
	//level variable and then call the real methods in order to pick up the value that has been 
	//'forced' by Whitebox
	@Test
	public void test_getDefaultLowStockThreshold_whitebox() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();

		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);	
		//Set the mock upon the CUT
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		//Connect the 2 methods we will be accessing
		PowerMockito.doCallRealMethod().when(mockAdvancedCmdImpl).getDefaultLowStockThreshold();		
		PowerMockito.doCallRealMethod().when(mockAdvancedCmdImpl).getIntToBeSet();
		
		//STATIC - Force the result to be 99
		Whitebox.setInternalState(AdvancedCmdImpl.class, "DEFAULT_LOW_STOCK_THRESHOLD" , 99);
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 99);
		
		//Non Static - Force the result to be 22
		Whitebox.setInternalState(mockAdvancedCmdImpl, "intToBeSet" , 22);
		int result = cutAdvanced.getTheAdvancedCmdImpl().getIntToBeSet();
		assertTrue(result == 22);
		
	}
	
	//Simple test - mocked results. Here we specify the behaviour to return a specific value whenever the
	//getDefaultLowStockThreshold is called.
	@Test
	public void test_getDefaultLowStockThreshold_mocked() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		//Set the mock upon the CUT
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		//Set up the behaviour of the mock
		PowerMockito.when(mockAdvancedCmdImpl.getDefaultLowStockThreshold()).thenReturn(98);
		
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 98);
	}	
	
	//Simple test - mocked results, followed by mocked result. As above but now we have a different value for the
	//first and second calls to : getDefaultLowStockThreshold
	@Test
	public void test_getDefaultLowStockThreshold_mocked_twice() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		//Set the mock upon the CUT
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		//Set up two behaviours, one followed by the other.
		PowerMockito.when(mockAdvancedCmdImpl.getDefaultLowStockThreshold())
			.thenReturn(98).thenReturn(97);
		
		int lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 98);
		
		lowStockDefaultThreshold = cutAdvanced.getTheAdvancedCmdImpl().getDefaultLowStockThreshold();
		assertTrue(lowStockDefaultThreshold == 97);		
		
	}		
	
	//Simple test - logic based result. This example shows how we have logic in our mocked methods and
	//return results based on the input parameter.
	@Test
	public void test_getSquared() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		PowerMockito.when(mockAdvancedCmdImpl.getSquare(Matchers.anyFloat())).
		thenAnswer(new Answer<Float>() {               
		                public Float answer (final InvocationOnMock invocation) throws Throwable {
		                	Float input = (Float) invocation.getArguments()[0];
		                	if (input.compareTo(12.5F) == 0) {
		                		return new Float (144);
		                	} else {
		                		return new Float (input * input);
		                	}
		                }
		            });
		
		
		Float result = cutAdvanced.getTheAdvancedCmdImpl().getSquare(new Float(10));
		assertTrue(result.compareTo(new Float(100)) == 0);
		
		result = cutAdvanced.getTheAdvancedCmdImpl().getSquare(new Float(12.5));
		assertTrue(result.compareTo(new Float(144.0)) == 0);

//		//Note that the above could have been done using the following notation
//		PowerMockito.when(mockAdvancedCmdImpl.getSquare(12.5F)).thenReturn(new Float (144));
//		PowerMockito.when(mockAdvancedCmdImpl.getSquare( Matchers.eq(12.5F) )).thenReturn( new Float (144) );
//		
//		result = cutAdvanced.getTheAdvancedCmdImpl().getSquare(new Float(10));
//		assertTrue(result.compareTo(new Float(100)) == 0);
//		
//		result = cutAdvanced.getTheAdvancedCmdImpl().getSquare(new Float(12.5));
//		assertTrue(result.compareTo(new Float(144.0)) == 0);
		
	}		
	
	
	//Test of a static method. Note that this is significantly different to non-static mocking.
	@Test
	public void test_getCubed() {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		
		//Mock the class which contains the static method using mockStatic
		PowerMockito.mockStatic(AdvancedCmdImpl.class); 
		
		PowerMockito.when(AdvancedCmdImpl.getCube(Matchers.anyFloat())).thenReturn(new Float (43));
		
		//Finally assert that the value was correct.
		Float result = cutAdvanced.getTheAdvancedCmdImpl().getCube(new Float(12.5));
		assertTrue(result.compareTo(new Float(43)) == 0);		

		//The following shows how we verify that the static method was called. 
		//Note that we do not check the return value, just the invocation of the method.
		PowerMockito.verifyStatic();
		AdvancedCmdImpl.getCube(new Float(12.5));
		
		
	}
	
	//Test of a private method
	@Test
	public void test_getHalf() throws Exception {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		//Set the mock upon the CUT
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		//The main point here is that we cannot use the dot notation (myClassInstance.myMethod) as the
		//method is private, so we must 'identify' the method by a String
		PowerMockito.when(mockAdvancedCmdImpl ,
				//This lines identifies the method and signature
				PowerMockito.method(AdvancedCmdImpl.class, "getHalf" , Float.class)).
				//Having identified the method, we specify what type of call we are matching
				withArguments(Matchers.anyFloat()).thenCallRealMethod();
				            
		//Invoke with
		Float result = Whitebox.<Float> invokeMethod(mockAdvancedCmdImpl , "getHalf" , 10F);

		assertTrue(result.compareTo(new Float(5)) == 0);
		
	}
	
	//Test of a void method
	@Test
	public void test_void() throws Exception {
		
		MyAdvanced cutAdvanced = new MyAdvanced();
		
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		PowerMockito.doCallRealMethod().when(mockAdvancedCmdImpl).setIntToBeSet(Matchers.anyInt());
		
		cutAdvanced.getTheAdvancedCmdImpl().setIntToBeSet(22);
		
		int result = Whitebox.getInternalState(mockAdvancedCmdImpl, "intToBeSet" );
		
		assertTrue(result  == 22);
		
	}	
	
	//Intercept the instantiation of a new Object with our Mocked object
	@Test
	public void test_interceptNew() throws Exception {
		//Instantiate the CUT
		MyAdvanced cutAdvanced = new MyAdvanced();
		//Create the mock in default (all methods not connected) mode.
		AdvancedCmdImpl mockAdvancedCmdImpl = PowerMockito.mock(AdvancedCmdImpl.class);
		PowerMockito.doCallRealMethod().when(mockAdvancedCmdImpl).getLowStockDao();
		
		//Set the mock upon the CUT
		cutAdvanced.setTheAdvancedCmdImpl(mockAdvancedCmdImpl);
		
		//Declare a mock for LowStockDao
		LowStockDao mockLowStockDao = PowerMockito.mock(LowStockDao.class);
		
		//Listen out for the instantiation of a new LowStockDao and when heard, inject the mockLowStockDao
		PowerMockito.whenNew(LowStockDao.class).withNoArguments().thenReturn(mockLowStockDao);
		
		//Generate some test data for the DAO
		List<LowStockDto> listLowStockDto = new ArrayList<LowStockDto>();
		LowStockDto lowStockDto1 = new LowStockDto();
		lowStockDto1.setCatentryId(10101L);
		lowStockDto1.setStockPosition(22L);
		listLowStockDto.add(lowStockDto1);
		
		PowerMockito.when(mockLowStockDao.getAllLowStockRecords()).thenReturn(listLowStockDto);

		List<LowStockDto> listLowStockDto2 = cutAdvanced.getTheAdvancedCmdImpl().getLowStockDao().getAllLowStockRecords();
		
		assertTrue(listLowStockDto2.size() == 1);
		
		
	}

	
	
	
}
