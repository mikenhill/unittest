package com.salmon.commerce.lowstock.beans.unittest;

import static org.junit.Assert.assertTrue ;
import static org.mockito.Mockito.mock ;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
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

import com.ibm.commerce.exception.ECException;
import com.ibm.disthub2.impl.util.AssertFailureError;
import com.salmon.commerce.commonregistry.utils.CommonRegistryHelper;
import com.salmon.commerce.lowstock.beans.LowStockDataBean;

import com.salmon.commerce.lowstock.dao.LowStockDao;
import com.salmon.commerce.lowstock.dto.LowStockDto;
import com.salmon.common.SalmonConstants;

@RunWith(PowerMockRunner.class)

@PrepareForTest(
        {
        	LowStockDao.class
        }
    )

public class LowStockDataBeanTest2 {

	@Test
	public void test_performExecute() throws Exception {
		
		LowStockDataBean cutLowStockDataBean = new LowStockDataBean();
		LowStockDao mockLowStockDao = PowerMockito.mock(LowStockDao.class, Mockito.CALLS_REAL_METHODS);
		
		//Ensure that when we call the getLowStockDao() method, we return our mock version
		cutLowStockDataBean.setLowStockDao(mockLowStockDao);
		
		//Ensure that when our mockLowStockDao.getAllLowStockRecords is called, we return our stubbed
		//results, initially build the local list of List<LowStockDto>
		List<LowStockDto> listLowStockDtoList = new ArrayList<LowStockDto>();
		LowStockDto lowStockDto1 = new LowStockDto();
		LowStockDto lowStockDto2 = new LowStockDto();
		listLowStockDtoList.add(lowStockDto1);
		listLowStockDtoList.add(lowStockDto2);
		
		//Cannot use this as it will actually call getAllLowStockRecords 
		PowerMockito.when(mockLowStockDao.getAllLowStockRecords()).thenReturn(listLowStockDtoList);
		
		//Must now change to:
		//PowerMockito.doReturn(listLowStockDtoList).when(mockLowStockDao).getAllLowStockRecords();
		
		
		//Ensure we are running in non-debug mode
		Whitebox.setInternalState(cutLowStockDataBean, "debugMode" , false);
		Whitebox.setInternalState(cutLowStockDataBean, "listLowStockRecords" , new ArrayList<LowStockDto>());
		
		cutLowStockDataBean.populate();
		
		//Verify that we receive 2 DTOs back
		assertTrue(cutLowStockDataBean.getListLowStockRecords().size() == listLowStockDtoList.size());
		
	}	
	
}
