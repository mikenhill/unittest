package com.salmon.commerce.lowstock.beans.unittest;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.salmon.commerce.commonregistry.utils.CommonRegistryHelper;
import com.salmon.commerce.lowstock.beans.LowStockDataBean;
import com.salmon.commerce.lowstock.dao.LowStockDao;
import com.salmon.commerce.lowstock.dto.LowStockDto;

@RunWith(PowerMockRunner.class)

@PrepareForTest(
        {        	
        	LowStockDao.class
        }
    )


public class LowStockDataBeanTest1 {

	
	
	
	@Before
    public void initialise() {       
		//Initialise here
    }

	@Test
	public void test_performExecute() throws Exception {
		
		LowStockDataBean cutLowStockDataBean = new LowStockDataBean();
		LowStockDao mockLowStockDao = PowerMockito.mock(LowStockDao.class);
		
		//Ensure that when we call the getLowStockDao() method, we return our mock version
		cutLowStockDataBean.setLowStockDao(mockLowStockDao);
		
		//Ensure that when our mockLowStockDao.getAllLowStockRecords is called, we return our stubbed
		//results, initially build the local list of List<LowStockDto>
		List<LowStockDto> listLowStockDtoList = new ArrayList<LowStockDto>();
		LowStockDto lowStockDto1 = new LowStockDto();
		LowStockDto lowStockDto2 = new LowStockDto();
		listLowStockDtoList.add(lowStockDto1);
		listLowStockDtoList.add(lowStockDto2);
		
		PowerMockito.when(mockLowStockDao.getAllLowStockRecords()).thenReturn(listLowStockDtoList);
		
		//Ensure we are running in non-debug mode
		Whitebox.setInternalState(cutLowStockDataBean, "debugMode" , false);
		Whitebox.setInternalState(cutLowStockDataBean, "listLowStockRecords" , new ArrayList<LowStockDto>());
		
		cutLowStockDataBean.populate();
		//Verify that we receive 2 DTOs back
		assertTrue(cutLowStockDataBean.getListLowStockRecords().size() == listLowStockDtoList.size());
		
		//Now perform the test for the debug mode
		Whitebox.setInternalState(cutLowStockDataBean, "debugMode" , true);
		Whitebox.setInternalState(cutLowStockDataBean, "listLowStockRecords" , new ArrayList<LowStockDto>());
		cutLowStockDataBean.populate();
		//Verify that we receive 1 DTO back
		assertTrue(cutLowStockDataBean.getListLowStockRecords().size() == 1);
		
	}
}
