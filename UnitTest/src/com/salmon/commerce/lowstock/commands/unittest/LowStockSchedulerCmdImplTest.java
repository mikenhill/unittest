package com.salmon.commerce.lowstock.commands.unittest;

import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.salmon.commerce.commonregistry.utils.CommonRegistryHelper;
import com.salmon.commerce.lowstock.commands.LowStockSchedulerCmdImpl;
import com.salmon.commerce.lowstock.dao.LowStockDao;
import com.salmon.commerce.lowstock.dto.LowAtpInventoryDto;
import com.salmon.common.utils.SchedulerHelper;

@RunWith(PowerMockRunner.class)

@PrepareForTest(
        {        	
        	SchedulerHelper.class,
        	CommonRegistryHelper.class,
        	LowStockDao.class
        }
    )
public class LowStockSchedulerCmdImplTest {

	LowStockSchedulerCmdImpl cutLowStockSchedulerCmdImpl = null;
	LowStockDao mockLowStockDao = null;
	Integer commandStoreId = 10001;
	
	@Before
	public void setup() {
		cutLowStockSchedulerCmdImpl = new LowStockSchedulerCmdImpl();
		cutLowStockSchedulerCmdImpl.setCommandStoreId(commandStoreId);
		mockLowStockDao = PowerMockito.mock(LowStockDao.class);
	}
	
	
}
