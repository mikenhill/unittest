package com.salmon.dataload.iface.unittest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.salmon.dataload.helper.MonitiseEtlHelper;
import com.salmon.dataload.helper.PromotionEtlHelper;
import com.salmon.dataload.iface.CreatePromotionXMLFile;
import com.salmon.dataload.iface.DataLoadConstants;
import com.salmon.dataload.utils.UnicodeBOMInputStream;


@RunWith(PowerMockRunner.class)
@PrepareForTest({CreatePromotionXMLFile.class, File.class, FileOutputStream.class, System.class})

public class CreatePromotionXMLFileTest {
    
    @Test
    public void test_CreatePromotionXMLFile() throws Exception {

        String[] promoXml = {"XML"};
        
        PromotionEtlHelper mockPromotionEtlHelper = PowerMockito.mock(PromotionEtlHelper.class);
        PowerMockito.whenNew(PromotionEtlHelper.class).withArguments("JDBC_DRIVER", "DB_URL", "DB_USERNAME", "DB_PASSWORD").thenReturn(mockPromotionEtlHelper);
        PowerMockito.doNothing().when(mockPromotionEtlHelper, "commit");   
        PowerMockito.doNothing().when(mockPromotionEtlHelper, "close");   
        PowerMockito.doReturn(promoXml).when(mockPromotionEtlHelper, "createPromotionXML");         

        File mockFile = PowerMockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments("SourceDirectory"+"/promotion.xml").thenReturn(mockFile);
        
        FileOutputStream mockFileOutputStream = PowerMockito.mock(FileOutputStream.class);
        PowerMockito.whenNew(FileOutputStream.class).withArguments(mockFile).thenReturn(mockFileOutputStream);
        
        CreatePromotionXMLFile cut = new CreatePromotionXMLFile();   
        cut.setPromotionEtlHelper(mockPromotionEtlHelper);
        cut.run("SourceDirectory",
                "JDBC_DRIVER", 
                "DB_URL", 
                "DB_USERNAME", 
                "DB_PASSWORD");    
        
        Mockito.verify(mockFile, Mockito.times(1)).createNewFile();
        Mockito.verify(mockFileOutputStream, Mockito.times(1)).write(promoXml[0].getBytes());
        Mockito.verify(mockFileOutputStream, Mockito.times(1)).flush();
        Mockito.verify(mockFileOutputStream, Mockito.times(1)).close();
        
        Mockito.verify(mockPromotionEtlHelper, Mockito.times(1)).commit();
        Mockito.verify(mockPromotionEtlHelper, Mockito.times(1)).close();
        
    }
    
    @Test
    public void test_TestBadFileOpen() throws Exception {

        PowerMockito.mockStatic(System.class);
        
        PromotionEtlHelper mockPromotionEtlHelper = PowerMockito.mock(PromotionEtlHelper.class);
        PowerMockito.whenNew(PromotionEtlHelper.class).withArguments("JDBC_DRIVER", "DB_URL", "DB_USERNAME", "DB_PASSWORD").thenReturn(mockPromotionEtlHelper);
        CreatePromotionXMLFile cut = new CreatePromotionXMLFile();   
        cut.setPromotionEtlHelper(mockPromotionEtlHelper);
        cut.run("SourceDirectory",
                "JDBC_DRIVER", 
                "DB_URL", 
                "DB_USERNAME", 
                "DB_PASSWORD");        
        
        PowerMockito.verifyStatic();
        System.exit(DataLoadConstants.ERROR_WRITING_TARGET_FILE);
    }
    
    
   

}