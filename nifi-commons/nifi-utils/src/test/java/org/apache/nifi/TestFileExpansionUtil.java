package org.apache.nifi;

import org.apache.nifi.util.FileExpansionUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFileExpansionUtil {

   @Test
   public void  testExpandPath() {
       System.out.println("User Home: " + System.getProperty("user.home"));

       // arrange
       System.getProperties().setProperty("user.home", "/Users/testuser");

       // act
       String result = FileExpansionUtil.expandPath("~/somedirectory");

       // assert
       assertEquals("/Users/testuser/somedirectory", result);
   }

   @Test
    public void testExpandPathShouldReturnNullWhenNullIsInput() {
       // act
       String result = FileExpansionUtil.expandPath(null);

       // assert
       assertNull(result);
   }

}
