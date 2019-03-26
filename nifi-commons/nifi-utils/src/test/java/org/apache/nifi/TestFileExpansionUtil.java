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
       System.setProperty("user.home", "/Users/testuser");

       // act
       String result = FileExpansionUtil.expandPath("~/somedirectory");

       // assert
       assertEquals("/Users/testuser/somedirectory", result);
   }

   @Test
    public void testExpandPathShouldReturnNullWhenNullIsInput() {
       System.out.println("User Home: " + System.getProperty("user.home"));

       // arrange
       System.setProperty("user.home", "/Users/testuser");

       // act
       String result = FileExpansionUtil.expandPath(null);

       // assert
       assertNull(result);
   }

   @Test(expected = RuntimeException.class)
    public void testExceptionIsThrownWhenUserHomeIsEmpty() {
       System.setProperty("user.home", "");

       // act
       FileExpansionUtil.expandPath("~/somedirectory");
   }

}
