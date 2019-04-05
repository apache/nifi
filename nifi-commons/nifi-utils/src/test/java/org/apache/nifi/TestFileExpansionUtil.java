package org.apache.nifi;

import org.apache.nifi.util.FileExpansionUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFileExpansionUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestFileExpansionUtil.class);

   @Test
   public void  testExpandPath() {
       logger.debug("User Home: " + System.getProperty("user.home"));

       // arrange
       System.setProperty("user.home", "/Users/testuser");

       // act
       String result = FileExpansionUtil.expandPath("~/somedirectory");

       // assert
       assertEquals("/Users/testuser/somedirectory", result);
   }

   @Test
    public void testExpandPathShouldReturnNullWhenNullIsInput() {
       logger.debug("User Home: " + System.getProperty("user.home"));

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

   @Test
    public void testExceptionIsThrownWhenUserHomeIsNull() {
       String expectedErrorMessage = "Nifi assumes user.home is set to your home directory.  Nifi detected user.home is " +
               "either null or empty and this means your environment can't determine a value for this information.  " +
               "You can get around this by specifying a -Duser.home=<your home directory> when running nifi.";

       System.getProperties().remove("user.home");

       //act
       try {
           FileExpansionUtil.expandPath("~/somedirectory");
           fail();
       } catch (RuntimeException runEx) {
           assertEquals(expectedErrorMessage, runEx.getMessage());
       }
   }

}
