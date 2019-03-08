package org.apache.nifi;

import org.apache.nifi.util.FileExpansionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFileExpansionUtil {
    private static final Logger logger = LoggerFactory.getLogger(TestFileExpansionUtil.class);
    private String originalHome = "";

    @Rule public ExpectedException exception = ExpectedException.none();

    @Before
    public void beforeEach() {
        originalHome = System.getProperty("user.home");
        System.setProperty("user.home", "/Users/testuser");
    }

    @After
    public void afterEach() {
        System.setProperty("user.home", originalHome);
    }

   @Test
   public void  testExpandPath() {
       logger.debug("User Home: " + System.getProperty("user.home"));
       // arrange
       System.getProperties().setProperty("user.home", "/Users/testuser");

       // act
       String result = FileExpansionUtil.expandPath("~/somedirectory");

       // assert
       assertEquals("/Users/testuser/somedirectory", result);
   }

   @Test
    public void testExpandPathShouldReturnNullWhenNullIsInput() {
       logger.debug("User Home: " + System.getProperty("user.home"));

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

       exception.expect(RuntimeException.class);
       exception.expectMessage(equalTo(expectedErrorMessage));

       System.getProperties().remove("user.home");

       FileExpansionUtil.expandPath("~/somedirectory");
   }

}
