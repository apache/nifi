/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.registry.ui;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.NoAlertPresentException;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import io.github.bonigarcia.wdm.WebDriverManager;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ITDeleteSingleBucket {
    private WebDriver driver;
    private String baseUrl;
    private boolean acceptNextAlert = true;
    private WebDriverWait wait;
    private StringBuffer verificationErrors = new StringBuffer();

    @Before
    public void setUp() throws Exception {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        baseUrl = "http://localhost:18080/nifi-registry";
        wait = new WebDriverWait(driver, 30);
    }

    @Test
    public void testDeleteSingleBucket() throws Exception {
        // go directly to settings by URL
        driver.get(baseUrl + "/#/administration/workflow");

        // wait for administration route to load
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='no-buckets-message']")));

        // confirm new bucket button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='new-bucket-button']")));

        // select new bucket button
        WebElement newBucketButton = driver.findElement(By.cssSelector("[data-automation-id='new-bucket-button']"));
        newBucketButton.click();

        // wait for new bucket dialog
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // confirm bucket name field exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input")));

        // place cursor in bucket name field
        WebElement bucketNameInput = driver.findElement(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input"));
        bucketNameInput.clear();

        // name the bucket ABC
        bucketNameInput.sendKeys("ABC");

        // confirm create bucket button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='create-new-bucket-button']")));

        // select create bucket button
        WebElement createNewBucketButton = driver.findElement(By.cssSelector("[data-automation-id='create-new-bucket-button']"));
        createNewBucketButton.click();

        // wait for new bucket dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // verify bucket added
        List<WebElement> bucketCount = driver.findElements(By.cssSelector("#nifi-registry-workflow-administration-buckets-list-container > div"));
        assertEquals(1, bucketCount.size());

        // confirm trash icon button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.className("fa-trash")));

        // select trash icon for ABC bucket
        WebElement manageBucketButton = driver.findElement(By.className("fa-trash"));
        manageBucketButton.click();

        // wait for confirm dialog
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane")));

        // confirm delete button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane button.mat-fds-warn")));

        // select delete button
        WebElement deleteButton = driver.findElement(By.cssSelector("div.cdk-overlay-pane button.mat-fds-warn"));
        deleteButton.click();

        // wait for the confirm dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane")));

        // confirm no buckets exist
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='no-buckets-message']")));
    }

    @After
    public void tearDown() throws Exception {
        driver.quit();
        String verificationErrorString = verificationErrors.toString();
        if (!"".equals(verificationErrorString)) {
            fail(verificationErrorString);
        }
    }

    private boolean isElementPresent(By by) {
        try {
            driver.findElement(by);
            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    private boolean isAlertPresent() {
        try {
            driver.switchTo().alert();
            return true;
        } catch (NoAlertPresentException e) {
            return false;
        }
    }

    private String closeAlertAndGetItsText() {
        try {
            Alert alert = driver.switchTo().alert();
            String alertText = alert.getText();
            if (acceptNextAlert) {
                alert.accept();
            } else {
                alert.dismiss();
            }
            return alertText;
        } finally {
            acceptNextAlert = true;
        }
    }
}