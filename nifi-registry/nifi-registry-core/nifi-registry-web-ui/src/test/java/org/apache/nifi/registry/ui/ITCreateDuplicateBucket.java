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
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoAlertPresentException;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import io.github.bonigarcia.wdm.WebDriverManager;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ITCreateDuplicateBucket {
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
    public void testCreateDuplicateBucket() throws Exception {
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

        // wait for create bucket dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // verify bucket added
        List<WebElement> bucketCount = driver.findElements(By.cssSelector("#nifi-registry-workflow-administration-buckets-list-container > div"));
        assertEquals(1, bucketCount.size());

        // confirm new bucket button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='new-bucket-button']")));

        // select new bucket button
        newBucketButton = driver.findElement(By.cssSelector("[data-automation-id='new-bucket-button']"));
        newBucketButton.click();

        // wait for new bucket dialog
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // confirm bucket name field exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input")));

        // place cursor in bucket name field
        bucketNameInput = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input")));
        bucketNameInput.clear();

        // name the bucket ABC again
        bucketNameInput.sendKeys("ABC");

        // confirm create bucket button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='create-new-bucket-button']")));

        // select create bucket button
        createNewBucketButton = driver.findElement(By.cssSelector("[data-automation-id='create-new-bucket-button']"));
        createNewBucketButton.click();

        // wait for the new bucket dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // wait for error dialog
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane")));

        // confirm the duplicate bucket error
        WebElement selectOKButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane")));
        Actions actions = new Actions(driver);
        actions.moveToElement(selectOKButton).click().build().perform();

        // wait for the confirm dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("div.cdk-overlay-pane")));

        // verify bucket ABC still there
        bucketCount = driver.findElements(By.cssSelector("#nifi-registry-workflow-administration-buckets-list-container > div"));
        assertEquals(1, bucketCount.size());
    }

    @After
    public void tearDown() throws Exception {
        // bucket cleanup

        // confirm all buckets checkbox exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-workflow-administration-buckets-list-container-column-header div.mat-checkbox-inner-container")));

        // select all buckets checkbox
        WebElement selectAllCheckbox = driver.findElement(By.cssSelector("#nifi-registry-workflow-administration-buckets-list-container-column-header div.mat-checkbox-inner-container"));
        selectAllCheckbox.click();

        // confirm actions drop down menu exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-workflow-administration-perspective-buckets-container button.mat-fds-primary")));

        // select actions drop down
        WebElement selectActions = driver.findElement(By.cssSelector("#nifi-registry-workflow-administration-perspective-buckets-container button.mat-fds-primary"));
        selectActions.click();

        // select delete
        WebElement selectDeleteBucket = driver.findElement(By.cssSelector("div.mat-menu-content button.mat-menu-item"));
        JavascriptExecutor executor = (JavascriptExecutor)driver;
        executor.executeScript("arguments[0].click();", selectDeleteBucket);

        // verify bucket deleted
        WebElement confirmDelete = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("div.fds-dialog-actions button.mat-fds-warn")));
        confirmDelete.click();
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='no-buckets-message']")));

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