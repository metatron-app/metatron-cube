/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.http.client.pool;

import com.google.common.base.Throwables;
import io.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.netty.handler.timeout.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class ResourcePoolTest
{
  ResourceFactory<String, String> resourceFactory;
  ResourcePool<String, String> pool;

  @Before
  public void setUp() throws Exception
  {
    resourceFactory = createMock();

    EasyMock.replay(resourceFactory);
    pool = new ResourcePool<String, String>(
        resourceFactory,
        new ResourcePoolConfig(2, TimeUnit.SECONDS.toMillis(3), TimeUnit.MINUTES.toMillis(4))
    );

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testSanity() throws Exception
  {
    primePool();
    EasyMock.replay(resourceFactory);
  }

  private void primePool() throws InterruptedException
  {
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.generate("sally")).andAnswer(new StringIncrementingAnswer("sally")).times(2);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    ResourceContainer<String> sallyString = pool.take("sally");
    Assert.assertEquals("billy0", billyString.get());
    Assert.assertEquals("sally0", sallyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isValid("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isValid("sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    billyString.returnResource();
    sallyString.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  private static boolean isGood(ResourceFactory<?, String> factory, String value) throws InterruptedException
  {
    return factory.isGood(EasyMock.eq(value), EasyMock.anyLong());
  }

  @Test
  public void testFailedResource() throws Exception
  {
    primePool();

    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isValid("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy0", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testFaultyFailedResourceReplacement() throws Exception
  {
    primePool();

    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(false).times(1);
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(isGood(resourceFactory, "billy2")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isValid("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testTakeMoreThanAllowed() throws Exception
  {
    primePool();
    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
//    EasyMock.expect(resourceFactory.isValid("billy1")).andReturn(true).times(1);
//    EasyMock.expect(resourceFactory.isValid("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isValid("billy0")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    Assert.assertEquals("billy0", blockedThread.getValue());
  }

  @Test
  public void testCloseUnblocks() throws InterruptedException
  {
    primePool();
    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    resourceFactory.close("sally1");
    EasyMock.expectLastCall().times(1);
    resourceFactory.close("sally0");
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(resourceFactory);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    pool.close();


    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    blockedThread.join();
    // pool returns null after close
    Assert.assertNull(blockedThread.getValue());
  }

  @Test
  public void testTimedOutResource() throws Exception
  {
    resourceFactory = createMock();

    pool = new ResourcePool<String, String>(
        resourceFactory,
        new ResourcePoolConfig(2, TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS.toMillis(10))
    );

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isValid("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    billyString.returnResource();

    //make sure resources have been timed out.
    Thread.sleep(100);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    resourceFactory.close("billy0");
    EasyMock.expectLastCall().once();
    resourceFactory.close("billy1");
    EasyMock.expectLastCall().once();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1").times(1);
    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isValid("billy1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy1", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testWait() throws Exception
  {
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(isGood(resourceFactory, "billy0")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy0 = pool.take("billy");
    Assert.assertEquals("billy0", billy0.get());
    ResourceContainer<String> billy1 = pool.take("billy");
    Assert.assertEquals("billy1", billy1.get());

    Exception ex = null;
    try {
      pool.take("billy");
    }
    catch (Exception e) {
      ex = e;
    }
    Assert.assertTrue(ex instanceof TimeoutException);
    Assert.assertEquals("Timeout getting connection for 'billy' (3000 msec)", ex.getMessage());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isValid("billy1")).andReturn(true).times(1);
    EasyMock.expect(isGood(resourceFactory, "billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isValid("billy1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    MyThread billy1Thread = new MyThread(new CountDownLatch(0), "billy");
    billy1Thread.start();
    Assert.assertFalse(billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS));
    Assert.assertEquals(1, billy1Thread.gotValueLatch.getCount());

    billy1.returnResource();
    Assert.assertTrue(billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS));
    Assert.assertEquals(0, billy1Thread.gotValueLatch.getCount());
    Assert.assertEquals("billy1", billy1Thread.value);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @SuppressWarnings("unchecked")
  private ResourceFactory<String, String> createMock()
  {
    return (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);
  }

  private static class StringIncrementingAnswer implements IAnswer<String>
  {
    int count = 0;
    private final String string;

    public StringIncrementingAnswer(String string)
    {
      this.string = string;
    }

    @Override
    public String answer() throws Throwable
    {
      return string + count++;
    }
  }

  private class MyThread extends Thread
  {
    private final CountDownLatch gotValueLatch = new CountDownLatch(1);
    private final CountDownLatch retValueLatch;
    private final String resourceName;

    volatile String value;

    public MyThread(CountDownLatch retValueLatch, String resourceName)
    {
      this.retValueLatch = retValueLatch;
      this.resourceName = resourceName;
    }

    @Override
    public void run()
    {
      ResourceContainer<String> resourceContainer = take();
      value = resourceContainer.get();
      gotValueLatch.countDown();
      try {
        retValueLatch.await();
      }
      catch (InterruptedException e) {
      }
      resourceContainer.returnResource();
    }

    private ResourceContainer<String> take()
    {
      try {
        return pool.take(resourceName);
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    public String getValue()
    {
      return value;
    }

    public boolean waitForValueToBeGotten(long length, TimeUnit timeUnit) throws InterruptedException
    {
      return gotValueLatch.await(length, timeUnit);
    }
  }
}
