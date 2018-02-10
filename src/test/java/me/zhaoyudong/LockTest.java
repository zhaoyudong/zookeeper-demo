package me.zhaoyudong;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class LockTest {


    @Test
    public void lockTest() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 1000, null);
        DistributedLock distributedLock = new DistributedLock(zooKeeper, "/app");
        DistributedLock distributedLock2 = new DistributedLock(zooKeeper, "/app");
        distributedLock.lock();
        Assert.assertFalse(distributedLock2.trylock(10, TimeUnit.MILLISECONDS));
        distributedLock2.unlock();
        distributedLock.unlock();
        zooKeeper.close();
    }

}
