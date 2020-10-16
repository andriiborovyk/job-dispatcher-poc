package com.totango;

import com.totango.dispatcher.*;
import com.totango.dispatcher.zk.ZooKeeperService;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Example dispatcher app
 */
@Log4j2
public class App {

    public static String DISPATCHER_CONFIG = "/dispatcher/config";

    public static void main(String[] args) throws Exception {
        log.info("Connecting to " + args[0]);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(args[0], new RetryForever(1000));
        curatorFramework.start();
        curatorFramework.blockUntilConnected();
        log.info("Connection established. Check is default main config exists.");


        curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).inBackground().forPath("/dispatcher/workers");

        List<JobDetails> jobDetailsList = new ArrayList<>();
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(3).setTopic("topic-a").build());
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(2).setTopic("topic-b").build());
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(1).setTopic("topic-c").build());
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(4).setTopic("topic-d").build());
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(5).setTopic("topic-e").build());
        jobDetailsList.add(JobDetails.newBuilder().setConsumers(6).setTopic("topic-f").build());
        ConfigDetails configDetails = ConfigDetails.newBuilder()
                .setJobs(jobDetailsList)
                .build();

        CuratorOp create = curatorFramework.transactionOp().create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(DISPATCHER_CONFIG);
        CuratorOp set = curatorFramework.transactionOp().setData().forPath(DISPATCHER_CONFIG, configDetails.toByteBuffer().array());
        curatorFramework.transaction().inBackground().forOperations(create, set);

        Worker worker = new DummyWorker();
        ZooKeeperService zooKeeperService = new ZooKeeperService(curatorFramework, "/dispatcher/workers", "/dispatcher/workers/worker-", DISPATCHER_CONFIG);
        DispatcherNode dispatcherNode = new DispatcherNode(zooKeeperService, worker);
        log.info("Starting dispatcher node.");
        dispatcherNode.start();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.info("Stopping dispatcher node.");
                dispatcherNode.stop();
                curatorFramework.close();
                break;
            }
        }
    }
}
