package com.totango.dispatcher.zk;

import com.totango.dispatcher.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;


@RequiredArgsConstructor
@Log4j2
public class ZooKeeperService {

    private final CuratorFramework client;

    private final String electionRootNode;

    private final String electionPrefixNode;

    private String myElectionNode;

    private final String mainConfigNode;

    public void init() throws Exception {
        client.blockUntilConnected(1, TimeUnit.MINUTES);
        myElectionNode = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(electionPrefixNode, new byte[0]);
    }

    public void addUpdateChildrenListener(NodeListUpdateListener updateListener, boolean callWithInitial) throws Exception {
        List<String> children = client.getChildren().usingWatcher((CuratorWatcher) event -> {
            log.info(() -> String.format("Received event %s on %s", event.getType(), event.getPath()));
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                addUpdateChildrenListener(updateListener, true);
            } else {
                addUpdateChildrenListener(updateListener, false);
            }
        }).forPath(electionRootNode);
        if (callWithInitial) {
            updateListener.onNodeListUpdated(children.stream().map(path -> electionRootNode + "/" + path).collect(Collectors.toList()));
        }
    }

    public void addNodeUpdateListener(NodeContentUpdateListener contentUpdateListener, boolean callWithInitial) throws Exception {
        byte[] data = client.getData().usingWatcher((CuratorWatcher) event-> {
            log.info(() -> String.format("Received event %s on %s", event.getType(), event.getPath()));
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                addNodeUpdateListener(contentUpdateListener, true);
            } else {
                addNodeUpdateListener(contentUpdateListener, false);
            }
        }).forPath(myElectionNode);
        if (callWithInitial) {
            try {
                contentUpdateListener.onNodeContentUpdated(ConfigDetails.fromByteBuffer(ByteBuffer.wrap(data)));
            } catch (Exception e) {
                log.error(e.toString() + " [" +  Arrays.toString(data) + "]");
            }
        }
    }

    public void addMainConfigUpdateListener(NodeContentUpdateListener contentUpdateListener, boolean callWithInitial) throws Exception {
        byte[] data = client.getData().usingWatcher((CuratorWatcher) event-> {
            log.info(() -> String.format("Received event %s on %s", event.getType(), event.getPath()));
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                addMainConfigUpdateListener(contentUpdateListener, true);
            } else {
                addMainConfigUpdateListener(contentUpdateListener, false);
            }
        }).forPath(mainConfigNode);
        if (callWithInitial) {
            try {
                contentUpdateListener.onNodeContentUpdated(ConfigDetails.fromByteBuffer(ByteBuffer.wrap(data)));
            } catch (Exception e) {
                log.error(e.toString() + " [" +  Arrays.toString(data) + "]");
            }
        }
    }

    public void updateNodeConfig(String node, ConfigDetails configDetails) throws Exception {
        client.setData().inBackground().forPath(node, configDetails.toByteBuffer().array());
    }

    public boolean electLeader(LeaderElectedListener listener) throws Exception {
        log.info(() -> "Starting leader election process...");
        boolean isLeader = false;
        List<String> children = client.getChildren().usingWatcher((CuratorWatcher) event -> {
            log.info(() -> String.format("Received event %s on %s", event.getType(), event.getPath()));
            ZooKeeperService.this.electLeader(listener);
        }).forPath(electionRootNode);

        Collections.sort(children);
        String leader = children.get(0);
        if (myElectionNode.equals(electionRootNode + "/" + leader)) {
            isLeader = true;
            log.info(() -> "I am a new leader. I am: " + myElectionNode.substring(electionRootNode.length() + 1));
            listener.onElectedAsLeader();
        } else {
            isLeader = false;
            log.info(() -> "I am not a leader.  Leader is " + leader + " and I am: " + myElectionNode.substring(electionRootNode.length() + 1));
        }
        log.info(() -> "Leader election finished...");
        return isLeader;
    }


}
