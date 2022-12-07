package com.puneet;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class NodeWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        System.out.println("---------Received event in NodeWatcher");
        System.out.println(event.getType());
    }
}
