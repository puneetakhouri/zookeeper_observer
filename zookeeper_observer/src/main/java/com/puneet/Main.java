package com.puneet;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;

public class Main implements Watcher {

    private static final String LEADER_ELECTION_ROOT_NODE = "/election";
    private static final String PROCESS_NODE_PREFIX = "/p_";
    private static volatile boolean keepRunning = true;

    private String rootPath;
    private ZooKeeper zooKeeper;
    private String observerPath;

    public static void main(String[] args) throws IOException {
        Main main = new Main();
        main.start();
        //Keep Running
        while(keepRunning);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("-----Received shutdown command");
                main.stop();
                keepRunning = false;
            }
        });
    }

    private void stop() {

    }

    private void start() throws IOException {
        zooKeeper = new ZooKeeper("localhost:2181", 2000, this);
        rootPath = createRoodNode();

        try {
            zooKeeper.addWatch(LEADER_ELECTION_ROOT_NODE, this, AddWatchMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //createObserverNode();

    }

//    private void createObserverNode() {
//        try {
//            observerPath = zooKeeper.create(rootPath + File.separator + "observer", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("---Received event in event watcher - " + event.getPath() + ", " + event.getType() + ", " + event.getState());
        if(event.getType() == Event.EventType.None) return;
        try {
            System.out.println(zooKeeper.getChildren(event.getPath(), false).size());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Event.EventType eventType = event.getType();
        if(Event.EventType.NodeDataChanged.equals(eventType)){
            byte[] data = new byte[0];
            try {
                data = zooKeeper.getData(event.getPath(), false, null);
                String str = new String(data);
                System.out.println("----Received -- " + str);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String createRoodNode() {
        String rootPath = LEADER_ELECTION_ROOT_NODE;
        try {
            if (zooKeeper.exists(LEADER_ELECTION_ROOT_NODE, false) == null) {
                System.out.println("-----root node path created");
                rootPath = zooKeeper.create(LEADER_ELECTION_ROOT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }else{
                System.out.println("-----root node path already exists");
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return rootPath;
    }
}
