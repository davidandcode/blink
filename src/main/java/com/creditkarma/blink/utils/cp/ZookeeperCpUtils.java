package com.creditkarma.blink.utils.cp;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shengwei.wang on 11/19/16.
 *
 * Get zookeeper ensemble up and running
 *
 * Use the corrrect port number or just the hostname only
 */
public class ZookeeperCpUtils {


    public static void create(String path, byte[] mydata,String hostport) throws
            KeeperException,InterruptedException,IOException {

        byte[] data = mydata;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);

            // try to create existing node will give you the exception
            // may need to change the CreateMode type to persistent + sequential for the same topic and partition
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        } catch (Exception e){
            throw e;
        }finally {
            zk.close();
            conn.close();
        }
    }


    public static void delete(String path,String hostport) throws KeeperException,InterruptedException,IOException {

        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {
            // try to delete a node which doesn't exist will give you an exception
            zk = conn.connect(hostport);
            zk.delete(path,zk.exists(path,true).getVersion());

        } catch (Exception e){
            throw e;
        }
        finally {

            zk.close();
            conn.close();
        }
    }


    public static boolean znodeExists(String path,String hostport) throws
            KeeperException,InterruptedException,IOException {

        boolean result = false;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {
            zk = conn.connect(hostport);
            result= zk.exists(path, true) == null?false:true;

        } catch (Exception e){
            throw e;
        }
        finally {
            zk.close();
            conn.close();
        }
        return result;
    }

    public static List<String> getChildren(String path,String hostport) throws
            KeeperException,InterruptedException,IOException {

        List<String> result = new ArrayList<String>();

        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            if(znodeExists(path,hostport))
                result = zk.getChildren(path,false);

        } catch (Exception e){
            throw e;
        }finally {
            zk.close();
            conn.close();
        }
        return result;

    }




    public static byte[] getDataBytes(String path,String hostport) throws Exception {
        byte[] result = null;
        String resultFinal;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {

            zk = conn.connect(hostport);
            if(znodeExists(path,hostport))
                result = zk.getData(path,false,null);
        } catch (Exception e){
            throw e;
        }finally {
            zk.close();
            conn.close();
        }


        return result;
    }


    public static void update(String path, byte[] mydata,String hostport) throws
            KeeperException,InterruptedException,IOException {

        byte[] data = mydata;
        ZooKeeperConnection conn = new ZooKeeperConnection();
        ZooKeeper zk = null;
        try {
            zk = conn.connect(hostport);
            zk.setData(path, data, zk.exists(path,true).getVersion());
        } catch (Exception e){
            throw e;
        }finally {
            zk.close();
            conn.close();
        }

    }


}