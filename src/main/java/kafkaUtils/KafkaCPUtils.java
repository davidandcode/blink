package kafkaUtils;

import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;


/**
 * Created by shengwei.wang on 11/29/16.
 */
public class KafkaCPUtils {

    public static void createTopic(String topicName, Integer numPartitions,String zookeeperString) {

        ZkUtils zkUtils = null;

        try {
            // setup
            String[] arguments = new String[9];
            arguments[0] = "--create";
            arguments[1] = "--zookeeper";
            arguments[2] = zookeeperString;
            arguments[3] = "--replication-factor";
            arguments[4] = "1";
            arguments[5] = "--partitions";
            arguments[6] = "" + Integer.valueOf(numPartitions);
            arguments[7] = "--topic";
            arguments[8] = topicName;
            TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

            zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                    10000, 10000, JaasUtils.isZkSecurityEnabled());

            TopicCommand.createTopic(zkUtils, opts);
        }catch(Exception e){

        }finally{
            zkUtils.close();}
    }
}
