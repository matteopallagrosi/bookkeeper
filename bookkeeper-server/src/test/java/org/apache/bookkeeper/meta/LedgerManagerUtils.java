package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.ZooKeeper;

public class LedgerManagerUtils {

    public static HierarchicalLedgerManager getHierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zooKeeper) {
        return new HierarchicalLedgerManager(conf, zooKeeper);
    }

}
