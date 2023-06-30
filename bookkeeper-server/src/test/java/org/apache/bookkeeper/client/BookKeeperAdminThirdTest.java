package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.meta.*;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.client.LedgerEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.awt.print.Book;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class BookKeeperAdminThirdTest {
    private static final int NUM_BOOKIES = 5;
    private static final int NUM_ENTRIES = 100;
    private static final int ENS_SIZE = 3;
    private static final int WRITE_QUORUM = 3;
    private static final int ACK_QUORUM = 2;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;

    private BookKeeperAdmin bookKeeperAdmin;
    private BookKeeper bookKeeper;

    @Before
    public void setup() throws Exception {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_BOOKIES);
        //start dei cluster bookkeeper e zookkeeper
        try {
            bookKeeperCluster.setUp();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }
        zooKeeperCluster = bookKeeperCluster.getzkCluster();

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setZkServers(zooKeeperCluster.getZooKeeperConnectString())
                .setMetadataServiceUri(zooKeeperCluster.getMetadataServiceUri());

        // Create a BookKeeper client and admin instance
        bookKeeper = new BookKeeper(clientConfiguration);
        bookKeeperAdmin = new BookKeeperAdmin(bookKeeper);
    }

    @Test
    public void testAreEntriesOfLedgerStoredInTheBookie() throws InterruptedException, BKException {
        // Create a ledger
        LedgerHandle ledger = bookKeeper.createLedger(ENS_SIZE, WRITE_QUORUM, ACK_QUORUM, BookKeeper.DigestType.CRC32, "testPassword".getBytes());
        LedgerManager ledgerManager = LedgerManagerUtils.getHierarchicalLedgerManager(bookKeeper.getConf(), zooKeeperCluster.getZooKeeperClient());

        //scrive delle entry sul leddger
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ledger.addEntry(("Entry " + i).getBytes());
        }

        ledger.close();

        // Get the ledger metadata
        LedgerMetadata metadata = bookKeeperAdmin.getLedgerMetadata(ledger);
        assertEquals(NUM_ENTRIES - 1, metadata.getLastEntryId());

        //Get bookies in which ledger must be stored
        Set<BookieId> bookies = LedgerMetadataUtils.getBookiesInThisLedger(metadata);
        assertEquals(ENS_SIZE, bookies.size());

        for (BookieId bookie : bookies) {
            System.err.println("contenuto in " + bookie);
            // Verify that the entries are stored in the bookie
            boolean areEntriesStored = BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledger.getId(), bookie, ledgerManager);
            assertTrue(areEntriesStored);

        }

        Set<BookieId> allBookies = bookKeeper.getBookieWatcher().getBookies();
        for (BookieId bookie : allBookies) {
            if (!bookies.contains(bookie))  {
                System.err.println(bookie);
                boolean areEntriesStored = BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledger.getId(), bookie, metadata);
                assertFalse(areEntriesStored);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        // Close the BookKeeper client and admin instance
        bookKeeperAdmin.close();
        bookKeeper.close();

        bookKeeperCluster.tearDown();
    }
}

