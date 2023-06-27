package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.bookkeeper.client.api.LedgerMetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BookKeeperNewTest {
    private static final int NUM_BOOKIES = 10;
    private static final int NUM_FRAGMENTS = 3;
    private static final int ENSEMBLE_SIZE = 3;
    private static final int QUORUM_SIZE = 2;

    private BookieServer leaderBookie;
    private BookKeeperAdmin bookKeeperAdmin;
    private BookKeeperClusterTestCase bookKeeperCluster;
    private ZooKeeperCluster zooKeeperCluster;

    @Before
    public void setUp() throws Exception {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_BOOKIES);
        //start dei cluster bookkeeper e zookkeeper
        try {
            bookKeeperCluster.setUp();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }

        zooKeeperCluster = bookKeeperCluster.getzkCluster();

        // Create a BookKeeperAdmin instance
        bookKeeperAdmin = new BookKeeperAdmin(zooKeeperCluster.getZooKeeperConnectString());
    }

    @After
    public void tearDown() throws Exception {
        // Close the BookKeeperAdmin instance
        if (bookKeeperAdmin != null) {
            bookKeeperAdmin.close();
        }
        try {
            bookKeeperCluster.tearDown();
        } catch (Exception e ) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while cleaning environment");
        }
    }

    @Test
    public void testReplicateLedgerFragment() throws Exception {
        // Create a ledger
        BookKeeper bk = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString());
        LedgerHandle ledger = bk.createLedger(NUM_FRAGMENTS, ENSEMBLE_SIZE, QUORUM_SIZE, BookKeeper.DigestType.MAC, "testPassword".getBytes());

        // Write some entries to the ledger
        int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            ledger.addEntry(("Entry " + i).getBytes());
        }

        ledger.close();

        // Fetch the ledger fragment metadata
        LedgerMetadata ledgerMetadata = ledger.getLedgerMetadata();
        long lastEntryId = ledgerMetadata.getLastEntryId();
        long ledgerId = ledger.getId();

        // Create a callback to handle read entry failures
        BiConsumer<Long, Long> onReadEntryFailureCallback = (entryId, bookieId) -> {
            System.out.println("Failed to read entry " + entryId + " from bookie " + bookieId);
            // Custom handling logic for read entry failures
        };

        // Replicate the ledger fragment to other bookies using BookKeeperAdmin
        CountDownLatch replicationLatch = new CountDownLatch(NUM_BOOKIES - 1);
        Set<Integer> indexes = new HashSet<Integer>();
        for (int i = 0 ; i < ENSEMBLE_SIZE; i++) {
            indexes.add(i);
        }

        bookKeeperAdmin.replicateLedgerFragment(ledger, new LedgerFragment(ledger, 0, lastEntryId, indexes), onReadEntryFailureCallback);

        // Verify that the ledger fragment has been replicated to all bookies
        /*for () {
            long numFragments = ledger.getNumFragments();
            assertEquals("Incorrect number of fragments", NUM_FRAGMENTS, numFragments);
        }*/
        ledgerMetadata = ledger.getLedgerMetadata();
        return;

    }
}
