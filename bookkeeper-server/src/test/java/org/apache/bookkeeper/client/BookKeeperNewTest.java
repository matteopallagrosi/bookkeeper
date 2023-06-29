package org.apache.bookkeeper.client;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.bookkeeper.client.api.LedgerMetadata;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

public class BookKeeperNewTest {
    private static final int NUM_BOOKIES = 10;
    private static final int ENS_SIZE = 3;
    private static final int WRITE_QUORUM = 3;
    private static final int ACK_QUORUM = 2;

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
        //crea un ledger
        BookKeeper bk = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString());
        LedgerHandle ledger = bk.createLedger(ENS_SIZE, WRITE_QUORUM, ACK_QUORUM, BookKeeper.DigestType.MAC, "testPassword".getBytes());

        //scrive delle entry sul ledger
        int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            ledger.addEntry(("Entry " + i).getBytes());
        }

        //il ledger deve essere chiuso affinchè lastEntryId sia correttamente disponibile
        ledger.close();

        //recupera i metadati del ledger
        LedgerMetadata ledgerMetadata = ledger.getLedgerMetadata();
        long lastEntryId = ledgerMetadata.getLastEntryId();
        assertTrue(lastEntryId != -1);

        //crea una callback per gestire le letture fallite
        BiConsumer<Long, Long> onReadEntryFailureCallback = (entryId, bookieId) -> {
            System.err.println("Failed to read entry " + entryId + " from bookie " + bookieId);
            // Custom handling logic for read entry failures
        };

        Set<Integer> indexes = new HashSet<Integer>();
        for (int i = 0; i < WRITE_QUORUM; i++) {
            indexes.add(i);
        }

        bookKeeperAdmin.replicateLedgerFragment(ledger, new LedgerFragment(ledger, 0, lastEntryId, indexes), onReadEntryFailureCallback);

        //tutte le entry devono essere state replicate su un numero di bookies >= ACKQUORUM
        ledgerMetadata = ledger.getLedgerMetadata();
        for (int i = 0; i < numEntries; i++) {
            assertTrue("Number of bookies on which entries have been replicated is < ACKQUORUM", ledgerMetadata.getEnsembleAt(i).size() >= ACK_QUORUM);
        }
    }
}
