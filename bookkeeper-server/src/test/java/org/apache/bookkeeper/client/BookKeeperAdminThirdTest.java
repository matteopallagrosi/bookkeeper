package org.apache.bookkeeper.client;

import org.apache.bookkeeper.meta.*;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.ZooKeeper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;

import java.awt.print.Book;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;


import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value= Parameterized.class)
public class BookKeeperAdminThirdTest {
    private static final int NUM_BOOKIES = 5;
    private static final int NUM_ENTRIES = 100;
    private static final int ENS_SIZE = 3;
    private static final int WRITE_QUORUM = 3;
    private static final int ACK_QUORUM = 2;
    private static ClientConfiguration conf;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;

    private BookKeeperAdmin bookKeeperAdmin;
    private BookKeeper bookKeeper;
    private LedgerHandle ledger;
    private long ledgerId;
    private BookieId bookieId;
    private LedgerManager ledgerManager;
    private boolean expectedOutput;
    private Exception expectedException;


    public BookKeeperAdminThirdTest(Instance ledgerHandle , Instance address , Instance manager , Object expectedOutput , Exception expectedException) throws IOException {
        MockitoAnnotations.initMocks(this);
        switch (ledgerHandle) {
            case VALID : {
                createValidLedger();
                break;
            }
            case INVALID: {
                this.ledgerId = -1;
                break;
            }
        }

        switch(address) {
            case VALID_CORRECT: {
                getValidCorrectBookieAddress();
                break;
            }
            case VALID_INCORRECT: {
                getValidIncorrectBookieAddress();
                break;
            }
            case INVALID: {
                createInvalidBookieAddress();
                break;
            }
            case EMPTY: {
                createEmptyBookieAddress();
                break;
            }
            case NULL : {
                this.bookieId = null;
                break;
            }
        }

        switch (manager) {
            case VALID: {
                createValidLedgerManager();
                break;
            }
            case INVALID:  {
                createInvalidLedgerManager();
                break;
            }
            case NULL: {
                this.ledgerManager = null;
                break;
            }
        }

        if (expectedOutput != null) this.expectedOutput = (boolean)expectedOutput;
        this.expectedException = expectedException;

    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                        // ledgerId, bookieAddress , ledgerManager, expectedOutput, expectedException
                        {Instance.VALID, Instance.VALID_CORRECT, Instance.VALID, true, null},
                        {Instance.VALID, Instance.VALID_INCORRECT, Instance.VALID, false, null},
                        {Instance.INVALID, Instance.VALID_INCORRECT, Instance.VALID, false, null},
                        {Instance.VALID, Instance.INVALID, Instance.VALID, false, null},
                        {Instance.INVALID, Instance.INVALID, Instance.VALID, false, null},
                        {Instance.VALID, Instance.EMPTY, Instance.VALID, false, null},
                        {Instance.INVALID, Instance.EMPTY, Instance.VALID, false, null},
                        {Instance.VALID, Instance.NULL, Instance.VALID, false, null},
                        {Instance.INVALID, Instance.NULL, Instance.VALID, false, null},
                        {Instance.VALID, Instance.VALID_CORRECT, Instance.INVALID, null, new RuntimeException()},
                        {Instance.VALID, Instance.VALID_CORRECT, Instance.NULL, null, new NullPointerException()}
                });
    }

    public enum Instance {
        VALID_CORRECT, VALID_INCORRECT , VALID , INVALID, EMPTY, NULL
    }

    public void createValidLedger() {
        try {
            BookKeeper testClient = new BookKeeper(conf);
            this.ledger = testClient.createLedger(ENS_SIZE, WRITE_QUORUM, ACK_QUORUM, BookKeeper.DigestType.CRC32, "testPassword".getBytes());
            this.ledgerId = ledger.getId();

        } catch (IOException | InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while creating a valid ledger");
        }
    }

    public void getValidCorrectBookieAddress() {
        LedgerMetadata metadata = ledger.getLedgerMetadata();
        //recupera la lista dei bookie su cui una entry del ledger (quindi il ledger) è archiviata
        List<BookieId> bookies = metadata.getEnsembleAt(0);
        //recupera uno di questi bookie (quindi un bookie valido e corretto)
        this.bookieId = bookies.get(0);
    }

    public void getValidIncorrectBookieAddress() {
        //il ledger valido esistente ma il bookie scelto non è associato ad esso
        if (ledgerId != -1) {
            LedgerMetadata metadata = ledger.getLedgerMetadata();
            //recupera la lista di tutti i bookie su cui il ledger è archiviato
            Set<BookieId> bookies = LedgerMetadataUtils.getBookiesInThisLedger(metadata);
            try (BookKeeper testClient = new BookKeeper(conf)) {
                Set<BookieId> allBookies = testClient.getBookieWatcher().getAllBookies();
                for (BookieId bookie : allBookies) {
                    //seleziona un bookie valido ma non contenente il ledger
                    if (!bookies.contains(bookie)) {
                        this.bookieId = bookie;
                        return;
                    }
                }
            } catch (InterruptedException | IOException | BKException e) {
                e.printStackTrace();
                Assert.fail("An exception has been thrown while getting valid incorrect bookie");
            }
        }
        //quando non ho un ledger valido, costruisco un indirizzo sintatticamente valido ma non associato ad alcun ledger valido
        else {
            this.bookieId = BookieId.parse("126.0.0.1:45000");
        }
    }

    public void createInvalidBookieAddress() {
        BookieId id = mock(BookieId.class);
        when(id.getId()).thenReturn("0000");
        this.bookieId = id;
    }

    public void createEmptyBookieAddress() {
    BookieId id = mock(BookieId.class);
    when(id.getId()).thenReturn("");
    this.bookieId = id;
    }

    public void createValidLedgerManager() {
        LedgerManager ledgerManager = LedgerManagerUtils.getHierarchicalLedgerManager(conf, zooKeeperCluster.getZooKeeperClient());
        this.ledgerManager = ledgerManager;
    }

    //il ledgerManager in valido è costruito associandolo ad un client zookeeper (responsabile dello store dei metadati del ledger) non valido (stringa di connessione invalida)
    public void createInvalidLedgerManager() throws IOException {
        ClientConfiguration conf = new ClientConfiguration();
        ZooKeeper zk = new ZooKeeper("000", 1000, null);
        LedgerManager ledgerManager = LedgerManagerUtils.getHierarchicalLedgerManager(conf, zk);
        this.ledgerManager = ledgerManager;
    }




    @BeforeClass
    public static void configureCluster() {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_BOOKIES);
        //start dei cluster bookkeeper e zookkeeper
        try {
            bookKeeperCluster.setUp();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }
        zooKeeperCluster = bookKeeperCluster.getzkCluster();

        conf = new ClientConfiguration().
                setZkServers(zooKeeperCluster.getZooKeeperConnectString())
                .setMetadataServiceUri(zooKeeperCluster.getMetadataServiceUri());
    }

    @Before
    public void setUpClient() {
        // Create a BookKeeper client and admin instance
        try {
            bookKeeper = new BookKeeper(conf);
            bookKeeperAdmin = new BookKeeperAdmin(bookKeeper);
        } catch (IOException | InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }
    }

    @Test
    public void testAreEntriesOfLedgerStoredInTheBookie() throws InterruptedException, BKException {
        if (ledgerId != -1) {
            //scrive delle entry sul leddger
            for (int i = 0; i < NUM_ENTRIES; i++) {
                this.ledger.addEntry(("Entry " + i).getBytes());
            }


            ledger.close();

            // Get the ledger metadata
            LedgerMetadata metadata = bookKeeperAdmin.getLedgerMetadata(ledger);
            assertEquals(NUM_ENTRIES - 1, metadata.getLastEntryId());
        }

        try{
            // Verify that the entries are stored in the bookie
            boolean areEntriesStored = BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(this.ledgerId, this.bookieId, this.ledgerManager);
            assertEquals(this.expectedOutput, areEntriesStored);
        } catch(Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }

    }

    @After
    public void tearDown() throws Exception {
        // Close the BookKeeper client and admin instance
        bookKeeperAdmin.close();
        bookKeeper.close();
    }

    @AfterClass
    public static void shutDownCluster() {
        try {
            bookKeeperCluster.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while cleaning environment");
        }
    }
}

