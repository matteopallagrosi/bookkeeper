package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value= Parameterized.class)
public class BookKeeperRecoverBookieTest {
    private static final int NUM_BOOKIES = 5;
    private static final int NUM_ENTRIES = 100;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static BookKeeper.DigestType digestType;
    private static String ledgerManagerFactory;
    private BookKeeperAdmin bkAdmin;

    private static ServerConfiguration baseConf;
    private static ClientConfiguration baseClientConf;

    private static BookKeeperTestClient client;
    private static Random random;

    //parametri input
    long ledgerId;
    LedgerHandle ledger;
    Set<BookieId> bookieSrc;
    boolean dryrun;
    boolean skipOpenLedger;
    boolean equals;
    Exception expectedException;

    public BookKeeperRecoverBookieTest(LedgerState ledgerState, BookiesState bookiesState, boolean dryrun, boolean skipOpenLedger, EnsembleState ensState, Exception expectedException) {
        this.dryrun = dryrun;
        this.skipOpenLedger = skipOpenLedger;
        this.expectedException = expectedException;
        this.bookieSrc = new HashSet<>();

        switch (ledgerState) {
            case VALID_OPEN: {
                createValidLedger();
                break;
            }

            case VALID_CLOSED: {
                createValidLedger();
                try {
                    this.ledger.close();
                } catch(InterruptedException | BKException e) {
                    e.printStackTrace();
                    Assert.fail("Exception thrown while closing valid ledger");
                }
                break;
            }

            case INVALID: {
                this.ledgerId = -1;
                break;
            }
        }

        switch (bookiesState) {
            case VALID_CORRECT: {
                this.bookieSrc.add(createValidCorrectBookies());
                break;
            }

            case VALID_INCORRECT: {
                this.bookieSrc.add(getValidIncorrectBookieAddress());
                break;
            }

            case INVALID: {
                this.bookieSrc.add(createInvalidBookieAddress());
                break;
            }
            case NON_EXISTENT: {
                this.bookieSrc.add(createNonExistentBookie());
                break;
            }
            case MIXED: {
                this.bookieSrc.add(createValidCorrectBookies());
                this.bookieSrc.add(getValidIncorrectBookieAddress());
                this.bookieSrc.add(createInvalidBookieAddress());
                this.bookieSrc.add(createNonExistentBookie());
                break;
            }
            case EMPTY: {
                this.bookieSrc = new HashSet<>();
                break;
            }
            case NULL: {
              this.bookieSrc = null;
              break;
            }
        }

        switch (ensState) {
            case SAME: {
                this.equals = true;
                break;
            }

            case DIFFERENT: {
                this.equals = false;
                break;
            }
        }



    }


    @Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                // lid, bookieSrc, dryrun, skipOpenLedger, ensembleState, expectedException
                {LedgerState.VALID_OPEN, BookiesState.VALID_CORRECT, false, false, EnsembleState.DIFFERENT, null},
                {LedgerState.VALID_OPEN, BookiesState.VALID_INCORRECT, false, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.INVALID, false, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NON_EXISTENT, false, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.MIXED, false, false, EnsembleState.DIFFERENT, null},
                {LedgerState.VALID_OPEN, BookiesState.EMPTY, false, false, EnsembleState.SAME, null},
//              {LedgerState.VALID_OPEN, BookiesState.NULL, false, false, EnsembleState.SAME, new NullPointerException()},

                {LedgerState.VALID_OPEN, BookiesState.VALID_CORRECT, true, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.VALID_INCORRECT, true, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.INVALID, true, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NON_EXISTENT, true, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.MIXED, true, false, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.EMPTY, true, false, EnsembleState.SAME, null},
//              {LedgerState.VALID_OPEN, BookiesState.NULL, true, false, EnsembleState.SAME, new NullPointerException()},

                {LedgerState.VALID_OPEN, BookiesState.VALID_CORRECT, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.VALID_INCORRECT, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.INVALID, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NON_EXISTENT, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.MIXED, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.EMPTY, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NULL, false, true, EnsembleState.SAME, null},

                {LedgerState.VALID_OPEN, BookiesState.VALID_CORRECT, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.VALID_INCORRECT, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.INVALID, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NON_EXISTENT, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.MIXED, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.EMPTY, true, true, EnsembleState.SAME, null},
                {LedgerState.VALID_OPEN, BookiesState.NULL, true, true, EnsembleState.SAME, null},

                {LedgerState.VALID_CLOSED, BookiesState.VALID_CORRECT, false, true, EnsembleState.DIFFERENT, null},
                {LedgerState.VALID_CLOSED, BookiesState.VALID_INCORRECT, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_CLOSED, BookiesState.INVALID, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_CLOSED, BookiesState.NON_EXISTENT, false, true, EnsembleState.SAME, null},
                {LedgerState.VALID_CLOSED, BookiesState.MIXED, false, true, EnsembleState.DIFFERENT, null},
                {LedgerState.VALID_CLOSED, BookiesState.EMPTY, false, true, EnsembleState.SAME, null},
//              {LedgerState.VALID_CLOSED, BookiesState.NULL, false, true, EnsembleState.SAME, new NullPointerException()},

                {LedgerState.INVALID, BookiesState.VALID_INCORRECT, false, true, EnsembleState.SAME, new BKException.BKNoSuchLedgerExistsOnMetadataServerException()},
                {LedgerState.INVALID, BookiesState.INVALID, false, true, EnsembleState.SAME, new BKException.BKNoSuchLedgerExistsOnMetadataServerException()},
                {LedgerState.INVALID, BookiesState.NON_EXISTENT, false, true, EnsembleState.SAME, new BKException.BKNoSuchLedgerExistsOnMetadataServerException()},
                {LedgerState.INVALID, BookiesState.EMPTY, false, true, EnsembleState.SAME, new BKException.BKNoSuchLedgerExistsOnMetadataServerException()},
                {LedgerState.INVALID, BookiesState.NULL, false, true, EnsembleState.SAME, new BKException.BKNoSuchLedgerExistsOnMetadataServerException()},
        });
    }

    enum BookiesState {
        VALID_CORRECT, VALID_INCORRECT, INVALID, MIXED, NON_EXISTENT, EMPTY, NULL
    }

    enum LedgerState {
        VALID_OPEN, VALID_CLOSED, INVALID
    }

    enum EnsembleState {
        SAME , DIFFERENT
    }

    private void createValidLedger() {
        try {
            //crea un ledger
            this.ledger = client.createLedger(3, 2, 2,
                    digestType, baseClientConf.getBookieRecoveryPasswd());
            this.ledgerId = ledger.getId();

            //scrive delle entries sul ledger
            for (int i = 0; i < 100; i++) {
                ledger.addEntry(("Entry " + i).getBytes());
            }
        } catch(InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("Exception thrown while creating a valid ledger");
        }
    }

    private BookieId createValidCorrectBookies() {
        LedgerMetadata metadata = this.ledger.getLedgerMetadata();
        //recupera la lista dei bookie su cui una entry del ledger (quindi il ledger) è archiviata
        List<BookieId> bookies = metadata.getEnsembleAt(0);
        //recupera uno di questi bookie (quindi un bookie valido e corretto)
        return bookies.get(0);
    }

    private BookieId getValidIncorrectBookieAddress() {
        //il ledger valido esistente ma il bookie scelto non è associato ad esso
        if (ledgerId != -1) {
            LedgerMetadata metadata = ledger.getLedgerMetadata();
            //recupera la lista di tutti i bookie su cui il ledger è archiviato
            Set<BookieId> bookies = LedgerMetadataUtils.getBookiesInThisLedger(metadata);
            try  {
                Set<BookieId> allBookies = client.getBookieWatcher().getAllBookies();
                for (BookieId bookie : allBookies) {
                    //seleziona un bookie valido ma non contenente il ledger
                    if (!bookies.contains(bookie)) {
                        return bookie;

                    }
                }
            } catch (BKException e) {
                e.printStackTrace();
                Assert.fail("An exception has been thrown while getting valid incorrect bookie");
            }
        }
        //quando non ho un ledger valido, costruisco un indirizzo sintatticamente valido ma non associato ad alcun ledger valido
        else {
            return BookieId.parse("126.0.0.1:45000");
        }
        return null;
    }

    private BookieId createInvalidBookieAddress() {
        BookieId id = mock(BookieId.class);
        when(id.getId()).thenReturn("0000");
        return id;
    }

    private BookieId createNonExistentBookie() {
        try {
            //recupera tutti i bookies
            Set<BookieId> allBookies = client.getBookieWatcher().getBookies();
            boolean found = true;
            String bookieAddress = "127.0.0.1:3545";
            while (found) {
                found = false;
                bookieAddress  += random.nextInt(10);
                for (BookieId bookie : allBookies) {
                    if (bookie.getId().equals(bookieAddress))
                        found = true;
                }
            }
            //crea un bookieId non esistente
           return BookieId.parse(bookieAddress);
        } catch(BKException e) {
            e.printStackTrace();
            Assert.fail("Exception thrown while creating non existent bookie address");
        }
        return null;
    }

    public void createMixedBookieSet() {

    }


    @BeforeClass
    public static void configureCluster() throws Exception {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_BOOKIES, 1);
        digestType = BookKeeper.DigestType.CRC32;
        ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";

        baseConf = bookKeeperCluster.getConfiguration();
        baseClientConf = bookKeeperCluster.getClientConf();

        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);

        baseClientConf.setBookieRecoveryPasswd("".getBytes());
        bookKeeperCluster.setUp();
        client = bookKeeperCluster.getClient();
        random = new Random();
    }

    @Before
    public void setUpAdmin() throws Exception {
        ClientConfiguration adminConf = new ClientConfiguration(baseClientConf);
        adminConf.setMetadataServiceUri(bookKeeperCluster.getzkCluster().getMetadataServiceUri());
        bkAdmin = new BookKeeperAdmin(adminConf);
    }

    @Test
    public void testRecoverLedger() throws Exception {
        /*for (BookieId bookie : this.bookieSrc) {
            System.err.println("elimina " + bookie);
        }*/

        List<BookieId> oldEnsemble = null;
        if (this.ledgerId != -1) {
            LedgerMetadata metadata = this.ledger.getLedgerMetadata();
            oldEnsemble =  this.ledger.getLedgerMetadata().getEnsembleAt(0);
        }

        System.err.println(oldEnsemble);

        if (bookieSrc != null) {
            //interrompe un bookie su cui il ledger è archiviato (simula una failure)
            for (int i = 0; i < 3; i++) {
                BookieId currentBookie = bookKeeperCluster.getBookie(i);
                if (bookieSrc.contains(currentBookie)) {
                    bookKeeperCluster.killBookie(i);
                    break;
                }
            }
        }

        try {
            // Call the sync recover bookie method.
            bkAdmin.recoverBookieData(this.ledgerId, this.bookieSrc, this.dryrun, this.skipOpenLedger);

            LedgerHandle ledgerOpened = client.openLedger(ledger.getId(),
                    digestType, baseClientConf.getBookieRecoveryPasswd());

            for (BookieId bookie : ledgerOpened.getLedgerMetadata().getEnsembleAt(0)) {
                System.err.println(bookie);
            }

            LedgerHandle recoveredLedger = client.openLedger(ledgerId, digestType, baseClientConf.getBookieRecoveryPasswd());

            //verifica che l'ensemble su cui è archiviato il ledger sia rimasto lo stesso (in caso di mancato recovery) oppure dopo il recovery è stato configurato un nuovo ensemble
            List<BookieId> newEnsemble;
            if (this.ledgerId != -1) {
                newEnsemble = ledgerOpened.getLedgerMetadata().getEnsembleAt(0);
                System.err.println(newEnsemble);
                assert oldEnsemble != null;
                assertEquals(this.equals, oldEnsemble.equals(newEnsemble));
            }

            if (!this.equals) {
                //verifica che tutte le entry del ledger siano presenti sui bookie dopo il recovery (in caso di recovery effettuato e quindi nuovo ensemble configurato)
                Enumeration<LedgerEntry> entries = recoveredLedger.readEntries(0, NUM_ENTRIES - 1);
                int i = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    assertEquals("Entry " + i, new String(entry.getEntry()));
                    i++;
                }
            }
        } catch (Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }
    }

    @After
    public void tearDown() throws Exception {
        //fa partire un nuovo bookie in modo che prima e dopo ogni test ci siano sempre NUM_BOOKIES server attivi
        bookKeeperCluster.startNewBookie();
        // Release any resources used by the BookieRecoveryTest instance.
        if (bkAdmin != null){
            bkAdmin.close();
        }
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

