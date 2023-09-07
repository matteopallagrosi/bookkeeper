package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.commons.collections4.functors.ExceptionPredicate;
import org.junit.*;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(value=Parameterized.class)
public class BookKeeperAdminReadEntriesTest {

    private static final int NUM_OF_BOOKIES = 5;
    private static final int LEDGER_ID = 0;
    private static final int ENS_SIZE = 3;
    private static final int WRITE_QUORUM = 2;
    private static final int ACK_QUORUM = 2;
    private static final DigestType DIGEST_TYPE = DigestType.CRC32;
    private static final byte[] PASSWORD = "password".getBytes();
    private static final int NUM_ENTRIES = 5;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;
    private long ledgerId;
    private long firstEntry;
    private long lastEntry;

    private Class<? extends Exception> expectedException;
    private byte[] expectedEntry;
    private static BookKeeper testClient;

    public BookKeeperAdminReadEntriesTest(long ledgerId, long firstEntry, long lastEntry, byte[] expectedValue, Class<? extends Exception> expectedException) {
        this.ledgerId = ledgerId;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.expectedEntry = expectedValue;
        this.expectedException = expectedException;

    }

    @Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                // ledgerId, firstEntry, lastEntry, expectedValue
                {-1, 0, 1, null, IllegalArgumentException.class},
                {LEDGER_ID, -1, 0, null, IllegalArgumentException.class},
                {LEDGER_ID, 0, -1, "correctEntry".getBytes(), null},
                {LEDGER_ID, 0, 1, "correctEntry".getBytes(), null},
                {LEDGER_ID, 0, 0, "correctEntry".getBytes(), null},
                {LEDGER_ID, 0, -2, null, null}, //l'output atteso non prevede nessuna lettura (legge 0 entry)

                //casi di test aggiunti per aumentare la coverage
                {LEDGER_ID + 1, 0, -1, "correctEntry".getBytes(), RuntimeException.class}, //tentativo di lettura da ledger non esistente
                {LEDGER_ID, 0, NUM_ENTRIES, "correctEntry".getBytes(), null}, //tentativo di lettura entry non esistente
        });
    }

    //configura l'environment in cui i test verranno eseguiti (crea i server bookkeeper e zookkeeper localmente)
    @BeforeClass
    public static void configureBookKeeperCluster() {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_OF_BOOKIES);
        //start dei cluster bookkeeper e zookkeeper
        try {
            bookKeeperCluster.setUp();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }
        zooKeeperCluster = bookKeeperCluster.getzkCluster();
    }

    //instanzia un ledger di test e inserisce delle entries
    @Before
    public void initializeLedger() {
        try {
            testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString());
            LedgerHandle ledger = testClient.createLedgerAdv(LEDGER_ID, ENS_SIZE, WRITE_QUORUM, ACK_QUORUM, DIGEST_TYPE, PASSWORD, null);

            //scrive delle entries sul ledger
            for (int i = 0; i < NUM_ENTRIES; i++) {
                ledger.addEntry(i, ("correctEntry").getBytes());
            }

            ledger.close();
        } catch (IOException | BKException | InterruptedException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during test initialization");
        }
    }

    //testa la lettura sincrona di entries da un ledger
    @Test
    public void testReadEntries() {
        try (BookKeeperAdmin bookKeeperAdmin = new BookKeeperAdmin(zooKeeperCluster.getZooKeeperConnectString())) {

            if (ledgerId < 0) {
                assertThrows(expectedException, () -> {
                    bookKeeperAdmin.readEntries(this.ledgerId, this.firstEntry, this.lastEntry);
                });
            } else if (firstEntry < 0) {
                assertThrows(expectedException, () -> {
                    bookKeeperAdmin.readEntries(this.ledgerId, this.firstEntry, this.lastEntry);
                });
            } else {
                //procede a leggere le entries
                Iterable<LedgerEntry> entries = bookKeeperAdmin.readEntries(this.ledgerId, this.firstEntry, this.lastEntry);

                if (lastEntry >= firstEntry || lastEntry == -1) {
                    //codice modificato per aumentare coverage
                    /*for (LedgerEntry entry : entries) {
                        assertEquals(new String(expectedEntry), new String(entry.getEntry()));
                    }*/
                    try {
                        BookKeeperAdmin.LedgerEntriesIterator entriesIterator = (BookKeeperAdmin.LedgerEntriesIterator) entries.iterator();
                        while (entriesIterator.hasNext()) {
                            //verifico che richiamando la hasNext l'entry non venga "consumata" ma è ancora possibile leggerla
                            assertTrue(entriesIterator.hasNext());
                            LedgerEntry currentEntry = entriesIterator.next();
                            assertEquals(new String(expectedEntry), new String(currentEntry.getEntry()));
                        }
                        //linea di codice aggiunta per aumentare coverage
                        //se tento di leggere un'ulteriore entry mi aspetto che mi venga sollevata una eccezione (non ci sono più entry da leggere)
                        try {
                            entriesIterator.next();
                        } catch(Exception e) {
                            e.printStackTrace();
                            assertThat(e, instanceOf(NoSuchElementException.class));
                        }
                        //porzione di codice aggiunta per aumentare coverage
                        //forzo la richiesta di una ulteriore entry manipolando appositamente i parametri nextEntry e lastAddConfirmed dell'iteratore,
                        //per simulare la lettura di entry non esistente
                        //vengono testate entrambe le possibilità (entry con id valido ed esistente ed entry con id non valido)
                        try {
                            entriesIterator.nextEntryId = NUM_ENTRIES;
                            entriesIterator.handle.lastAddConfirmed = NUM_ENTRIES;
                            entriesIterator.hasNext();
                        } catch (Exception e) {
                            e.printStackTrace();
                            assertThat(e, instanceOf(RuntimeException.class));
                        }
                        try {
                            entriesIterator.nextEntryId = -1;
                            entriesIterator.hasNext();
                        } catch(Exception e) {
                            e.printStackTrace();
                            assertThat(e, instanceOf(RuntimeException.class));
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                        assertThat(e, instanceOf(expectedException));
                    }
                }
                //non legge nessuna entry
                else if (lastEntry < -1) {
                    int count = 0;
                    for (LedgerEntry entry : entries) {
                        count++;
                        assertEquals(new String(expectedEntry), new String(entry.getEntry()));
                    }
                    assertEquals(0, count);
                } else {
                    //range (firstEntry, lastEntry) non valido
                    assertThrows(expectedException, () -> {
                        for (LedgerEntry entry : entries) {
                            System.err.println(entry.getEntryId());
                        }
                    });
                }
            }
        }

        catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Exception is not expected");
        }
    }


    //rimuove il ledger precedentemente creato
    @After
    public void deleteLedgers() {
        try {
            testClient.deleteLedger(LEDGER_ID);
            testClient.close();
        } catch (InterruptedException  | BKException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while deleting test client");
        }
    }


    //rimuove i cluster creati
    @AfterClass
    public static void clearEnvironment() {
        try {
            bookKeeperCluster.tearDown();
        } catch (Exception e ) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while cleaning environment");
        }
    }
}
