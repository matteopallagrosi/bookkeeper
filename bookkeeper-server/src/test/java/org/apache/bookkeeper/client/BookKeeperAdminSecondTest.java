package org.apache.bookkeeper.client;

import static org.hamcrest.CoreMatchers.instanceOf;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value= Parameterized.class)
public class BookKeeperAdminSecondTest {
    private static final int NUM_BOOKIES = 10;
    private static final int ENS_SIZE = 3;
    private static final int WRITE_QUORUM = 3;
    private static final int ACK_QUORUM = 2;
    private static final int NUM_ENTRIES = 100;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;
    private BookKeeperAdmin bookKeeperAdmin;
    private LedgerHandle ledgerHandle;
    private LedgerFragment ledgerFragment;
    private BiConsumer<Long, Long> onReadEntryFailureCallback;  //i due Long sono l'id del ledger e della entry la cui lettura non è andata a buon fine
    private Exception expectedException;

    public BookKeeperAdminSecondTest(Instance lh, Instance lf, Instance onReadEntryFailureCallback, Exception exception) {
        MockitoAnnotations.initMocks(this);
        this.expectedException = exception;
        switch(lh) {
            case VALID: {
                this.ledgerHandle = createValidLedgerHandle();
                break;
            }
            case INVALID: {
                this.ledgerHandle = createInvalidHandle();
                break;
            }
            case NULL: {
                this.ledgerHandle = null;
                break;
            }
        }
        switch(lf) {
            case VALID: {
                this.ledgerFragment = createValidLedgerFragment();
                break;
            }
            case INVALID: {
                this.ledgerFragment= createInvalidLedgerFragment();
                break;
            }
            case NULL: {
                this.ledgerFragment = null;
                break;
            }
        }
        switch(onReadEntryFailureCallback) {
            case VALID: {
                this.onReadEntryFailureCallback = getValidCallBack();
                break;
            }
            case INVALID: {
                this.onReadEntryFailureCallback = getInvalidCallBack();
                break;
            }
            case NULL: {
                this.onReadEntryFailureCallback = null;
                break;
            }
        }
    }

    @Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                // lh, lf , onReadEntryFailureCallBack , expectedException
                {Instance.VALID, Instance.VALID , Instance.VALID, null},
                {Instance.INVALID , Instance.VALID , Instance.VALID, new Exception()},
                {Instance.NULL, Instance.VALID , Instance.VALID, new NullPointerException()} ,
               {Instance.VALID , Instance.INVALID , Instance.VALID, new Exception()},
               {Instance.VALID , Instance.NULL , Instance.VALID, new NullPointerException()},
                {Instance.VALID , Instance.VALID , Instance.INVALID, null},
                {Instance.VALID , Instance.VALID , Instance.NULL, null}
        });
    }

    //ritorna un ledger handle valido, configurato tramite un client di test
    private LedgerHandle createValidLedgerHandle() {
        LedgerHandle lh = null;
        try  {
            BookKeeper testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString());
            lh = testClient.createLedger(ENS_SIZE, WRITE_QUORUM, ACK_QUORUM, BookKeeper.DigestType.MAC, "password".getBytes());

            //scrive delle entry sul leddger
            for (int i = 0; i < NUM_ENTRIES; i++) {
                lh.addEntry(("Entry " + i).getBytes());
            }

            //il ledger deve essere chiuso affinchè lastEntryId sia correttamente disponibile
            lh.close();

        } catch(IOException | InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while creating testing ledger");
        }

        return lh;
    }

    private LedgerHandle createInvalidHandle () {
        LedgerHandle lh = null;
        try (BookKeeper testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString())) {
            ClientContext context = testClient.getClientCtx();
            LedgerMetadata mockedMetadata = mock(LedgerMetadata.class);
            Versioned<LedgerMetadata> versionedMetadata = new Versioned<>(mockedMetadata, null);
            when(mockedMetadata.isClosed()).thenReturn(true);

            //l'handle creato ha un id non valido (ossia -1)
            lh = new LedgerHandle(context, -1, versionedMetadata, DigestType.CRC32, "".getBytes(), WriteFlag.NONE);

        } catch(Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during creation test parameters");
        }
        return lh;
    }

    private LedgerFragment createValidLedgerFragment() {
        //recupero l'id dell'ultima entry inserita nel ledger
        try {
            LedgerMetadata ledgerMetadata = this.ledgerHandle.getLedgerMetadata();
            long lastEntryId = ledgerMetadata.getLastEntryId();

            Set<Integer> indexes = new HashSet<Integer>();
            for (int i = 0; i < ENS_SIZE; i++) {
                indexes.add(i);
            }

            return new LedgerFragment(this.ledgerHandle, 0, lastEntryId, indexes);
        }
        catch (Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }
        return null;
    }

    private LedgerFragment createInvalidLedgerFragment() {
        Set<Integer> bookies = mock(Set.class);
        LedgerHandle mockedHandle = mock(LedgerHandle.class);
        when(mockedHandle.getId()).thenReturn(-1L);
        when(mockedHandle.getLedgerMetadata()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                LedgerMetadata mockedMetadata = mock(LedgerMetadata.class);
                when(mockedMetadata.getEnsembleAt(anyLong())).thenReturn(null);
                when(mockedMetadata.isClosed()).thenReturn(true);
                return mockedMetadata;
            }
        });
        //il fragment è stato creato con parametri non validi (entryId = -1)
        return new LedgerFragment(mockedHandle, -1 , -1, bookies);
    }

    //crea una callback per gestire le letture fallite
    private BiConsumer<Long, Long> getValidCallBack() {
        return (ledgerid, entryid) -> {
            System.err.println("Failed reading entry " + entryid + " on ledger " + ledgerid);
        };
    }

    private BiConsumer<Long, Long> getInvalidCallBack(){
        return (ledgerid, entryid) -> {
            throw new RuntimeException();
        };
    }

    public enum Instance {
        VALID, INVALID, NULL
    }

    //configura l'environment in cui i test verranno eseguiti (crea i server bookkeeper e zookkeeper localmente)
    @BeforeClass
    public static void configureBookKeeperCluster() {
        bookKeeperCluster = new BookKeeperClusterTestCase(NUM_BOOKIES);
        //start dei cluster bookkeeper e zookkeeper
        try {
            bookKeeperCluster.setUp();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during environment set up");
        }
        zooKeeperCluster = bookKeeperCluster.getzkCluster();
    }

    @Before
    public void setUp() {
        try {
            bookKeeperAdmin = new BookKeeperAdmin(zooKeeperCluster.getZooKeeperConnectString());
        } catch (IOException | InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("Error during admin initialization");

        }
    }

    @Test
    public void testReplicateFragment() {
        try {
            //recupera i metadati del ledger per verificare che le entry (quindi il fragment) siano presenti correttamente
            LedgerMetadata ledgerMetadata = this.ledgerHandle.getLedgerMetadata();
            long lastEntryId = ledgerMetadata.getLastEntryId();
            assertTrue(lastEntryId != -1);

            bookKeeperAdmin.replicateLedgerFragment(this.ledgerHandle, this.ledgerFragment, onReadEntryFailureCallback);

            //tutte le entry devono essere state replicate su un numero di bookies >= ACKQUORUM
            ledgerMetadata = this.ledgerHandle.getLedgerMetadata();
            for (int i = 0; i < NUM_ENTRIES; i++) {
                assertTrue("Number of bookies on which entries have been replicated is < ACKQUORUM", ledgerMetadata.getEnsembleAt(i).size() >= ACK_QUORUM);
            }
        } catch(Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }
    }

    @After
    public void tearDown() {
        try {
            bookKeeperAdmin.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("Failed while closing admin");
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
