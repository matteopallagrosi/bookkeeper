package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.management.InstanceAlreadyExistsException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

@RunWith(value= Parameterized.class)
public class BookKeeperAdminSecondTest {
    private static final int NUM_OF_BOOKIES = 5;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;
    private LedgerHandle ledgerHandle;
    private LedgerFragment ledgerFragment;
    private BiConsumer<Long, Long> onReadEntryFailureCallback;  //i due Long sono l'id del ledger e della entry la cui lettura non è andata a buon fine


    public BookKeeperAdminSecondTest(Instance lh, Instance lf, Instance onReadEntryFailureCallback) {
        MockitoAnnotations.initMocks(this);

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
                {Instance.VALID, Instance.VALID , Instance.VALID},
                {Instance.INVALID , Instance.VALID , Instance.VALID},
                {Instance.NULL, Instance.VALID , Instance.VALID} ,
                {Instance.VALID , Instance.INVALID , Instance.VALID},
                {Instance.VALID , Instance.NULL , Instance.VALID},
                {Instance.VALID , Instance.VALID , Instance.INVALID},
                {Instance.VALID , Instance.VALID , Instance.NULL}
        });
    }

    //ritorna un ledger handle valido, configurato tramite un client di test
    private LedgerHandle createValidLedgerHandle() {
        LedgerHandle lh = null;
        try (BookKeeper testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString())){
            testClient.getBookieWatcher().getBookies();
            lh = testClient.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32, "password".getBytes());
            //scrive delle entry sul leddger
            int numEntries = 100;
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(("Entry " + i).getBytes());
            }
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

            //l'handle creato ha un id non valido (ossia -1)
            lh = new LedgerHandle(context, -1, versionedMetadata, DigestType.CRC32, "".getBytes(), WriteFlag.NONE);
        } catch(Exception e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown during creation test parameters");
        }
        return lh;
    }

    private LedgerFragment createValidLedgerFragment() {
    /*LedgerFragment fragment = mock(LedgerFragment.class);
    when(fragment.getReplicateType()).thenReturn(LedgerFragment.ReplicateType.DATA_LOSS);
    //mocko i comportamenti che servono
    return fragment;*/
        LedgerMetadata ledgerMetadata = this.ledgerHandle.getLedgerMetadata();
        long lastEntryId = ledgerMetadata.getLastEntryId();
        List<BookieId> bookieIds = new ArrayList<>();
        for (int i = 0 ; i < lastEntryId ; i++) {
            bookieIds.addAll(ledgerMetadata.getEnsembleAt(i));
        }
        Set<Integer> indexes = new HashSet<Integer>();
        for (BookieId id : bookieIds) {
            indexes.add(Integer.valueOf(id.getId()));
        }
        return new LedgerFragment(this.ledgerHandle, 0, lastEntryId, indexes);
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

    enum Instance {
        VALID, INVALID, NULL
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

    @Test
    public void testReplicateFragment() {
        try (BookKeeperAdmin bookKeeperAdmin = new BookKeeperAdmin(zooKeeperCluster.getZooKeeperConnectString())) {
            LedgerMetadata ledgerMetadata = this.ledgerHandle.getLedgerMetadata();
            long lastEntryId = ledgerMetadata.getLastEntryId();
            long ledgerId = this.ledgerHandle.getId();
            bookKeeperAdmin.replicateLedgerFragment(this.ledgerHandle, this.ledgerFragment, this.onReadEntryFailureCallback);



        } catch (IOException | BKException | InterruptedException e ) {
            e.printStackTrace();
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
