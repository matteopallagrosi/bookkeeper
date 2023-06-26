package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;

@RunWith(value= Parameterized.class)
public class BookKeeperAdminSecondTest {
    private static final int NUM_OF_BOOKIES = 5;
    private static BookKeeperClusterTestCase bookKeeperCluster;
    private static ZooKeeperCluster zooKeeperCluster;
    private LedgerHandle ledgerHandle;
    private LedgerFragment ledgerFragment;
    private BiConsumer<Long, Long> onReadEntryFailureCallback;  //i due Long sono l'id del ledger e della entry la cui lettura non è andata a buon fine

    public BookKeeperAdminSecondTest(LedgerHandle lh, LedgerFragment lf, BiConsumer<Long, Long> onReadEntryFailureCallback) {
        this.ledgerHandle = lh;
        this.ledgerFragment = lf;
        this.onReadEntryFailureCallback = onReadEntryFailureCallback;
    }

    @Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                // lh, lf , onReadEntryFailureCallBack , expectedException
                {createValidLedgerHandle(), , },
        });
    }

    //ritorna un ledger handle valido, configurato tramite un client di test
    private static LedgerHandle createValidLedgerHandle() {
        LedgerHandle lh = null;
        try (BookKeeper testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString())){
            lh = testClient.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32, "password".getBytes());
        } catch(IOException | InterruptedException | BKException e) {
            e.printStackTrace();
            Assert.fail("An exception has been thrown while creating testing ledger");
        }
        return lh;
    }

    private static LedgerHandle createInvalidHandle () throws BKException, IOException, InterruptedException, GeneralSecurityException {
        BookKeeper testClient = new BookKeeper(zooKeeperCluster.getZooKeeperConnectString());
        ClientContext context = testClient.getClientCtx();
        //TODO create mocked metadata
        return new LedgerHandle(context, -1, null, DigestType.CRC32, null, WriteFlag.NONE);
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




}
