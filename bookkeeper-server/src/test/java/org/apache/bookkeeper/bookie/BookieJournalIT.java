package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.NetworkInterface;
import java.io.File;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

import static org.apache.bookkeeper.net.NetworkTopologyImpl.LOG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class BookieJournalIT {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    private ServerConfiguration serverConf;
    private static final long LEDGER_ID = 1;
    private static final long ENTRY_ID = 0;
    private static final long LAST_ADD_CONFIRMED = -1;
    private static final byte[] TEST_BYTES = "test".getBytes();

    @Before
    public void setUpEnvironment() {
        try {
            //crea delle directories temporanee (eliminate in automatico al termine del test) in cui i journal e i ledger saranno archiviati
            File journalDir = tempDir.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
            File ledgerDir = tempDir.newFolder();
            BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

            //crea la configurazione del server necessaria a runnare il Bookie (gli aggancia le directories create)
            this.serverConf = createServerConfiguration();
            this.serverConf.setJournalDirName(journalDir.getPath())
                    .setLedgerDirNames(new String[]{ledgerDir.getPath()})
                    .setMetadataServiceUri(null);
        }  catch (SocketException e) {
            e.printStackTrace();
            Assert.fail("Exception thrown during server configuration");
        }
        catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Exception thrown while creating temporary folder");
        }
    }

    @Test
    public void addLogEntryITCase() throws Exception {
        TestBookieImpl b = spy(new TestBookieImpl(serverConf));
        b.start();

        BookieId bookieId = BookieImpl.getBookieId(serverConf);

        //recupera il journal su cui l'operazione di addEntry dovrebbe essere registrata
        Journal expectedJournal = b.getJournal();

        //meccanismo ci sincronizzazione per attendere il completamento dell'operazione di addEntry
        CountDownLatch latch = new CountDownLatch(1);

        //costruisce la entry da scrivere nel Bookie (rispettando la struttura indicata nella documentazione)
        final ByteBuf entry = Unpooled.buffer();
        entry.writeLong(LEDGER_ID);
        entry.writeLong(ENTRY_ID);
        entry.writeLong(LAST_ADD_CONFIRMED);
        entry.writeBytes(TEST_BYTES);

        String cxt = "test";

        //costruisce la callback invocata al termine dell'operazione di scrittura sul journal della transazione di addEntry
        WriteCallback callBack = (rc, ledgerId, entryId, addr, ctx) -> {
            if (rc != BKException.Code.OK) {
                LOG.error("Failed to write entry: {}", BKException.getMessage(rc));
                latch.countDown();
                return;
            }

            //controlla che l'interazione tra il Bookie e il Journal sia avvenuta (con i parametri previsti)
            try {
                verify(expectedJournal, atLeastOnce()).logAddEntry(eq(entry), eq(false), any(WriteCallback.class), eq(cxt));
            } catch(InterruptedException e)  {
                e.printStackTrace();
                Assert.fail("Exception thrown while checking bookie-journal interaction");
            }
            //inoltre tale interazione deve essere avvenuta secondo il comportamento previsto (scrittura della entry costruita in precedenza)
            assertEquals(1, ledgerId);
            assertEquals(0, entryId);

            //rilascia il "token" così da permettere al main thread di proseguire
            latch.countDown();
        };

        byte[] masterKey = new byte[64];

        b.addEntry(entry, false, callBack , cxt,  masterKey);

        latch.await();

        //verifica se la entry è stata scritta correttamente (check di tutti i campi)
        ByteBuf readEntry = b.readEntry(LEDGER_ID, ENTRY_ID);
        assertEquals(readEntry.getLong(0), LEDGER_ID);
        assertEquals(readEntry.getLong(8), ENTRY_ID);
        assertEquals(readEntry.getLong(16), LAST_ADD_CONFIRMED);
        byte[] data = new byte[TEST_BYTES.length];
        readEntry.getBytes(24, data);
        assertArrayEquals(TEST_BYTES, data);

        b.shutdown();
    }

    private ServerConfiguration createServerConfiguration() throws SocketException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setJournalFormatVersionToWrite(5);
        conf.setAllowEphemeralPorts(false);
        conf.setBookiePort(PortManager.nextFreePort());
        conf.setGcWaitTime(1000);
        conf.setDiskUsageThreshold(0.999f);
        conf.setDiskUsageWarnThreshold(0.99f);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        conf.setZkRetryBackoffMaxRetries(0);

        String loopBackAddress = null;
        Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface nif : Collections.list(nifs)) {
            if (nif.isLoopback()) {
                loopBackAddress = nif.getName();
            }
        }

        conf.setListeningInterface(loopBackAddress);
        conf.setAllowLoopback(true);

        return conf;
    }
}
