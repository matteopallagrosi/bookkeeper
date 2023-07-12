package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.net.NetworkInterface;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.net.NetworkTopologyImpl.LOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class provaIT {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void addLogEntryITCase() throws Exception {
        //crea delle directories temporanee (eliminate al termine del test) in cui i journal e i ledger saranno archiviati
        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        //crea la configurazione del server necessaria a runnare il Bookie (gli aggancia le directories create)
        ServerConfiguration serverConf = createServerConfiguration();
        serverConf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[]{ledgerDir.getPath()})
                .setMetadataServiceUri(null);

        TestBookieImpl b = spy(new TestBookieImpl(serverConf));
        b.start();

        //recupera il journal su cui l'operazione di addEntry dovrebbe essere registrata
        Journal expectedJournal = b.getJournal();

        //meccanismo ci sincronizzazione per attendere il completamento dell'operazione di addEntry
        CountDownLatch latch = new CountDownLatch(1);

        WriteCallback callBack = (rc, ledgerId, entryId, addr, ctx) -> {
            if (rc != BKException.Code.OK) {
                LOG.error("Failed to write entry: {}", BKException.getMessage(rc));
                latch.countDown();
                return;
            }

            //controlla che l'interazione tra il Bookie e il Journal sia avvenuta
            try {
                verify(expectedJournal, atLeastOnce()).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), eq("foo"));
            } catch(InterruptedException e)  {
                e.printStackTrace();
                Assert.fail("Exception thrown while checking bookie-journal interaction");
            }
            //inoltre tale interazione deve essere avvenuta secondo il comportamento previsto (scrittura della entry costruita in precedenza)
            assertEquals(1, ledgerId);
            assertEquals(0, entryId);

            latch.countDown();
        };

        byte[] masterKey = new byte[64];

        ByteBuf entry = buildEntry(1, 0, -1);

        b.addEntry(entry, false, callBack , "foo",  masterKey);

        latch.await();

        b.shutdown();
    }

    private ServerConfiguration createServerConfiguration() throws SocketException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        conf.setJournalFlushWhenQueueEmpty(true);
        // enable journal format version
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

    private WriteCallback getMockedCallBack() {
        return mock(WriteCallback.class);
    }

    private static ByteBuf buildEntry(long ledgerId, long entryId, long lastAddConfirmed) {
        final ByteBuf data = Unpooled.buffer();
        data.writeLong(ledgerId);
        data.writeLong(entryId);
        data.writeLong(lastAddConfirmed);
        return data;
    }

}
