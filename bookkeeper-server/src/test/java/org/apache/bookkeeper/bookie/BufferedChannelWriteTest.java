package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value= Parameterized.class)
public class BufferedChannelWriteTest {
    private static final String PATH = "src/test/resources/test_buffered_channel.txt";
    private static final byte[] TEST_BYTES = "test".getBytes();
    //parameters
    private ByteBuf src;
    private byte[] expectedOutput;
    private Exception expectedException;
    private int capacity;
    private long startPosition;

    public BufferedChannelWriteTest(Buffer src , Capacity capacityType, long startPosition, byte[] expectedOutput , Exception expectedException) {
        switch (src) {
            case VALID: {
                this.src = Unpooled.buffer(100);
                this.src.writeBytes(TEST_BYTES);
                break;
            }
            case EMPTY: {
                this.src = Unpooled.buffer(100);
                this.src.writeBytes(new byte[0]);
                break;
            }
            case NULL: {
                this.src = null;
                break;
            }
        }

        switch(capacityType) {
            case SUFFICIENT: {
                this.capacity = TEST_BYTES.length;
                break;
            }
            case NOT_SUFFICIENT: {
                this.capacity = TEST_BYTES.length - 1;
                break;
            }
            case NEGATIVE: {
                this.capacity = - 1;
                break;
            }
        }

        this.startPosition = startPosition;
        this.expectedOutput = expectedOutput;
        this.expectedException = expectedException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //src ,capacity (aggiunta in seguito), startPosition (aggiunto in seguito), expectedOutput, expectedException
                //aggiungo la capacità da allocare al writebuffer e la startPosition come ulteriori parametri di input al test per migliorare mutation coverage
                {Buffer.VALID , Capacity.SUFFICIENT, 0,  TEST_BYTES, null},
                {Buffer.EMPTY , Capacity.SUFFICIENT, 0, "".getBytes(), null},
                {Buffer.NULL , Capacity.SUFFICIENT, 0, null, new NullPointerException()},

                //casi di test aggiunti per aumentare coverage
                {Buffer.VALID, Capacity.NOT_SUFFICIENT, 0, TEST_BYTES, null}, //quando il buffer si riempe, il suo contenuto dovrebbe essere flushato per recuperare spazio, in modo che la scrittura vada comunque a buon fine
                {Buffer.VALID, Capacity.NEGATIVE, 0, TEST_BYTES, new IllegalArgumentException()},
                {Buffer.VALID , Capacity.SUFFICIENT, 1,  TEST_BYTES, null}, //testo la scrittura da una posizione iniziale diversa da 0 (positiva)
                {Buffer.VALID , Capacity.SUFFICIENT, -1,  TEST_BYTES, new IllegalArgumentException()} //testo la scrittura da una posizione iniziale diversa da 0 (negativa)
        });
    }

    enum Buffer{
        VALID , EMPTY , NULL
    }
    enum Capacity {
        SUFFICIENT, NOT_SUFFICIENT, NEGATIVE
    }


    @BeforeClass
    public static void setUpTestFile() {
        //crea un file di test
        File file = new File(PATH);
        try {
            boolean fileCreated = file.createNewFile();
            assertTrue(fileCreated);
        } catch (IOException e) {
            Assert.fail("Exception thrown while creating test file");
        }
    }

    @Test
    public void writeTest() {
        File file = new File(PATH);
        try (FileChannel fileChannel = spy(new RandomAccessFile(file, "rw").getChannel())) {

            //setto la posizione iniziale di scrittura (aggiunta per aumentare coverage)
            fileChannel.position(startPosition);

            BufferedChannel channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT , fileChannel , capacity);

            //verifico che il canale bufferizzato abbia correttamente registrato la startPosition (aggiunta successiva)
            assertEquals(this.startPosition, channel.getFileChannelPosition());

            //eseguo la write
            channel.write(this.src);

            //assert aggiunto per aumentare coverage
            //verfica che l'operazione di write abbia correttamente aggiornato la posizione a cui avverrà la prossima scrittura
            assertEquals(startPosition + expectedOutput.length, channel.position());

            //forza la scrittura su file così da poter testare il risultato della write
            channel.flush();

            //assert aggiunto per aumentare coverage
            //dopo la flush il puntatore al fileChannel dovrebbe essere stato aggiornato correttamente
            assertEquals(startPosition + expectedOutput.length, channel.getFileChannelPosition());

            //verifica il contenuto scritto dalla write
            fileChannel.position(startPosition);
            ByteBuffer result = ByteBuffer.allocate((int) (fileChannel.size()-startPosition));
            fileChannel.read(result);
            byte[] actualOutput = result.array();

            assertArrayEquals(this.expectedOutput, actualOutput);

            channel.close();

            //aggiunta successiva per aumentare mutation coverage
            //verifica che il canale wrappato nel bufferedChannel sia stato effettivamente chiuso
            verify(fileChannel, atLeastOnce()).close();

        } catch (Exception e) {
            e.printStackTrace();
            assertThat(e, instanceOf(expectedException.getClass()));
        }
    }

    @After
    public void clearFile() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(PATH))) {
            // Pulisce il contenuto del file dopo ogni test flushando una stringa vuota
            writer.print("");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void deleteTestFile() {
        File file = new File(PATH);
        boolean fileDeleted = file.delete();
        assertTrue("Error while deleting test file" , fileDeleted);
    }
}
