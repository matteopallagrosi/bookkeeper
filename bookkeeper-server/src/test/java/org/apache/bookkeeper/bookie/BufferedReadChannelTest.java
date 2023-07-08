package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

@RunWith(value= Parameterized.class)
public class BufferedReadChannelTest {
    private static final String PATH = "src/test/resources/test_buffered_channel.txt";
    private static final byte[] TEST_BYTES = "test".getBytes();

    private static final int NUM_BYTES = TEST_BYTES.length;

    //parameters
    ByteBuf dest;
    long pos;
    int length;
    int expectedOutput; //numero di byte letti
    Exception expectedException;


    public BufferedReadChannelTest(Buffer dest_capacity , long position , int length , int output , Exception expectedException) {
        this.pos = position;
        this.length = length;

        switch(dest_capacity) {
            case SUFFICIENT: {
                this.dest = Unpooled.buffer(NUM_BYTES);
                break;
            }
            case NOT_SUFFICIENT: {
                this.dest = Unpooled.buffer(NUM_BYTES-1, NUM_BYTES - 1);
                break;
            }
            case NULL: {
                this.dest = null;
                break;
            }
        }

        this.expectedOutput = output;
        this.expectedException = expectedException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // dest, pos , length, expectedOutput, expectedException
                {Buffer.SUFFICIENT , 0 , -1 , 0 , null},
                {Buffer.SUFFICIENT , 0 , 0 , 0 , null},
                {Buffer.SUFFICIENT , 0 , NUM_BYTES , NUM_BYTES , null},
                {Buffer.SUFFICIENT , 0 , NUM_BYTES + 1 , NUM_BYTES , null},

                {Buffer.SUFFICIENT , -1 , -1 , 0 , null},
                {Buffer.SUFFICIENT , -1 , 0 , 0 , null},
                {Buffer.SUFFICIENT , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException()},
                {Buffer.SUFFICIENT , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException()},

                {Buffer.SUFFICIENT , NUM_BYTES - 1 , -1 , 0 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 0 , 0 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 1 , 1 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 2 , 1 , null},

                {Buffer.SUFFICIENT , NUM_BYTES , -1 , -1 , null},
                {Buffer.SUFFICIENT , NUM_BYTES , 0 , -1 , null},
                {Buffer.SUFFICIENT , NUM_BYTES , 1 , -1 , null},


                {Buffer.NOT_SUFFICIENT , 0 , -1 , 0 , null},
                {Buffer.NOT_SUFFICIENT , 0 , 0 , 0 , null},
                {Buffer.NOT_SUFFICIENT , 0 , NUM_BYTES , 0 , new IndexOutOfBoundsException()},
                {Buffer.NOT_SUFFICIENT , 0 , NUM_BYTES + 1 , 0 , new IndexOutOfBoundsException()},

                {Buffer.NOT_SUFFICIENT , -1 , -1 , 0 , null},
                {Buffer.NOT_SUFFICIENT , -1 , 0 , 0 , null},
                {Buffer.NOT_SUFFICIENT , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException()},
                {Buffer.NOT_SUFFICIENT , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException()},

                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , -1 , 0 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 0 , 0 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 1 , 1 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 2 , 1 , null},

                {Buffer.NOT_SUFFICIENT , NUM_BYTES , -1 , -1 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES , 0 , -1 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES , 1 , -1 , null},


                {Buffer.NULL , 0 , -1 , 0 , null},
                {Buffer.NULL , 0 , 0 , 0 , null},
                {Buffer.NULL , 0 , NUM_BYTES , 0 , new NullPointerException()},
                {Buffer.NULL , 0 , NUM_BYTES + 1 , 0 , new NullPointerException()},

                {Buffer.NULL , -1 , -1 , 0 , null},
                {Buffer.NULL , -1 , 0 , 0 , null},
                {Buffer.NULL , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException()},
                {Buffer.NULL , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException()},

                {Buffer.NULL , NUM_BYTES - 1 , -1 , 0 , null},
                {Buffer.NULL , NUM_BYTES - 1 , 0 , 0 , null},
                {Buffer.NULL , NUM_BYTES - 1 , 1 , 0 , new NullPointerException()},
                {Buffer.NULL , NUM_BYTES - 1 , 2 , 0 , new NullPointerException()},

                {Buffer.NULL , NUM_BYTES , -1 , -1 , null},
                {Buffer.NULL , NUM_BYTES , 0 , -1 , null},
                {Buffer.NULL , NUM_BYTES , 1 , -1 , null},
        });
    }

    enum Buffer {
       SUFFICIENT  , NOT_SUFFICIENT , NULL
    }


    @BeforeClass
    public static void setUpEnvironment() {
        try {
            writeFile();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Exception thrown while creating example file for testing purposes");
        }
    }

    public static void writeFile() throws IOException {
        //crea un file di test
        File file = new File(PATH);
        boolean fileCreated = file.createNewFile();
        assertTrue(fileCreated);

        //apre un FileChannel verso quel file
        FileChannel fc = new RandomAccessFile(file, "rw").getChannel();

        // Crea un byte buffer con i dati da scrivere
        ByteBuffer buffer = ByteBuffer.wrap(TEST_BYTES);

        int numWrite = fc.write(buffer);
        assertEquals(NUM_BYTES , numWrite);
        fc.close();
    }


    @Test
    public void readTest() {
        try (FileInputStream fis = new FileInputStream(PATH);
             FileChannel fileChannel = fis.getChannel()) {

            BufferedReadChannel readChannel = new BufferedReadChannel(fileChannel, (int) fileChannel.size());

            //testo la read
            int numRead = readChannel.read(this.dest, this.pos, this.length);

            //testo che il corretto numero di bytes sia stato letto
            assertEquals(expectedOutput, numRead);

            byte[] actualBytes = null;
            if (this.dest != null) {
                actualBytes = new byte[this.dest.readableBytes()];
                this.dest.readBytes(actualBytes);
            }

            if (numRead == NUM_BYTES) {
                //testo che i bytes inseriti siano stati letti correttamente
                assertArrayEquals(TEST_BYTES, actualBytes);
            }
        } catch (Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }
    }

    @AfterClass
    public static void clearEnvironment() {
        deleteTestFile();
    }


    public static void deleteTestFile() {
        File file = new File("src/test/resources/test_buffered_channel.txt");
        boolean fileDeleted = file.delete();
        assertTrue("Error while deleting test file" , fileDeleted);
    }
}
