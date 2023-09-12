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
    //output override method
    int overrideOutput;
    Exception overrideException;



    public BufferedReadChannelTest(Buffer dest_capacity , long position , int length , int output , Exception expectedException, int overrideOutput , Exception overrideException) {
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

        this.overrideOutput = overrideOutput;
        this.overrideException = overrideException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // dest, pos , length, expectedOutput, expectedException, overrideOutput, overrideException
                {Buffer.SUFFICIENT , 0 , -1, 0 , null, 0 , null},
                {Buffer.SUFFICIENT , 0 , 0 , 0 , null, 0 , null},
                {Buffer.SUFFICIENT , 0 , NUM_BYTES , NUM_BYTES , null, NUM_BYTES , null},
                {Buffer.SUFFICIENT , 0 , NUM_BYTES + 1 , NUM_BYTES , null, 0 , new IOException()}, //diverso

                {Buffer.SUFFICIENT , -1 , -1 , 0 , null , 0 , null},
                {Buffer.SUFFICIENT , -1 , 0 , 0 , null , 0 , null},
                {Buffer.SUFFICIENT , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException() , 0 , new IllegalArgumentException()},
                {Buffer.SUFFICIENT , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException() , 0  , new IllegalArgumentException()},

                {Buffer.SUFFICIENT , NUM_BYTES - 1 , -1 , 0 , null , 0 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 0 , 0 , null , 0 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 1 , 1 , null , 1 , null},
                {Buffer.SUFFICIENT , NUM_BYTES - 1 , 2 , 1 , null, 0 , new IOException()}, //diverso

                {Buffer.SUFFICIENT , NUM_BYTES , -1 , -1 , null , 0 , new IOException()}, //diverso
                {Buffer.SUFFICIENT , NUM_BYTES , 0 , -1 , null , 0 , new IOException()},  //diverso
                {Buffer.SUFFICIENT , NUM_BYTES , 1 , -1 , null , 0 , new IOException()},  //diverso


                {Buffer.NOT_SUFFICIENT , 0 , -1 , 0 , null , 0  , null},
                {Buffer.NOT_SUFFICIENT , 0 , 0 , 0 , null , 0 , null},
                {Buffer.NOT_SUFFICIENT , 0 , NUM_BYTES , 0 , new IndexOutOfBoundsException() , 0 , new IOException()},   //diverso
                {Buffer.NOT_SUFFICIENT , 0 , NUM_BYTES + 1 , 0 , new IndexOutOfBoundsException() , 0 , new IOException()}, //diverso

                {Buffer.NOT_SUFFICIENT , -1 , -1 , 0 , null , 0 , null},
                {Buffer.NOT_SUFFICIENT , -1 , 0 , 0 , null , 0 , null},
                {Buffer.NOT_SUFFICIENT , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException() , 0 , new IllegalArgumentException()},
                {Buffer.NOT_SUFFICIENT , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException() , 0 , new IllegalArgumentException()},

                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , -1 , 0 , null , 0 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 0 , 0 , null , 0 ,null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 1 , 1 , null , 1 , null},
                {Buffer.NOT_SUFFICIENT , NUM_BYTES - 1 , 2 , 1 , null , 0 , new IOException()}, //diverso

                {Buffer.NOT_SUFFICIENT , NUM_BYTES , -1 , -1 , null , 0 , null}, //prima controlla la length e poi la posizione nel caso override
                {Buffer.NOT_SUFFICIENT , NUM_BYTES , 0 , -1 , null , 0 , null}, //prima controlla la length e poi la posizione nel caso override
                {Buffer.NOT_SUFFICIENT , NUM_BYTES , 1 , -1 , null , 0 , new IOException()}, //diverso


                {Buffer.NULL , 0 , -1 , 0 , null , 0, null},
                {Buffer.NULL , 0 , 0 , 0 , null , 0 , null},
                {Buffer.NULL , 0 , NUM_BYTES , 0 , new NullPointerException() , 0, new NullPointerException()},
                {Buffer.NULL , 0 , NUM_BYTES + 1 , 0 , new NullPointerException() , 0 , new NullPointerException()},

                {Buffer.NULL , -1 , -1 , 0 , null, 0 , null},
                {Buffer.NULL , -1 , 0 , 0 , null , 0 , null},
                {Buffer.NULL , -1 , NUM_BYTES + 1 , 0 , new IllegalArgumentException() , 0 , new IllegalArgumentException()},
                {Buffer.NULL , -1 , NUM_BYTES + 2 , 0 , new IllegalArgumentException() , 0 , new IllegalArgumentException()},

                {Buffer.NULL , NUM_BYTES - 1 , -1 , 0 , null , 0 , null},
                {Buffer.NULL , NUM_BYTES - 1 , 0 , 0 , null , 0 , null},
                {Buffer.NULL , NUM_BYTES - 1 , 1 , 0 , new NullPointerException() , 0 , new NullPointerException()},
                {Buffer.NULL , NUM_BYTES - 1 , 2 , 0 , new NullPointerException() , 0 , new NullPointerException()},

                {Buffer.NULL , NUM_BYTES , -1 , -1 , null , 0 , null}, //nel caso override controlla prima length < 0
                {Buffer.NULL , NUM_BYTES , 0 , -1 , null , 0 , null},  //nel caso override controlla prima length < 0
                {Buffer.NULL , NUM_BYTES , 1 , -1 , null , 0 , new NullPointerException()}, //nel caso override non controlla pos > EOF
        });
    }

    enum Buffer {
       SUFFICIENT  , NOT_SUFFICIENT , NULL
    }

    enum startPosition {
        DEFAULT, DIFFERENT
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

    //aggiunta per aumentare coverage
    //mi aspetto che il comportamento sia il medesimo del test precedente (anche se cambio start position, essendo non superiore all'EOF,
    //il comportamento previsto è che venga riportata pari alla current position)
    @Test
    public void readDifferentStartPositionTest() {
        try (FileInputStream fis = new FileInputStream(PATH);
             FileChannel fileChannel = fis.getChannel()) {

            BufferedReadChannel readChannel = new BufferedReadChannel(fileChannel, (int) fileChannel.size());

            readChannel.readBufferStartPosition = 1;

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

    //testo l'ovveride del metodo read() in BufferedChannel (il comportamento atteso è diverso con alcune configurazioni)
    @Test
    public void readWriteBufferTest() {
        try  {
            File file = new File(PATH);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();


            BufferedChannel channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT , fileChannel , 100);

            //scrive dei byte sul buffer
            ByteBuf src = Unpooled.buffer(100);
            src.writeBytes("test".getBytes());

            channel.write(src);

            //testo la read
            int numRead = channel.read(this.dest, this.pos, this.length);

            //testo che il corretto numero di bytes sia stato letto
            assertEquals(overrideOutput, numRead);

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
            e.printStackTrace();
            assertThat(e, instanceOf(overrideException.getClass()));
        }
    }

    @AfterClass
    public static void clearEnvironment() {
        deleteTestFile();
    }

    public static void deleteTestFile() {
        File file = new File(PATH);
        boolean fileDeleted = file.delete();
        assertTrue("Error while deleting test file" , fileDeleted);
    }
}
