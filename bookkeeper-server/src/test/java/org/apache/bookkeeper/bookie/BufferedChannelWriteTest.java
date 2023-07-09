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
public class BufferedChannelWriteTest {
    private static final String PATH = "src/test/resources/test_buffered_channel.txt";

    //parameters
    ByteBuf src;
    byte[] expectedOutput;
    Exception expectedException;

    public BufferedChannelWriteTest(Buffer src , byte[] expectedOutput , Exception expectedException) {
        switch (src) {
            case VALID: {
                this.src = Unpooled.buffer(100);
                this.src.writeBytes("test".getBytes());
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

        this.expectedOutput = expectedOutput;
        this.expectedException = expectedException;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //src , expectedOutput , expectedException
                {Buffer.VALID , "test".getBytes(), null},
                {Buffer.EMPTY , "".getBytes() , null},
                {Buffer.NULL , null , new NullPointerException()}
        });
    }

    enum Buffer{
        VALID , EMPTY , NULL
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
        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {

            BufferedChannel channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT , fileChannel , 100);

            channel.write(this.src);

            //forza la scrittura su file così da poter testare il risultato della write
            channel.flush();

            //verifica il contenuto scritto dalla write
            fileChannel.position(0);
            ByteBuffer result = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(result);
            byte[] actualOutput = result.array();

            assertArrayEquals(this.expectedOutput, actualOutput);


        } catch (Exception e) {
            assertThat(e, instanceOf(expectedException.getClass()));
        }
    }

    @After
    public void clearFile() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(PATH))) {
            // Clears the content of the file by writing an empty string
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
