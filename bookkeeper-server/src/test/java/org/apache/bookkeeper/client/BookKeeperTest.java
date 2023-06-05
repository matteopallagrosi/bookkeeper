package org.apache.bookkeeper.client;

import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BookKeeperTest {

    //test di prova
    @Test
    public void asyncCreateLedgerIllegalArgument() {
        BookKeeper client = new BookKeeper();
        assertThrows(IllegalArgumentException.class , () -> {
            client.asyncCreateLedger(1, 2, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, null, null);
        });
    }

    @Test
    public void methodTest() {
        BookKeeper client = new BookKeeper();
        client.method();
    }


}
