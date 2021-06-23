package au.org.ala.common.beam;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CsvReader implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(CsvReader.class);
    private long recordsReturned;
    private List<String> current;
    private final CSVReader csvReader;
    private final Iterator<String[]> iterator;

    public static CsvReader fromLocation(String path) throws IOException {
        return new CsvReader(CsvFiles.fromLocation(Paths.get(path)));
    }

    public static CsvReader fromCompressed(String path) throws IOException {
        return new CsvReader(CsvFiles.fromCompressed(Paths.get(path)));
    }

    private CsvReader(Reader reader) {
        this.csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build();
        this.iterator = this.csvReader.iterator();
    }

    public boolean hasNext() {
        return this.csvReader.iterator().hasNext();
    }

    public boolean advance() {
        if (!this.iterator.hasNext()) {
            return false;
        } else {
            ++this.recordsReturned;
            if (this.recordsReturned % 10000L == 0L) {
                log.info("Read [{}] records", this.recordsReturned);
            }

            this.current = Arrays.asList(this.iterator.next());
            return true;
        }
    }

    public List<String> getCurrent() {
        if (this.current == null) {
            throw new NoSuchElementException("No current record found (Hint: did you init() the reader?)");
        } else {
            return this.current;
        }
    }

    public long getRecordsReturned() {
        return this.recordsReturned;
    }

    public void close() throws IOException {
        if (this.csvReader != null) {
            log.info("Closing Csv reader having read [{}] records", this.recordsReturned);
            this.csvReader.close();
        }
    }
}
