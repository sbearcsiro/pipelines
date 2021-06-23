package au.org.ala.common.beam;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.directory.api.util.Strings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CsvIO {

    private static class BoundedCsvReader extends BoundedSource.BoundedReader<List<String>> {
        private final Counter csvCount;
        private final CsvSource source;
        private CsvReader reader;

        private BoundedCsvReader(CsvIO.CsvSource source) {
            this.csvCount = Metrics.counter("CsvIO", "stringToListStringCount");
            this.source = source;
        }

        public boolean start() throws IOException {
            this.reader = this.source.read.unCompressed ? CsvReader.fromLocation(this.source.read.path) : CsvReader.fromCompressed(this.source.read.path);
            return this.reader.advance();
        }

        public boolean advance() {
            this.csvCount.inc();
            return this.reader.advance();
        }

        public List<String> getCurrent() {
            return this.reader.getCurrent();
        }

        public void close() throws IOException {
            this.reader.close();
        }

        public BoundedSource<List<String>> getCurrentSource() {
            return this.source;
        }
    }

    private static class CsvSource extends BoundedSource<List<String>> {
        private final CsvIO.Read read;

        public Coder<List<String>> getOutputCoder() {
            return ListCoder.of(StringUtf8Coder.of());
        }

        public List<? extends BoundedSource<List<String>>> split(long desiredBundleSizeBytes, PipelineOptions options) {
            return Collections.singletonList(this);
        }

        public long getEstimatedSizeBytes(PipelineOptions options) {
            return 0L;
        }

        public BoundedReader<List<String>> createReader(PipelineOptions options) {
            return new CsvIO.BoundedCsvReader(this);
        }

        CsvSource(CsvIO.Read read) {
            this.read = read;
        }
    }

    public static class Read extends PTransform<PBegin, PCollection<List<String>>> {
        private final String path;
        private final boolean unCompressed;

        public static CsvIO.Read fromLocation(String file) {
            return new CsvIO.Read(file);
        }

        public static CsvIO.Read fromCompressed(String file) {
            return new CsvIO.Read(file, false);
        }

        private Read(String filePath) {
            this(filePath, true);
        }

        private Read(String filePath, boolean unCompressed) {
            this.path = filePath;
            this.unCompressed = unCompressed;
        }

        public PCollection<List<String>> expand(PBegin input) {
            CsvIO.CsvSource source = new CsvIO.CsvSource(this);
            return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
        }

        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            if (Strings.isNotEmpty(this.path)) {
                builder.add(DisplayData.item("Csv Path", this.path));
            }

        }
    }
}

