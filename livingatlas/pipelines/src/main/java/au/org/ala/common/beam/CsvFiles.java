package au.org.ala.common.beam;

import org.gbif.dwc.UnsupportedArchiveException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

public class CsvFiles {
    private CsvFiles() {
    }

    public static Reader fromLocation(Path csvLocation) throws IOException, UnsupportedArchiveException {
        return new BufferedReader(new InputStreamReader(Files.newInputStream(csvLocation), StandardCharsets.UTF_8));
    }

    public static Reader fromCompressed(Path csvLocation) throws IOException, UnsupportedArchiveException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(csvLocation)), StandardCharsets.UTF_8));
    }
}
