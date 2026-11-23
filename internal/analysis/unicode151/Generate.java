import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.util.VersionInfo;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

// java -cp icu4j-75.1.jar Generate.java tables.gz
class Generate {
    public static void main(String[] args) throws Exception {
        if (!VersionInfo.ICU_VERSION.equals(VersionInfo.getInstance(75,1))
                || !UCharacter.getUnicodeVersion().equals(VersionInfo.getInstance(15,1)))
            throw new IllegalStateException("ICU4J 75.1 with UCD 15.1 required");
        Normalizer2 nfc = Normalizer2.getNFCInstance();
        try (var out = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(
                new FileOutputStream(args[0])), StandardCharsets.US_ASCII))) {
            for (int c = 0; c <= Character.MAX_CODE_POINT; c++) {
                String s = Character.toString(c);
                String fold = UCharacter.foldCase(s, true);
                if (!s.equals(fold)) row(out, "F", c, fold.codePoints().toArray());
                String d = nfc.getDecomposition(c);
                if (d != null) row(out, "D", c, d.codePoints().toArray());
                String raw = nfc.getRawDecomposition(c);
                if (raw != null) {
                    int[] pair = raw.codePoints().toArray();
                    if (pair.length == 2 && nfc.composePair(pair[0], pair[1]) == c)
                        row(out, "P", c, pair);
                }
                int cc = nfc.getCombiningClass(c);
                if (cc != 0) row(out, "C", c, cc);
                if (UCharacter.isUWhiteSpace(c)) row(out, "W", c);
            }
            if (out.checkError()) throw new IOException("writing Unicode tables");
        }
    }
    static void row(PrintWriter out, String kind, int c, int... values) {
        out.print(kind + " " + c);
        for (int value : values) out.print(" " + value);
        out.print('\n');
    }
}
