import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public class Preprocess {

    private static final Pattern TZ_PAREN = Pattern.compile("\\s*\\([^)]*\\)\\s*$");

    // 列下标（CSV 列顺序固定）：Message-ID,Date,From,To,Subject,Message,Cc,Bcc
    private static final int DATE = 1, FROM = 2, TO = 3, CC = 6;

    public static void main(String[] args) throws Exception {
        String inPath  = args.length > 0 ? args[0] : "enron_sample_1000.csv";
        String edgeOut = args.length > 1 ? args[1] : "clean_edges.tsv";
        String nodeOut = args.length > 2 ? args[2] : "persons.txt";

        Set<String> persons = new HashSet<>();
        int emailCount = 0, edgeCount = 0, skipped = 0, naDate = 0;

        try (Reader reader = new InputStreamReader(
                     new FileInputStream(inPath), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader);   // 默认引号是 "，能正确处理多行正文
             BufferedWriter ew = new BufferedWriter(new FileWriter(edgeOut))) {

            boolean first = true;
            for (CSVRecord rec : parser) {
                if (first) { first = false; continue; }   // 跳过表头行

                String from = safe(rec, FROM).toLowerCase();
                String to   = safe(rec, TO);
                String cc   = safe(rec, CC);
                String date = safe(rec, DATE);
                // Bcc 是 Cc 副本，忽略

                if (from.isEmpty() || to.trim().isEmpty()) { skipped++; continue; }
                emailCount++;

                String ym = parseYearMonth(date);
                if ("NA".equals(ym)) naDate++;
                persons.add(from);

                edgeCount += writeEdges(ew, from, to, "to", ym, persons);
                edgeCount += writeEdges(ew, from, cc, "cc", ym, persons);
            }
        }

        try (BufferedWriter nw = new BufferedWriter(new FileWriter(nodeOut))) {
            for (String p : persons) { nw.write(p); nw.newLine(); }
        }

        System.out.println("有效邮件数 emailCount = " + emailCount);
        System.out.println("跳过(缺From/To)       = " + skipped);
        System.out.println("生成边数 edgeCount    = " + edgeCount);
        System.out.println("去重人数(节点)        = " + persons.size());
        System.out.println("日期解析失败(NA)      = " + naDate);
    }

    private static int writeEdges(BufferedWriter w, String from, String field,
                                  String type, String ym, Set<String> persons)
            throws IOException {
        if (field == null || field.trim().isEmpty()) return 0;
        int n = 0;
        for (String raw : field.split("[,;]")) {
            String dst = raw.trim().toLowerCase();
            if (dst.isEmpty() || dst.equals(from)) continue;
            persons.add(dst);
            w.write(from + "\t" + dst + "\t" + type + "\t" + ym);
            w.newLine();
            n++;
        }
        return n;
    }

    private static String safe(CSVRecord rec, int idx) {
        if (idx >= rec.size()) return "";
        String v = rec.get(idx);
        return v == null ? "" : v.trim();
    }

    private static String parseYearMonth(String date) {
        if (date == null || date.trim().isEmpty()) return "NA";
        String cleaned = TZ_PAREN.matcher(date.trim()).replaceAll("");
        SimpleDateFormat in  = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US);
        SimpleDateFormat out = new SimpleDateFormat("yyyy-MM", Locale.US);
        try {
            Date d = in.parse(cleaned);
            return out.format(d);
        } catch (Exception e) {
            return "NA";
        }
    }
}