package com.example.search.pipelines;

import static java.util.stream.Collectors.toList;

import com.example.search.runners.PipelineOptionsDefs.IndexerOptions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;            
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.GroupByKey;   // <--
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TupleTag;         // <-- (jeśli zostawiasz TupleTag)

public class IndexerPipeline {
    static class TokenizeFn extends DoFn<KV<String, String>, KV<String, String>> {
    private final Set<String> stop;
    private static final Pattern SPLIT = Pattern.compile("[^\\p{L}\\p{N}]+");

    TokenizeFn(Set<String> stop) {
      this.stop = stop;
    }

    @ProcessElement
    public void process(@Element KV<String, String> doc, OutputReceiver<KV<String, String>> out) {
      String docId = doc.getKey();
      String text = doc.getValue().toLowerCase(Locale.ROOT);
      for (String tok : SPLIT.split(text)) {
        if (tok.isEmpty()) continue;
        if (!stop.isEmpty() && stop.contains(tok)) continue;
        out.output(KV.of(tok, docId));
      }
    }
  }

  /** Wczytanie stoplisty lokalnie/ze storage (opcjonalnie). */
  private static Set<String> readStopwords(String path) {
    if (path == null || path.isEmpty()) return Collections.emptySet();
    try {
      FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
      try (BufferedReader br =
          new BufferedReader(new InputStreamReader(Channels.newInputStream(FileSystems.open(FileSystems.matchSingleFileSpec(path).resourceId()))))) {
        Set<String> s = new HashSet<>();
        for (String line; (line = br.readLine()) != null; ) {
          line = line.trim().toLowerCase(Locale.ROOT);
          if (!line.isEmpty() && !line.startsWith("#")) s.add(line);
        }
        return s;
      }
    } catch (Exception e) {
      System.err.println("WARN: nie udało się wczytać stopwords: " + e.getMessage());
      return Collections.emptySet();
    }
  }

  /** Format linii indeksu: term \t df=...;postings=doc1:tf,doc2:tf2 */
  static class FormatPostingLine extends DoFn<KV<String, Iterable<KV<String, Long>>>, String> {
    @ProcessElement
    public void process(@Element KV<String, Iterable<KV<String, Long>>> e, OutputReceiver<String> out) {
      String term = e.getKey();
      List<KV<String, Long>> postings = new ArrayList<>();
      e.getValue().forEach(postings::add);
      int df = postings.size();
      StringBuilder sb = new StringBuilder();
      sb.append(term).append('\t');
      sb.append("df=").append(df).append(";postings=");
      for (int i = 0; i < postings.size(); i++) {
        KV<String, Long> p = postings.get(i);
        if (i > 0) sb.append(',');
        sb.append(p.getKey()).append(':').append(p.getValue());
      }
      out.output(sb.toString());
    }
  }

  /** Parsuje wejście TSV do par (docId, tekst). */
  static class ParseDocTsv extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void process(@Element String line, OutputReceiver<KV<String, String>> out) {
      int tab = line.indexOf('\t');
      if (tab <= 0) return; // pomiń linie niepoprawne
      String docId = line.substring(0, tab).trim();
      String text  = line.substring(tab + 1).trim();
      if (!docId.isEmpty() && !text.isEmpty()) {
        out.output(KV.of(docId, text));
      }
    }
  }

  public static void main(String[] args) {
    IndexerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerOptions.class);
    Pipeline p = Pipeline.create(options);

    // 1) Wczytaj dokumenty: docId \t tekst
    PCollection<KV<String, String>> docs =
        p.apply("ReadDocs", TextIO.read().from(options.getInput()))
         .apply("ParseTSV", ParDo.of(new ParseDocTsv()))
         .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // 2) Policz N (liczbę unikalnych dokumentów)
    PCollection<Long> docCount =
        docs.apply("OnlyDocIds", Keys.<String>create())
            .apply("DistinctDocIds", Distinct.<String>create())
            .apply("CountDocs", Count.globally());

    // 3) Tokenizacja -> (term, docId)
    Set<String> stop = readStopwords(options.getStopwords());
    PCollection<KV<String, String>> termDoc =
        docs.apply("Tokenize", ParDo.of(new TokenizeFn(stop)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // 4) Policz TF: (term, docId) -> (term, (docId, tf))
    final TupleTag<Long> ONES = new TupleTag<>();
    PCollection<KV<KV<String, String>, Long>> termDocOne =
        termDoc.apply("AttachOne", MapElements.via(new SimpleFunction<KV<String, String>, KV<KV<String, String>, Long>>() {
          @Override
          public KV<KV<String, String>, Long> apply(KV<String, String> input) {
            return KV.of(KV.of(input.getKey(), input.getValue()), 1L);
          }
        }));
    PCollection<KV<KV<String, String>, Long>> termDocTf =
        termDocOne.apply("CountPerTermDoc", Sum.longsPerKey());

    // 5) Zgrupuj po termie: term -> Iterable(docId, tf)
    PCollection<KV<String, Iterable<KV<String, Long>>>> inverted =
        termDocTf
            .apply("KeyByTerm", MapElements.via(new SimpleFunction<KV<KV<String, String>, Long>, KV<String, KV<String, Long>>>() {
              @Override
              public KV<String, KV<String, Long>> apply(KV<KV<String, String>, Long> in) {
                return KV.of(in.getKey().getKey(), KV.of(in.getKey().getValue(), in.getValue()));
              }
            }))
            .apply("GroupByTerm", GroupByKey.create());

    // 6) Zapis indeksu w formacie tekstowym oraz N do metadata
    inverted
        .apply("FormatPostingLines", ParDo.of(new FormatPostingLine()))
        .apply("WriteIndex", TextIO.write()
            .to(options.getIndexPath() + "/postings")
            .withSuffix(".txt")
            .withNumShards(options.getNumShards()));

    docCount
        .apply("FormatN", MapElements.into(TypeDescriptors.strings()).via(n -> Long.toString(n)))
        .apply("WriteDocCount", TextIO.write()
            .to(options.getIndexPath() + "/metadata/doc_count")
            .withSuffix(".txt")
            .withNumShards(1));

    p.run().waitUntilFinish();
  }
}
