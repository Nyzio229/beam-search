package com.example.search.runners;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public class PipelineOptionsDefs {
    public interface IndexerOptions extends PipelineOptions {
    @Description("Wejście dokumentów: ścieżka/pattern (np. data/docs/*.tsv)")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Katalog wyjściowy indeksu (np. data/index)")
    @Validation.Required
    String getIndexPath();
    void setIndexPath(String value);

    @Description("Ścieżka pliku z listą stopwords (opcjonalnie)")
    @Default.String("")
    String getStopwords();
    void setStopwords(String value);

    @Description("Liczba shardów dla wyjścia")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);
  }

  /** Opcje wyszukiwania. */
  public interface SearchOptions extends PipelineOptions {
    @Description("Ścieżka do indeksu (taki sam jak --indexPath w indeksowaniu)")
    @Validation.Required
    String getIndexPath();
    void setIndexPath(String value);

    @Description("Plik/plik(i) z zapytaniami: qId<TAB>query")
    @Validation.Required
    String getQueries();
    void setQueries(String value);

    @Description("Katalog wyjściowy wyników")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("Top-K wyników na zapytanie")
    @Default.Integer(10)
    Integer getTopK();
    void setTopK(Integer value);
  }
}
