%{
  configs: [
    %{
      name: "default",
      strict: false,
      checks: %{
        extra: [
          {Credo.Check.Refactor.LongQuoteBlocks, max_line_count: 200},
        ]
      }
    }
  ]
}
