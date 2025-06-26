%{
  configs: [
    %{
      name: "default",
      strict: true,
      checks: %{
        extra: [
          {Credo.Check.Refactor.LongQuoteBlocks, max_line_count: 200},
        ]
      }
    }
  ]
}
