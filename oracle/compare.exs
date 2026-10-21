alias GrindOracle.Comparison

[catalog_path, grind_path, oban_path] = System.argv()
grind = Comparison.read_results!(grind_path)
oban = Comparison.read_results!(oban_path)

count =
  Comparison.compare!(
    Comparison.read_json!(catalog_path),
    grind,
    oban
  )

IO.puts("Paired oracle: #{count} scenarios compared from both committed result streams")
