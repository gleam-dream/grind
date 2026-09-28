# This check uses only Elixir/OTP; neither package needs to be built.
# It validates historical ledger references without claiming that every
# referenced test was paired or executed in this invocation.
root = Path.expand("..", __DIR__)

{ledger_path, catalog_path} =
  case System.argv() do
    [] -> {Path.join(root, "oracle/ORACLE-LEDGER.md"), Path.join(root, "oracle/scenarios.json")}
    [ledger_path, catalog_path] -> {ledger_path, catalog_path}
    _ -> raise ArgumentError, "expected no arguments, or ledger and catalog fixture paths"
  end

ledger = File.read!(ledger_path)

sources =
  Enum.flat_map(["test", "consumer/test"], &Path.wildcard(Path.join([root, &1, "**/*.gleam"])))

functions =
  for path <- sources,
      [_, name] <- Regex.scan(~r/\bpub fn ([a-z][a-z0-9_]*_test)\s*\(/, File.read!(path)),
      into: MapSet.new(),
      do: name

rows =
  ledger
  |> String.split("\n")
  |> Enum.filter(&String.starts_with?(&1, "|"))
  |> Enum.map(fn line ->
    line |> String.split("|") |> Enum.drop(1) |> Enum.drop(-1) |> Enum.map(&String.trim/1)
  end)
  |> Enum.reject(fn [category | _] ->
    category == "Category" or String.starts_with?(category, "-")
  end)

errors =
  Enum.flat_map(rows, fn row ->
    if length(row) != 7 do
      ["ledger row has #{length(row)} columns rather than seven"]
    else
      [category, upstream, _trigger, _observations, local, _normalization, _difference] = row

      categories = [
        "faithful port",
        "paired execution",
        "inspired by upstream",
        "original Grind contract",
        "unverified"
      ]

      names =
        Regex.scan(~r/`([a-z][a-z0-9_]*_test)`/, local, capture: :all_but_first) |> List.flatten()

      missing =
        Enum.reject(names, &MapSet.member?(functions, &1))
        |> Enum.map(&"missing local test: #{&1}")

      missing =
        if category in categories,
          do: missing,
          else: ["unknown evidence category: #{category}" | missing]

      missing =
        if names == [] and category != "unverified",
          do: ["#{category} row has no executable local test reference" | missing],
          else: missing

      paths =
        Regex.scan(~r/`([^`]+\.(?:exs|ex|gleam|md))`/, upstream, capture: :all_but_first)
        |> List.flatten()

      missing ++
        Enum.flat_map(paths, fn path ->
          resolved =
            if String.starts_with?(path, ["lib/", "test/oban/"]),
              do: "oracle/deps/oban/" <> path,
              else: path

          if File.regular?(Path.join(root, resolved)),
            do: [],
            else: ["missing upstream source: #{path}"]
        end)
    end
  end)

errors = if rows == [], do: ["ledger has no evidence rows" | errors], else: errors

catalog = File.read!(catalog_path) |> :json.decode()

catalog_paths =
  Enum.flat_map(catalog["scenarios"] ++ catalog["gaps"], fn entry ->
    entry["sources"] ++ Enum.reject([entry["grind_adapter"], entry["oban_adapter"]], &is_nil/1)
  end)

errors =
  errors ++
    Enum.flat_map(Enum.uniq(catalog_paths), fn path ->
      if File.regular?(Path.join(root, path)), do: [], else: ["missing catalog source: #{path}"]
    end)

{commit, status} =
  System.cmd("git", ["-C", Path.join(root, "oracle/deps/oban"), "rev-parse", "HEAD"])

errors =
  if status == 0 and String.trim(commit) == catalog["oracle"]["commit"],
    do: errors,
    else: ["Oban checkout does not match the catalog pin" | errors]

{dirty, dirty_status} =
  System.cmd("git", ["-C", Path.join(root, "oracle/deps/oban"), "status", "--porcelain"])

errors =
  if dirty_status == 0 and String.trim(dirty) == "",
    do: errors,
    else: ["Oban checkout has local modifications; pinned oracle requires clean source" | errors]

if errors != [] do
  Enum.each(Enum.uniq(errors), &IO.puts(:stderr, &1))
  System.halt(1)
end

IO.puts(
  "Oracle ledger: #{length(rows)} rows and #{length(catalog["scenarios"])} paired scenario references resolved"
)
