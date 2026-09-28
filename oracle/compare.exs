alias GrindOracle.Comparison

[catalog_path, grind_path, oban_path, provenance_path] = System.argv()
provenance = Comparison.read_json!(provenance_path)

digest = fn path ->
  path |> File.read!() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
end

true = provenance["catalog_sha256"] == digest.(catalog_path)
grind = Comparison.read_results!(grind_path)
oban = Comparison.read_results!(oban_path)
true = Enum.all?(grind ++ oban, &(&1["run_id"] == provenance["run_id"]))

# A saved, completed artifact additionally binds the exact output bytes.
# During the live run the exit trap fills these hashes after comparison.
if provenance["status"] == "passed" do
  true = provenance["artifact_sha256"]["grind.jsonl"] == digest.(grind_path)
  true = provenance["artifact_sha256"]["oban.jsonl"] == digest.(oban_path)
else
  "started" = provenance["status"]
  true = System.get_env("GRIND_ORACLE_RUN_ID") == provenance["run_id"]
end

count =
  Comparison.compare!(
    Comparison.read_json!(catalog_path),
    grind,
    oban
  )

IO.puts("Paired oracle: #{count} scenarios compared from both committed result streams")
