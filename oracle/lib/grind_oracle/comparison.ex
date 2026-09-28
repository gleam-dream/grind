defmodule GrindOracle.Comparison do
  @moduledoc "Strict comparison of two complete executions of the versioned scenario catalog."
  @classifications [
    "equivalent",
    "intentional-grind-semantics",
    "missing-accidental-divergence",
    "deferred"
  ]

  def compare!(catalog, grind, oban) do
    validate_catalog!(catalog)
    expected_ids = catalog["scenarios"] |> Enum.map(& &1["id"]) |> MapSet.new()
    grind = index!(grind, "grind", catalog["version"], expected_ids)
    oban = index!(oban, "oban", catalog["version"], expected_ids)
    run_ids = Enum.map(Map.values(grind) ++ Map.values(oban), & &1["run_id"]) |> Enum.uniq()

    require!(
      match?([id] when is_binary(id) and id != "", run_ids),
      "outputs do not belong to one paired run"
    )

    Enum.each(catalog["scenarios"], fn scenario ->
      id = scenario["id"]
      left = grind[id]["result"]
      right = oban[id]["result"]

      require!(
        left === scenario["expected"]["grind"],
        "#{id}: Grind output differs from catalog: #{inspect(left)}"
      )

      require!(
        right === scenario["expected"]["oban"],
        "#{id}: Oban output differs from catalog: #{inspect(right)}"
      )

      if scenario["classification"] == "equivalent" do
        require!(left === right, "#{id}: paired outputs diverged")
      end
    end)

    length(catalog["scenarios"])
  end

  def validate_catalog!(catalog) do
    require!(catalog["version"] == 1, "unsupported catalog version")

    require!(
      is_list(catalog["scenarios"]) and catalog["scenarios"] != [],
      "empty scenario catalog"
    )

    entries = catalog["scenarios"] ++ Map.fetch!(catalog, "gaps")
    ids = Enum.map(entries, & &1["id"])
    require!(Enum.all?(ids, &(is_binary(&1) and &1 != "")), "missing scenario id")
    require!(length(Enum.uniq(ids)) == length(ids), "duplicate scenario id")

    Enum.each(entries, fn entry ->
      require!(
        entry["classification"] in @classifications,
        "#{entry["id"]}: unknown classification"
      )

      require!(
        is_binary(entry["reason"]) and entry["reason"] != "",
        "#{entry["id"]}: missing justification"
      )

      require!(
        is_list(entry["sources"]) and entry["sources"] != [],
        "#{entry["id"]}: missing source references"
      )
    end)

    Enum.each(catalog["scenarios"], fn scenario ->
      id = scenario["id"]

      require!(
        scenario["classification"] in ["equivalent", "intentional-grind-semantics"],
        "#{id}: incomplete work cannot count as paired evidence"
      )

      require!(
        Map.keys(scenario["expected"]) |> Enum.sort() == ["grind", "oban"],
        "#{id}: missing expected outputs"
      )

      if scenario["classification"] == "equivalent" do
        require!(
          scenario["expected"]["grind"] == scenario["expected"]["oban"],
          "#{id}: equivalent scenario has different expectations"
        )
      end
    end)
  end

  def read_json!(path), do: path |> File.read!() |> Jason.decode!()

  def read_results!(path) do
    path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
  end

  defp index!(rows, engine, version, expected_ids) do
    ids = Enum.map(rows, & &1["scenario"])
    require!(length(Enum.uniq(ids)) == length(ids), "#{engine}: duplicate result")
    require!(MapSet.new(ids) == expected_ids, "#{engine}: missing or unexpected scenario results")

    Enum.each(rows, fn row ->
      require!(
        Enum.sort(Map.keys(row)) == ["catalog_version", "engine", "result", "run_id", "scenario"],
        "#{engine}: unexpected result envelope"
      )

      require!(
        row["engine"] == engine and row["catalog_version"] == version,
        "#{engine}: wrong engine or catalog version"
      )

      require!(is_map(row["result"]), "#{engine}: missing observation")
    end)

    Map.new(rows, &{&1["scenario"], &1})
  end

  defp require!(true, _message), do: :ok
  defp require!(false, message), do: raise(ArgumentError, message)
end
