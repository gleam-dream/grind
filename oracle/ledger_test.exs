ExUnit.start()

defmodule GrindOracle.LedgerTest do
  use ExUnit.Case

  test "reference validation fails closed on broken tests, sources, pins, and empty ledgers" do
    root = Path.expand("..", __DIR__)
    ledger = File.read!(Path.join(root, "oracle/ORACLE-LEDGER.md"))
    catalog = File.read!(Path.join(root, "oracle/scenarios.json")) |> Jason.decode!()
    directory = Path.join(System.tmp_dir!(), "grind-ledger-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    changed =
      String.replace(
        ledger,
        "postgres_worker_snooze_ack_receipt_binds_delay_test",
        "nonexistent_oracle_regression_test"
      )

    assert changed != ledger

    missing_source =
      put_in(catalog, ["scenarios", Access.at(0), "sources"], ["oracle/missing-source.ex"])

    wrong_pin = put_in(catalog, ["oracle", "commit"], String.duplicate("0", 40))

    fixtures = [
      {ledger, catalog, 0, "Oracle ledger:"},
      {changed, catalog, 1, "missing local test: nonexistent_oracle_regression_test"},
      {ledger, missing_source, 1, "missing catalog source: oracle/missing-source.ex"},
      {ledger, wrong_pin, 1, "Oban checkout does not match the catalog pin"},
      {"# Empty ledger\n", catalog, 1, "ledger has no evidence rows"}
    ]

    for {text, data, expected_status, expected_message} <- fixtures do
      ledger_path = Path.join(directory, "ledger.md")
      catalog_path = Path.join(directory, "catalog.json")
      File.write!(ledger_path, text)
      File.write!(catalog_path, Jason.encode!(data))

      {output, status} =
        System.cmd(
          System.find_executable("elixir"),
          [Path.join(root, "scripts/check-oracle-ledger.exs"), ledger_path, catalog_path],
          stderr_to_stdout: true
        )

      assert status == expected_status, output
      assert output =~ expected_message
    end
  end
end
