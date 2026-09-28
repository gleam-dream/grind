ExUnit.start()

defmodule GrindOracle.ComparisonTest do
  use ExUnit.Case, async: true
  alias GrindOracle.Comparison

  defp catalog do
    %{
      "version" => 1,
      "gaps" => [],
      "scenarios" => [
        %{
          "id" => "success",
          "classification" => "equivalent",
          "reason" => "same committed state",
          "sources" => ["source"],
          "expected" => %{
            "grind" => %{"state" => "succeeded"},
            "oban" => %{"state" => "succeeded"}
          }
        }
      ]
    }
  end

  defp result(engine, state \\ "succeeded") do
    %{
      "catalog_version" => 1,
      "engine" => engine,
      "run_id" => "comparison-test-run",
      "scenario" => "success",
      "result" => %{"state" => state}
    }
  end

  test "compares actual observations and rejects even matching wrong outputs" do
    assert Comparison.compare!(catalog(), [result("grind")], [result("oban")]) == 1

    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog(), [result("grind", "executing")], [result("oban", "executing")])
    end

    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog(), [result("grind")], [result("oban", "executing")])
    end
  end

  test "missing, duplicate and unexpected observations fail closed" do
    assert_raise ArgumentError, fn -> Comparison.compare!(catalog(), [], [result("oban")]) end

    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog(), [result("grind"), result("grind")], [result("oban")])
    end

    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog(), [Map.put(result("grind"), "scenario", "extra")], [
        result("oban")
      ])
    end
  end

  test "intentional divergence must match both declared outcomes" do
    scenario = hd(catalog()["scenarios"])

    scenario = %{
      scenario
      | "classification" => "intentional-grind-semantics",
        "expected" => %{
          "grind" => %{"state" => "business_failed"},
          "oban" => %{"state" => "discarded"}
        }
    }

    catalog = %{catalog() | "scenarios" => [scenario]}

    assert Comparison.compare!(catalog, [result("grind", "business_failed")], [
             result("oban", "discarded")
           ]) == 1

    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog, [result("grind", "discarded")], [result("oban", "discarded")])
    end
  end

  test "stale versions, wrong engines and undeclared fields cannot pass" do
    for bad <- [
          Map.put(result("grind"), "catalog_version", 0),
          result("oban"),
          put_in(result("grind"), ["result", "ignored"], true)
        ] do
      assert_raise ArgumentError, fn ->
        Comparison.compare!(catalog(), [bad], [result("oban")])
      end
    end
  end

  test "matching observations from different runs cannot be paired" do
    assert_raise ArgumentError, fn ->
      Comparison.compare!(catalog(), [result("grind")], [
        Map.put(result("oban"), "run_id", "older-run")
      ])
    end
  end

  test "a deferred gap cannot be advertised as executable parity" do
    scenario = hd(catalog()["scenarios"]) |> Map.put("classification", "deferred")

    assert_raise ArgumentError, fn ->
      Comparison.compare!(%{catalog() | "scenarios" => [scenario]}, [result("grind")], [
        result("oban")
      ])
    end
  end
end
