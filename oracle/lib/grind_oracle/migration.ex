defmodule GrindOracle.ObanMigration do
  use Ecto.Migration

  def change, do: Oban.Migrations.up()
end
