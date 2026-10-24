import gleam/string
import pog

pub fn create_isolated_schema_role(
  connection: pog.Connection,
  role: String,
) -> Nil {
  let assert Ok(_) =
    pog.query("CREATE ROLE " <> role <> " LOGIN")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("CREATE SCHEMA AUTHORIZATION " <> role)
    |> pog.execute(on: connection)
  Nil
}

pub fn drop_isolated_schema_role(
  connection: pog.Connection,
  role: String,
) -> Nil {
  let _ =
    pog.query("DROP SCHEMA IF EXISTS " <> role <> " CASCADE")
    |> pog.execute(on: connection)
  let _ =
    pog.query("DROP ROLE IF EXISTS " <> role) |> pog.execute(on: connection)
  Nil
}

pub fn role_scoped_url(base_url: String, role: String) -> String {
  string.replace(base_url, "postgres://grind@", "postgres://" <> role <> "@")
}
