//// Reads and verifies PostgreSQL's physical Grind schema shape.
//// A malformed shape is distinct from a failed catalog query so the public
//// migration runner can report its existing StorageError variants exactly.

import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import grind/internal/migrations
import grind/internal/store
import pog

pub type ProbeError {
  ProbeQueryFailed(pog.QueryError)
  ProbeMalformed
}

/// Quotes the schema name (`quote_ident`) before embedding it in the
/// textual argument `to_regclass` parses — a bare `current_schema() || '.'
/// || ...` would silently fold a mixed-case or otherwise identifier-quoted
/// schema name to lower case, `to_regclass` would then look up a schema
/// that does not exist, and this would wrongly report the marker table
/// absent even when installed and fully functional.
pub fn schema_migrations_table_exists(
  connection: pog.Connection,
) -> Result(Bool, ProbeError) {
  let query =
    pog.query(
      "SELECT to_regclass(quote_ident(current_schema()) || '.grind_schema_migrations') IS NOT NULL",
    )
    |> pog.returning({
      use present <- decode.field(0, decode.bool)
      decode.success(present)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ProbeQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [present] -> Ok(present)
        _ -> Error(ProbeMalformed)
      }
  }
}

/// Every `grind_`-prefixed relation (table, sequence, index, ...) in the
/// current schema, as `#(relname, relkind)` — `relkind` is PostgreSQL's own
/// single-character code (`r`/`S`/`i`/...), cast to `text` explicitly since
/// its native `"char"` pseudo-type has no unique `||` overload against
/// `text` (`42725 ambiguous_function`) should a caller ever concatenate it.
/// Backs both the fresh-schema check (an empty result) and the per-version
/// exact-shape check (`relation_shape_matches`).
pub fn read_grind_relations(
  connection: pog.Connection,
) -> Result(List(#(String, String)), ProbeError) {
  let query =
    pog.query(
      "SELECT c.relname, c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND left(c.relname, 6) = 'grind_'",
    )
    |> pog.returning({
      use name <- decode.field(0, decode.string)
      use kind <- decode.field(1, decode.string)
      decode.success(#(name, kind))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ProbeQueryFailed(error))
    Ok(returned) -> Ok(returned.rows)
  }
}

pub fn read_schema_marker(
  connection: pog.Connection,
) -> Result(#(Int, Int, Int), ProbeError) {
  let query =
    pog.query(
      "SELECT count(*)::bigint, COALESCE(min(version), 0)::bigint, COALESCE(max(version), 0)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use minimum <- decode.field(1, decode.int)
      use maximum <- decode.field(2, decode.int)
      decode.success(#(count, minimum, maximum))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ProbeQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [version] -> Ok(version)
        _ -> Error(ProbeMalformed)
      }
  }
}

fn relation_kind_code(kind: migrations.RelationKind) -> String {
  case kind {
    migrations.Table -> "r"
    migrations.Sequence -> "S"
    migrations.Index -> "i"
  }
}

pub fn relation_shape_matches(
  connection: pog.Connection,
  shape: List(migrations.ExpectedRelation),
) -> Result(Bool, ProbeError) {
  use actual <- result.try(read_grind_relations(connection))
  let expected =
    shape
    |> list.map(fn(relation) {
      #(relation.name, relation_kind_code(relation.kind))
    })
    |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
  case list.sort(actual, fn(a, b) { string.compare(a.0, b.0) }) == expected {
    False -> Ok(False)
    True -> relation_key_columns_match(connection, shape)
  }
}

fn relation_key_columns_match(
  connection: pog.Connection,
  shape: List(migrations.ExpectedRelation),
) -> Result(Bool, ProbeError) {
  shape
  |> list.filter(fn(relation) { relation.key_columns != [] })
  |> list.try_fold(True, fn(all_matched_so_far, relation) {
    case all_matched_so_far {
      False -> Ok(False)
      True ->
        relation_has_columns(connection, relation.name, relation.key_columns)
    }
  })
}

fn relation_has_columns(
  connection: pog.Connection,
  table_name: String,
  columns: List(String),
) -> Result(Bool, ProbeError) {
  let query =
    pog.query(
      "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = ANY($2)",
    )
    |> pog.parameter(pog.text(table_name))
    |> pog.parameter(pog.array(pog.text, columns))
    |> pog.returning({
      use present <- decode.field(0, decode.int)
      decode.success(present)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ProbeQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [present] -> Ok(present == list.length(columns))
        _ -> Error(ProbeMalformed)
      }
  }
}

/// Every one of `foreign_keys` (constraint names) must exist as a real
/// foreign key (`pg_constraint.contype = 'f'`) in the current schema — a
/// step whose `foreign_keys` is `[]` (every version before `grind_v12`)
/// always matches without a query. `pg_constraint`, never `pg_class`: a
/// plain foreign key creates no relation of its own (unlike a `PRIMARY
/// KEY`/`UNIQUE` constraint's backing index, already covered by `shape`
/// itself), so it would otherwise never be checked at all — a database
/// missing one of `grind_v12`'s three `ON DELETE CASCADE` constraints (say,
/// dropped by hand) must fail closed exactly like a missing relation or
/// column does, not silently pass as if the receipt-orphan backstop
/// `docs/RECOVERY-EVIDENCE.md` Increment 24 describes were still in place.
pub fn relation_foreign_keys_match(
  connection: pog.Connection,
  foreign_keys: List(String),
) -> Result(Bool, ProbeError) {
  case foreign_keys {
    [] -> Ok(True)
    _ -> {
      let query =
        pog.query(
          "SELECT count(*) FROM pg_constraint WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND contype = 'f' AND conname = ANY($1)",
        )
        |> pog.parameter(pog.array(pog.text, foreign_keys))
        |> pog.returning({
          use present <- decode.field(0, decode.int)
          decode.success(present)
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(ProbeQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [present] -> Ok(present == list.length(foreign_keys))
            _ -> Error(ProbeMalformed)
          }
      }
    }
  }
}

/// The inverse of `relation_key_columns_match`: none of `forbidden` (each a
/// `#(table_name, column_name)` pair — see `migrations.Migration`'s own doc
/// comment) may exist in the current schema. `[]` always matches without a
/// query, exactly like `relation_foreign_keys_match`'s own empty case.
pub fn forbidden_columns_absent(
  connection: pog.Connection,
  forbidden: List(#(String, String)),
) -> Result(Bool, ProbeError) {
  case forbidden {
    [] -> Ok(True)
    _ -> {
      let #(tables, columns) = list.unzip(forbidden)
      let query =
        pog.query(
          "SELECT count(*) FROM information_schema.columns c JOIN unnest($1::text[], $2::text[]) AS forbidden(table_name, column_name) ON c.table_name = forbidden.table_name AND c.column_name = forbidden.column_name WHERE c.table_schema = current_schema()",
        )
        |> pog.parameter(pog.array(pog.text, tables))
        |> pog.parameter(pog.array(pog.text, columns))
        |> pog.returning({
          use present <- decode.field(0, decode.int)
          decode.success(present)
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(ProbeQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [present] -> Ok(present == 0)
            _ -> Error(ProbeMalformed)
          }
      }
    }
  }
}
