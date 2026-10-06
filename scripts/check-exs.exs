# Compile script expressions without executing their database/process actions.
# Compile their top-level modules separately so inner compiler warnings are checked.
Mix.start()
ExUnit.start(autorun: false)

files = System.argv()
if files == [], do: raise("no authored Elixir scripts selected")

Enum.with_index(files, fn file, index ->
  ast = file |> File.read!() |> Code.string_to_quoted!(file: file)

  expressions =
    case ast do
      {:__block__, _, expressions} -> expressions
      expression -> [expression]
    end

  {_, diagnostics} =
    Code.with_diagnostics([log: true], fn ->
      Enum.each(expressions, fn
        {:defmodule, _, _} = expression -> Code.compile_quoted(expression, file)
        _ -> :ok
      end)

      # Module declarations were compiled once above; do not redefine them in
      # the function used to check non-module script expressions.
      remaining =
        Enum.map(expressions, fn
          {:defmodule, _, _} -> :ok
          expression -> expression
        end)

      Module.create(
        Module.concat(GrindScriptCheck, "File#{index}"),
        quote do
          def run do
            unquote({:__block__, [], remaining})
          end
        end,
        file: file,
        line: 1
      )
    end)

  if Enum.any?(diagnostics, &(&1.severity in [:warning, :error])) do
    raise "compiler diagnostics in #{file}"
  end
end)
