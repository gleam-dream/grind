{
  description = "Development environment for grind";

  inputs = {
    design-layer.url = "github:lostbean/design-layer/main";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      design-layer,
      nixpkgs,
      flake-utils,
      treefmt-nix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        elixirPackage = pkgs.beam29Packages.elixir_1_20;

        treefmtEval = treefmt-nix.lib.evalModule pkgs {
          projectRootFile = "flake.nix";
          settings.global.excludes = [
            "**/*.pdf"
            ".render/**"
            "bench/results/**"
            # Preserve compact historical attribution tables.
            "oracle/ORACLE-LEDGER.md"
            "oracle/results/**"
            "resilience/results/**"
          ];
          programs.gleam.enable = true;
          programs.mix-format.enable = true;
          programs.mix-format.package = elixirPackage;
          programs.nixfmt.enable = true;
          programs.prettier.enable = true;
        };
      in
      {
        apps.snips = design-layer.apps.${system}.snips;
        apps.design-gate-check = design-layer.apps.${system}.check;
        apps.design-gate-render = design-layer.apps.${system}.render;
        apps.design-gate-context = design-layer.apps.${system}.context;

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            lefthook
            git
            gleam
            beam29Packages.erlang
            elixirPackage
            postgresql_16
            python3
            beam29Packages.rebar3
          ];
        };

        formatter = treefmtEval.config.build.wrapper;

        checks.formatting = treefmtEval.config.build.check ./.;
      }
    );
}
