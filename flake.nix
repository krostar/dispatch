{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    harmony = {
      url = "git+ssh://git@github.com/krostar/harmony";
      inputs = {
        synergy.follows = "synergy";
        nixpkgs-unstable.follows = "nixpkgs";
      };
    };
    synergy = {
      url = "git+ssh://git@github.com/krostar/synergy";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {synergy, ...} @ inputs:
    synergy.lib.mkFlake {
      inherit inputs;
      src = ./nix;
      eval.synergy.restrictDependenciesUnits.harmony = ["harmony"];
    };
}
