# Run fast and full validation profiles in CI and keep endurance evidence in a weekly workflow

<a id="adr-0018"></a>

- **Decision.** CI offers a fast profile for the edit loop and a full profile for integration. The final CI status requires full validation and the design gate. The benchmark matrix and the two-hour soak run as a separate weekly or manual workflow with its own final status. No profile repairs source files, and each run writes its evidence to a fresh artifact directory.

- **Rationale.** The full gate needs disposable PostgreSQL clusters and takes minutes, while load and endurance evidence needs hours and a quiet host. Mixing them would make every push slow and would make a required check depend on machine load. A separate status keeps the required checks deterministic and the endurance evidence reviewable.

- **Alternatives.** Running the soak on every push is impractical. Qualifying only by hand leaves no recurring evidence that the harnesses still run. A single profile either omits the database and fault checks or burdens every edit.

- **Evidence and history.** scripts/ci.sh implements the fast, full, matrix and soak profiles, and the CI and qualification workflows under .github/workflows select them. The approved endurance requirement is two hours of mixed faults after warm-up plus fourteen standalone cases, and a twenty-four-hour run is not required.

- **Examples.** The fast profile builds examples/usage with warnings as errors and runs its offline topics. The full profile runs every topic against disposable databases through the PostgreSQL gate.
