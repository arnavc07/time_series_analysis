# CLAUDE.md

## Project Overview
This is a Python monorepo for time series analysis. Calculators are ETL pipelines
implemented as classes that extend `CalculatorBase` from
`time_series_analysis/calculator_base.py`.

## Architecture
- `calculators/` — Each calculator is a single file implementing `CalculatorBase`
- `calculator_configs/` — Each calculator has a corresponding config dataclass
  extending `BaseConfig`
- `tests/` — pytest tests mirroring the source structure

## Calculator Contract
Every calculator must:
1. Subclass `CalculatorBase`
2. Accept a config object (subclass of `BaseConfig`) in `__init__`
3. Implement `output_schema() -> dict[str, pl.DataType]`
4. Implement `calculate() -> pl.DataFrame`
5. Pass validation via the inherited `execute()` method

## Coding Standards
- Use Polars (not pandas) for all dataframe operations
- Prefer lazy evaluation (`.lazy()` / `.collect()`) where possible
- Type hints on all function signatures
- Docstrings on public classes and methods (Google style)
- Keep calculator classes focused — extract helpers as private methods

## Testing Standards
- Framework: pytest
- Test files mirror source: `calculators/foo.py` → `tests/test_foo.py`
- Mock all external data sources (APIs, databases, S3) — never make real calls in tests
- Test fixtures should produce DataFrames matching the real schema
- Every public method should have at least one test
- Test `execute()` (not just `calculate()`) to validate schema enforcement

## Git Workflow
- Create a feature branch per user story: `feature/<story-short-name>`
- Commit frequently with conventional commit messages (e.g., `feat:`, `test:`, `refactor:`)
- Do NOT push to remote — only commit locally. I will push when ready.
- Do NOT modify files outside your assigned scope without asking first.

## Environment
- Python environment managed via conda (`environment.yml`)
- Activate with: `conda activate time_series_analysis`
- Additional pip packages are listed in `environment.yml` under `pip:`
- Run tests with: `pytest tests/ -v`