<!--
Sync Impact Report
===================
- Version change: 0.0.0 → 1.0.0
- Modified principles: N/A (initial creation)
- Added sections:
  - Core Principles: Code Quality, Testing Standards, UX Consistency,
    Performance Requirements
  - Quality Gates
  - Development Workflow
  - Governance
- Removed sections: None
- Templates requiring updates:
  - .specify/templates/plan-template.md ✅ no updates needed
    (Constitution Check section already references constitution)
  - .specify/templates/spec-template.md ✅ no updates needed
    (success criteria align with performance principle)
  - .specify/templates/tasks-template.md ✅ no updates needed
    (test-first phasing aligns with testing principle)
- Follow-up TODOs: None
-->

# Offset Consumer Constitution

## Core Principles

### I. Code Quality

All code MUST be clear, consistent, and maintainable.

- Every public function and type MUST have a clear, single responsibility.
- Code MUST follow the language's idiomatic conventions and standard
  formatting tools (e.g., `gofmt`, `golangci-lint` for Go).
- All exported symbols MUST have documentation comments that describe
  behavior, not implementation.
- Error handling MUST be explicit: never silently discard errors. Every
  error MUST be returned, logged, or handled with a documented rationale.
- Dependencies MUST be minimal and justified. Prefer standard library
  solutions over third-party packages when the standard library
  provides equivalent functionality.
- Code reviews MUST verify adherence to these quality standards before
  merge.

### II. Testing Standards

All features MUST be backed by tests that prove correctness and prevent
regressions.

- Unit tests MUST cover all public functions and methods. Target a
  minimum of 80% line coverage for new code.
- Integration tests MUST be written for any cross-boundary interaction
  (network, file system, external service).
- Test names MUST clearly describe the scenario being tested using the
  pattern `Test<Function>_<Scenario>_<ExpectedBehavior>`.
- Tests MUST be deterministic: no reliance on timing, external state,
  or execution order. Use mocks or test fixtures for external
  dependencies.
- Test failures MUST block merges. No code ships with known test
  failures unless explicitly triaged and tracked as a known issue.
- Regression tests MUST be added for every bug fix to ensure the defect
  does not recur.

### III. User Experience Consistency

All user-facing interfaces MUST provide a predictable, coherent
experience.

- CLI output MUST follow a consistent format: structured data on stdout,
  errors on stderr, and support both human-readable and JSON output
  modes where applicable.
- Error messages MUST be actionable: describe what went wrong, why, and
  what the user can do to resolve it.
- Configuration options MUST use consistent naming conventions and be
  documented with defaults, valid ranges, and examples.
- Breaking changes to user-facing behavior MUST be versioned and
  communicated through changelogs or migration guides.
- All user-facing features MUST be documented before release, including
  usage examples and expected behavior.

### IV. Performance Requirements

The system MUST meet defined performance targets and MUST NOT regress.

- All operations MUST define explicit latency and throughput targets
  appropriate to their use case (e.g., message processing latency,
  connection setup time).
- Memory allocations in hot paths MUST be minimized. Prefer pre-allocated
  buffers and pooling over per-request allocations.
- Benchmarks MUST be written for performance-critical code paths using
  the language's standard benchmarking tools (e.g., `go test -bench`).
- Performance regressions MUST be detected via benchmark comparison
  before merge. Any regression exceeding 10% on a critical path MUST
  be justified or resolved.
- Resource usage (CPU, memory, file descriptors, connections) MUST be
  bounded and documented. The system MUST gracefully handle resource
  exhaustion rather than crash.

## Quality Gates

All code changes MUST pass through the following gates before merge:

- **Lint gate**: Zero warnings from the configured linter.
- **Test gate**: All unit and integration tests pass.
- **Coverage gate**: New code meets the 80% line coverage minimum.
- **Benchmark gate**: No unexplained regressions on critical-path
  benchmarks.
- **Documentation gate**: All new public APIs are documented.
- **Review gate**: At least one approval from a code reviewer who
  verified compliance with this constitution.

## Development Workflow

The development process MUST follow these practices:

- **Branch strategy**: Feature branches off main; short-lived branches
  preferred (merge within days, not weeks).
- **Commit discipline**: Commit messages MUST follow conventional commit format
  (e.g., `feat:`, `fix:`, `docs:`, `test:`, `perf:`).
- **Test-first for bug fixes**: Every bug fix MUST include a failing
  test that reproduces the issue before the fix is applied.
- **Incremental delivery**: Features MUST be decomposed into
  independently testable and deliverable increments.
- **CI enforcement**: All quality gates MUST be enforced by CI. Manual
  overrides MUST be logged and justified.

## Governance

This constitution is the authoritative source for development standards
in the offset-consumer project. All code contributions, reviews, and
architectural decisions MUST comply with the principles defined here.

- **Amendments**: Any change to this constitution MUST be proposed via
  pull request, reviewed by at least one maintainer, and include a
  migration plan for existing code if the change affects current
  practices.
- **Versioning**: This constitution follows semantic versioning. MAJOR
  for principle removals or incompatible redefinitions, MINOR for new
  principles or material expansions, PATCH for clarifications and
  wording fixes.
- **Compliance review**: Constitution compliance MUST be checked during
  code review. Violations MUST be resolved before merge unless an
  explicit, time-bound exception is documented.
- **Conflict resolution**: When project practices conflict with this
  constitution, the constitution takes precedence. Update the
  constitution if the practice is correct; update the practice if the
  constitution is correct.

**Version**: 1.0.0 | **Ratified**: 2026-02-26 | **Last Amended**: 2026-02-26
