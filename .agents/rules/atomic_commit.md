# Atomic Commit Enforcement Rule

## Mandatory Constraint

Every git commit MUST be strictly atomic (100% single responsibility):

1. **One Logical Task Per Commit**: Never combine multiple un-related features, languages, or bug fixes (e.g., Python CLI + C++ FFI headers, or NIC discovery + Failover state machine) into a single commit.
2. **Granular Scoping**: Separate Python, Rust, Go, C++, and Documentation changes into distinct, logically scoped commits.
3. **Conventional Commits**: Use Conventional Commits formatting (`feat(scope): ...`, `fix(scope): ...`, `docs(scope): ...`).
4. **Clean Reversibility**: Every commit MUST be safely revertible via `git revert` without breaking unrelated features.
