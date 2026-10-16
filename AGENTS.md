# Agent guidance

## Code conventions

- Write code, identifiers, and comments in English.
- Use pnpm for package management and repository scripts.
- Follow the repository's lint and formatting configuration.
- Prefer the smallest change that solves the task; generalize for an existing need.
- Keep functions focused. Extract helpers when they clarify a responsibility, not just to meet a line count.
- Explain non-obvious constraints and decisions in comments; avoid restating the code. Document public contracts with TSDoc.
- Use objects, maps, or arrays instead of `switch/case`.
- Use named interfaces or types instead of anonymous object types.
- Give non-obvious values named constants.
- Prefer early returns to deeply nested branches, and reuse existing logic rather than duplicating it.
- Name identifiers for their role; use boolean names such as `isActive` or `shouldRefresh`.
- In index files, prefer `export * from './module'` over named re-exports.
