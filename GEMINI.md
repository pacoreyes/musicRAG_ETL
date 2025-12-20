RULES: # Project Context: musicRAG

This file provides the coding agentic model with essential context, constraints, and style guidelines for working with this codebase. All code generation, review, and documentation tasks should strictly adhere to these instructions.

---

# Python Code Style & Collaboration Guide

You are an AI pair programmer working *inside this repository*.
Your primary goals are:

1. Produce correct, well-tested Python code.
2. Follow the coding style and practices defined in this document.
3. Minimize unnecessary churn in existing code.
4. Ask for clarification when requirements or trade-offs are ambiguous.

Always follow this document unless the user explicitly overrides a rule
for a specific task.

---

## 1. Collaboration & Workflow

- Prefer **small, safe, incremental changes** over large refactors.
- When a task is non-trivial, respond in this order:
  1. **Plan** – brief bullet list of steps you will take.
  2. **Proposed changes** – code edits grouped by file.
  3. **Rationale** – short explanation of key design decisions.
- When modifying existing code, **preserve structure and style** unless
  you are explicitly refactoring.
- If key requirements are unclear, ask **one or two focused questions**
  before making big decisions.
- When suggesting edits, show only the **minimal diff / snippets**
  necessary, not entire large files, unless requested.

---

## 2. Target Environment

- Language: **Python 3.11+** (assume modern Python).
- Tooling assumptions (unless told otherwise):
  - Formatter: `black` (line length **88**).
  - Linting: `ruff` or `flake8`.
  - Type checker: `mypy` or `pyright`.

Write code that is compatible with these tools out of the box.

---

## 3. Python Style – PEP 8 + Enhancements

### 3.1 General

- Follow **PEP 8** for naming, spacing, and layout.
- Use **4 spaces** for indentation. No tabs.
- Keep lines ≤ **88 characters** (or less when it improves readability).
- Prefer **explicit over implicit** and **readability over cleverness**.

### 3.2 Naming

- Use descriptive names:
  - `snake_case` for functions, variables, and module-level constants
    (constants in `UPPER_SNAKE_CASE`).
  - `CapWords` / `PascalCase` for classes and exceptions.
- Avoid single-letter names except for conventional short scopes (`i`,
  `j`, `k` in short loops).

### 3.3 Imports

- Use **absolute imports** when possible.
- Group imports in this order, separated by blank lines:
  1. Standard library
  2. Third-party packages
  3. Local application imports
- No wildcard imports (`from module import *`).

---

## 4. Typing – Pragmatic, Explicit Where It Matters

Typing is an important tool for clarity and safety, but it must not make
the code harder to read or maintain.

### 4.1 Functions & Methods

- **Public interfaces must be typed**:
  - All public functions and methods (used outside their module) should
    have:
    - Type hints for all parameters.
    - A clear return type (use `None` when appropriate).
- **Non-trivial internal functions should be typed**:
  - For internal/private functions with meaningful logic, prefer adding
    type hints when they improve understanding.
- **Small, obvious helpers may omit types**:
  - Tiny, local helpers or very simple internal functions may skip type
    hints if annotations would add noise rather than clarity.
- Prefer simple, readable types:
  - Use `list[str]`, `dict[str, float]`, `set[int]`, etc., where
    appropriate.
  - Use abstract types like `Sequence`, `Mapping`, `Iterable` when they
    clearly express intent, but don’t over-generalize just for the sake
    of it.

### 4.2 Classes & Data Models

- For simple data carriers, prefer `@dataclass` with typed fields, when
  it keeps the code clear.
- Use `Enum`, `TypedDict`, `Protocol`, or generics **only when they
  materially clarify domain concepts or contracts**.
- Avoid overly complex type constructs that make the code harder to
  read than the untyped version.

### 4.3 Type Safety

- Aim for code that passes normal type checking (`mypy` / `pyright`)
  under the project’s configuration.
- Avoid `Any` when a more specific type is easy and natural to express.
  Using `Any` is acceptable when:
  - The dynamic nature is intentional, or
  - The precise type would be unreasonably complex.
- Use `# type: ignore` sparingly, and only when there is a specific,
  justified reason (e.g. third-party library limitations).

Overall rule: **typing should reduce cognitive load; if a type
annotation makes code significantly harder to read, simplify it or
omit it for internal helpers.**

---

## 5. Clean Code Principles

When generating or refactoring Python code, apply these principles:

1. **Small functions and methods**
   - Keep functions focused on a single responsibility.
   - Prefer splitting complex logic into smaller helpers over long,
     deeply nested blocks.

2. **Meaningful abstractions**
   - Introduce helpers or classes only when they clarify the design.
   - Avoid over-engineering and unnecessary patterns.

3. **Readable control flow**
   - Prefer early returns instead of deep nesting.
   - Avoid long boolean expressions; break them into well-named
     intermediate variables.

4. **No magic values**
   - Replace “magic numbers” and string literals with named constants or
     enums when they matter semantically.

5. **Comments & docstrings**
   - Code should be self-describing first. Add comments for **why**, not
     **what**.
   - Use docstrings for public modules, classes, and functions.
   - **MUST use a consistent style (Google or NumPy docstring style).**
   - **Docstrings for public functions/methods MUST include:**
     - **A description of all parameters, including their types.**
     - **A description of the return value, including its type.**

6. **Separation of concerns**
   - Keep business logic separate from I/O, frameworks, and UI.
   - Avoid mixing persistence, transport, and domain logic in the same
     layer.

---

## 6. Error Handling, Logging & Validation

- Use **exceptions** for exceptional conditions, not normal flow.
- Catch only the exceptions you can handle meaningfully.
- When logging:
  - Use structured and leveled logging (`debug`, `info`, `warning`,
    `error`, `critical`).
  - Avoid logging sensitive information (secrets, access tokens, PII).
- Validate external inputs (user input, network data, file contents)
  explicitly and fail fast with clear messages.
- **No Import Hacks:** Do not introduce `sys.path.append` or modification of `PYTHONPATH` inside Python scripts. Relied strictly on the installed package structure. An exception is the `src/config.py` file, which adds the project root to the Python path to resolve imports.

---

## 7. I/O, Side Effects & Resources

- Use context managers (`with`) for files, locks, and other resources.
- Avoid global state and mutable module-level singletons.
- Design functions to be **pure** where reasonable (no hidden
  side-effects), especially for core logic.

---

## 7.1. Package Structure & Import Standards
The project is divided into two distinct zones: the Core Application (src/) and ETL Operations (scripts_etl/).

A. Core Application (src/rag_system)
- Purpose: The installable library containing the business logic.
- Import Style: Absolute imports rooted at rag_system.
  - ✅ from rag_system.retrieval import vector_store
  - ❌ from ..retrieval import vector_store (Avoid relative imports in main logic)
- Restriction: Code here cannot import from scripts_etl.

B. ETL Scripts (scripts_etl/)
- Purpose: Standalone scripts for cleaning, extraction, and loading.
- Import Style: Absolute imports rooted at scripts_etl or rag_system.
  - ✅ from scripts_etl.utils.preprocess_json import process_file
  - ✅ from rag_system.generation import llm_client (Scripts can use the App)
- Execution: Scripts must be run as modules from the project root to resolve paths correctly.
  - ✅ python -m scripts_etl.clean.clean_text
  - ❌ cd scripts_etl/clean && python clean_text.py
  - 
C. Path Resolution (Data & Configs)
Do not hardcode strings. Use pathlib relative to the __file__ to locate assets dynamically.
Example for a script in scripts_etl/clean/clean_text.py accessing data:

```
from pathlib import Path
```

# 1. Define current file location
CURRENT_DIR = Path(__file__).resolve().parent

# 2. Navigate to data (e.g., sibling folder 'data')
# Structure: scripts_etl/clean/clean_text.py -> scripts_etl/data/file.json
```
DATA_FILE = CURRENT_DIR.parent / "data" / "artist_entity.json"

if not DATA_FILE.exists():
    raise FileNotFoundError(f"Missing data file: {DATA_FILE}")
```
---

## 8. Testing Standards (Automated)

Testing is a strict requirement, not an afterthought.

- **Framework:** Use `pytest` for all unit and integration tests.
- **Location:** All tests **must** reside within the `src/tests/` directory. This is the only permitted location for test files; do not create a root-level `tests/` folder. The test directory must also mirror the structure of the source code.
  - Source: `src/rag_system/data_etl/utils/formatting.py`
  - Test:   `src/tests/rag_system/data_etl/utils/test_formatting.py`
  - There are no tests under the `scripts_etl` folder
- **Automatic Test Generation:**
  - **Whenever you create or modify logic** in `src/`, you must explicitly generate or update the corresponding test file in `src/tests/` in the same response.
  - Do not wait for the user to ask for tests; include them in your proposed changes.
- **Test Style:**
  - Use `conftest.py` for shared fixtures.
  - Use `unittest.mock` or `pytest-mock` to mock external dependencies (APIs, DBs, File I/O).
  - Aim for high coverage of the "happy path" and at least one edge case per function.
- **Naming:**
  - Test files: `test_<module_name>.py`
  - Test functions: `test_<function_name>_<condition>_<expected_result>`

When you create or modify code paths, suggest appropriate tests and, if
the user agrees, generate them.

---

## 9. Processes and progress bars

- Whenever is possible, a process should add a progress bar using tqdm to tell the user what is happening and how the progress
goes. The user should not be left blind if it possible to help their with a visual cue.

---

## 10. Repository Conventions

- **Structure:**
  - `src/`: Contains all Python source code.
    - `rag_system/`: The main, installable application package.
    - `tests/`: Contains all tests, mirroring the `rag_system` structure. This directory is not included in the production build.
- **Dependency Management:** strictly adhere to `pyproject.toml`.
- **Scripts:** Any executable logic inside `src/` should be runnable via `python -m rag_system.path.to.module`.

When creating new modules, place them where they logically belong,
following existing patterns.

---

## 11. Non-Negotiables (Hard Rules)

These rules have highest priority. Do **not** violate them unless the
user explicitly instructs you to do so for a specific operation.

1. **Do NOT edit sensitive files** such as:
   - `.env`, credentials, or keys.
   - Deployment state files (`terraform.tfstate`, cloud state dumps).
2. Do not introduce dependencies that require system-wide changes or
   privileged access without user approval.
3. Do not remove existing tests without explicit user consent.
4. When performing potentially destructive refactors, present a **plan**
   first and wait for user confirmation.
5. Strict Dependency Flow:
- scripts_etl/ MAY import from src/rag_system/.
- src/rag_system/ MUST NOT import from scripts_etl/. The application core must remain independent of operational scripts.
6. No Import Hacks: Do not use sys.path.append(...). If an import fails, it is an execution context issue (run from root using -m) or a structure issue.

---

## 12. Response Formatting

When you propose changes:

- Group by file, and clearly label each file:
  - `File: path/to/file.py`
- For new or updated files, show them as fenced code blocks:

  ```python
  # File: path/to/file.py
  ...
