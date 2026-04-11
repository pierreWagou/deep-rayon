---
name: docs
description: MkDocs Material documentation site — structure, conventions, and how to add or edit pages
---

## Documentation Stack

- **MkDocs Material** with custom Vusion theming
- Config: `mkdocs.yml`
- Content: `docs/` directory
- Served locally on **http://localhost:8100** (via `mprocs` or `mise run docs`)

## How Pages Work

Most docs pages are **symlinks** to sub-project READMEs:

```
docs/index.md        -> ../README.md
docs/dbt.md          -> ../dbt_project/README.md
docs/airflow.md      -> ../airflow/README.md
docs/databricks.md   -> ../databricks/README.md
docs/benchmarks.md   -> ../benchmarks/README.md
```

**To update a docs page, edit the source README** (e.g., `dbt_project/README.md`),
not the symlink in `docs/`.

The only non-symlinked content pages are:
- `docs/assets/` — logo, favicon, custom CSS

## Adding a New Page

1. Write the markdown file (or create a symlink to an existing README)
2. Add the page to the `nav:` section in `mkdocs.yml`
3. MkDocs hot-reloads automatically when running locally

## Navigation Structure

```yaml
nav:
  - Home: index.md
  - dbt Project: dbt.md
  - Orchestration:
    - Airflow: airflow.md
    - Databricks: databricks.md
  - Benchmarks: benchmarks.md
```

## Markdown Features Available

MkDocs Material extensions are configured — use them freely:

- **Admonitions:** `!!! note`, `!!! warning`, `!!! tip`, `!!! info`
- **Collapsible blocks:** `??? note "Title"` (collapsed), `???+ note` (open)
- **Content tabs:** `=== "Tab 1"` / `=== "Tab 2"`
- **Mermaid diagrams:** fenced code blocks with ` ```mermaid `
- **Code highlighting:** fenced blocks with language annotation + copy button
- **Task lists:** `- [x] Done` / `- [ ] Todo`
- **Tables, footnotes, definition lists**

## Writing Conventions

- Keep content in sub-project READMEs when possible (single source of truth)
- Prefer Mermaid for diagrams over images
- Use admonitions for callouts (`!!! info`, `!!! warning`)
- Use content tabs for alternative code examples
