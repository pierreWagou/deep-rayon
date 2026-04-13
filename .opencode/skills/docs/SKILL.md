---
name: docs
description: MkDocs Material documentation site — structure, conventions, and how to add or edit pages
---

## Documentation Stack

- **MkDocs Material** with custom Vusion theming
- Config: `mkdocs.yml`
- Content: `docs/` directory (standalone markdown files)
- Served locally on **http://localhost:8100** (via `mprocs` or `mise run docs`)

## How Pages Work

Docs pages are standalone markdown files in `docs/`:

```
docs/
├── index.md          # Home page (architecture, quick start, design decisions)
├── dbt.md            # dbt project details (models, tests, optimization)
├── airflow.md        # Airflow DAG, operators, local dev with Docker Compose
├── databricks.md     # Databricks Asset Bundle, targets, CI/CD
├── benchmarks.md     # Benchmark queries and optimization impact
└── assets/           # Logo, favicon, custom CSS
```

**To update a docs page, edit the file directly in `docs/`.**

## Adding a New Page

1. Create the markdown file in `docs/`
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

- Edit docs pages directly in `docs/`
- Prefer Mermaid for diagrams over images
- Use admonitions for callouts (`!!! info`, `!!! warning`)
- Use content tabs for alternative code examples
