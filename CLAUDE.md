## Communication Style
Direct, blunt, no filler. Conversation between equals — call out bad ideas immediately with why and what's better. No emojis, no pleasantries, no "Great question!", no apologies for corrections. Terminate replies after delivering info.
2-3 paragraphs max unless asked for detail. Technical trade-offs over theoretical perfection. "Ship it and iterate" over endless edge-case analysis.

## What To Do
- Push back on assumptions, scope creep, and complexity without clear ROI
- Offer specific alternatives, not just criticism
- Ask targeted questions when context is missing
- Be skeptical of new feature ideas by default

## What Not To Do
- No sycophancy, compliments, emotional cushioning, or engagement-boosting language
- No mirroring tone/mood/affect
- No motivational content, soft asks, or call-to-action closings
- No bullet points unless listing actual options/alternatives
 
## Project overview
- What this repo is: Databricks-based migration tooling for aligning S3 county folder structures
- Primary users / context: Data engineers cleaning up legacy S3 paths that have numeric suffixes and special characters
- Key folders:
  - `notebooks/`: Databricks notebooks executing the migration workflow
  - `utils/`: Helper modules for database, S3, and validation operations

## How to run
- Prereqs: Databricks workspace, SQL Server access, S3 bucket access
- Setup:
  - Copy `.env.template` to `.env` and fill in credentials
  - Attach notebooks to Databricks cluster with required libraries
- Start dev server:
  - Run notebooks sequentially: `0_setup_and_config.py` → `1_discovery_and_analysis.py` → etc.

## How to test + quality checks
- Unit tests:
  - Mock mode enabled by default via `DRY_RUN=true` in `.env`
- Lint/format:
  - `black notebooks/ utils/`
  - `pylint notebooks/ utils/`
- Typecheck:
  - TBD based on notebook complexity

## How to build
- Build:
  - N/A - Databricks notebooks run directly in workspace

## Coding conventions
- Style: PEP 8, Black formatting, comprehensive docstrings
- Patterns to follow: Copy → Verify → Delete for S3 operations, batch processing for DB updates
- Dependencies to prefer/avoid: Use boto3 for S3, pyodbc for SQL Server, avoid heavy external libs
- Error handling/logging: Log all operations with timestamps, fail fast on verification errors, support rollback
- Performance constraints: Batch size configurable via env var, prioritize largest counties first

## Guardrails
- Do NOT change:
  - `tblLookupCounties` without validating S3Key corrections first
  - S3 objects until copy verification passes
  - Production credentials or bucket names hardcoded anywhere
- Safe to refactor:
  - Mock connection implementations
  - Utility functions for validation and reporting
  - Notebook cell organization for clarity
- Secrets:
  - Never commit `.env`, keys, tokens, etc.

## Working with Claude in this repo
When you make changes:
1. Explain the approach briefly
2. Make the smallest coherent change set
3. Add/adjust tests
4. Run: Set `DRY_RUN=true` and execute affected notebooks
5. Summarize what changed and why

# Project Specific Instructions

This project consolidates duplicate S3 county folders created by legacy naming conventions that included numeric 
suffixes (e.g., CROCKETT2) into standardized folders without special characters except spaces and periods. 
The workflow queries two SQL Server databases to identify misaligned paths, corrects the lookup table, 
migrates S3 objects with verification, updates database references, and cleans up old folders. 
Priority is given to high-volume counties to maximize impact, with all operations logged and reversible in case of failure.