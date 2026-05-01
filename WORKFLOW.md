---
workflow_schema_version: cds-symphony-lite-workflow/v1
receipt_schema_version: cds-symphony-lite-receipt/v1
canonical_workflow_contract: ../sovereign-root/contracts/CDS_SYMPHONY_LITE_WORKFLOW.md
canonical_receipt_schema: ../sovereign-root/contracts/CDS_SYMPHONY_LITE_RECEIPT_SCHEMA.md
control_plane:
  kind: local_spec_queue
  mvp_source: ~/ai/specs/queue
repo:
  name: fleet-watch
  path: .
workspace_root_pattern: ~/Workspace/worktrees/cds-symphony-lite/fleet-watch/{ticket_id}
required_repo_files:
  - AGENTS.md
  - WORKFLOW.md
required_preflight_gates:
  - repo_registered_in_portfolio_state
  - repo_under_workspace_active
  - repo_local_agents_present
  - repo_local_workflow_present
  - fleet_guard_allowed
  - governance_validation_passed
  - portfolio_test_command_present
  - git_worktree_clean
  - duplicate_active_claim_absent
  - workspace_path_safe
  - run_state_written
verification_command: python3 -m pytest tests -q
additional_validation:
  - python3 -m fleet_watch.cli guard --json
scheduler_responsibilities:
  - select_eligible_ticket
  - acquire_atomic_claim
  - create_workspace
  - write_run_state
  - launch_codex_execution
  - run_verification_command
  - emit_receipt
agent_responsibilities:
  - implement_ticket_scope_inside_workspace
  - report_actions_and_blockers
failure_terminal_states:
  missing_agents_md: blocked_missing_agents
  missing_workflow_md: blocked_missing_workflow
  failed_fleet_guard: blocked_fleet_guard
  failed_governance_validation: blocked_governance_validation
  missing_test_command: blocked_missing_test_command
  unsafe_workspace_path: blocked_unsafe_workspace_path
  duplicate_active_claim: blocked_duplicate_claim
  dirty_repo_state: blocked_dirty_repo
  receipt_write_failure: blocked_receipt_write_failure
---

# Fleet Watch CDS Symphony-Lite Workflow

This repo-local workflow instance adopts:

- `../sovereign-root/contracts/CDS_SYMPHONY_LITE_WORKFLOW.md`
- `../sovereign-root/contracts/CDS_SYMPHONY_LITE_RECEIPT_SCHEMA.md`

Repo-specific values:

- Repo: `.`
- MVP control plane: local spec queue at `~/ai/specs/queue`
- Workspace pattern: `~/Workspace/worktrees/cds-symphony-lite/fleet-watch/{ticket_id}`
- Verification command: `python3 -m pytest tests -q`
- Additional repo validation: `python3 -m fleet_watch.cli guard --json`

The scheduler owns eligibility, workspace creation, run state, verification, receipt emission, and handoff state.

The agent owns only code changes inside the assigned workspace. The agent must not decide eligibility, mutate tracker state, write receipts, or decide whether verification passed.

Dirty repo state blocks dispatch in MVP with no exceptions.
