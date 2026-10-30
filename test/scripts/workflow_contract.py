"""Offline routing checks against the checked-in GitHub workflows."""

import ast
from pathlib import Path
import re
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


def enabled(expression, values):
    # Interpret only the boolean/comparison subset used by these job guards.
    expression = re.sub(
        r"\b(?:github|inputs|vars)\.[A-Za-z_0-9.]+",
        lambda match: repr(values.get(match.group(), False)),
        " ".join(expression.split()),
    )
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()

    def visit(node):
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.BoolOp):
            operands = [visit(value) for value in node.values]
            return all(operands) if isinstance(node.op, ast.And) else any(operands)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not visit(node.operand)
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = visit(node.left), visit(node.comparators[0])
            if isinstance(node.ops[0], ast.Eq):
                return left == right
            if isinstance(node.ops[0], ast.NotEq):
                return left != right
        raise AssertionError(f"Unsupported workflow expression: {ast.dump(node)}")

    return bool(visit(ast.parse(expression, mode="eval").body))


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load(
            (ROOT / ".github/workflows/end-to-end.yaml").read_text()
        )
        cls.jobs = cls.workflow["jobs"]
        cls.java = yaml.safe_load(
            (ROOT / ".github/workflows/IntegrationTest.yml").read_text()
        )

    def live_jobs(self, event, **values):
        context = {"github.event_name": event, **values}
        return {
            name
            for name, job in self.jobs.items()
            if "if" in job and enabled(job["if"], context)
        }

    def test_draft_and_ready_keep_broad_coverage(self):
        for draft in (True, False):
            with self.subTest(draft=draft):
                self.assertEqual(
                    self.live_jobs(
                        "pull_request", **{"github.event.pull_request.draft": draft}
                    ),
                    {"functional", "fault_injection"},
                )
        self.assertNotIn("if", self.java["jobs"]["build_and_test"])

    def test_trial_requires_explicit_manual_input(self):
        self.assertEqual(self.live_jobs("workflow_dispatch"), set())
        self.assertEqual(
            self.live_jobs("workflow_dispatch", **{"inputs.run_v4_trial": True}),
            {"v4-trial"},
        )

    def test_inspection_excludes_every_live_job(self):
        inputs = {
            f"inputs.{name}": True
            for name in (
                "inspect_spcs",
                "run_v4_trial",
                "run_functional",
                "run_fault_injection",
                "run_core_legacy",
                "run_stress",
                "run_soak_with_chaos",
                "run_spcs",
            )
        }
        inputs["vars.SPCS_RELEASE_ENABLED"] = "true"
        self.assertEqual(
            self.live_jobs("workflow_dispatch", **inputs), {"spcs-inspect"}
        )

    def test_push_and_schedule_keep_broad_coverage(self):
        expected = {"functional", "fault_injection", "core_legacy", "stress"}
        self.assertEqual(self.live_jobs("push"), expected)
        self.assertEqual(self.live_jobs("schedule"), expected | {"soak_with_chaos"})

    def test_manual_trial_excludes_all_other_live_inputs(self):
        inputs = {
            f"inputs.{name}": True
            for name in (
                "run_v4_trial",
                "run_functional",
                "run_fault_injection",
                "run_core_legacy",
                "run_stress",
                "run_soak_with_chaos",
                "run_spcs",
            )
        }
        inputs["vars.SPCS_RELEASE_ENABLED"] = "true"
        self.assertEqual(self.live_jobs("workflow_dispatch", **inputs), {"v4-trial"})

    def test_spcs_requires_both_activation_controls(self):
        self.assertEqual(
            self.live_jobs("workflow_dispatch", **{"inputs.run_spcs": True}), set()
        )
        self.assertEqual(
            self.live_jobs(
                "workflow_dispatch", **{"vars.SPCS_RELEASE_ENABLED": "true"}
            ),
            set(),
        )
        self.assertEqual(
            self.live_jobs(
                "workflow_dispatch",
                **{"inputs.run_spcs": True, "vars.SPCS_RELEASE_ENABLED": "true"},
            ),
            {"spcs-release"},
        )

    def test_existing_pr_events_and_staggered_lanes(self):
        for workflow in (self.workflow, self.java):
            events = workflow.get("on", workflow.get(True))["pull_request"]
            self.assertNotIn("types", events)
        functional = self.jobs["functional"]
        self.assertNotIn("max-parallel", functional["strategy"])
        offsets = [
            lane["start_offset"] for lane in functional["strategy"]["matrix"]["include"]
        ]
        self.assertEqual(offsets, [30 * index for index in range(len(offsets))])
        self.assertEqual(len(offsets), 7)
        names = [step.get("name", step.get("uses", "")) for step in functional["steps"]]
        stagger = names.index("Stagger start (${{ matrix.start_offset }}s)")
        self.assertEqual(names[stagger + 1], "./.github/actions/run-e2e-tests")
        self.assertEqual(
            functional["steps"][stagger]["run"], "sleep ${{ matrix.start_offset }}"
        )
        self.assertEqual(self.jobs["v4-trial"]["timeout-minutes"], 45)
        self.assertFalse(self.jobs["v4-trial"]["concurrency"]["cancel-in-progress"])

    def test_spcs_egress_and_artifact_scope(self):
        job = self.jobs["spcs-release"]
        step = next(
            step for step in job["steps"] if step.get("name") == "Run SPCS smoke test"
        )
        self.assertEqual(step["env"]["SPCS_EGRESS"], "${{ vars.SPCS_EGRESS }}")
        self.assertIn('egress_args=(--egress "$SPCS_EGRESS")', step["run"])
        self.assertIn('"${egress_args[@]}"', step["run"])
        upload = next(
            step
            for step in job["steps"]
            if step.get("uses", "").startswith("actions/upload-artifact@")
        )
        self.assertEqual(upload["with"]["path"], "test/spcs/.scratch/result.json")


if __name__ == "__main__":
    unittest.main()
