import copy
import unittest

from fault_compare import grind_m2


class GrindWitness:
    def __init__(self):
        self.events = {
            "effect_fsync_confirmed": dict(at_ns=1_000_000),
            "pre_replay_quarantine_observed": dict(at_ns=3_000_000, state="uncertain", effects=1,
                                                   rows=[dict(id=7, state="uncertain", attempt_id=10, attempt_epoch=1)]),
            "audited_replay_requested": dict(at_ns=4_000_000, job_id=7, resolution_id="approved"),
        }
        self.effect_rows = [dict(at_ms=1, os_pid=10), dict(at_ms=5, os_pid=11)]
        self.final = dict(jobs=[dict(id=7, state="succeeded")], receipts=[dict(job_id=7, attempt_id=11, attempt_epoch=2)],
                          resolutions=[dict(job_id=7, decision="authorize_replay",
                                            resolved_by="resilience-controller", resolution_id="approved",
                                            attempt_id=10, attempt_epoch=1)])

    def death(self, _scenario):
        return dict(at_ns=2_000_000, pid=10)

    def event(self, _scenario, name):
        return self.events[name]

    def effects(self, _scenario, _key):
        return self.effect_rows

    def read(self, _name):
        return self.final


class FaultComparisonTests(unittest.TestCase):
    def test_real_fields_are_normalized_separately_from_approval(self):
        observed = grind_m2(GrindWitness())
        self.assertEqual(1, observed["effects_before_operator_replay"])
        self.assertEqual(2, observed["final_effects"])
        self.assertEqual("uncertain", observed["state_before_operator_replay"])

    def test_a_second_effect_before_replay_cannot_be_hidden_by_passed_status(self):
        witness = GrindWitness()
        witness.effect_rows[1]["at_ms"] = 3
        with self.assertRaisesRegex(ValueError, "second effect preceded"):
            grind_m2(witness)

    def test_missing_or_unattributed_audit_rejects_the_comparison(self):
        for rows in ([], [dict(job_id=7, decision="authorize_replay", resolved_by="", resolution_id="approved",
                              attempt_id=10, attempt_epoch=1)]):
            witness = GrindWitness()
            witness.final["resolutions"] = copy.deepcopy(rows)
            with self.assertRaises(ValueError):
                grind_m2(witness)

    def test_a_pre_replay_terminal_row_cannot_substitute_for_quarantine(self):
        witness = GrindWitness()
        witness.events["pre_replay_quarantine_observed"]["rows"][0]["state"] = "succeeded"
        with self.assertRaisesRegex(ValueError, "durable quarantine"):
            grind_m2(witness)


if __name__ == "__main__":
    unittest.main()
