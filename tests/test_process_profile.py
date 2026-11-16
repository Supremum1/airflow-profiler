"""Resource attribution must preserve task boundaries and missing measurements."""

import copy
import unittest

from process_report import build_profile
from monitoring.process_sampler import descendants
from trace_report import timeline_gaps, cpu_intervals


def proc(key, parent=None, owner=None, pss=100, cpu=0, pid=10):
    return {"key": key, "parent_key": parent, "task_instance_id": owner,
            "pss_bytes": pss, "private_bytes": pss // 2 if pss is not None else None,
            "cpu_seconds": cpu, "pid": pid, "start_ticks": pid}


class AttributionTests(unittest.TestCase):
    def setUp(self):
        self.record = {"released_at": "1970-01-01T00:00:10+00:00",
                       "finished_at": "1970-01-01T00:00:13+00:00",
                       "tasks": [{"ti_id": "ti", "run_id": "run", "task_id": "transform",
                                  "start_date": "1970-01-01T00:00:10+00:00",
                                  "end_date": "1970-01-01T00:00:13+00:00"}],
                       "operations": [{"run_id": "run", "task_id": "transform", "pid": 20,
                                       "start": 11, "end": 12}]}

    def sample(self, at, processes):
        return {"at": at, "processes": processes, "interval_seconds": .5, "worker_memory": {}}

    def test_retroactive_title_and_descendants_do_not_charge_parent(self):
        rows = []
        for at in (10.5, 11, 11.5, 12, 12.5):
            rows.append(self.sample(at, [proc("worker", pss=200, cpu=at),
                proc("task", "worker", "ti" if at >= 11 else None, pss=500, cpu=2*at, pid=20),
                proc("child", "task", pss=300, cpu=at, pid=21),
                proc("other", "worker", "another-ti", pss=50, pid=30)]))
        result = build_profile(self.record, rows)
        point = result["samples"][0]["groups"]
        self.assertEqual(point["service"]["pss_bytes"], 200)
        self.assertEqual(point["tasks"]["pss_bytes"], 800)
        self.assertEqual(point["other_tasks"]["pss_bytes"], 50)
        self.assertEqual(result["tasks"][0]["cpu_observed_seconds"], 6)
        memory = self.record["operations"][0]["process_memory"]
        self.assertEqual(memory["pss_peak_bytes"], 500)  # Operation is root-only, not child PSS.

    def test_pid_reuse_and_missing_pss_are_not_zero(self):
        rows = [self.sample(10.5, [proc("old:20:100", owner="ti", pid=20)]),
                self.sample(11, [proc("new:20:200", pss=None, pid=20)])]
        result = build_profile(self.record, rows)
        self.assertEqual(result["samples"][1]["groups"]["tasks"]["count"], 0)
        self.assertIsNone(result["samples"][1]["groups"]["service"]["pss_bytes"])
        self.assertTrue(any("PSS" in warning for warning in result["warnings"]))

    def test_short_call_has_no_invented_peak_and_gap_has_no_cpu(self):
        self.record["finished_at"] = "1970-01-01T00:00:20+00:00"
        self.record["operations"][0].update(start=11.01, end=11.02)
        rows = [self.sample(10.5, [proc("task", owner="ti", cpu=1, pid=20)]),
                self.sample(17, [proc("task", owner="ti", cpu=100, pid=20)])]
        result = build_profile(self.record, rows)
        self.assertIsNone(self.record["operations"][0]["process_memory"]["pss_peak_bytes"])
        self.assertIsNone(result["samples"][1]["groups"]["tasks"]["cores"])
        self.assertEqual(result["max_gap_seconds"], 6.5)

    def test_no_samples_is_unavailable(self):
        result = build_profile(copy.deepcopy(self.record), [])
        self.assertFalse(result["available"])
        self.assertEqual(result["matched_tasks"], 0)

    def test_short_task_boundary_is_an_observation_not_a_periodic_peak(self):
        self.record["task_boundaries"] = [
            {"run_id": "run", "task_id": "transform", "pid": 20, "at": at, "memory_start": at-.001,
             "pss_bytes": memory, "private_bytes": memory//2}
            for at,memory in [(10.9,500),(12.1,600)]]
        result = build_profile(self.record, [self.sample(11, [proc("worker", pid=1)])])
        self.assertEqual(result["matched_tasks"], 1)
        self.assertEqual(result["periodic_matched_tasks"], 0)
        self.assertEqual(result["tasks"][0]["binding"], "code_boundary")
        self.assertEqual(result["tasks"][0]["pss_peak_bytes"], 600)
        self.assertIsNone(self.record["operations"][0]["process_memory"]["pss_peak_bytes"])

    def test_trace_pid_recovers_bootstrap_without_uuid_title(self):
        rows = [self.sample(10.5, [proc("worker", pid=1), proc("task", "worker", pid=20),
                                   proc("child", "task", pid=21)]),
                self.sample(11, [proc("worker", pid=1), proc("task", "worker", pid=20)])]
        result = build_profile(self.record, rows)
        self.assertEqual(result["matched_tasks"], 1)
        self.assertEqual(result["trace_pid_bindings"], 1)
        self.assertEqual(result["tasks"][0]["binding"], "trace_pid")
        self.assertEqual(result["samples"][0]["groups"]["tasks"]["count"], 2)

    def test_trace_pid_does_not_guess_between_two_process_lifetimes(self):
        rows = [self.sample(10.5, [proc("old:20:100", pid=20)]),
                self.sample(11, [proc("new:20:200", pid=20)])]
        result = build_profile(self.record, rows)
        self.assertEqual(result["matched_tasks"], 0)
        self.assertEqual(result["trace_pid_bindings"], 0)

    def test_descendants_excludes_observer_outside_worker_tree(self):
        self.assertEqual(descendants({1: {"ppid": 0}, 2: {"ppid": 1}, 3: {"ppid": 0}}, 1), {1, 2})

    def test_resource_gaps_are_reported_for_gauges_and_counters(self):
        record = {**self.record, "timeline": {
            metric: [{"labels": {"service": "worker"}, "values": [[9, 100], [21, 101]]}]
            for metric in ("cpu", "memory")}}
        gaps = timeline_gaps(record)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["gap_seconds"], 12)
        self.assertEqual(gaps[0]["metrics"], ["cpu", "memory"])

    def test_new_source_does_not_draw_cpu_across_a_missing_interval(self):
        record={**self.record,"measurement_quality":{"allowed_gap_seconds":3},
                "timeline": {"cpu":[{"labels":{"service":"worker"},"values":[[10,100],[14,110]]}]}}
        self.assertEqual(cpu_intervals(record),[])
        self.assertEqual(timeline_gaps(record)[0]['gap_seconds'],4)


if __name__ == "__main__":
    unittest.main()
