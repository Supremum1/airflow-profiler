import importlib.util
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from monitoring.resource_sampler import exposition, read_cgroup
from resource_report import SERVICES, apply_resources, verify_prometheus, verify_process_profile


class ResourceTests(unittest.TestCase):
    def record(self):
        return {"released_at":"1970-01-01T00:00:10+00:00", "finished_at":"1970-01-01T00:00:14+00:00"}

    def rows(self, times=(9,10,11,12,13,14,15)):
        return [{"at":at,"services":{service:{"at":at,"id":service,"cpu_seconds":at*2,
                  "working_set_bytes":100+at} for service in SERVICES},"errors":{}} for at in times]

    def test_full_window_has_correct_cpu_and_memory(self):
        record=self.record()
        apply_resources(record,self.rows())
        self.assertTrue(record["measurement_quality"]["valid"])
        self.assertEqual(record["resources"]["worker"]["cpu_seconds"],8)
        self.assertEqual(record["resources"]["worker"]["memory_peak_bytes"],114)

    def test_missing_middle_invalidates_aggregate(self):
        record=self.record()
        apply_resources(record,self.rows((9,10,14,15)))
        self.assertFalse(record["measurement_quality"]["valid"])
        self.assertNotIn("cpu_seconds",record["resources"]["worker"])

    def test_restart_and_uncovered_edges_are_invalid(self):
        for rows in (self.rows((11,12,13)),self.rows()):
            if len(rows)>3:
                rows[3]["services"]["worker"]["id"]="restarted"
            record=self.record()
            apply_resources(record,rows)
            self.assertFalse(record["measurement_quality"]["valid"])

    def test_process_coverage_includes_the_edges_of_the_window(self):
        record=self.record()
        record['finished_at']='1970-01-01T00:00:20+00:00'
        record['measurement_quality']={'valid':True,'issues':[]}
        record['process_profile']={'available':True,'samples':[{'at':10.1},{'at':10.6}],
                                   'matched_tasks':0,'started_tasks':0}
        verify_process_profile(record)
        self.assertFalse(record['measurement_quality']['valid'])

    def test_stale_export_does_not_emit_cached_memory(self):
        text=exposition({"worker":{"at":time.time()-10,"id":"old","working_set_bytes":999}})
        self.assertIn('bench_resource_up{service="worker"} 0',text)
        self.assertNotIn('container_memory_working_set_bytes',text)

    def test_kernel_working_set_subtracts_inactive_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)
            (path/"cpu.stat").write_text("usage_usec 2000000\nthrottled_usec 10000\n")
            (path/"memory.current").write_text("1000")
            (path/"memory.stat").write_text("inactive_file 200\nanon 500\nfile 300\nkernel 200\n")
            sample=read_cgroup("id",path)
            self.assertEqual(sample["working_set_bytes"],800)
            self.assertEqual(sample["cpu_seconds"],2)

    @unittest.skipUnless(importlib.util.find_spec("httpx"),"HTTP dependency is installed in the runner image")
    def test_failed_prometheus_scrape_invalidates_good_raw_samples(self):
        record=self.record()
        apply_resources(record,self.rows())
        response=Mock()
        response.json.return_value={"data":{"result":[{"values":[[10,"1"],[11,"0"],[12,"1"],[13,"1"],[14,"1"]]}]}}
        with patch("httpx.get",return_value=response):
            verify_prometheus(record,"http://test")
        self.assertFalse(record["measurement_quality"]["valid"])


if __name__=="__main__": unittest.main()
