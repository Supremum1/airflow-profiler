import unittest
from flow_report import stage_results


class FlowVerdictTests(unittest.TestCase):
    def experiment(self, bad_oldest=0, count=24):
        start=1000
        iso=lambda t:__import__('datetime').datetime.fromtimestamp(t,__import__('datetime').timezone.utc).isoformat()
        document={"config":{"queue_limit":5},"measurement_quality":{"valid":True},"errors":[],
                  "stages":[{"name":"12/min","start":0,"end":120,"rate":12}],
                  "arrivals":[{"stage":0,"run_id":str(i),"sent_at":start+i*5,"planned_at":start+i*5,"accepted_at":start+i*5} for i in range(count)],
                  "queue":[{"at":start+i,"pending":1,"oldest_queued_seconds":bad_oldest,"oldest_scheduled_seconds":0} for i in range(121)]}
        record={"released_at":iso(start),"tasks":[{"run_id":str(i),"state":"success","scheduled_dttm":iso(start+i*5),"start_date":iso(start+i*5+1)} for i in range(count)],
                "runs":[{"end_date":iso(start+i*5+2)} for i in range(count)]}
        return document,record

    def test_stable_stage_meets_conditions(self):
        d,r=self.experiment()
        self.assertEqual(stage_results(d,r)[0]["verdict"],"Очередь стабильна")

    def test_pending_age_prevents_survivor_bias(self):
        d,r=self.experiment(bad_oldest=30)
        self.assertEqual(stage_results(d,r)[0]["verdict"],"Долгое ожидание запуска")

    def test_invalid_data_cannot_prove_capacity(self):
        d,r=self.experiment()
        d["measurement_quality"]["valid"]=False
        self.assertEqual(stage_results(d,r)[0]["verdict"],"Измерение недостоверно")

    def test_small_sample_does_not_prove_capacity(self):
        d,r=self.experiment(count=3)
        self.assertEqual(stage_results(d,r)[0]["verdict"],"Недостаточно данных")

    def test_short_test_still_reports_observed_latency_violation(self):
        d,r=self.experiment(bad_oldest=30)
        d["stages"][0]["end"]=30
        self.assertEqual(stage_results(d,r)[0]["verdict"],"Долгое ожидание запуска")


if __name__=="__main__": unittest.main()
