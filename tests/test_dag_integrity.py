"""DAG structure test. Needs Airflow, so it runs inside the container and is skipped elsewhere:

    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q -p no:cacheprovider
"""
import importlib.util
import unittest
from pathlib import Path

try:
    import airflow  # noqa: F401

    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "sales_pipeline_dag.py"


@unittest.skipUnless(HAS_AIRFLOW, "Airflow is not installed here")
class DagStructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("sales_pipeline_dag", DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # raises if the DAG has import/definition errors
        cls.dag = module.dag

    def test_basic_settings(self):
        self.assertEqual(self.dag.dag_id, "sales_pipeline")
        self.assertEqual(self.dag.schedule, "0 9 * * *")
        self.assertFalse(self.dag.catchup)

    def test_has_the_three_task_groups(self):
        groups = {c for c in self.dag.task_group.children if not c.startswith("check")}
        self.assertTrue({"api_to_stage", "build_silver", "build_mart"} <= groups)

    def test_tasks_exist(self):
        ids = set(self.dag.task_dict)
        for expected in (
            "api_to_stage.load_customers", "api_to_stage.load_products",
            "api_to_stage.load_sales", "api_to_stage.check_stage",
            "build_silver.dbt_run_silver", "build_silver.dbt_test_silver",
            "build_mart.dbt_run_mart", "build_mart.dbt_test_mart",
        ):
            self.assertIn(expected, ids)

    def test_dependency_chain_tg1_then_tg2_then_tg3(self):
        t = self.dag.task_dict
        self.assertIn("api_to_stage.check_stage", t["build_silver.dbt_run_silver"].upstream_task_ids)
        self.assertIn("build_silver.dbt_test_silver", t["build_mart.dbt_run_mart"].upstream_task_ids)
        for load in ("customers", "products", "sales"):
            self.assertIn(
                "api_to_stage.check_stage", t[f"api_to_stage.load_{load}"].downstream_task_ids
            )


if __name__ == "__main__":
    unittest.main()
