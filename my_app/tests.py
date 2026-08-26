from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from my_app.models import PressureMeasurement
from my_app.views import _run_pressure_measure_async


class PressureMeasurementApiTests(TestCase):
    def post_pressure(self, payload):
        return self.client.post(
            reverse("esfigmo"),
            data=payload,
            content_type="application/json",
        )

    @patch("my_app.views.threading.Thread")
    def test_start_persists_pending_measurement_before_starting_thread(self, thread):
        response = self.post_pressure({"channel": "34", "step": "start"})

        self.assertEqual(response.status_code, 202)
        body = response.json()
        measurement = PressureMeasurement.objects.get(pk=body["run_id"])
        self.assertEqual(measurement.cabin, "34")
        self.assertEqual(measurement.status, PressureMeasurement.Status.PENDING)
        thread.return_value.start.assert_called_once_with()

    def test_result_returns_latest_pending_measurement(self):
        measurement = PressureMeasurement.objects.create(cabin="34")

        response = self.post_pressure({"channel": "34", "step": "result"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "result": True,
                "status": "pending",
                "channel": "34",
                "run_id": str(measurement.pk),
            },
        )

    def test_result_returns_completed_measurement(self):
        measurement = PressureMeasurement.objects.create(
            cabin="34",
            status=PressureMeasurement.Status.DONE,
            data={"sis": 120, "dias": 80, "map": 93, "bpm": 70},
        )

        response = self.post_pressure(
            {"channel": "34", "step": "result", "run_id": str(measurement.pk)}
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["result"])
        self.assertEqual(body["status"], "done")
        self.assertEqual(body["data"]["sis"], 120)
        self.assertEqual(body["run_id"], str(measurement.pk))

    def test_result_does_not_return_measurements_older_than_ttl(self):
        measurement = PressureMeasurement.objects.create(cabin="34")
        PressureMeasurement.objects.filter(pk=measurement.pk).update(
            created_at=timezone.now() - timedelta(minutes=11)
        )

        response = self.post_pressure({"channel": "34", "step": "result"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "no-active-run")

    def test_result_rejects_invalid_run_id(self):
        response = self.post_pressure(
            {"channel": "34", "step": "result", "run_id": "invalid"}
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["result"])


class PressureMeasurementWorkerTests(TestCase):
    @patch("my_app.views.broker.publish_and_wait")
    def test_worker_persists_completed_readings(self, publish_and_wait):
        measurement = PressureMeasurement.objects.create(cabin="34")

        def receive_readings(**kwargs):
            predicate = kwargs["predicate"]
            for vital_sign, value in (
                ("sis", 120),
                ("dias", 80),
                ("map", 93),
                ("bpm", 70),
            ):
                predicate({"vs": vital_sign, "valor": value})
            return {"channel": "34", "message": {}}

        publish_and_wait.side_effect = receive_readings

        _run_pressure_measure_async("34", str(measurement.pk))

        measurement.refresh_from_db()
        self.assertEqual(measurement.status, PressureMeasurement.Status.DONE)
        self.assertEqual(
            measurement.data,
            {"sis": 120, "dias": 80, "map": 93, "bpm": 70},
        )

    @patch("my_app.views.broker.publish_and_wait", return_value=None)
    def test_worker_persists_timeout(self, publish_and_wait):
        measurement = PressureMeasurement.objects.create(cabin="34")

        _run_pressure_measure_async("34", str(measurement.pk))

        measurement.refresh_from_db()
        self.assertEqual(measurement.status, PressureMeasurement.Status.TIMEOUT)
