from django.http import HttpResponse, JsonResponse
import time
import json
import libhoney
import requests
from datetime import datetime, timedelta, timezone
from rest_framework import generics, views, response, status, filters, viewsets
from django.core.exceptions import ValidationError
from django.db import close_old_connections
from django.utils import timezone as django_timezone
import threading
from django.shortcuts import render

from honeycomb.socket import send_socket_message
from honeycomb.socket_client import broker
from my_app.models import PressureMeasurement


CALLS_BY_CABIN_URL = "https://api2.bitmec.com/api/cabin/calls-by-cabin/{cabin}/"
CALLS_BY_CABIN_TOKEN = "Token 85e95e0a0a65010672f85c0f4f03f93a2ec190ae"
PRESSURE_RUN_TTL_SECONDS = 600


def index(request):
    return HttpResponse("Hello, world!")


libhoney.init(writekey='cQDbjNT0FAoOYWFA5XJwBC', dataset='telemetry')


class SendHoneyCombData(views.APIView):
    def options(self, request, *args, **kwargs):
        """Manejar solicitudes OPTIONS para CORS preflight."""
        response = HttpResponse()
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response

    def post(self, request):
        req = json.loads(self.request.body)
        data = req
        event = libhoney.new_event()
        payload = data

        # Simular un proceso de espera
        count = 0
        while True:
            count += 1
            time.sleep(1)
            print(f'{count} seconds elapsed')
            if count == 10:
                break

        event.add(payload)
        event.send()

        # Responder con los encabezados CORS
        response = JsonResponse({'result': True, 'data': data})
        response['Access-Control-Allow-Origin'] = '*'  # Cambia '*' por tu dominio de origen si es necesario
        return response

# Height
# class SendHeightSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "height"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendHeightSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "height"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "height"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )

# Weight
# class SendWeightSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "weight"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendWeightSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "weight"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "Weight"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )


# Pressure
# class SendPressureSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "esfigmo"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

def _run_pressure_measure_async(cabin: str, run_id: str):
    close_old_connections()
    try:
        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "esfigmo",
        }

        readings = {}

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message", msg)
            if not isinstance(inner, dict):
                return False

            vs = inner.get("vs")
            valor = inner.get("valor")

            if vs in ("sis", "dias", "map", "bpm"):
                readings[vs] = valor

            return all(k in readings for k in ("sis", "dias", "map", "bpm"))

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=240,
        )

        if result is None:
            PressureMeasurement.objects.filter(pk=run_id).update(
                status=PressureMeasurement.Status.TIMEOUT,
                data=None,
                error=None,
                updated_at=django_timezone.now(),
            )
        else:
            PressureMeasurement.objects.filter(pk=run_id).update(
                status=PressureMeasurement.Status.DONE,
                data=readings,
                error=None,
                updated_at=django_timezone.now(),
            )

    except Exception as e:
        try:
            PressureMeasurement.objects.filter(pk=run_id).update(
                status=PressureMeasurement.Status.ERROR,
                data=None,
                error=str(e),
                updated_at=django_timezone.now(),
            )
        except Exception:
            pass
    finally:
        close_old_connections()


class SendPressureSocketMessage(views.APIView):
    def post(self, request):
        try:
            req = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse(
                {"result": False, "error": "Body JSON inválido"},
                status=400,
            )

        cabin = req.get("channel")
        step = req.get("step")

        if not cabin:
            return JsonResponse(
                {"result": False, "error": "Debe enviar 'channel'"},
                status=400,
            )

        if step not in ("start", "result"):
            return JsonResponse(
                {"result": False, "error": "Debe enviar 'step': 'start' o 'result'"},
                status=400,
            )

        if step == "start":
            measurement = PressureMeasurement.objects.create(cabin=str(cabin))
            run_id = str(measurement.pk)

            t = threading.Thread(
                target=_run_pressure_measure_async,
                args=(cabin, run_id),
                daemon=True,
            )
            t.start()

            return JsonResponse(
                {
                    "result": True,
                    "status": "started",
                    "channel": cabin,
                    "run_id": run_id,
                },
                status=202,
            )

        requested_run_id = req.get("run_id")
        measurements = PressureMeasurement.objects.filter(
            cabin=str(cabin),
            created_at__gte=(
                django_timezone.now() - timedelta(seconds=PRESSURE_RUN_TTL_SECONDS)
            ),
        )

        try:
            if requested_run_id:
                measurement = measurements.filter(pk=requested_run_id).first()
            else:
                measurement = measurements.first()
        except (ValidationError, ValueError):
            return JsonResponse(
                {"result": False, "error": "El 'run_id' no es válido"},
                status=400,
            )

        if measurement is None:
            return JsonResponse(
                {
                    "result": False,
                    "status": "no-active-run",
                    "channel": cabin,
                },
                status=200,
            )

        if measurement.status == PressureMeasurement.Status.PENDING:
            return JsonResponse(
                {
                    "result": True,
                    "status": "pending",
                    "channel": cabin,
                    "run_id": str(measurement.pk),
                },
                status=200,
            )

        return JsonResponse(
            {
                "result": True,
                "status": measurement.status,
                "channel": cabin,
                "data": measurement.data,
                "run_id": str(measurement.pk),
                "error": measurement.error,
            },
            status=200,
        )

# Oxygen
# class SendOxygenSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "oxygen"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendOxygenSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "oxygen",
        }

        readings = {}

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message", msg)

            if not isinstance(inner, dict):
                return False

            vs = inner.get("vs")
            valor = inner.get("valor")

            if vs in ("SpO2", "bpm"):
                readings[vs] = valor

            return all(k in readings for k in ("SpO2", "bpm"))

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": readings,
            },
            status=200,
        )



# Temperature
# class SendTemperatureSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "temperature"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendTemperatureSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "temperature"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "tmp"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )


# Esteto
class SendEstetoSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "mic"
        }
        send_socket_message(f"{cabin}-cmd", message)
        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


# Booth control
class SendPatientExitSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "navigation",
            "vital-sign": "end-screen"
        }
        send_socket_message(f"{cabin}-cmd", message)
        send_socket_message(cabin, message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


# Emergency
class SendActivateEmergencySocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "e-stop",
        }
        send_socket_message(f"{cabin}-cmd", message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


class SendDeactivateEmergencySocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "N-e-stop",
        }
        send_socket_message
        return HttpResponse(json.dumps({'result': True}), content_type='application/json')
    

def _get_latest_call_id(cabin):
    response = requests.get(
        CALLS_BY_CABIN_URL.format(cabin=cabin),
        headers={"Authorization": CALLS_BY_CABIN_TOKEN},
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()
    calls = data.get("calls", [])
    if not calls:
        return None

    def call_timestamp(call):
        try:
            parsed = datetime.fromisoformat(call.get("timestamp", "").replace("Z", "+00:00"))
        except (AttributeError, ValueError):
            return datetime.min.replace(tzinfo=timezone.utc)

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)

        return parsed

    latest_call = max(calls, key=call_timestamp)
    return latest_call.get("id")


def _get_cabin_number_from_channel(channel):
    return str(channel).rsplit("-", 1)[-1]


# Take call
class TakeCallSocketMessage(views.APIView):
    def post(self, request):
        try:
            req = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse(
                {"result": False, "error": "Body JSON inválido"},
                status=400,
            )

        cabin = req.get("channel")
        if not cabin:
            return JsonResponse(
                {"result": False, "error": "Debe enviar 'channel'"},
                status=400,
            )

        cabin_number = _get_cabin_number_from_channel(cabin)
        if not cabin_number.isdigit():
            return JsonResponse(
                {
                    "result": False,
                    "error": "El channel debe tener formato 'cabin-34'",
                    "channel": cabin,
                },
                status=400,
            )

        try:
            latest_call_id = _get_latest_call_id(cabin_number)
        except requests.RequestException as e:
            return JsonResponse(
                {
                    "result": False,
                    "error": "No se pudo obtener la llamada más reciente",
                    "detail": str(e),
                },
                status=502,
            )
        except ValueError as e:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Respuesta inválida del servicio de llamadas",
                    "detail": str(e),
                },
                status=502,
            )

        if latest_call_id is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "No hay llamadas para esta cabina",
                    "channel": cabin,
                },
                status=404,
            )

        message = {
            "type": "doctor-take-call",
            "status": 'answered',
            "id": latest_call_id,
            "assistant-id": 'None',
            "cabin-id": f"{cabin}"
        }
        send_socket_message(f"{cabin}-cmd", message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


# End Call
class EndCallSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']

        end_call_message = {
            'type': 'navigation',
            'screen': 'end-screen',
        }

        send_socket_message(f'{cabin}-cmd', end_call_message)

        close_cabin_message = {
            'type': 'command',
            'vital-sign': 'close',
        }

        send_socket_message(f'{cabin}-cmd', close_cabin_message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


#Obtener info de los dispositivos médicos de la cabina
class GetDiveces(views.APIView):
    def post(self, request):
        try:
            req = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse(
                {"result": False, "error": "Body JSON inválido"},
                status=400,
            )

        cabin = req.get("channel")
        if not cabin:
            return JsonResponse(
                {"result": False, "error": "Debe enviar 'channel'"},
                status=400,
            )

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "get-devices",
        }

        expected = {"esfigmo", "oximetro", "peso", "temperatura", "altura"}
        readings = {}

        def _normalize_inner(msg):
            if not isinstance(msg, dict):
                return None

            inner = msg.get("message", msg)

            if isinstance(inner, dict):
                return inner

            if isinstance(inner, str):
                try:
                    parsed = ast.literal_eval(inner)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    pass

                try:
                    parsed = json.loads(inner)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    pass

            return None

        def predicate(msg: dict) -> bool:
            inner = _normalize_inner(msg)
            if not inner:
                return False

            dv = inner.get("dv")
            valor = inner.get("valor")

            if dv in expected:
                readings[dv] = valor

            return expected.issubset(readings.keys())

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=10,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "status": "timeout",
                    "channel": cabin,
                    "data": readings if readings else None,
                },
                status=408,
            )

        return JsonResponse(
            {
                "result": True,
                "status": "done",
                "channel": cabin,
                "data": readings,
            },
            status=200,
        )
        
# Cambiar a la cámara dermatologica 

class SendDermCameraSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "derm-camera"
        }
        send_socket_message(f"{cabin}", message)
        return HttpResponse(json.dumps({'result': True}), content_type='application/json')
    
